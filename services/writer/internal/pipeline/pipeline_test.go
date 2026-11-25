package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/writer/internal/rows"
)

type fakeInserter struct {
	failures int // fail this many calls, then succeed
	calls    int
	stored   []rows.Rows
}

func (f *fakeInserter) Insert(_ context.Context, r rows.Rows) error {
	f.calls++
	if f.calls <= f.failures {
		return errors.New("clickhouse unavailable")
	}
	f.stored = append(f.stored, r)
	return nil
}

type dlqRecord struct {
	key string
	dl  *tracewellv1.DeadLetter
}

type fakeDLQ struct {
	sent []dlqRecord
	err  error
}

func (f *fakeDLQ) Send(_ context.Context, key []byte, dl *tracewellv1.DeadLetter) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, dlqRecord{string(key), dl})
	return nil
}

func newWriter(ins *fakeInserter, dlq *fakeDLQ) (*Writer, *[]time.Duration) {
	var slept []time.Duration
	return &Writer{
		Inserter:    ins,
		DeadLetters: dlq,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Backoff:     DefaultBackoff,
		Sleep:       func(d time.Duration) { slept = append(slept, d) },
	}, &slept
}

func traceMsg(t *testing.T, offset int64, spans int) Message {
	t.Helper()
	tr := &tracewellv1.Trace{TraceId: "t"}
	for range spans {
		tr.Spans = append(tr.Spans, &tracewellv1.Span{SpanId: "s"})
	}
	b, err := proto.Marshal(&tracewellv1.IngestBatch{IdempotencyKey: "k", ProjectId: "proj_a",
		Traces: []*tracewellv1.Trace{tr}})
	if err != nil {
		t.Fatal(err)
	}
	return Message{Topic: TopicTraces, Partition: 2, Offset: offset, Key: []byte("proj_a"), Value: b}
}

func evalMsg(t *testing.T) Message {
	t.Helper()
	b, _ := proto.Marshal(&tracewellv1.EvalResult{ProjectId: "proj_a", TraceId: "t", Evaluator: "latency"})
	return Message{Topic: TopicEvals, Key: []byte("proj_a"), Value: b}
}

func TestBuffersAcrossMessagesAndCountsRows(t *testing.T) {
	ins, dlq := &fakeInserter{}, &fakeDLQ{}
	w, _ := newWriter(ins, dlq)
	w.Add(traceMsg(t, 1, 3)) // 3 spans + 1 trace + 1 ingest_batches row
	w.Add(traceMsg(t, 2, 1)) // 1 + 1 + 1
	w.Add(evalMsg(t))        // 1
	if w.Rows() != 9 || w.Messages() != 3 {
		t.Fatalf("Rows = %d, Messages = %d", w.Rows(), w.Messages())
	}
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ins.calls != 1 || len(ins.stored[0].Spans) != 4 || len(ins.stored[0].Traces) != 2 ||
		len(ins.stored[0].Evals) != 1 || len(ins.stored[0].Batches) != 2 {
		t.Fatalf("one insert with every row expected, got %d calls: %+v", ins.calls, ins.stored)
	}
	if len(dlq.sent) != 0 || w.Rows() != 0 || w.Messages() != 0 {
		t.Fatalf("dlq = %d, buffer not reset", len(dlq.sent))
	}
}

func TestRetriesThenSucceeds(t *testing.T) {
	ins, dlq := &fakeInserter{failures: 2}, &fakeDLQ{}
	w, slept := newWriter(ins, dlq)
	w.Add(traceMsg(t, 1, 1))
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ins.calls != 3 || len(ins.stored) != 1 || len(dlq.sent) != 0 {
		t.Fatalf("calls = %d, stored = %d, dlq = %d", ins.calls, len(ins.stored), len(dlq.sent))
	}
	if len(*slept) != 2 || (*slept)[0] != 2*time.Second || (*slept)[1] != 5*time.Second {
		t.Fatalf("backoff = %v", *slept)
	}
}

func TestDeadLettersAfterThreeFailures(t *testing.T) {
	ins, dlq := &fakeInserter{failures: 100}, &fakeDLQ{}
	w, _ := newWriter(ins, dlq)
	m1, m2 := traceMsg(t, 10, 1), traceMsg(t, 11, 2)
	w.Add(m1)
	w.Add(m2)
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err) // nil: every message is now in the DLQ, so the caller may commit
	}
	if ins.calls != 3 {
		t.Fatalf("insert attempts = %d, want 3", ins.calls)
	}
	if len(dlq.sent) != 2 {
		t.Fatalf("dlq messages = %d, want 2", len(dlq.sent))
	}
	for i, m := range []Message{m1, m2} {
		got := dlq.sent[i]
		if got.key != "proj_a" || got.dl.SourceTopic != TopicTraces || got.dl.Partition != 2 ||
			got.dl.Offset != m.Offset || got.dl.Attempts != 3 || !proto.Equal(decode(t, got.dl.Payload), decode(t, m.Value)) ||
			got.dl.Error != "clickhouse unavailable" {
			t.Errorf("dead letter %d = %+v", i, got)
		}
	}
}

func decode(t *testing.T, b []byte) *tracewellv1.IngestBatch {
	t.Helper()
	var out tracewellv1.IngestBatch
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func TestUndecodableMessageGoesStraightToDLQ(t *testing.T) {
	ins, dlq := &fakeInserter{}, &fakeDLQ{}
	w, _ := newWriter(ins, dlq)
	w.Add(Message{Topic: TopicTraces, Offset: 5, Key: []byte("proj_a"), Value: []byte{0xff, 0xff}})
	w.Add(Message{Topic: TopicEvals, Offset: 6, Value: mustMarshal(t, &tracewellv1.EvalResult{TraceId: "no project"})})
	w.Add(traceMsg(t, 7, 1))
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(dlq.sent) != 2 || dlq.sent[0].dl.Offset != 5 || dlq.sent[0].dl.Attempts != 1 || dlq.sent[1].dl.Offset != 6 {
		t.Fatalf("dlq = %+v", dlq.sent)
	}
	if ins.calls != 1 || len(ins.stored[0].Traces) != 1 {
		t.Fatalf("good message should still be inserted: calls=%d", ins.calls)
	}
}

func TestDLQFailureReturnsErrorSoCallerDoesNotCommit(t *testing.T) {
	ins, dlq := &fakeInserter{failures: 100}, &fakeDLQ{err: errors.New("kafka down")}
	w, _ := newWriter(ins, dlq)
	w.Add(traceMsg(t, 1, 1))
	if err := w.Flush(context.Background()); err == nil {
		t.Fatal("Flush returned nil although the message is neither stored nor dead-lettered")
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
