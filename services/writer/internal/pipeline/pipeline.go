// Package pipeline buffers decoded Kafka messages, inserts them into ClickHouse in batches with
// retries, and sends what can't be stored to traces.dlq. It knows nothing about the Kafka client:
// the caller polls, hands messages to Add, calls Flush, and commits offsets only if Flush returned
// nil. That keeps the commit-after-durable rule in one place (cmd/writer) and makes this testable.
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
	"github.com/eugenelacatis/tracewell/services/writer/internal/rows"
)

const (
	TopicTraces = "traces.raw"
	TopicEvals  = "evals.results"
)

var (
	rowsInserted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "writer_rows_inserted_total",
		Help: "Rows inserted into ClickHouse, by table. Redelivered messages count again.",
	}, []string{"table"})
	dlqTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "writer_dlq_total",
		Help: "Messages sent to traces.dlq, by reason: decode (bad message) or insert (ClickHouse kept failing).",
	}, []string{"reason"})
)

// DLQ reasons, the values of writer_dlq_total{reason}.
const (
	reasonDecode = "decode"
	reasonInsert = "insert"
)

// Message is one Kafka record, copied out of the client's type.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Inserter stores rows. It must be safe to call again with rows it already stored, which holds
// because every table is a ReplacingMergeTree.
type Inserter interface {
	Insert(ctx context.Context, r rows.Rows) error
}

// DeadLetters produces to traces.dlq and returns once the broker has acked.
type DeadLetters interface {
	Send(ctx context.Context, key []byte, dl *tracewellv1.DeadLetter) error
}

type pending struct {
	msg  Message
	rows rows.Rows
	err  error // set when the message couldn't be decoded; it goes straight to the DLQ
}

type Writer struct {
	Inserter    Inserter
	DeadLetters DeadLetters
	Logger      *slog.Logger
	// Backoff is the wait before each retry, so len(Backoff)+1 is the number of attempts.
	Backoff []time.Duration
	Sleep   func(time.Duration) // time.Sleep when nil; tests replace it

	buf  []pending
	rows int
}

// DefaultBackoff gives 3 attempts in total, per CLAUDE.md, spread over ~7s. That rides out a
// ClickHouse restart or a replica pod being replaced (~3s measured), which would otherwise send
// healthy messages to the DLQ.
var DefaultBackoff = []time.Duration{2 * time.Second, 5 * time.Second}

// Add decodes m and buffers its rows.
func (w *Writer) Add(m Message) {
	p := pending{msg: m}
	switch m.Topic {
	case TopicTraces:
		var b tracewellv1.IngestBatch
		if p.err = proto.Unmarshal(m.Value, &b); p.err == nil {
			p.rows, p.err = rows.FromIngestBatch(&b, m.Partition, m.Offset)
		}
	case TopicEvals:
		var e tracewellv1.EvalResult
		if p.err = proto.Unmarshal(m.Value, &e); p.err == nil {
			p.rows, p.err = rows.FromEvalResult(&e)
		}
	default:
		p.err = fmt.Errorf("unexpected topic %q", m.Topic)
	}
	w.buf = append(w.buf, p)
	w.rows += p.rows.Len()
}

// Rows is how many rows are buffered. The caller flushes at 500.
func (w *Writer) Rows() int { return w.rows }

// Messages is how many messages are buffered.
func (w *Writer) Messages() int { return len(w.buf) }

// Flush stores everything buffered and empties the buffer. It returns nil when every message is
// either in ClickHouse or in traces.dlq, which is when the caller may commit the offsets. A non-nil
// error means a DLQ produce failed; the caller must not commit, so Kafka redelivers the messages.
func (w *Writer) Flush(ctx context.Context) error {
	var all rows.Rows
	var good []pending
	for _, p := range w.buf {
		if p.err != nil {
			// Decoding is deterministic, so retrying a message that didn't decode can't help.
			if err := w.deadLetter(ctx, p.msg, p.err, 1, reasonDecode); err != nil {
				return err
			}
			continue
		}
		all.Append(p.rows)
		good = append(good, p)
	}

	if err := w.insertWithRetry(ctx, all); err != nil {
		for _, p := range good {
			if dlErr := w.deadLetter(ctx, p.msg, err, uint32(len(w.Backoff)+1), reasonInsert); dlErr != nil {
				return dlErr
			}
		}
	} else {
		rowsInserted.WithLabelValues("spans").Add(float64(len(all.Spans)))
		rowsInserted.WithLabelValues("traces").Add(float64(len(all.Traces)))
		rowsInserted.WithLabelValues("evals").Add(float64(len(all.Evals)))
		rowsInserted.WithLabelValues("ingest_batches").Add(float64(len(all.Batches)))
		w.Logger.Info("flushed", "messages", len(good), "traces", len(all.Traces), "spans", len(all.Spans),
			"evals", len(all.Evals))
	}
	w.buf, w.rows = nil, 0
	return nil
}

func (w *Writer) insertWithRetry(ctx context.Context, r rows.Rows) error {
	if r.Len() == 0 {
		return nil
	}
	sleep := w.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	var err error
	for attempt := 0; ; attempt++ {
		if err = w.Inserter.Insert(ctx, r); err == nil {
			return nil
		}
		if attempt == len(w.Backoff) {
			return err
		}
		w.Logger.Warn("clickhouse insert failed, retrying", "attempt", attempt+1, "err", err)
		sleep(w.Backoff[attempt])
	}
}

func (w *Writer) deadLetter(ctx context.Context, m Message, cause error, attempts uint32, reason string) error {
	dl := &tracewellv1.DeadLetter{
		SourceTopic: m.Topic,
		Partition:   m.Partition,
		Offset:      m.Offset,
		Payload:     m.Value,
		Error:       cause.Error(),
		Attempts:    attempts,
	}
	if err := w.DeadLetters.Send(ctx, m.Key, dl); err != nil {
		return fmt.Errorf("send %s/%d@%d to dlq: %w", m.Topic, m.Partition, m.Offset, err)
	}
	dlqTotal.WithLabelValues(reason).Inc()
	w.Logger.Warn("sent message to dlq", "project_id", string(m.Key), "topic", m.Topic,
		"partition", m.Partition, "offset", m.Offset, "attempts", attempts, "err", cause)
	return nil
}
