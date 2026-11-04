package rows

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
)

func TestEnumMapping(t *testing.T) {
	kinds := map[tracewellv1.SpanKind]string{
		tracewellv1.SpanKind_SPAN_KIND_UNSPECIFIED: "UNSPECIFIED",
		tracewellv1.SpanKind_SPAN_KIND_GENERATION:  "GENERATION",
		tracewellv1.SpanKind_SPAN_KIND_RETRIEVAL:   "RETRIEVAL",
		tracewellv1.SpanKind_SPAN_KIND_TOOL:        "TOOL",
		tracewellv1.SpanKind_SPAN_KIND_CHAIN:       "CHAIN",
	}
	for k, want := range kinds {
		if got := Kind(k); got != want {
			t.Errorf("Kind(%v) = %q, want %q", k, got, want)
		}
	}
	statuses := map[tracewellv1.Status]string{
		tracewellv1.Status_STATUS_UNSPECIFIED: "UNSPECIFIED",
		tracewellv1.Status_STATUS_OK:          "OK",
		tracewellv1.Status_STATUS_ERROR:       "ERROR",
	}
	for s, want := range statuses {
		if got := Status(s); got != want {
			t.Errorf("Status(%v) = %q, want %q", s, got, want)
		}
	}
}

func gen(total uint32, cost float64) *tracewellv1.Generation {
	return &tracewellv1.Generation{
		Provider: "openai", Model: "gpt-4o-mini", Prompt: "p", Completion: "c",
		Params: &structpb.Struct{Fields: map[string]*structpb.Value{"temperature": structpb.NewNumberValue(0.2)}},
		Usage:  &tracewellv1.Usage{PromptTokens: total - 1, CompletionTokens: 1, TotalTokens: total, CostUsd: cost},
	}
}

func TestFromIngestBatchRollups(t *testing.T) {
	t0 := time.Date(2026, 10, 26, 17, 4, 5, 123456000, time.UTC)
	b := &tracewellv1.IngestBatch{
		IdempotencyKey: "inst:7", ProjectId: "proj_a", SdkInstanceId: "inst", BatchSeq: 7,
		Traces: []*tracewellv1.Trace{
			{
				TraceId: "t1", Name: "ok trace", StartTime: timestamppb.New(t0), EndTime: timestamppb.New(t0.Add(time.Second)),
				Spans: []*tracewellv1.Span{
					{SpanId: "s1", Kind: tracewellv1.SpanKind_SPAN_KIND_CHAIN, Status: tracewellv1.Status_STATUS_OK},
					{SpanId: "s2", ParentSpanId: "s1", Kind: tracewellv1.SpanKind_SPAN_KIND_GENERATION,
						Status: tracewellv1.Status_STATUS_OK, Generation: gen(12, 0.0000045)},
					{SpanId: "s3", ParentSpanId: "s1", Kind: tracewellv1.SpanKind_SPAN_KIND_GENERATION,
						Status: tracewellv1.Status_STATUS_OK, Generation: gen(30, 0.25)},
				},
			},
			{
				TraceId: "t2", Name: "failed trace", Tags: []string{"x"},
				Spans: []*tracewellv1.Span{
					{SpanId: "s4", Status: tracewellv1.Status_STATUS_OK},
					{SpanId: "s5", Status: tracewellv1.Status_STATUS_ERROR, ErrorMessage: "boom"},
				},
			},
			{TraceId: "t3", Name: "no spans"},
		},
	}

	r, err := FromIngestBatch(b, 3, 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Traces) != 3 || len(r.Spans) != 5 || len(r.Batches) != 1 || len(r.Evals) != 0 {
		t.Fatalf("row counts: traces=%d spans=%d batches=%d", len(r.Traces), len(r.Spans), len(r.Batches))
	}

	t1, t2, t3 := r.Traces[0], r.Traces[1], r.Traces[2]
	if t1.Status != "OK" || t1.SpanCount != 3 || t1.TotalTokens != 42 || t1.CostUSD != 0.0000045+0.25 {
		t.Errorf("t1 rollup = %+v", t1)
	}
	if !t1.StartTime.Equal(t0) || t1.IdempotencyKey != "inst:7" || t1.ProjectID != "proj_a" {
		t.Errorf("t1 fields = %+v", t1)
	}
	if t2.Status != "ERROR" || t2.SpanCount != 2 || t2.TotalTokens != 0 || t2.CostUSD != 0 {
		t.Errorf("t2 rollup = %+v", t2)
	}
	if t3.Status != "OK" || t3.SpanCount != 0 || t3.Tags == nil {
		t.Errorf("t3 rollup = %+v (tags must be non-nil for ClickHouse)", t3)
	}

	g := r.Spans[1]
	if g.Kind != "GENERATION" || g.TraceID != "t1" || g.ParentSpanID != "s1" || g.Model != "gpt-4o-mini" ||
		g.PromptTokens != 11 || g.CompletionTokens != 1 || g.TotalTokens != 12 || g.Params != `{"temperature":0.2}` ||
		g.Metadata != "{}" {
		t.Errorf("generation span = %+v", g)
	}
	if s := r.Spans[0]; s.Kind != "CHAIN" || s.Params != "" || s.Model != "" {
		t.Errorf("non-generation span should have empty generation fields: %+v", s)
	}
	if s := r.Spans[4]; s.Status != "ERROR" || s.ErrorMessage != "boom" {
		t.Errorf("error span = %+v", s)
	}

	want := IngestBatch{ProjectID: "proj_a", SDKInstanceID: "inst", BatchSeq: 7, IdempotencyKey: "inst:7",
		TraceCount: 3, SpanCount: 5, KafkaPartition: 3, KafkaOffset: 42}
	if r.Batches[0] != want {
		t.Errorf("batch row = %+v, want %+v", r.Batches[0], want)
	}
}

func TestSpanTraceIDComesFromEnclosingTrace(t *testing.T) {
	b := &tracewellv1.IngestBatch{ProjectId: "p", Traces: []*tracewellv1.Trace{
		{TraceId: "real", Spans: []*tracewellv1.Span{{SpanId: "s", TraceId: "other"}}},
	}}
	r, _ := FromIngestBatch(b, 0, 0)
	if r.Spans[0].TraceID != "real" {
		t.Fatalf("span trace id = %q", r.Spans[0].TraceID)
	}
}

func TestMissingProjectIsAnError(t *testing.T) {
	if _, err := FromIngestBatch(&tracewellv1.IngestBatch{IdempotencyKey: "k"}, 0, 0); !errors.Is(err, ErrNoProject) {
		t.Errorf("IngestBatch err = %v", err)
	}
	if _, err := FromEvalResult(&tracewellv1.EvalResult{TraceId: "t"}); !errors.Is(err, ErrNoProject) {
		t.Errorf("EvalResult err = %v", err)
	}
}

func TestFromEvalResult(t *testing.T) {
	at := time.Date(2026, 10, 26, 17, 4, 7, 0, time.UTC)
	r, err := FromEvalResult(&tracewellv1.EvalResult{
		TraceId: "t", SpanId: "s", ProjectId: "p", Evaluator: "latency", Score: 1, Label: "ok",
		Reasoning: "1000ms, under 5000ms", CreatedAt: timestamppb.New(at),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := Eval{ProjectID: "p", TraceID: "t", SpanID: "s", Evaluator: "latency", Score: 1, Label: "ok",
		Reasoning: "1000ms, under 5000ms", CreatedAt: at}
	if len(r.Evals) != 1 || r.Evals[0] != want || r.Len() != 1 {
		t.Fatalf("rows = %+v", r)
	}
}
