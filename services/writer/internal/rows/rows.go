// Package rows turns Kafka messages (IngestBatch, EvalResult) into rows for the ClickHouse tables
// in docs/contracts/clickhouse.sql. It is pure: no I/O, so all the mapping rules are unit-tested.
package rows

import (
	"errors"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	tracewellv1 "github.com/eugenelacatis/tracewell/proto/tracewellv1"
)

type Span struct {
	ProjectID, TraceID, SpanID, ParentSpanID, Name, Kind string
	StartTime, EndTime                                   time.Time
	Status, ErrorMessage                                 string
	Provider, Model, Prompt, Completion, Params          string
	PromptTokens, CompletionTokens, TotalTokens          uint32
	CostUSD                                              float64
	Metadata                                             string
}

type Trace struct {
	ProjectID, TraceID, SessionID, UserID, Name string
	Tags                                        []string
	StartTime, EndTime                          time.Time
	Status                                      string
	SpanCount                                   uint32
	TotalTokens                                 uint64
	CostUSD                                     float64
	IdempotencyKey                              string
}

type Eval struct {
	ProjectID, TraceID, SpanID, Evaluator string
	Score                                 float64
	Label, Reasoning                      string
	CreatedAt                             time.Time
}

// IngestBatch is one row of tracewell.ingest_batches: proof that a whole batch was stored.
type IngestBatch struct {
	ProjectID, SDKInstanceID string
	BatchSeq                 uint64
	IdempotencyKey           string
	TraceCount, SpanCount    uint32
	KafkaPartition           int32
	KafkaOffset              int64
}

// Rows is everything one or more messages produce, grouped by table.
type Rows struct {
	Spans   []Span
	Traces  []Trace
	Evals   []Eval
	Batches []IngestBatch
}

func (r *Rows) Append(o Rows) {
	r.Spans = append(r.Spans, o.Spans...)
	r.Traces = append(r.Traces, o.Traces...)
	r.Evals = append(r.Evals, o.Evals...)
	r.Batches = append(r.Batches, o.Batches...)
}

func (r Rows) Len() int { return len(r.Spans) + len(r.Traces) + len(r.Evals) + len(r.Batches) }

// ErrNoProject means the message has no project_id. ingest-svc and the eval workers always set
// one, so a message without it didn't come from them and is sent to the DLQ instead of stored.
var ErrNoProject = errors.New("message has no project_id")

// Kind and status enums are stored without their proto prefix: SPAN_KIND_GENERATION -> GENERATION,
// STATUS_ERROR -> ERROR, *_UNSPECIFIED -> UNSPECIFIED.
func Kind(k tracewellv1.SpanKind) string { return strings.TrimPrefix(k.String(), "SPAN_KIND_") }
func Status(s tracewellv1.Status) string { return strings.TrimPrefix(s.String(), "STATUS_") }

// FromIngestBatch builds the span, trace and ingest_batches rows for one traces.raw message.
// partition and offset are where the message sat in Kafka.
func FromIngestBatch(b *tracewellv1.IngestBatch, partition int32, offset int64) (Rows, error) {
	if b.ProjectId == "" {
		return Rows{}, ErrNoProject
	}
	var out Rows
	var spanTotal uint32
	for _, t := range b.Traces {
		tr := Trace{
			ProjectID:      b.ProjectId,
			TraceID:        t.TraceId,
			SessionID:      t.SessionId,
			UserID:         t.UserId,
			Name:           t.Name,
			Tags:           t.Tags,
			StartTime:      t.StartTime.AsTime(),
			EndTime:        t.EndTime.AsTime(),
			Status:         "OK",
			SpanCount:      uint32(len(t.Spans)),
			IdempotencyKey: b.IdempotencyKey,
		}
		if tr.Tags == nil {
			tr.Tags = []string{}
		}
		for _, s := range t.Spans {
			row := spanRow(b.ProjectId, t.TraceId, s)
			if s.Status == tracewellv1.Status_STATUS_ERROR {
				tr.Status = "ERROR"
			}
			tr.TotalTokens += uint64(row.TotalTokens)
			tr.CostUSD += row.CostUSD
			out.Spans = append(out.Spans, row)
		}
		spanTotal += tr.SpanCount
		out.Traces = append(out.Traces, tr)
	}
	out.Batches = []IngestBatch{{
		ProjectID:      b.ProjectId,
		SDKInstanceID:  b.SdkInstanceId,
		BatchSeq:       b.BatchSeq,
		IdempotencyKey: b.IdempotencyKey,
		TraceCount:     uint32(len(b.Traces)),
		SpanCount:      spanTotal,
		KafkaPartition: partition,
		KafkaOffset:    offset,
	}}
	return out, nil
}

// spanRow uses the enclosing trace's id rather than span.trace_id, so a span can never be filed
// under a different trace than the one it arrived in.
func spanRow(projectID, traceID string, s *tracewellv1.Span) Span {
	row := Span{
		ProjectID:    projectID,
		TraceID:      traceID,
		SpanID:       s.SpanId,
		ParentSpanID: s.ParentSpanId,
		Name:         s.Name,
		Kind:         Kind(s.Kind),
		StartTime:    s.StartTime.AsTime(),
		EndTime:      s.EndTime.AsTime(),
		Status:       Status(s.Status),
		ErrorMessage: s.ErrorMessage,
		Metadata:     structJSON(s.Metadata),
	}
	if g := s.Generation; g != nil {
		row.Provider, row.Model, row.Prompt, row.Completion = g.Provider, g.Model, g.Prompt, g.Completion
		row.Params = structJSON(g.Params)
		row.PromptTokens = g.Usage.GetPromptTokens()
		row.CompletionTokens = g.Usage.GetCompletionTokens()
		row.TotalTokens = g.Usage.GetTotalTokens()
		row.CostUSD = g.Usage.GetCostUsd()
	}
	return row
}

// FromEvalResult builds the evals row for one evals.results message.
func FromEvalResult(e *tracewellv1.EvalResult) (Rows, error) {
	if e.ProjectId == "" {
		return Rows{}, ErrNoProject
	}
	return Rows{Evals: []Eval{{
		ProjectID: e.ProjectId,
		TraceID:   e.TraceId,
		SpanID:    e.SpanId,
		Evaluator: e.Evaluator,
		Score:     e.Score,
		Label:     e.Label,
		Reasoning: e.Reasoning,
		CreatedAt: e.CreatedAt.AsTime(),
	}}}, nil
}

// structJSON is the protojson form of a Struct, "{}" when it's unset.
func structJSON(s *structpb.Struct) string {
	if s == nil {
		return "{}"
	}
	b, err := protojson.Marshal(s)
	if err != nil { // only possible for NaN/Inf values, which protojson can't represent
		return "{}"
	}
	return string(b)
}
