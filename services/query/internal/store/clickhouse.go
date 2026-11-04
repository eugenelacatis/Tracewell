// Package store reads traces, spans and evals from ClickHouse (docs/contracts/clickhouse.sql).
//
// Every query uses FINAL. The tables are ReplacingMergeTree, so a Kafka redelivery can leave two
// copies of a row until ClickHouse merges them in the background; FINAL collapses them at read time.
// Every value comes in as a bound parameter (?), never formatted into the SQL string.
//
// Times are bound as microsecond integers through fromUnixTimestamp64Micro(?). Binding a
// time.Time to a positional ? makes clickhouse-go round it to whole seconds, which would make a
// cursor at 17:04:05.5 skip every trace between .0 and .5.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/eugenelacatis/tracewell/services/query/internal/model"
)

// ErrNotFound means the trace isn't in this project.
var ErrNotFound = errors.New("not found")

// ListParams filters GET /v1/traces. Zero times mean "no bound". After, when set, returns only
// traces strictly older than that (start_time, trace_id) pair.
type ListParams struct {
	ProjectID      string
	From, To       time.Time
	AfterStartTime time.Time
	AfterTraceID   string
	Limit          int
}

type ClickHouse struct{ Conn driver.Conn }

func OpenClickHouse(dsn string) (*ClickHouse, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &ClickHouse{Conn: conn}, nil
}

func (c *ClickHouse) Ping(ctx context.Context) error { return c.Conn.Ping(ctx) }
func (c *ClickHouse) Close() error                   { return c.Conn.Close() }

const traceColumns = `trace_id, name, session_id, user_id, tags, start_time, end_time,
	duration_ms, status, span_count, total_tokens, cost_usd`

func scanTrace(rows driver.Rows) (model.Trace, error) {
	var t model.Trace
	var start, end time.Time
	err := rows.Scan(&t.TraceID, &t.Name, &t.SessionID, &t.UserID, &t.Tags, &start, &end,
		&t.DurationMs, &t.Status, &t.SpanCount, &t.TotalTokens, &t.CostUsd)
	t.StartTime, t.EndTime = model.Time(start), model.Time(end)
	t.Status = model.Enum(t.Status, "STATUS_")
	if t.Tags == nil {
		t.Tags = []string{}
	}
	return t, err
}

// ListTraces returns up to p.Limit traces, newest first.
func (c *ClickHouse) ListTraces(ctx context.Context, p ListParams) ([]model.Trace, error) {
	q := `SELECT ` + traceColumns + ` FROM tracewell.traces FINAL WHERE project_id = ?`
	args := []any{p.ProjectID}
	if !p.From.IsZero() {
		q += ` AND start_time >= fromUnixTimestamp64Micro(?)`
		args = append(args, p.From.UnixMicro())
	}
	if !p.To.IsZero() {
		q += ` AND start_time < fromUnixTimestamp64Micro(?)`
		args = append(args, p.To.UnixMicro())
	}
	if p.AfterTraceID != "" {
		q += ` AND (start_time < fromUnixTimestamp64Micro(?)
			OR (start_time = fromUnixTimestamp64Micro(?) AND trace_id < ?))`
		after := p.AfterStartTime.UnixMicro()
		args = append(args, after, after, p.AfterTraceID)
	}
	q += ` ORDER BY start_time DESC, trace_id DESC LIMIT ?`
	args = append(args, p.Limit)

	rows, err := c.Conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Trace{}
	for rows.Next() {
		t, err := scanTrace(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// GetTrace returns one trace with its spans (by start time) and evals.
func (c *ClickHouse) GetTrace(ctx context.Context, projectID, traceID string) (model.TraceDetail, error) {
	var d model.TraceDetail

	rows, err := c.Conn.Query(ctx, `SELECT `+traceColumns+` FROM tracewell.traces FINAL
		WHERE project_id = ? AND trace_id = ? LIMIT 1`, projectID, traceID)
	if err != nil {
		return d, err
	}
	found := rows.Next()
	if found {
		d.Trace, err = scanTrace(rows)
	}
	rows.Close()
	if err != nil {
		return d, err
	}
	if err := rows.Err(); err != nil {
		return d, err
	}
	if !found {
		return d, ErrNotFound
	}

	if d.Spans, err = c.spans(ctx, projectID, traceID); err != nil {
		return d, err
	}
	d.Evals, err = c.evals(ctx, projectID, traceID)
	return d, err
}

func (c *ClickHouse) spans(ctx context.Context, projectID, traceID string) ([]model.Span, error) {
	rows, err := c.Conn.Query(ctx, `SELECT span_id, parent_span_id, name, kind, start_time, end_time,
			duration_ms, status, error_message, provider, model, prompt, completion, params,
			prompt_tokens, completion_tokens, total_tokens, cost_usd, metadata
		FROM tracewell.spans FINAL
		WHERE project_id = ? AND trace_id = ?
		ORDER BY start_time, span_id`, projectID, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Span{}
	for rows.Next() {
		var r model.SpanRow
		if err := rows.Scan(&r.SpanID, &r.ParentSpanID, &r.Name, &r.Kind, &r.StartTime, &r.EndTime,
			&r.DurationMs, &r.Status, &r.ErrorMessage, &r.Provider, &r.Model, &r.Prompt,
			&r.Completion, &r.Params, &r.PromptTokens, &r.CompletionTokens, &r.TotalTokens,
			&r.CostUsd, &r.Metadata); err != nil {
			return nil, err
		}
		out = append(out, r.ToSpan())
	}
	return out, rows.Err()
}

func (c *ClickHouse) evals(ctx context.Context, projectID, traceID string) ([]model.Eval, error) {
	rows, err := c.Conn.Query(ctx, `SELECT span_id, evaluator, score, label, reasoning, created_at
		FROM tracewell.evals FINAL
		WHERE project_id = ? AND trace_id = ?
		ORDER BY span_id, evaluator`, projectID, traceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Eval{}
	for rows.Next() {
		var e model.Eval
		var created time.Time
		if err := rows.Scan(&e.SpanID, &e.Evaluator, &e.Score, &e.Label, &e.Reasoning, &created); err != nil {
			return nil, err
		}
		e.CreatedAt = model.Time(created)
		out = append(out, e)
	}
	return out, rows.Err()
}
