// Package store inserts rows into the ClickHouse tables in docs/contracts/clickhouse.sql.
package store

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/eugenelacatis/tracewell/services/writer/internal/rows"
)

var insertDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name: "writer_insert_duration_seconds",
	Help: "Time for one batched INSERT into a table, successful or not.",
}, []string{"table"})

type ClickHouse struct{ Conn driver.Conn }

func OpenClickHouse(dsn string) (*ClickHouse, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	// On the 2x2 cluster the tables are Distributed. This makes an INSERT return only after the
	// shard has the rows, so committing the Kafka offset after Insert still means "durable"
	// (docs/contracts/clickhouse_cluster.sql). A single node ignores it.
	if opts.Settings == nil {
		opts.Settings = clickhouse.Settings{}
	}
	opts.Settings["distributed_foreground_insert"] = 1
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &ClickHouse{Conn: conn}, nil
}

func (c *ClickHouse) Ping(ctx context.Context) error { return c.Conn.Ping(ctx) }
func (c *ClickHouse) Close() error                   { return c.Conn.Close() }

// Insert writes spans, then traces, then evals, then ingest_batches, one INSERT per table.
// ingest_batches goes last so that a row there means the batch's traces and spans are stored.
// If an INSERT fails partway, the caller retries all of them; rows that already went in become
// duplicates, which ReplacingMergeTree collapses. duration_ms and ingested_at are filled in by
// ClickHouse (MATERIALIZED / DEFAULT), so they're not in the column lists.
func (c *ClickHouse) Insert(ctx context.Context, r rows.Rows) error {
	if err := send(ctx, c.Conn, "spans", `INSERT INTO tracewell.spans (project_id, trace_id, span_id,
		parent_span_id, name, kind, start_time, end_time, status, error_message, provider, model,
		prompt, completion, params, prompt_tokens, completion_tokens, total_tokens, cost_usd, metadata)`,
		r.Spans, func(s rows.Span) []any {
			return []any{s.ProjectID, s.TraceID, s.SpanID, s.ParentSpanID, s.Name, s.Kind, s.StartTime,
				s.EndTime, s.Status, s.ErrorMessage, s.Provider, s.Model, s.Prompt, s.Completion, s.Params,
				s.PromptTokens, s.CompletionTokens, s.TotalTokens, s.CostUSD, s.Metadata}
		}); err != nil {
		return err
	}
	if err := send(ctx, c.Conn, "traces", `INSERT INTO tracewell.traces (project_id, trace_id, session_id,
		user_id, name, tags, start_time, end_time, status, span_count, total_tokens, cost_usd,
		idempotency_key)`,
		r.Traces, func(t rows.Trace) []any {
			return []any{t.ProjectID, t.TraceID, t.SessionID, t.UserID, t.Name, t.Tags, t.StartTime,
				t.EndTime, t.Status, t.SpanCount, t.TotalTokens, t.CostUSD, t.IdempotencyKey}
		}); err != nil {
		return err
	}
	if err := send(ctx, c.Conn, "evals", `INSERT INTO tracewell.evals (project_id, trace_id, span_id,
		evaluator, score, label, reasoning, created_at)`,
		r.Evals, func(e rows.Eval) []any {
			return []any{e.ProjectID, e.TraceID, e.SpanID, e.Evaluator, e.Score, e.Label, e.Reasoning,
				e.CreatedAt}
		}); err != nil {
		return err
	}
	return send(ctx, c.Conn, "ingest_batches", `INSERT INTO tracewell.ingest_batches (project_id, sdk_instance_id,
		batch_seq, idempotency_key, trace_count, span_count, kafka_partition, kafka_offset)`,
		r.Batches, func(b rows.IngestBatch) []any {
			return []any{b.ProjectID, b.SDKInstanceID, b.BatchSeq, b.IdempotencyKey, b.TraceCount,
				b.SpanCount, b.KafkaPartition, b.KafkaOffset}
		})
}

// send runs one batched INSERT into table. With no rows it does nothing.
func send[T any](ctx context.Context, conn driver.Conn, table, query string, items []T, values func(T) []any) error {
	if len(items) == 0 {
		return nil
	}
	start := time.Now()
	defer func() { insertDuration.WithLabelValues(table).Observe(time.Since(start).Seconds()) }()
	b, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	defer b.Close()
	for _, it := range items {
		if err := b.Append(values(it)...); err != nil {
			return err
		}
	}
	return b.Send()
}
