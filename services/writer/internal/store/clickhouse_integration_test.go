//go:build integration

package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eugenelacatis/tracewell/services/writer/internal/rows"
)

// Inserts real rows into the compose ClickHouse (docs/contracts/clickhouse.sql applied), twice,
// and checks that FINAL shows each once with every column intact.
// go test -tags integration ./internal/store/
func TestInsertTwiceReadsBackOnceWithFinal(t *testing.T) {
	ctx := context.Background()
	ch, err := OpenClickHouse("clickhouse://tracewell:tracewell@localhost:9000/tracewell")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ch.Close() })
	if err := ch.Ping(ctx); err != nil {
		t.Skipf("clickhouse not reachable: %v", err)
	}

	project := fmt.Sprintf("proj_writertest_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, table := range []string{"spans", "traces", "evals", "ingest_batches"} {
			if err := ch.Conn.Exec(ctx, "DELETE FROM tracewell."+table+" WHERE project_id = ?", project); err != nil {
				t.Errorf("cleanup %s: %v", table, err)
			}
		}
	})
	start := time.Date(2026, 10, 26, 17, 4, 5, 123456000, time.UTC) // sub-second, to catch rounding
	end := start.Add(1200 * time.Millisecond)
	r := rows.Rows{
		Spans: []rows.Span{{
			ProjectID: project, TraceID: "t1", SpanID: "s1", Name: "llm", Kind: "GENERATION",
			StartTime: start, EndTime: end, Status: "OK", Provider: "openai", Model: "gpt-4o-mini",
			Prompt: "hi", Completion: "Hello!", Params: `{"temperature":0.2}`, PromptTokens: 9,
			CompletionTokens: 3, TotalTokens: 12, CostUSD: 0.0000045, Metadata: "{}",
		}},
		Traces: []rows.Trace{{
			ProjectID: project, TraceID: "t1", Name: "chat turn", Tags: []string{"a", "b"},
			StartTime: start, EndTime: end, Status: "OK", SpanCount: 1, TotalTokens: 12,
			CostUSD: 0.0000045, IdempotencyKey: "inst:1",
		}},
		Evals: []rows.Eval{{
			ProjectID: project, TraceID: "t1", SpanID: "s1", Evaluator: "latency", Score: 1,
			Label: "ok", CreatedAt: end,
		}},
		Batches: []rows.IngestBatch{{
			ProjectID: project, SDKInstanceID: "inst", BatchSeq: 1, IdempotencyKey: "inst:1",
			TraceCount: 1, SpanCount: 1, KafkaPartition: 3, KafkaOffset: 99,
		}},
	}
	for range 2 { // the second insert is a Kafka redelivery
		if err := ch.Insert(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	for _, table := range []string{"spans", "traces", "evals", "ingest_batches"} {
		var n uint64
		q := "SELECT count() FROM tracewell." + table + " FINAL WHERE project_id = ?"
		if err := ch.Conn.QueryRow(ctx, q, project).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("%s FINAL count = %d, want 1", table, n)
		}
	}

	var gotStart time.Time
	var duration, cost float64
	var tags []string
	var tokens uint64
	err = ch.Conn.QueryRow(ctx, `SELECT start_time, duration_ms, cost_usd, tags, total_tokens
		FROM tracewell.traces FINAL WHERE project_id = ?`, project).Scan(&gotStart, &duration, &cost, &tags, &tokens)
	if err != nil {
		t.Fatal(err)
	}
	if !gotStart.Equal(start) || duration != 1200 || cost != 0.0000045 || len(tags) != 2 || tokens != 12 {
		t.Errorf("trace row: start=%v duration=%v cost=%v tags=%v tokens=%d", gotStart, duration, cost, tags, tokens)
	}
}
