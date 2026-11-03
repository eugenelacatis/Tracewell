//go:build integration

// Runs the real SQL against the compose ClickHouse:
//
//	docker compose up -d clickhouse
//	go test -tags integration ./internal/store/
//
// It applies docs/contracts/clickhouse.sql (every statement is IF NOT EXISTS), seeds rows under a
// fresh project_id, and inserts every row twice to check that FINAL hides the duplicate.
package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func openTest(t *testing.T) *ClickHouse {
	t.Helper()
	dsn := os.Getenv("QUERY_CLICKHOUSE_DSN")
	if dsn == "" {
		dsn = "clickhouse://tracewell:tracewell@localhost:9000/tracewell"
	}
	ch, err := OpenClickHouse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := ch.Ping(ctx); err != nil {
		t.Skipf("ClickHouse not reachable at %s: %v", dsn, err)
	}
	schema, err := os.ReadFile("../../../../docs/contracts/clickhouse.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(string(schema), ";") {
		var lines []string
		for _, l := range strings.Split(stmt, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(l), "--") {
				lines = append(lines, l)
			}
		}
		if sql := strings.TrimSpace(strings.Join(lines, "\n")); sql != "" {
			if err := ch.Conn.Exec(ctx, sql); err != nil {
				t.Fatalf("schema: %v\n%s", err, sql)
			}
		}
	}
	return ch
}

func TestClickHouseReadPath(t *testing.T) {
	ch := openTest(t)
	defer ch.Close()
	ctx := context.Background()
	pid := fmt.Sprintf("proj_it_%d", time.Now().UnixNano())
	base := time.Date(2026, 10, 26, 17, 0, 0, 0, time.UTC)

	// Three traces, the two newest share a start_time so the trace_id tiebreak gets exercised.
	traces := []struct {
		id    string
		start time.Time
	}{
		{"aaaa0000000000000000000000000001", base},
		{"aaaa0000000000000000000000000002", base.Add(time.Second)},
		{"aaaa0000000000000000000000000003", base.Add(time.Second)},
	}
	for range 2 { // insert twice: FINAL must collapse the copies
		for _, tr := range traces {
			if err := ch.Conn.Exec(ctx, `INSERT INTO tracewell.traces
				(project_id, trace_id, session_id, user_id, name, tags, start_time, end_time,
				 status, span_count, total_tokens, cost_usd, idempotency_key)
				VALUES (?, ?, 's', 'u', 'chat turn', ['example'], ?, ?, 'OK', 2, 12, 0.0000045, 'k:1')`,
				pid, tr.id, tr.start, tr.start.Add(1200*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}
		if err := ch.Conn.Exec(ctx, `INSERT INTO tracewell.spans
			(project_id, trace_id, span_id, parent_span_id, name, kind, start_time, end_time, status,
			 error_message, provider, model, prompt, completion, params, prompt_tokens,
			 completion_tokens, total_tokens, cost_usd, metadata)
			VALUES
			(?, ?, '00000000000000a1', '', 'root', 'SPAN', ?, ?, 'OK', '', '', '', '', '', '', 0, 0, 0, 0, '{}'),
			(?, ?, '00000000000000a2', '00000000000000a1', 'openai.chat', 'GENERATION', ?, ?, 'OK', '',
			 'openai', 'gpt-4o-mini', 'hi', 'Hello!', '{"temperature":0.2}', 9, 3, 12, 0.0000045, '{}')`,
			pid, traces[0].id, base, base.Add(1200*time.Millisecond),
			pid, traces[0].id, base.Add(100*time.Millisecond), base.Add(1100*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if err := ch.Conn.Exec(ctx, `INSERT INTO tracewell.evals
			(project_id, trace_id, span_id, evaluator, score, label, reasoning, created_at)
			VALUES (?, ?, '00000000000000a2', 'latency', 1.0, 'ok', '1000ms', ?)`,
			pid, traces[0].id, base.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	page1, err := ch.ListTraces(ctx, ListParams{ProjectID: pid, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || page1[0].TraceID != traces[2].id || page1[1].TraceID != traces[1].id {
		t.Fatalf("page 1 wrong order or duplicates: %+v", page1)
	}
	if page1[0].DurationMs != 1200 || page1[0].Tags[0] != "example" || page1[0].Status != "OK" {
		t.Fatalf("page 1 fields wrong: %+v", page1[0])
	}

	last := page1[1]
	page2, err := ch.ListTraces(ctx, ListParams{
		ProjectID: pid, Limit: 2,
		AfterStartTime: time.Time(last.StartTime), AfterTraceID: last.TraceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 1 || page2[0].TraceID != traces[0].id {
		t.Fatalf("page 2 wrong: %+v", page2)
	}

	filtered, err := ch.ListTraces(ctx, ListParams{ProjectID: pid, Limit: 10, From: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 {
		t.Fatalf("from filter: want 2, got %d", len(filtered))
	}

	d, err := ch.GetTrace(ctx, pid, traces[0].id)
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Spans) != 2 || d.Spans[0].Generation != nil || d.Spans[1].Generation == nil {
		t.Fatalf("spans wrong: %+v", d.Spans)
	}
	if d.Spans[1].Generation.Usage.TotalTokens != 12 || d.Spans[1].DurationMs != 1000 {
		t.Fatalf("generation span wrong: %+v", d.Spans[1])
	}
	if len(d.Evals) != 1 || d.Evals[0].Evaluator != "latency" {
		t.Fatalf("evals wrong: %+v", d.Evals)
	}

	if _, err := ch.GetTrace(ctx, "proj_other", traces[0].id); err != ErrNotFound {
		t.Fatalf("other project should get ErrNotFound, got %v", err)
	}
}
