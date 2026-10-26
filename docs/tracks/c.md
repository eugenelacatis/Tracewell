# Track C: Evals & alerting

Folders: `workers/evals/`, `services/alerter/`
DS concepts to demo: consumer groups, stream processing.

## Round 1: walking skeleton

Branch: `c/round-1-skeleton`

Score every generation span asynchronously, and fire one alert rule.

### Build

1. **Eval worker** (`workers/evals`, Python, `uv`)
   - Depend on the generated protobuf package: `uv add --editable ../../proto/gen/python`.
   - Kafka consumer group `evals` on `traces.raw` (`confluent-kafka`), manual commit. Decode each message
     as `IngestBatch`.
   - Run every evaluator on every `GENERATION` span and produce one `EvalResult` per (span, evaluator)
     to `evals.results`, keyed by `project_id`. Flush the producer, then commit the offset.
   - Round 1 evaluators. Put them behind a small interface, since round 2 adds LLM-as-judge,
     toxicity and PII:
     - `latency`: score 1.0 with label `ok` if the span took under 5000 ms, else score 0.0 and label `slow`.
       `reasoning` says the duration.
     - `empty_completion`: score 0.0 and label `empty` if the completion is blank, else score 1.0 and
       label `ok`.
   - 3 failed attempts on a message: send a `DeadLetter` to `traces.dlq`, then commit.
   - `/healthz`, `/readyz`, `/metrics` on `:8085` (`prometheus_client`). Count messages processed and
     evals produced. SIGTERM: finish the current message, flush, commit, exit.
   - Tests: each evaluator, non-generation spans skipped, DLQ after 3 failures (fake producer).

2. **Alerter** (`services/alerter`, Go)
   - Consumer group `alerter` on `traces.raw`.
   - Load enabled rows from `alert_rules` at start and every 30s.
   - Round 1 implements the `error_rate` metric: per project, the share of spans with `STATUS_ERROR`
     over the rule's `window_seconds`. A tumbling window is fine for now. Sliding windows come in round 2.
   - When the value is above `threshold` and the rule's `cooldown_seconds` have passed since it last
     fired: insert into `alert_events`, then deliver. Round 1 delivery is `log` (an slog line with
     `rule_id`, `project_id`, value) and `webhook` (POST JSON to `target`).
   - Window state lives in memory. Restart behavior is a round 3 topic, so write down what happens on
     restart in the service doc.
   - Tests: window math, threshold plus cooldown, rule reload.

### Contracts used
`proto/trace.proto` (IngestBatch, EvalResult, DeadLetter), `docs/contracts/postgres.sql`
(alert_rules, alert_events, and the seeded `error rate above 50%` rule), `docs/contracts/README.md`
(Kafka, env vars, Dockerfile layout). The writer (Track A) stores your `EvalResult`s, and the dashboard
(Track D) shows them. You don't touch ClickHouse.

### Done when
- `uv run pytest` and `go test ./...` pass.
- With infra up, ingest and writer running (or, until Track A lands, a small script of your own that
  produces a hand-built `IngestBatch` to `traces.raw`), each generation span yields 2 `EvalResult`
  messages on `evals.results`.
- Sending a burst of failed spans makes the seeded rule fire once, write one `alert_events` row, and
  not fire again inside the cooldown.
- Dockerfiles for both build from the repo root.
- `docs/services/evals.md` and `docs/services/alerter.md` are written.

### Deferred to later rounds
LLM-as-judge, toxicity and PII scorers, sliding windows, `cost_usd` and `p95_latency_ms` metrics,
Slack delivery (round 2). Scaling eval workers by partition under load, plus HPA (round 3).
