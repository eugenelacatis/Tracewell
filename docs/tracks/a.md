# Track A: Write path

Folders: `sdk/`, `services/ingest/`, `services/writer/`, `examples/`
DS concept to demo: message queues and streaming, idempotency.

## Round 1: walking skeleton

Branch: `a/round-1-skeleton`

Get a trace from a Python program into ClickHouse.

### Build

1. **Python SDK** (`sdk/python`, package `tracewell`, managed with `uv`)
   - API: a context manager for traces and nested spans, plus a way to record a generation
     (provider, model, prompt, completion, params, token usage, cost). Also an `@observe` decorator that
     wraps a function in a span. Pick the exact names, then document them in `sdk/python/README.md`.
   - Config from `TRACEWELL_HOST` and `TRACEWELL_API_KEY`, overridable in the constructor.
   - Background thread: buffer finished traces, flush every 1s or every 100 traces, whichever comes
     first. `flush()` and `atexit` flush so a short script loses nothing.
   - Each process picks a random `sdk_instance_id`. `batch_seq` starts at 1. `idempotency_key` =
     `"<sdk_instance_id>:<batch_seq>"`. A retry resends the same batch with the same seq.
   - Retry policy and status-code handling exactly as in `docs/contracts/http.md`.
   - Send protojson by building dicts. The SDK doesn't depend on the generated protobuf package.
   - Never raise into the user's code because Tracewell is down. Log a warning instead.
   - Tests: batching thresholds, seq increments once per batch (not per retry), retry/backoff on
     5xx, no retry on 400/401.

2. **ingest-svc** (`services/ingest`, Go)
   - `POST /v1/ingest` per `docs/contracts/http.md`: auth via Postgres `api_keys`, accept JSON or binary
     protobuf, overwrite `project_id` from the key, reject a missing `idempotency_key`.
   - Produce the `IngestBatch` as binary protobuf to `traces.raw`, keyed by `project_id`, `acks=all`,
     idempotent producer. Respond `202` only after the broker acks.
   - Cache API key lookups in memory for 60s so Postgres isn't hit on every request.
   - Kafka client: `franz-go` (recommended) or `confluent-kafka-go`. Pick one and use it in the writer too.
   - Tests: auth (good key, bad key, revoked key), `project_id` overwrite, bad body -> 400.

3. **writer-svc** (`services/writer`, Go)
   - Consumer group `writer`, subscribed to `traces.raw` and `evals.results`.
   - For each `IngestBatch`: insert `spans`, then `traces` (computing `status`, `span_count`,
     `total_tokens`, `cost_usd` from the spans), then one `ingest_batches` row. Map enums by dropping the
     proto prefix. Batch inserts across messages, flushing every 500 rows or 1s.
   - For each `EvalResult`: insert into `evals`.
   - Commit offsets only after the ClickHouse insert succeeds. On failure retry 3 times, then send a
     `DeadLetter` to `traces.dlq` and commit.
   - Redelivered messages are fine because the tables are `ReplacingMergeTree`. Don't add a separate
     dedupe store.
   - Tests: enum mapping, rollup math, DLQ after 3 failures (with a fake inserter).

4. **Example** (`examples/chatbot.py`)
   - Uses the SDK to record a short chat turn with one generation span inside a parent span.
   - Calls OpenAI or Anthropic if a key is set, otherwise uses a `--fake` mode with canned output
     and realistic token counts, so the integration run needs no API keys.
   - `--error` flag marks the generation span as failed. The alerter test in `docs/INTEGRATION.md`
     uses it.
   - `--count N` sends N traces.

### Contracts used
`proto/trace.proto` (IngestBatch, EvalResult, DeadLetter), `docs/contracts/http.md` (ingest),
`docs/contracts/clickhouse.sql` (writer tables), `docs/contracts/postgres.sql` (api_keys),
`docs/contracts/README.md` (Kafka, env vars, Dockerfile layout).

### Done when
- `go test ./...` passes in both services and `uv run pytest` passes in `sdk/python`.
- With `docker compose up -d` and both services running locally,
  `uv run python examples/chatbot.py --fake --count 5` returns without errors, and
  `SELECT count() FROM tracewell.traces FINAL` shows 5 new rows with correct `cost_usd`.
- Killing the writer mid-run and restarting it doesn't lose or double-count traces (`FINAL` count
  matches what was sent).
- Dockerfiles for both services build from the repo root.
- `docs/services/ingest.md`, `docs/services/writer.md` and `sdk/python/README.md` are written.

### Deferred to later rounds
TS SDK, gRPC ingest on 9091, Redis rate limiting (round 2). DLQ replay CLI (round 3). Writer gap
tracking for the completeness page (round 4).
