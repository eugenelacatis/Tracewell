# Track A: Write path

Folders: `sdk/`, `services/ingest/`, `services/writer/`, `examples/`
DS concept to demo: message queues and streaming, idempotency.

Round 1 (walking skeleton: Python SDK, HTTP ingest, writer, chatbot example) is done, merged in PR #6.
Its brief is in git history.

## Round 2: second SDK, second transport, rate limits

Branch: `a/round-2`

### Build

1. **TS SDK** (`sdk/ts`, npm package `@tracewell/sdk`, TypeScript strict)
   - Same behavior as the Python SDK: traces and nested spans, generation recording, a wrapper that
     turns a function into a span, background flush every 1s or 100 traces, `flush()` on exit.
   - Same wire rules: random `sdkInstanceId` per process, `batchSeq` from 1, the same idempotency key
     on a retry, the retry table in `docs/contracts/http.md`, including `429` with `Retry-After`.
   - Transport: HTTP protojson by default. gRPC (`@grpc/grpc-js`) as an option, using the generated
     code in `sdk/ts/src/gen`. Both go through the gateway at `TRACEWELL_HOST`.
   - Works in Node 20+. Browser support isn't needed.
   - Never throws into user code because Tracewell is down.
   - Tests (vitest): batching thresholds, seq once per batch, retry/backoff, no retry on 400/401,
     honoring `Retry-After`.
   - `examples/chatbot.ts`: the TS twin of `chatbot.py`, with the same `--fake`, `--error` and
     `--count` flags.

2. **gRPC ingest** (`services/ingest`)
   - Serve `tracewell.v1.IngestService/Ingest` on `INGEST_GRPC_ADDR`, from the generated code in
     `proto/gen/go`. Share the auth, validation and Kafka write with the HTTP handler. Don't copy them.
   - Status mapping exactly as in `docs/contracts/http.md`.
   - SIGTERM: `GracefulStop` the gRPC server alongside the HTTP server, then flush the producer.
   - Tests: the same auth and validation cases as HTTP, over an in-process gRPC connection (`bufconn`).

3. **Per-project rate limit** (`services/ingest`)
   - Redis fixed-window counter as specified in `docs/contracts/http.md` (Rate limit) and the Redis key
     table in `docs/contracts/README.md`. Client: `go-redis/v9`.
   - Fail open when Redis is down, and count it.
   - Python SDK: confirm it already honors `Retry-After` on `429`. Fix it if not.
   - Tests: under the limit passes, over it gets 429 with `Retry-After: 1`, Redis down passes.

4. **Metrics**: the ingest and writer metrics listed in `docs/contracts/README.md`.

5. **Point both SDKs at the gateway**: `TRACEWELL_HOST` defaults to `http://localhost:8080` in the
   Python and TS SDKs, per `docs/contracts/README.md`.

### Contracts used
`proto/trace.proto` (`IngestService`), `docs/contracts/http.md` (rate limit, gRPC, SDK retry table),
`docs/contracts/README.md` (env vars, Redis keys, gateway routes, metrics).

### Done when
- `go test ./...` passes in both services, `uv run pytest` in `sdk/python`, `npm test` and
  `npm run lint` in `sdk/ts`.
- Through the gateway (`TRACEWELL_HOST=http://localhost:8080`), `chatbot.ts --fake --count 5` adds 5
  rows to `tracewell.traces`, once over HTTP and once over gRPC.
- A loop that sends 500 requests in one second gets some `429`s, and the SDK retries them until every
  trace lands. `SELECT count() FROM tracewell.traces FINAL` matches what was sent.
- `docs/services/ingest.md` and `sdk/ts/README.md` are updated or written.

### Deferred
DLQ replay CLI and retry hardening (round 3). Writer gap tracking for the completeness page
(round 4).
