# Tracewell: Claude Code project rules

Distributed LLM observability platform (CMPE 273). Four teammates each work in their own Claude Code
session. These rules keep those sessions consistent. Full plan: `docs/PLAN.md`.
Cross-track contracts: `docs/contracts/`. This round's brief per track: `docs/tracks/`.

## Architecture

```
SDK (py/ts) --> API gateway --> ingest-svc (Go) --> Kafka: traces.raw
                                                     |-> writer-svc (Go) --> ClickHouse
                                                     |-> eval-workers (Py) --> Kafka: evals.results --> writer-svc
                                                     |-> alerter (Go) --> Slack/webhook
Dashboard (Next.js) --> API gateway --> query-svc (Go) --> ClickHouse + Postgres + Redis
Processing failures --> Kafka: traces.dlq
```

## Track ownership

Only change files in your own track's folders. If a task needs a change in another track's folder,
stop and tell the user which track owns it and what change is needed. Don't make the change.

| Track | Folders |
|---|---|
| A: Write path | `sdk/`, `services/ingest/`, `services/writer/`, `examples/` |
| B: Data platform & infra | `deploy/`, `docker-compose.yml`, `loadtest/`, `.github/`, ClickHouse/Postgres migrations (`deploy/migrations/`) |
| C: Evals & alerting | `workers/evals/`, `services/alerter/` |
| D: Read path & product | `services/query/`, `web/`, gateway config |
| Shared (all tracks review) | `proto/`, `docs/`, `CLAUDE.md` |

## Contract rules (`proto/`)

- `proto/trace.proto` is the contract between every service. Any change needs a PR reviewed by at least
  one person from each affected track.
- Never change or reuse a field number (`= 1`, `= 2`, ...). To remove a field, mark the number
  `reserved`. To add a field, give it the next unused number.
- Never rename a field or change its type. Add a new field instead.
- Generated code (Go/Python/TS from `buf generate`) is never edited by hand. Regenerate it instead.
- `project_id` always comes from the API key on the server. Never trust a value sent by the client.

## Kafka

| Topic | Message | Key |
|---|---|---|
| `traces.raw` | `IngestBatch` | `project_id` |
| `evals.results` | `EvalResult` | `project_id` |
| `traces.dlq` | `DeadLetter` | original key |

- Consumers must be idempotent, because Kafka redelivers messages. Dedupe on `idempotency_key` (traces)
  or `trace_id + span_id + evaluator` (evals).
- Commit offsets only after the work is durably done.
- After 3 failed attempts, send the message to `traces.dlq`. Never drop it silently.

## Conventions

**Go** (`services/*`)
- Each service is its own module: `cmd/<service>/main.go` plus `internal/...`.
- Logging: `log/slog` with JSON output. Every log line for a request includes `trace_id` and
  `project_id` when they're known.
- Config comes only from env vars, prefixed by service (`INGEST_KAFKA_BROKERS`,
  `QUERY_CLICKHOUSE_DSN`, ...). No config files.
- Every service exposes `GET /healthz` (liveness), `GET /readyz` (its dependencies are reachable) and
  `GET /metrics` (Prometheus).
- Handle SIGTERM: stop accepting work, finish the work already in flight, flush producers, then exit.
  Kubernetes depends on this.

**Python** (`workers/evals`, `sdk/python`)
- Manage dependencies with `uv`. Type hints on public functions.

**TypeScript** (`web`, `sdk/ts`)
- Strict mode, npm. Run `npm run lint` before committing.

**Local ports**

| Service | Port |
|---|---|
| ingest-svc | 8081 (HTTP), 9091 (gRPC) |
| query-svc | 8082 |
| alerter | 8083 |
| writer-svc | 8084 (health/metrics only) |
| eval-workers | 8085 (health/metrics only) |
| web | 3000 |

## Local dev

```
docker compose up -d    # Kafka :9092, ClickHouse :8123/:9000, Postgres :5432, Redis :6379
```
The local credentials in `docker-compose.yml` are `tracewell` / `tracewell`. Real secrets never go in the
repo, and `.env*` files are never committed.

## Git

- Branch names are prefixed with the track: `a/sdk-batching`, `d/trace-list-page`.
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `chore:`, `test:`.
- Every PR to `main` needs a review from someone on another track.
- Never push to `main` directly.

## Before calling a task done

1. The service builds and its tests pass.
2. `docker compose up` still works, and the service runs against it.
3. No files outside your track changed, except a proto change that has been agreed on.
4. If the service's behavior or API changed, update its section in `docs/`.
