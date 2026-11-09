# Contracts

These files pin every seam between tracks. Code against them, not against another track's code.
Changing one means a PR reviewed by every track it touches, same as `proto/`.

| File | What it pins |
|---|---|
| `../../proto/trace.proto` | Message shapes for Kafka and gRPC |
| `clickhouse.sql` | Trace, span, eval and batch tables (writer writes, query reads) |
| `postgres.sql` | Orgs, projects, API keys, alert rules/events, plus the dev seed |
| `postgres_round2.sql` | Users, memberships, alert-rule soft delete, second demo tenant (round 2) |
| `clickhouse_cluster.sql` | The same tables on the 2x2 cluster: replicated local tables + Distributed (round 2) |
| `http.md` | Ingest (HTTP + gRPC) and query APIs, auth, sessions, rate limits, health endpoints |
| this file | Generated code, Kafka, env vars, ports, Docker build layout, gateway, Redis keys, metrics, evaluators |

## Generated proto code

Configured in `proto/buf.yaml` and `proto/buf.gen.yaml`. Output is committed, so building a service
doesn't need `buf`. Regenerate after any `trace.proto` change:

```
cd proto && buf generate
```

| Language | Location | How to depend on it |
|---|---|---|
| Go | `proto/gen/go` (module `github.com/eugenelacatis/tracewell/proto/tracewellv1`) | `require github.com/eugenelacatis/tracewell/proto/tracewellv1 v0.0.0` plus `replace github.com/eugenelacatis/tracewell/proto/tracewellv1 => ../../proto/gen/go` |
| Python | `proto/gen/python` (package `tracewell-proto`, import `tracewell_proto.trace_pb2`) | `uv add --editable ../../proto/gen/python` |
| TypeScript | `sdk/ts/src/gen/trace_pb.ts` (round 2) | Inside the TS SDK, its only user, so the npm package ships it. Needs `@bufbuild/protobuf` at the plugin's version (2.17.0) |

The generated Go module needs Go 1.25 or newer (gRPC requires it), so every Go service uses `go 1.25`
or later in its `go.mod` and a `golang:1.25`+ base image.

The Python SDK sends protojson over HTTP and doesn't need the generated package. The eval workers decode
binary Kafka messages, so they do.

## Kafka

| Topic | Value | Key | Producer | Consumer groups |
|---|---|---|---|---|
| `traces.raw` | `IngestBatch`, binary protobuf | `project_id` | ingest-svc | `writer`, `evals`, `alerter` |
| `evals.results` | `EvalResult`, binary protobuf | `project_id` | eval-workers | `writer` |
| `traces.dlq` | `DeadLetter`, binary protobuf | original key | any consumer | replay CLI (round 3) |

- Local (compose): 6 partitions, replication factor 1. kind/EKS (Strimzi): 3 brokers, 6 partitions,
  RF 3, `min.insync.replicas=2`. Topic names don't change, so services don't either.
- Producers: `acks=all`, idempotence on.
- Consumers: manual commit, only after the work is durable. After 3 failed attempts on one message,
  produce a `DeadLetter` to `traces.dlq` and then commit. Never skip a message silently.
- Brokers: `kafka:29092` from inside compose, `localhost:9092` from the host.

## Runtime: ports, env vars, images

Every service reads config only from env vars. Values below are the in-compose defaults.

| Service | Folder | Ports | Env vars |
|---|---|---|---|
| ingest-svc | `services/ingest` | 8081 HTTP, 9091 gRPC | `INGEST_HTTP_ADDR=:8081`, `INGEST_GRPC_ADDR=:9091`, `INGEST_KAFKA_BROKERS=kafka:29092`, `INGEST_POSTGRES_DSN`, `INGEST_REDIS_ADDR=redis:6379`, `INGEST_RATE_LIMIT_RPS=200` |
| writer-svc | `services/writer` | 8084 health/metrics | `WRITER_HTTP_ADDR=:8084`, `WRITER_KAFKA_BROKERS=kafka:29092`, `WRITER_CLICKHOUSE_DSN` |
| query-svc | `services/query` | 8082 | `QUERY_HTTP_ADDR=:8082`, `QUERY_CLICKHOUSE_DSN`, `QUERY_POSTGRES_DSN`, `QUERY_REDIS_ADDR=redis:6379` |
| alerter | `services/alerter` | 8083 | `ALERTER_HTTP_ADDR=:8083`, `ALERTER_KAFKA_BROKERS=kafka:29092`, `ALERTER_POSTGRES_DSN` |
| eval-workers | `workers/evals` | 8085 health/metrics | `EVALS_HTTP_ADDR=:8085`, `EVALS_KAFKA_BROKERS=kafka:29092`, `EVALS_ANTHROPIC_API_KEY` (empty = judge off), `EVALS_JUDGE_MODEL=claude-haiku-5-5`, `EVALS_JUDGE_SAMPLE_RATE=1.0` |
| web | `web` | 3000 | `WEB_QUERY_URL=http://gateway:8080` (round 1's `WEB_API_KEY` goes away: the dashboard uses the logged-in user's session) |
| gateway (Envoy) | `gateway` | 8080 HTTP + gRPC, 9901 admin | none; config file `gateway/envoy.yaml` |
| Python SDK | `sdk/python` | none | `TRACEWELL_HOST=http://localhost:8080`, `TRACEWELL_API_KEY` |
| TS SDK | `sdk/ts` | none | same two as the Python SDK |
| Prometheus | `deploy/` | 9090 | Track B |
| Grafana | `deploy/` | 3300 | Track B; `admin` / `tracewell` locally |

Round 2 points the SDKs at the gateway. Hitting a service's own port directly still works for
debugging.

DSNs:
- ClickHouse: `clickhouse://tracewell:tracewell@clickhouse:9000/tracewell`
- Postgres: `postgres://tracewell:tracewell@postgres:5432/tracewell?sslmode=disable`

Docker: each app track owns a `Dockerfile` in its own folder (`services/ingest/Dockerfile`,
`workers/evals/Dockerfile`, `web/Dockerfile`, ...). The **build context is the repo root**, because Go
services and the eval workers need `proto/gen`. Track B wires them into `docker-compose.yml` with
`build: { context: ., dockerfile: services/ingest/Dockerfile }`.

## Gateway (round 2)

Envoy, config in `gateway/envoy.yaml` (Track D). Track B runs it in compose and Helm. One listener on
8080 that speaks HTTP/1.1 and HTTP/2 (gRPC).

| Match | Upstream | Notes |
|---|---|---|
| `POST /v1/ingest` | ingest-svc :8081 | 5 MB body limit |
| gRPC `/tracewell.v1.IngestService/` | ingest-svc :9091 | HTTP/2 upstream |
| `/v1/` (everything else) | query-svc :8082 | |

- The gateway adds `x-request-id` when it's missing. Services log it as `request_id`.
- Coarse per-client-IP limit (Envoy local rate limit): 1000 requests/s. This guards the services as a
  whole. The per-project limit lives in ingest-svc (Redis), because only ingest knows the project.
- No auth at the gateway in round 2.
- The dashboard's browser never calls the gateway. Next.js server code does.

## Redis keys (round 2)

One Redis, shared by ingest-svc and query-svc. Every key has a TTL. Prefixes are owned as follows:

| Key | Owner | Value | TTL |
|---|---|---|---|
| `rl:ingest:<project_id>:<unix second>` | ingest-svc | counter | 2s |
| `rl:login:<lowercased email>:<unix minute>` | query-svc | counter | 70s |
| `sess:<sha256 hex of token>` | query-svc | JSON `{"userId": "usr_..."}` | 7 days |
| `qc:<project_id>:<route>:<sha256 hex of sorted query string>` | query-svc | the response body | per `http.md` Cache |

Redis is a cache and a limiter here, never the only copy of anything.

## Prometheus metrics (round 2)

Every service already serves `/metrics`. Names use the service as a prefix (`ingest_`, `writer_`,
`query_`, `alerter_`, `evals_`). Never use `trace_id`, `span_id` or a user ID as a label, because each
new value makes a new time series. `project_id` is allowed. Each service exposes at least:

| Service | Metrics |
|---|---|
| all HTTP services | `<svc>_http_request_duration_seconds` histogram, labels `route`, `status` |
| ingest-svc | `ingest_traces_accepted_total`, `ingest_spans_accepted_total`, `ingest_rate_limited_total{project_id}`, `ingest_rate_limit_errors_total`, `ingest_kafka_produce_duration_seconds` |
| writer-svc | `writer_rows_inserted_total{table}`, `writer_insert_duration_seconds{table}`, `writer_dlq_total{reason}` |
| eval-workers | `evals_scored_total{evaluator}`, `evals_duration_seconds{evaluator}`, `evals_judge_errors_total`, `evals_dlq_total{reason}` |
| alerter | `alerter_rules_loaded`, `alerter_alerts_fired_total{metric, channel}`, `alerter_delivery_errors_total{channel}` |
| query-svc | `query_cache_requests_total{result="hit"/"miss"}`, `query_clickhouse_duration_seconds{route}` |

Consumer lag comes from a Kafka exporter that Track B runs, not from the services.

## Evaluators (round 2)

Every evaluator writes one `EvalResult` per `GENERATION` span. Scores run 0 to 1, and **1 is good**
for all of them, so the dashboard can color any score the same way.

| `evaluator` | Score | `label` | `reasoning` |
|---|---|---|---|
| `latency` (round 1) | 1 under 5000 ms, else 0 | `ok` / `slow` | the duration |
| `empty_completion` (round 1) | 1 if non-blank, else 0 | `ok` / `empty` | |
| `llm_judge` | 0-1 from the judge model | `pass` (>= 0.5) / `fail` | the judge's one-paragraph explanation |
| `toxicity` | 1 minus the toxicity probability | `clean` / `toxic` (score < 0.5) | which categories tripped |
| `pii` | 1 if no PII found, else 0 | `none`, or the comma-joined types (`email,phone`) | the types and counts, **never the matched text** |

`llm_judge` runs only when `EVALS_ANTHROPIC_API_KEY` is set, on a `EVALS_JUDGE_SAMPLE_RATE` fraction of
spans. A judge call that fails 3 times is skipped for that span and counted in
`evals_judge_errors_total`. It doesn't go to the DLQ, because the trace itself is fine.
