# Contracts

These files pin every seam between tracks. Code against them, not against another track's code.
Changing one means a PR reviewed by every track it touches, same as `proto/`.

| File | What it pins |
|---|---|
| `../../proto/trace.proto` | Message shapes for Kafka and gRPC |
| `clickhouse.sql` | Trace, span, eval and batch tables (writer writes, query reads) |
| `postgres.sql` | Orgs, projects, API keys, alert rules/events, plus the dev seed |
| `http.md` | Ingest and query HTTP APIs, auth, health endpoints |
| this file | Generated code, Kafka, env vars, ports, Docker build layout |

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

- Local: 6 partitions, replication factor 1. Round 2 (Strimzi): 3 brokers, RF 3, `min.insync.replicas=2`.
- Producers: `acks=all`, idempotence on.
- Consumers: manual commit, only after the work is durable. After 3 failed attempts on one message,
  produce a `DeadLetter` to `traces.dlq` and then commit. Never skip a message silently.
- Brokers: `kafka:29092` from inside compose, `localhost:9092` from the host.

## Runtime: ports, env vars, images

Every service reads config only from env vars. Values below are the in-compose defaults.

| Service | Folder | Ports | Env vars |
|---|---|---|---|
| ingest-svc | `services/ingest` | 8081 HTTP, 9091 gRPC (round 2) | `INGEST_HTTP_ADDR=:8081`, `INGEST_GRPC_ADDR=:9091`, `INGEST_KAFKA_BROKERS=kafka:29092`, `INGEST_POSTGRES_DSN`, `INGEST_REDIS_ADDR=redis:6379` (round 2) |
| writer-svc | `services/writer` | 8084 health/metrics | `WRITER_HTTP_ADDR=:8084`, `WRITER_KAFKA_BROKERS=kafka:29092`, `WRITER_CLICKHOUSE_DSN` |
| query-svc | `services/query` | 8082 | `QUERY_HTTP_ADDR=:8082`, `QUERY_CLICKHOUSE_DSN`, `QUERY_POSTGRES_DSN`, `QUERY_REDIS_ADDR=redis:6379` (round 2) |
| alerter | `services/alerter` | 8083 | `ALERTER_HTTP_ADDR=:8083`, `ALERTER_KAFKA_BROKERS=kafka:29092`, `ALERTER_POSTGRES_DSN` |
| eval-workers | `workers/evals` | 8085 health/metrics | `EVALS_HTTP_ADDR=:8085`, `EVALS_KAFKA_BROKERS=kafka:29092` (round 2 adds the judge model key) |
| web | `web` | 3000 | `WEB_QUERY_URL=http://query:8082`, `WEB_API_KEY=tw_dev_localkey` |
| Python SDK | `sdk/python` | none | `TRACEWELL_HOST=http://localhost:8081`, `TRACEWELL_API_KEY` |

DSNs:
- ClickHouse: `clickhouse://tracewell:tracewell@clickhouse:9000/tracewell`
- Postgres: `postgres://tracewell:tracewell@postgres:5432/tracewell?sslmode=disable`

Docker: each app track owns a `Dockerfile` in its own folder (`services/ingest/Dockerfile`,
`workers/evals/Dockerfile`, `web/Dockerfile`, ...). The **build context is the repo root**, because Go
services and the eval workers need `proto/gen`. Track B wires them into `docker-compose.yml` with
`build: { context: ., dockerfile: services/ingest/Dockerfile }`.
