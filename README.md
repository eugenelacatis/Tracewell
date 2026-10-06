# rescan.ai

Distributed LLM observability platform for CMPE 273. Apps send LLM traces through our SDK. We stream
them through Kafka into a sharded and replicated ClickHouse cluster, score them asynchronously with
eval workers, alert on cost/error/latency spikes, and show everything in a dashboard.

Team: Eugene Lacatis, Rosnita Dey, Robbie Tambunting, Lucas Nguyen

Full plan: [docs/PLAN.md](docs/PLAN.md)

## Tracks

| Track | Scope | Owner |
|---|---|---|
| A: Write path | SDKs, ingest-svc, writer-svc, DLQ | TBD |
| B: Data platform & infra | k8s/EKS, Kafka, ClickHouse cluster, CI, chaos + load tests | TBD |
| C: Evals & alerting | Python eval workers, Go alerter | TBD |
| D: Read path & product | API gateway, query-svc, Next.js dashboard, auth | Eugene (proposed) |

## Layout

```
proto/            shared protobuf contracts
services/         Go services: ingest, writer, query, alerter
workers/evals/    Python eval workers
sdk/              Python + TS client SDKs
web/              Next.js dashboard
deploy/           Helm charts, kind config
loadtest/         k6 scripts
docs/             plan, design doc, ADRs
```

## Local dev

```
docker compose up -d   # Kafka, ClickHouse, Postgres, Redis
```
