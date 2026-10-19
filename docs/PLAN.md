# Tracewell — LLM Observability Platform: Project Plan (CMPE 273)

## Context
Team: Eugene Lacatis, Rosnita Dey, Robbie Tambunting, Lucas Nguyen. The repo has only a README stub.
Goal: build an enterprise-style distributed LLM observability tool (like Langfuse/Helicone) that shows
four course concepts: message queues/streaming, microservices behind a gateway, replication/sharding,
and fault tolerance/scaling. MVP covers trace ingestion via SDK, a dashboard with search, async evals,
and alerting. Timeline: Oct 6 to Dec 5 (~9 weeks; change it once the syllabus dates are known).

## Stack decision (picked for learning value)
- **Go** for the hot-path services: ingest, writer, query and alerter. You learn concurrency, gRPC and
  backpressure.
- **Python** for the eval workers, because the LLM-as-judge and NLP tooling lives there.
- **TypeScript**: Next.js dashboard and the TS SDK.
- **Kafka** (Strimzi operator on k8s) as the event backbone. Use partitioned topics keyed by `project_id`.
- **ClickHouse**, 2 shards x 2 replicas (ReplicatedMergeTree + Distributed table), for traces and
  analytics. This is where you show sharding and replication.
- **Postgres** for metadata: orgs, projects, API keys, alert rules. **Redis** for rate limits and caching.
- **Kubernetes**: run `kind` locally, deploy to **EKS** for the final demo (Helm charts). Self-hosting
  Kafka and ClickHouse on k8s teaches more than managed Kinesis/Dynamo, because AWS-managed services hide
  the distributed mechanics you're graded on. The EKS deploy still gives you the AWS experience.
  Cost note: EKS is about $0.10/hr for the control plane plus nodes. Run it only during demo weeks.
- **Protobuf** contracts shared by every service. **Prometheus + Grafana + OpenTelemetry** to observe
  the observability platform itself.

## Architecture
```
SDK (py/ts) --> [API Gateway: Envoy/Kong] --> ingest-svc (Go) --> Kafka: traces.raw
                                                                   |-> writer-svc (Go) --> ClickHouse (sharded/replicated)
                                                                   |-> eval-workers (Py) --> Kafka: evals.results --> writer
                                                                   |-> alerter (Go, windowed aggs) --> Slack/webhook
Dashboard (Next.js) --> gateway --> query-svc (Go) --> ClickHouse + Postgres + Redis cache
Failed messages --> traces.dlq (retry/replay tool)
```

## Monorepo layout
`proto/`, `services/{ingest,writer,query,alerter}` (Go), `workers/evals` (Python), `sdk/{python,ts}`,
`web/` (Next.js), `deploy/{helm,kind}`, `docker-compose.yml` (fast local dev), `loadtest/` (k6),
`docs/` (design doc, ADRs, report).

## 4-way split
Each track owns one vertical slice end to end, plus one distributed-systems concept it must demo.

| Track | Owns | DS concept it demos |
|---|---|---|
| **A: Write path** | Python + TS SDKs (batching, async flush, retry with backoff), Go ingest-svc (gRPC + HTTP, API-key auth, Redis rate limit, idempotency keys), Kafka producer, Go writer-svc (batch inserts, exactly-once-ish dedupe), DLQ + replay CLI | Message queue/streaming, idempotency |
| **B: Data platform & infra** | kind + EKS clusters, Helm charts, Strimzi Kafka (3 brokers, RF=3), ClickHouse 2x2 cluster + schema/migrations, Postgres, GitHub Actions CI/CD, Prometheus/Grafana, chaos tests (kill broker/replica), k6 load tests, HPA | Replication/sharding, fault tolerance + scaling |
| **C: Evals & alerting** | Python eval workers (consumer group, LLM-as-judge, toxicity/PII/latency scorers, horizontal scaling by partition), Go alerter (sliding-window cost/error/p95 rules from Postgres, dedupe and cooldown, Slack/webhook) | Consumer groups, stream processing |
| **D: Read path & product** | API gateway config (Envoy/Kong: routing, auth, rate limits), Go query-svc (filtering, pagination, aggregations, Redis cache), Next.js dashboard (trace list/detail, session view, cost/latency charts, eval scores, alert-rule CRUD), auth/multi-tenancy (orgs/projects) | Microservices + gateway, caching |

Suggested owners: **Eugene → D** (Next.js background; as PM, D also gives a full view of the system).
Rosnita, Robbie and Lucas pick from A/B/C based on interest. B suits whoever wants infra/DevOps. C suits
whoever wants ML/Python. A suits whoever wants Go/systems.

**Shared contracts (all 4, week 1):** `proto/trace.proto` (Trace/Span/Generation: model, prompt,
completion, tokens, cost, latency, metadata), Kafka topic names and keys, ClickHouse table schema,
query API (OpenAPI). Nobody codes against another track until these are merged.

## Milestones
1. **Wk 1 (Oct 6–12): Design.** Design doc + ADRs (Kafka vs alternatives, ClickHouse vs Postgres for
   traces), proto contracts, repo skeleton, docker-compose with Kafka/ClickHouse/Postgres/Redis.
2. **Wk 2–3 (to Oct 26): Walking skeleton.** SDK → ingest → Kafka → writer → ClickHouse (single node)
   → query → dashboard trace list. One real LLM call shows up in the UI. *This is the most important
   milestone.*
3. **Wk 4–5 (to Nov 9): Features + k8s.** Evals and alerting live. Auth/multi-tenancy. Everything runs on
   kind via Helm. ClickHouse switches to 2x2, Kafka to 3 brokers.
4. **Wk 6–7 (to Nov 23): Resilience + scale.** DLQ/replay, retries, HPA on ingest/eval workers, chaos
   demos, k6 load test (target: sustain ~5k spans/s locally, p99 ingest < 100ms).
5. **Wk 8 (to Nov 30): EKS + polish.** Deploy to EKS, Grafana dashboards, report draft.
6. **Wk 9 (to Dec 5): Buffer + demo rehearsal.**

## Team process
- GitHub Projects board with one swimlane per track. Each PR needs 1 review from another track.
- Two short syncs per week (async standup in Discord/Slack on the other days).
- `main` protected; CI runs lint + unit tests + `docker compose up` smoke test.

## Verification (final demo script)
1. `examples/chatbot.py` makes OpenAI/Claude calls through the SDK. Traces appear in the dashboard
   within ~2s.
2. Eval scores attach to traces asynchronously. An injected cost spike fires a Slack alert.
3. k6 load test in Grafana: throughput and lag, with HPA scaling ingest pods.
4. Chaos: kill a Kafka broker and a ClickHouse replica mid-load. No data loss (row counts match the sent
   count), and queries keep serving.
5. Show shard distribution (`system.parts` per shard) and consumer-group partition assignment.

## Differentiation: provable completeness
Hosted tools like Respan, LangSmith and Helicone already cover gateways, tracing, evals and alerts, so
Tracewell won't compete on feature count. Its angle is showing that no trace was lost. Each SDK process
picks a random `sdk_instance_id` and numbers its batches with `batch_seq` (1, 2, 3...). The writer
tracks the highest contiguous `batch_seq` per instance and records any gaps. A dashboard page can then
show sent vs. stored, open gaps, the DLQ count and per-stage lag, which turns the chaos demo (step 4
above) into something visible in the product.

Status: the two fields are in `proto/trace.proto` now so nobody codes against `IngestBatch` without
them. The SDKs should fill them in from the start (Track A). The writer gap tracking (Track A) and the
integrity page (Track D) wait until after the walking skeleton. Decide around Nov 9 whether to build
them.

## On approval
Write this plan to `docs/PLAN.md`, update `README.md` with the project summary and team tracks, and
scaffold the monorepo directories + `proto/trace.proto` draft + `docker-compose.yml`.
