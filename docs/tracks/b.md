# Track B: Data platform & infra

Folders: `deploy/`, `docker-compose.yml`, `loadtest/`, `.github/`, ClickHouse/Postgres migrations
(`deploy/migrations/`)
DS concepts to demo: replication and sharding, fault tolerance and scaling.

Round 1 (compose, migrations, kafka-init, CI smoke test) is done, merged in PR #8. Its brief is in git
history.

## Round 2: the real cluster

Branch: `b/round-2`

Run the whole system on a local Kubernetes cluster with replicated Kafka and a sharded, replicated
ClickHouse. Keep compose working for everyday dev.

### Build

1. **Compose updates** (`docker-compose.yml`, `deploy/migrations/`)
   - `deploy/migrations/postgres/002_round2.sql`: copy of `docs/contracts/postgres_round2.sql`. Make it
     run on existing volumes too (a one-shot `migrate` job, or psql in an init container). Round 1's
     "only on an empty volume" rule would skip it.
   - Add `gateway` (`envoyproxy/envoy`, mounts `gateway/envoy.yaml`, port 8080, 9901) to the `apps`
     profile, plus Redis wiring for ingest and query.
   - Add a `light` way to run just infra plus one service, and document it in
     `docs/services/infra.md`. Laptops struggled with the full stack in round 1.

2. **kind cluster** (`deploy/kind/`)
   - A script that creates a 3-worker kind cluster, installs the operators, and deploys everything.
     Port-map the gateway to `localhost:8080` and Grafana to `localhost:3300`.
   - Document the RAM it needs. If it won't fit on a laptop, say so, and keep compose as the daily path.

3. **Strimzi Kafka** (`deploy/`)
   - 3 brokers in KRaft mode, the three topics as `KafkaTopic` resources: 6 partitions, RF 3,
     `min.insync.replicas=2`.
   - Kafka exporter for consumer-group lag.

4. **ClickHouse 2x2** (`deploy/`)
   - Altinity operator (or plain StatefulSets if the operator fights you): cluster `tracewell`,
     2 shards x 2 replicas, ClickHouse Keeper with 3 nodes.
   - `deploy/migrations/clickhouse-cluster/`: copy of `docs/contracts/clickhouse_cluster.sql`.
   - Prove it: insert through the Distributed table, then show rows on both shards
     (`system.parts`) and the same rows on both replicas of a shard.

5. **Helm charts** (`deploy/helm/`)
   - One chart per app (or one umbrella chart): Deployment, Service, env from the table in
     `docs/contracts/README.md`, liveness `/healthz`, readiness `/readyz`, `terminationGracePeriodSeconds: 30`.
   - Postgres and Redis as single instances (Bitnami charts are fine).
   - Images: build locally and `kind load docker-image`. No registry yet.

6. **Prometheus and Grafana** (`deploy/`)
   - kube-prometheus-stack, or plain Prometheus scraping every service's `/metrics`.
   - One Grafana dashboard checked in as JSON: ingest rate, writer rows/s, consumer lag per group,
     p95 request latency per service, eval scores/s, alerts fired.
   - Run the same Prometheus and Grafana in compose too, under a `monitoring` profile.

7. **CI**: build all images, then run `deploy/scripts/smoke.sh` against compose. Make the evals lint
   step blocking once Track C fixes it.

### Contracts used
`docs/contracts/README.md` (ports, env vars, gateway, metrics, Kafka settings),
`docs/contracts/postgres_round2.sql`, `docs/contracts/clickhouse_cluster.sql`.

### Done when
- `docker compose --profile apps up` still passes the round 2 integration steps.
- On kind, `chatbot.py --fake --count 20` through `localhost:8080` lands 20 rows, and the dashboard
  shows them.
- `kubectl delete pod` on one Kafka broker and on one ClickHouse replica: traffic keeps flowing, and
  the row count still matches. That's the first look at the round 3 chaos demo.
- Grafana shows the dashboard with live data.
- `docs/services/infra.md` covers kind, Helm, how to reach Grafana, and how much RAM it all takes.

### Deferred
HPA, chaos tests, k6 at 5k spans/s (round 3). EKS (round 4).
