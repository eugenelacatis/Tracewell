# Local infrastructure (docker compose, kind and Helm)

Track B. There are two ways to run Tracewell on one machine:

| | Compose (`docker-compose.yml`) | kind (`deploy/kind/`) |
|---|---|---|
| Use it for | Everyday development | Showing replication, sharding and fault tolerance |
| Kafka | 1 broker, replication factor 1 | 3 brokers (Strimzi, KRaft), RF 3, `min.insync.replicas=2` |
| ClickHouse | 1 node | 2 shards x 2 replicas + 3 Keeper nodes |
| Memory | a few GB | much more (see [kind memory](#how-much-memory-kind-needs)) |
| Start | `docker compose up -d` | `bash deploy/kind/up.sh` |

Compose is the daily path. Services behave the same on both, because only the addresses change.

## Start, stop, reset

```
docker compose up -d                          # infra only: Kafka, ClickHouse, Postgres, Redis + kafka-init
docker compose --profile apps up -d --build   # infra + ingest, writer, query, alerter, evals, web, gateway
docker compose --profile apps --profile monitoring up -d   # the same, plus Prometheus, Grafana, Kafka exporter
docker compose ps -a                          # status; kafka-init shows "Exited (0)", which is correct
docker compose logs -f writer                 # follow one service's logs
docker compose down                           # stop, keep data
docker compose down -v                        # stop and DELETE all data (the reset button)
```

The apps are in a compose *profile* called `apps`, so they stay off unless you ask for them. To make
every command include them, run `export COMPOSE_PROFILES=apps` once in your shell.

Rebuild after changing one service: `docker compose --profile apps up -d --build writer`.

### Light mode: infra plus one service

The full stack is heavy on a laptop. To work on one service, start only that service. Compose starts
just what it depends on:

| You're working on | Command | What starts besides it |
|---|---|---|
| query | `docker compose --profile apps up -d query` | ClickHouse, Postgres (+ migrate), Redis |
| ingest | `docker compose --profile apps up -d ingest` | Kafka (+ topics), Postgres (+ migrate), Redis |
| writer | `docker compose --profile apps up -d writer` | Kafka (+ topics), ClickHouse |
| alerter | `docker compose --profile apps up -d alerter` | Kafka (+ topics), Postgres (+ migrate) |
| evals | `docker compose --profile apps up -d evals` | Kafka (+ topics) |

Everything else stays off, so nothing else uses memory. Add `--build` after a code change. Kafka's heap is
capped at 512 MB in compose. The gateway and `web` aren't pulled in by these, so you reach a service on its
own port (8081 to 8085).

**Startup order is handled for you.** An app starts only after the databases are healthy (their tables
exist) and `kafka-init` has finished (the topics exist). The first `--build` takes a few minutes. Later
ones use the cache.

## What `up` sets up

| Piece | What happens | Where it's defined |
|---|---|---|
| ClickHouse tables | `traces`, `spans`, `evals`, `ingest_batches` | `deploy/migrations/clickhouse/001_init.sql` |
| Postgres tables + dev seed | `orgs`, `projects`, `api_keys`, `alert_rules`, `alert_events`, plus org `org_dev`, project `proj_dev`, API key `tw_dev_localkey` and one alert rule | `deploy/migrations/postgres/001_init.sql` |
| Postgres round 2 | `users`, `memberships`, `alert_rules.deleted_at`, a second org and project (`proj_demo`, key `tw_demo_otherkey`), dev login `dev@tracewell.local` / `tracewell` | `deploy/migrations/postgres/002_round2.sql`, applied by the `migrate` job |
| Kafka topics | `traces.raw`, `evals.results`, `traces.dlq`: 6 partitions, replication factor 1 | `kafka-init` in `docker-compose.yml` |

Every migration file is an exact copy of a contract in `docs/contracts/` (CI checks this). Change the
schema in the contract first (a PR every track reviews), then copy it.

### Postgres migrations run on every `up`

The Postgres image runs the files in `/docker-entrypoint-initdb.d` only the first time it starts with no
data, so a volume created in round 1 would never get `002_round2.sql`. The one-shot `migrate` service fixes
that: after Postgres is healthy it applies every file in `deploy/migrations/postgres/`, in order, on each
`up`. Every file is safe to run twice (`IF NOT EXISTS`, `ON CONFLICT`), and ingest, query and alerter wait
for it to finish. You never need `down -v` just to get a new Postgres migration. To run it by hand:

```
docker compose run --rm migrate
```

### ClickHouse migrations only run on an empty volume

ClickHouse still uses the init-directory rule, so `001_init.sql` runs only on an empty volume. Something
half-created, or a new ClickHouse schema? Run `docker compose down -v`, then `up` again. To re-apply
without wiping data (the file is safe to run twice):

```
docker compose exec -T clickhouse clickhouse-client -u tracewell --password tracewell --multiquery \
  < deploy/migrations/clickhouse/001_init.sql
```

`IF NOT EXISTS` doesn't change a table that already exists. A new column on an existing table needs a
numbered migration written for that purpose.

## Ports

| Port | What | Port | What |
|---|---|---|---|
| 8080 | gateway (Envoy): the front door for SDKs | 9092 | Kafka (from your machine) |
| 9901 | gateway admin (`/ready`, `/stats/prometheus`) | 8123 | ClickHouse HTTP |
| 8081 | ingest HTTP | 9000 | ClickHouse native |
| 9091 | ingest gRPC | 5432 | Postgres |
| 8082 | query | 6379 | Redis |
| 8083 | alerter | 9090 | Prometheus (`monitoring` profile) |
| 8084 | writer (health/metrics) | 3300 | Grafana (`monitoring` profile, also kind) |
| 8085 | evals (health/metrics) | 3000 | web |

SDKs should send to the gateway on 8080. Hitting a service's own port still works for debugging.

Local credentials everywhere are `tracewell` / `tracewell`. They are for local dev only.

## Shells and one-off queries

```
# ClickHouse: interactive shell, then a one-off query
docker compose exec clickhouse clickhouse-client -u tracewell --password tracewell
docker compose exec -T clickhouse clickhouse-client -u tracewell --password tracewell \
  -q "SELECT count() FROM tracewell.traces FINAL"
curl -s 'localhost:8123/?user=tracewell&password=tracewell' --data "SELECT count() FROM tracewell.traces FINAL"

# Postgres
docker compose exec postgres psql -U tracewell -d tracewell          # then \dt to list tables
docker compose exec -T postgres psql -U tracewell -d tracewell -c "SELECT * FROM alert_events"

# Redis
docker compose exec redis redis-cli
```

Always read ClickHouse tables with `FINAL`. Every table dedupes by merging rows in the background, so
without `FINAL` a redelivered row can show up twice for a while.

## Kafka: topics, consumer groups, lag

Run these inside the `kafka` container (it has the scripts). From there the broker is `localhost:9092`.

```
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic traces.raw

# Who is reading what, and how far behind
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

**Reading the consumer-group output.** One row per group and partition. `LOG-END-OFFSET` is how many
messages the partition holds. `CURRENT-OFFSET` is how far the group has committed. `LAG` is the
difference, so lag 0 means that group is caught up. A `-` in the offset and lag columns means the group
hasn't committed anything yet, which is normal before the first message arrives. You should see three
groups: `writer`, `evals` and `alerter`.

### Message size limits

Kafka's default maximum record size is about 1 MB, but ingest accepts bodies up to 5 MB. So
`kafka-init` raises it per topic: `traces.raw` allows 6 MiB (`max.message.bytes=6291456`, the limit of
ingest's producer) and `traces.dlq` allows 8 MiB (`8388608`), because a dead letter carries the original
payload plus error details. `evals.results` keeps the default. This is applied only when a topic is
created. To change it on an existing topic, delete the topic or run `kafka-configs.sh --alter`.

### Two Kafka addresses

Containers reach the broker at `kafka:29092`. Your machine uses `localhost:9092`. They are the same
broker, but a client reconnects to whatever address the broker advertises, and `localhost` inside a
container means that container. If a service on your machine can't connect, check you're using
`localhost:9092`. If a container can't, check `kafka:29092`.

## Gateway

Envoy runs as `gateway` in the `apps` profile: one listener on 8080 for HTTP and gRPC, admin on 9901. Its
routes are in `gateway/envoy.yaml`, which Track D owns. Until that file exists on your branch, start the
rest without it: `docker compose --profile apps up -d --scale gateway=0`. (Without the file, Docker creates
an empty folder with that name and the gateway container fails.)

## Monitoring (compose)

```
docker compose --profile apps --profile monitoring up -d
```

- **Grafana**: http://localhost:3300, login `admin` / `tracewell`. The "Tracewell overview" dashboard is
  already loaded (it refreshes every 10 s).
- **Prometheus**: http://localhost:9090. Status, Targets shows which services it can reach.
- **Kafka exporter** reads consumer-group offsets and exposes `kafka_consumergroup_lag`.

Everything comes from `deploy/monitoring/`: `prometheus.yml`, the Grafana provisioning files and
`dashboards/tracewell.json`. The same files are used on kind. To change the dashboard, edit it in Grafana,
export the JSON, and replace `tracewell.json` (provisioned dashboards can't be saved from the UI).

Panels: ingest rate, writer rows/s, consumer lag per group, p95 latency per service, eval scores/s, alerts
fired, query cache hits and misses, rate-limited requests. **Some panels are empty until the other tracks
emit the metrics `docs/contracts/README.md` promises** (for example `evals_scored_total`,
`ingest_spans_accepted_total`, `query_cache_requests_total`, `ingest_rate_limited_total`).

## Kubernetes on kind (the real cluster)

`deploy/kind/up.sh` builds a 4-node kind cluster (1 control plane, 3 workers) and deploys everything:

| Piece | How | Where |
|---|---|---|
| Kafka | Strimzi operator, 3 brokers in KRaft mode, topics as `KafkaTopic` resources (6 partitions, RF 3, `min.insync.replicas=2`), Kafka exporter | `deploy/k8s/kafka/` |
| ClickHouse | Altinity operator, cluster `tracewell`: 2 shards x 2 replicas, 3 Keeper nodes | `deploy/k8s/clickhouse/` |
| ClickHouse schema | replicated local tables + Distributed tables | `deploy/migrations/clickhouse-cluster/001_cluster.sql` |
| Apps, Postgres, Redis | one Helm chart | `deploy/helm/tracewell/` |
| Prometheus, Grafana | plain Deployments, same config as compose | `deploy/k8s/monitoring/` |

You need `docker`, `kind`, `kubectl` and `helm` (`brew install kind helm`), and Docker Desktop running.

```
bash deploy/kind/up.sh        # create everything (re-run any time; every step is idempotent)
bash deploy/kind/down.sh      # delete the cluster and ALL its data
kubectl --context kind-tracewell -n tracewell get pods
```

Everything lives in the `tracewell` namespace. Open `localhost:8080` for the gateway and
`localhost:3300` for Grafana (`admin` / `tracewell`). If `gateway/envoy.yaml` doesn't exist yet, `up.sh`
warns and deploys without the gateway, so nothing answers on 8080 until you re-run it.

The app images are built locally and loaded into kind (`bash deploy/scripts/build-images.sh --kind`), because
there is no registry yet. After a code change, rebuild with that script, then
`kubectl -n tracewell rollout restart deployment/<service>`.

### How much memory kind needs

**Estimate, not measured yet** (replace with the real number from a first run, using `docker stats` and
`kubectl top pods`): the pods *request* roughly 5.5 GB (Kafka 1.5, ClickHouse 2, Keeper 0.4, operators 0.4,
apps 0.6, Postgres/Redis/Prometheus/Grafana 0.4) and can burst higher, up to their limits. With kind's own
overhead, expect 8 to 10 GB used. Give Docker Desktop at least 10 GB. On a 16 GB laptop that leaves little
for anything else. If the cluster doesn't fit on your machine, that's expected: keep using compose day to
day and use kind for the replication and failure demos.

### Helm chart

`deploy/helm/tracewell/` is one chart. `values.yaml` has a `services:` map, and one template turns each
entry into a Deployment and a Service. Each gets its env from the table in `docs/contracts/README.md`,
`/healthz` liveness, `/readyz` readiness and `terminationGracePeriodSeconds: 30`. Postgres (a StatefulSet
with a volume) and Redis (no persistence) are in the chart too, using the same images as compose, and a Helm
hook runs the Postgres migrations after each install or upgrade. Check it without a cluster:

```
helm lint deploy/helm/tracewell
helm template tracewell deploy/helm/tracewell | kubeconform -strict -summary
```

Two probes are weaker than they should be until other tracks fix their side: `evals` uses `/healthz` for
readiness because its `/readyz` hangs (Track C), and `web` uses a TCP check because it has no `/healthz`
(Track D).

### Prove it works

```
bash deploy/scripts/cluster-proof.sh
```

Inserts rows through the Distributed table, then prints, per ClickHouse pod, how many rows it holds and its
`system.parts`. It checks that both shards have rows and that the two replicas of each shard match, then
removes its rows. To look yourself:

```
kubectl -n tracewell exec chi-tracewell-tracewell-0-0-0 -- clickhouse-client -u tracewell --password tracewell \
  -q "SELECT _shard_num, hostName(), count() FROM clusterAllReplicas('tracewell', tracewell.traces_local) GROUP BY 1, 2 ORDER BY 1, 2"
```

### Failure demo

Send traffic and delete pods while it runs (INTEGRATION steps 11 to 13):

```
# terminal 1: 200 traces through the gateway
cd sdk/python && TRACEWELL_HOST=http://localhost:8080 TRACEWELL_API_KEY=tw_dev_localkey \
  uv run python ../../examples/chatbot.py --fake --count 200
# terminal 2: kill one Kafka broker and one ClickHouse replica
kubectl -n tracewell delete pod tracewell-dual-role-1
kubectl -n tracewell delete pod chi-tracewell-tracewell-0-1-0
```

Traffic should keep flowing. When it finishes, the trace count in ClickHouse should still be exactly 200.
Watch the "Consumer lag" panel in Grafana climb and then drain.

## Health endpoints

Every app serves `/healthz` (is the process alive?), `/readyz` (are its dependencies reachable?) and
`/metrics` (Prometheus). Compose healthchecks use `/healthz`, so "healthy" in `docker compose ps`
means the process is up, not that Kafka and the databases are working. To check dependencies:

```
for p in 8081 8082 8083 8084 8085; do curl -fs -m 5 localhost:$p/readyz && echo " $p ok"; done
```

## Checks that run automatically

- **Smoke test:** `bash deploy/scripts/smoke.sh` builds the whole stack, waits for every `/healthz`
  (and the gateway's `/ready` once `gateway/envoy.yaml` exists), checks the round 2 Postgres migration ran,
  and tears it down. It uses its own compose project, so it won't touch your dev data, but it needs the
  host ports free: run `docker compose down` first. `SMOKE_KEEP=1` leaves the stack up afterwards.
  `SMOKE_TIMEOUT=300` waits longer.
- **CI** (`.github/workflows/ci.yml`, on PRs to `main`): `go vet` and `go test` per Go service, `ruff` and
  `pytest` for the Python projects, `npm run lint` and `npm run build` for web, `buf breaking` on
  `proto/`, an image build followed by the smoke test, and a `deploy-config` job that checks the Helm chart,
  the Kafka and ClickHouse manifests, the compose files, the Prometheus config and Grafana dashboard, and that
  each migration is still an exact copy of its contract. Jobs skip folders that don't exist yet.

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `port is already allocated` on `up` | Something on your machine already uses that port, often a local Postgres on 5432. Stop it, or stop the other compose stack. |
| Tables are missing or the schema looks old | The data volume predates the migration. `docker compose down -v`, then `up`. |
| An app never becomes healthy | `docker compose logs <service>`. Check that `docker compose ps -a` shows `kafka-init` as `Exited (0)`. |
| Container can't reach Kafka | It must use `kafka:29092`, not `localhost:9092`. |
| Consumer group shows lag that doesn't shrink | That consumer is down or stuck. `docker compose logs <service>`, then `docker compose restart <service>`. |
| Everything is slow or containers get killed | Give Docker Desktop more memory. ClickHouse and Kafka are the heavy ones. Try light mode (above). |
| `gateway` exits right away | `gateway/envoy.yaml` doesn't exist yet (Track D). Delete the empty `gateway/envoy.yaml` folder Docker created, and start with `--scale gateway=0`. |
| A Grafana panel is empty | That metric isn't emitted by the service yet, or no traffic has arrived. Check Prometheus, Status, Targets, then query the metric by name. |
| kind pods stuck `Pending` or `OOMKilled` | Docker Desktop memory is too low. Raise it, or `bash deploy/kind/down.sh` and use compose. |
| `up.sh` stops waiting on Kafka or ClickHouse | `kubectl -n tracewell get pods` and `describe` the pending one. First run downloads several GB of images, so just re-run it. |

## Known gaps

- `web` has no healthcheck, because it has no `/healthz` route yet (Track D). It starts after `query` is
  healthy. On kind it uses a TCP probe.
- `evals` `/readyz` (port 8085) hangs instead of answering. `/healthz` and `/metrics` are fine, so the
  compose healthcheck still works. On kind, readiness uses `/healthz` (Track C).
- `workers/evals` lint doesn't pass yet, so its `ruff` step in CI is non-blocking (Track C).
- `gateway/envoy.yaml` isn't merged yet (Track D), so the gateway runs only once it lands.
- Some dashboard panels stay empty until the services emit the contract's metric names (see Monitoring).
- The kind setup was written against the operators' documented behaviour. Operator versions are pinned in
  `deploy/kind/up.sh` (Strimzi 1.3.1, Altinity 0.27.4).
- Not built yet: autoscaling, chaos tests and the 5k spans/s load test (round 3), and EKS (round 4).
