# Local infrastructure (docker compose)

Track B. `docker-compose.yml` runs everything Tracewell needs on one machine: the four backing
services (Kafka, ClickHouse, Postgres, Redis) and, optionally, our six apps. The sharded and
replicated setup comes later, in `deploy/` (kind and Helm, round 2).

## Start, stop, reset

```
docker compose up -d                          # infra only: Kafka, ClickHouse, Postgres, Redis + kafka-init
docker compose --profile apps up -d --build   # infra + ingest, writer, query, alerter, evals, web
docker compose ps -a                          # status; kafka-init shows "Exited (0)", which is correct
docker compose logs -f writer                 # follow one service's logs
docker compose down                           # stop, keep data
docker compose down -v                        # stop and DELETE all data (the reset button)
```

The apps are in a compose *profile* called `apps`, so they stay off unless you ask for them. To make
every command include them, run `export COMPOSE_PROFILES=apps` once in your shell.

Rebuild after changing one service: `docker compose --profile apps up -d --build writer`.

**Startup order is handled for you.** An app starts only after the databases are healthy (their tables
exist) and `kafka-init` has finished (the topics exist). The first `--build` takes a few minutes. Later
ones use the cache.

## What `up` sets up

| Piece | What happens | Where it's defined |
|---|---|---|
| ClickHouse tables | `traces`, `spans`, `evals`, `ingest_batches` | `deploy/migrations/clickhouse/001_init.sql` |
| Postgres tables + dev seed | `orgs`, `projects`, `api_keys`, `alert_rules`, `alert_events`, plus org `org_dev`, project `proj_dev`, API key `tw_dev_localkey` and one alert rule | `deploy/migrations/postgres/001_init.sql` |
| Kafka topics | `traces.raw`, `evals.results`, `traces.dlq`: 6 partitions, replication factor 1 | `kafka-init` in `docker-compose.yml` |

The migration files are exact copies of `docs/contracts/clickhouse.sql` and `postgres.sql`. Change the
schema in the contract first (a PR every track reviews), then copy it.

### Migrations only run on an empty volume

ClickHouse and Postgres run the `.sql` files in `/docker-entrypoint-initdb.d` **the first time they
start with no data**. After that they are skipped. So:

- Pulled a schema change, or something is half-created? Run `docker compose down -v`, then `up` again.
- Want to re-apply without wiping data? Both files are safe to run twice (`IF NOT EXISTS`, `ON CONFLICT`):

```
docker compose exec -T clickhouse clickhouse-client -u tracewell --password tracewell --multiquery \
  < deploy/migrations/clickhouse/001_init.sql
docker compose exec -T postgres psql -U tracewell -d tracewell -v ON_ERROR_STOP=1 -q \
  < deploy/migrations/postgres/001_init.sql
```

This doesn't change a table that already exists. A new column on an existing table needs a numbered
migration (`002_...`) written for that purpose. Round 1 has none.

## Ports

| Port | What | Port | What |
|---|---|---|---|
| 8081 | ingest HTTP | 9092 | Kafka (from your machine) |
| 8082 | query | 8123 | ClickHouse HTTP |
| 8083 | alerter | 9000 | ClickHouse native |
| 8084 | writer (health/metrics) | 5432 | Postgres |
| 8085 | evals (health/metrics) | 6379 | Redis |
| 3000 | web | | |

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

## Health endpoints

Every app serves `/healthz` (is the process alive?), `/readyz` (are its dependencies reachable?) and
`/metrics` (Prometheus). Compose healthchecks use `/healthz`, so "healthy" in `docker compose ps`
means the process is up, not that Kafka and the databases are working. To check dependencies:

```
for p in 8081 8082 8083 8084 8085; do curl -fs -m 5 localhost:$p/readyz && echo " $p ok"; done
```

## Checks that run automatically

- **Smoke test:** `bash deploy/scripts/smoke.sh` builds the whole stack, waits for every `/healthz`
  and tears it down. It uses its own compose project, so it won't touch your dev data, but it needs the
  host ports free: run `docker compose down` first. `SMOKE_KEEP=1` leaves the stack up afterwards.
  `SMOKE_TIMEOUT=300` waits longer.
- **CI** (`.github/workflows/ci.yml`, on PRs to `main`): `go vet` and `go test` per Go service, `ruff` and
  `pytest` for the Python projects, `npm run lint` and `npm run build` for web, `buf breaking` on
  `proto/`, and the smoke test. Jobs skip folders that don't exist yet.

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `port is already allocated` on `up` | Something on your machine already uses that port, often a local Postgres on 5432. Stop it, or stop the other compose stack. |
| Tables are missing or the schema looks old | The data volume predates the migration. `docker compose down -v`, then `up`. |
| An app never becomes healthy | `docker compose logs <service>`. Check that `docker compose ps -a` shows `kafka-init` as `Exited (0)`. |
| Container can't reach Kafka | It must use `kafka:29092`, not `localhost:9092`. |
| Consumer group shows lag that doesn't shrink | That consumer is down or stuck. `docker compose logs <service>`, then `docker compose restart <service>`. |
| Everything is slow or containers get killed | Give Docker Desktop more memory. ClickHouse and Kafka are the heavy ones. |

## Known gaps (round 1)

- `web` has no healthcheck, because it has no `/healthz` route yet (Track D). It starts after `query` is
  healthy.
- `evals` `/readyz` (port 8085) hangs instead of answering. `/healthz` and `/metrics` are fine, so the
  compose healthcheck still works (Track C).
- `workers/evals` lint doesn't pass yet, so its `ruff` step in CI is non-blocking (Track C).
- Not here yet: a multi-broker Kafka (Strimzi), the 2x2 ClickHouse cluster, Prometheus and Grafana,
  kind and Helm. Those are round 2.
