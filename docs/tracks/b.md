# Track B: Data platform & infra

Folders: `deploy/`, `docker-compose.yml`, `loadtest/`, `.github/`, ClickHouse/Postgres migrations
(`deploy/migrations/`)
DS concepts to demo: replication and sharding, fault tolerance and scaling.

## Round 1: walking skeleton

Branch: `b/round-1-compose`

Make one command bring up the whole system, and make CI catch breakage.

### Build

1. **Migrations**
   - Copy `docs/contracts/clickhouse.sql` to `deploy/migrations/clickhouse/001_init.sql` and
     `docs/contracts/postgres.sql` to `deploy/migrations/postgres/001_init.sql` unchanged.
   - Apply them automatically on `docker compose up`. Mounting into each image's
     `/docker-entrypoint-initdb.d` is fine for round 1, but it only runs on an empty volume, so document
     `docker compose down -v` for resetting. Both files are safe to run twice.

2. **Kafka**
   - Add an internal listener so containers reach the broker at `kafka:29092` while the host keeps
     `localhost:9092`. Today the broker only advertises `localhost:9092`, which containers can't reach.
   - A one-shot `kafka-init` service that creates `traces.raw`, `evals.results` and `traces.dlq`
     (6 partitions, RF 1, `--if-not-exists`) and exits.

3. **App services in compose**
   - Add ingest, writer, query, alerter, evals and web under a compose profile `apps`, so
     `docker compose up -d` stays infra-only and `docker compose --profile apps up -d --build` runs
     everything.
   - Build from the repo root with each track's Dockerfile (`docs/contracts/README.md`). Use the env vars
     and ports from that same file. Healthchecks on `/healthz` and `depends_on` with
     `condition: service_healthy` / `service_completed_successfully` so apps start after infra and
     migrations.
   - The other tracks are writing their Dockerfiles at the same time. Until theirs land, test your wiring
     with a placeholder image, and don't add Dockerfiles in their folders.

4. **CI** (`.github/workflows/ci.yml`)
   - On PRs to `main`: for each Go module under `services/`, run `go vet` and `go test`. For
     `workers/evals` and `sdk/python`, run `uv run ruff check` and `uv run pytest`. For `web`, run
     `npm ci && npm run lint && npm run build`.
   - `buf breaking` on `proto/` against `main`. This enforces the never-reuse-a-field-number rule.
   - Skip a job when its folder has no code yet (path filters or a file-exists check), so CI is green
     from day one.
   - A compose smoke job that runs `docker compose --profile apps up -d --build` and waits for every
     `/healthz`. Mark it allowed-to-fail until all tracks have Dockerfiles, then make it required.

5. **Ops doc**: `docs/services/infra.md`, covering how to start, reset, open a ClickHouse or psql shell,
   and list consumer groups and lag (`kafka-consumer-groups.sh --describe --all-groups`).

### Contracts used
`docs/contracts/clickhouse.sql`, `docs/contracts/postgres.sql`, `docs/contracts/README.md` (topics,
ports, env vars, DSNs, Dockerfile layout).

### Done when
- `docker compose down -v && docker compose up -d` gives the 4 tables in ClickHouse, the 5 tables plus
  seed rows in Postgres, and the 3 topics in Kafka, with no manual steps.
- `docker compose --profile apps up -d --build` starts every app whose Dockerfile exists, and they show
  healthy.
- A throwaway PR shows CI running and passing.

### Deferred to later rounds
kind cluster, Helm charts, Strimzi Kafka with 3 brokers and RF 3, ClickHouse 2x2 with
ReplicatedMergeTree + Distributed, Prometheus/Grafana (round 2). HPA, chaos tests, k6 at 5k spans/s
(round 3). EKS (round 4). Starting round 2 research on kind + Strimzi early is a good use of spare time.
