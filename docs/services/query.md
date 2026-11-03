# query-svc

Track D. The read API for the dashboard. It reads traces, spans and evals from ClickHouse and
resolves API keys against Postgres. Endpoints and response shapes are in `docs/contracts/http.md`.

## Endpoints

| Path | Notes |
|---|---|
| `GET /v1/traces` | `limit` (default 50, clamped to 1..200), `cursor`, `from`, `to`. Newest first. |
| `GET /v1/traces/{traceId}` | Trace, spans ordered by `startTime`, evals ordered by span then evaluator. `404 not_found` if the trace isn't in the key's project. |
| `GET /healthz` | `200 ok` while the process is up. |
| `GET /readyz` | `200` when ClickHouse and Postgres both answer within 2s, else `503`. |
| `GET /metrics` | Prometheus. `query_http_request_duration_seconds{route,status}` plus Go runtime metrics. |

Behavior the contract leaves open:

- `from` is inclusive and `to` is exclusive (`from <= startTime < to`).
- A bad `limit` (not an integer), `cursor` or `from`/`to` returns `400 invalid_argument`. A `limit` out
  of range is clamped, not rejected.
- If Postgres is down during a key lookup, the response is `503 unavailable`. A ClickHouse failure is
  `500 internal`.
- A valid key is cached in memory for 60s, so a revoked key keeps working for up to a minute. Unknown
  keys are not cached.

## How it works

Auth: the middleware takes the `Bearer` key, hashes it with sha256, and looks the hash up in
`api_keys`. The `project_id` on that row scopes every query. Nothing in the request can choose the
project.

Paging: the cursor is base64url JSON `{"t": <startTime>, "id": <traceId>}` of the last row on the
page. The next page asks for rows strictly older than that pair, which is keyset pagination: new
traces landing at the top don't shift the pages the way an `OFFSET` would. The service fetches
`limit + 1` rows, and the extra row tells it whether to return a `nextCursor`.

ClickHouse: every read uses `FINAL`, so rows that Kafka redelivered and the writer inserted twice
show up once. All values are bound parameters. Times are bound as microsecond integers through
`fromUnixTimestamp64Micro(?)`, because clickhouse-go rounds a `time.Time` bound to `?` down to whole
seconds and that broke paging inside a single second.

## Env vars

| Var | Default |
|---|---|
| `QUERY_HTTP_ADDR` | `:8082` |
| `QUERY_CLICKHOUSE_DSN` | `clickhouse://tracewell:tracewell@localhost:9000/tracewell` |
| `QUERY_POSTGRES_DSN` | `postgres://tracewell:tracewell@localhost:5432/tracewell?sslmode=disable` |

The defaults point at the compose ports on localhost. Inside compose, set the DSNs from
`docs/contracts/README.md` (host `clickhouse` / `postgres`).

## Run locally

```
docker compose up -d clickhouse postgres
cd services/query && go run ./cmd/query
curl -H 'Authorization: Bearer tw_dev_localkey' localhost:8082/v1/traces
```

Until the writer is running, load demo rows for `proj_dev` from the repo root:

```
docker compose exec -T clickhouse clickhouse-client -u tracewell --password tracewell \
  --multiquery < services/query/testdata/dev_seed.sql
```

This needs the ClickHouse tables and the Postgres `api_keys` seed to exist. Track B's migrations
create them. Before those land, you can apply `docs/contracts/clickhouse.sql` and
`docs/contracts/postgres.sql` to your local containers by hand.

Docker, from the repo root:

```
docker build -f services/query/Dockerfile -t tracewell/query .
```

The runtime image is Alpine, so a compose healthcheck can run `wget -qO- localhost:8082/readyz`.

## Test

```
cd services/query
go test ./...                                  # unit tests, no dependencies
go test -tags integration ./internal/store/    # needs compose ClickHouse; skips if it's down
```

The unit tests cover cursor round trips, limit clamping, enum and `generation: null` mapping, the
API key hash and cache TTL, 401 and 404 responses, and project scoping. The integration test applies
`docs/contracts/clickhouse.sql`, inserts every row twice under a fresh `project_id`, and checks paging
order, the sub-second cursor, the `from` filter, `FINAL` dedupe and the detail query.
