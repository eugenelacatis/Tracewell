# ingest-svc

Track A. The front door for trace data. It authenticates the SDK's API key, applies the project's
rate limit, stamps the batch with the key's `project_id`, and writes it to the Kafka topic
`traces.raw`. It does this over HTTP (port 8081) and gRPC (port 9091). Request and response shapes are
in `docs/contracts/http.md`.

## Endpoints

| Path | Notes |
|---|---|
| `POST /v1/ingest` | One `IngestBatch`, `application/json` (protojson) or `application/x-protobuf`. `202 {"accepted": <traces>}` once Kafka has it. |
| gRPC `tracewell.v1.IngestService/Ingest` (:9091) | Same batch, same rules, same Kafka write. Key in metadata `authorization: Bearer <key>`. Returns `IngestResponse{accepted}`. |
| `GET /healthz` | `200 ok` while the process is up. |
| `GET /readyz` | `200` when Kafka and Postgres both answer within 2s, else `503`. Redis isn't checked: the rate limit fails open, so a Redis outage mustn't take ingest out of rotation. |
| `GET /metrics` | Prometheus. `ingest_http_request_duration_seconds{route,status}`, `ingest_traces_accepted_total`, `ingest_spans_accepted_total`, `ingest_rate_limited_total{project_id}`, `ingest_rate_limit_errors_total`, `ingest_kafka_produce_duration_seconds`, Go runtime metrics. The counters cover both transports. |

Responses, in the order they're checked:

| HTTP | gRPC | When |
|---|---|---|
| `401 unauthorized` | `UNAUTHENTICATED` | No `Bearer` key, or the key is unknown or revoked. |
| `503 unavailable` | `UNAVAILABLE` | Postgres failed during the key lookup. |
| `429 rate_limited`, `Retry-After: 1` | `RESOURCE_EXHAUSTED`, trailer `retry-after: 1` | The project is over `INGEST_RATE_LIMIT_RPS` this second. |
| `413 payload_too_large` | `RESOURCE_EXHAUSTED`, no trailer | Over 5 MB. On gRPC, grpc-go refuses the message before auth runs. |
| `400 invalid_argument` | `INVALID_ARGUMENT` | Body didn't parse, `Content-Type` is neither of the two above, or `idempotencyKey` is empty. Unknown JSON fields are ignored, so a newer SDK isn't rejected. |
| `503 unavailable` | `UNAVAILABLE` | Kafka didn't ack within 10s. |
| `202` | `OK` | Kafka acked with `acks=all`. |

Behavior the contract leaves open:

- Over HTTP, the key and the rate limit are checked before the body is read, so an unauthenticated
  or rate-limited client can't make the server parse 5 MB. Over gRPC, grpc-go decodes the message
  before the handler runs, so only the 5 MB cap applies before auth.
- The rate limit counts requests (batches), not traces. Rejected requests count too, so a client that
  ignores `Retry-After` keeps itself limited.
- If Redis doesn't answer (200ms to connect, 100ms per command, no retries), the request goes through
  and `ingest_rate_limit_errors_total` goes up.
- `X-Request-Id` (HTTP) or `x-request-id` (gRPC metadata), which the gateway adds, is logged as
  `request_id`.
- `project_id` is overwritten on the batch and on every trace inside it.
- Nothing else in the batch is validated (trace or span ids, timestamps). The writer stores what it
  gets.
- Kafka's record size limit has to match the 5 MB body limit. The producer allows records up to
  6 MiB, and `kafka-init` sets `max.message.bytes` on `traces.raw` to 6 MiB.
- A valid key is cached in memory for 60s, so a revoked key keeps working for up to a minute. Unknown
  keys are not cached.

## How it works

The HTTP handler (`internal/api`) and the gRPC server (`internal/grpcapi`) only decode the request
and map errors to status codes. Auth, the rate limit, validation and the Kafka write are one shared
implementation (`internal/ingest`), so the two transports can't drift.

**Rate limit.** A fixed one-second window per project in Redis: each request runs `INCR` on
`rl:ingest:<project_id>:<unix second>` and `EXPIRE` 2s, in one `MULTI` round trip. Over the limit,
the request is rejected. Fixed windows can let up to twice the limit through across a second
boundary, which is fine for protecting Kafka. The gateway also has a coarse per-IP limit (Track D).

The record goes to `traces.raw` as binary protobuf, keyed by `project_id`. Kafka hashes the key to
pick a partition, so all of one project's batches land on the same partition, in order. That's
also how consumer groups (writer, evals, alerter) split the work later.

The producer uses `acks=all`: the broker answers only after every in-sync replica has the record.
With 3 brokers in round 2, a 202 survives losing a broker. franz-go's producer is idempotent by
default, so if a produce request times out and the client retries internally, the broker drops
the duplicate. The handler blocks until the ack arrives, which is why a 202 means durable.

An SDK retry after a lost 202 (network drop after Kafka acked) does produce the batch twice. That's
expected: the batch carries the same `idempotencyKey` both times, and the writer's
ReplacingMergeTree tables collapse the copies.

SIGTERM: the HTTP server and the gRPC server (`GracefulStop`) both stop accepting connections and
wait up to 20s for in-flight requests, each of which is waiting on its own Kafka ack. Then it flushes
the producer and exits.

## Env vars

| Var | Default |
|---|---|
| `INGEST_HTTP_ADDR` | `:8081` |
| `INGEST_GRPC_ADDR` | `:9091` |
| `INGEST_KAFKA_BROKERS` | `localhost:9092` (comma-separated) |
| `INGEST_POSTGRES_DSN` | `postgres://tracewell:tracewell@localhost:5432/tracewell?sslmode=disable` |
| `INGEST_REDIS_ADDR` | `localhost:6379` |
| `INGEST_RATE_LIMIT_RPS` | `200` (requests per project per second) |

The defaults target compose from the host. Inside compose, use `kafka:29092`, `redis:6379` and the
`postgres` host from `docs/contracts/README.md`.

## Run locally

Needs the Postgres seed (`api_keys`) and the `traces.raw` topic. Track B's migrations and
`kafka-init` create them. Before those land, apply them by hand once:

```
docker compose up -d
docker compose exec -T postgres psql -U tracewell -d tracewell < docs/contracts/postgres.sql
for t in traces.raw evals.results traces.dlq; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic $t --partitions 6 --replication-factor 1
done

cd services/ingest && go run ./cmd/ingest
curl -X POST localhost:8081/v1/ingest -H 'Authorization: Bearer tw_dev_localkey' \
  -H 'Content-Type: application/json' -d '{"idempotencyKey":"manual:1","traces":[]}'
```

Docker, from the repo root (the build needs `proto/gen/go`):

```
docker build -f services/ingest/Dockerfile -t tracewell/ingest .
```

## Test

```
cd services/ingest
go test ./...                                 # unit tests, no dependencies
go test -tags integration ./internal/auth/    # needs compose Postgres with the seed; skips if it's down
```

Unit tests, HTTP: the exact request body from `docs/contracts/http.md` is accepted and produced as
binary protobuf with the right key; `project_id` overwrite on the batch and every trace; 401 for a
missing or unknown key; 429 with `Retry-After: 1` over the limit; a Redis failure lets the request
through; 503 when Postgres or Kafka fails; 400 for bad JSON, unknown fields, bad protobuf, wrong
content type and a missing idempotency key; 413 over 5 MB. gRPC, over an in-process `bufconn`
connection: the same auth, rate-limit, validation, size, Kafka-failure and overwrite cases, with the
contract's status codes and the `retry-after` trailer only on the rate limit. Rate limiter, against
an in-memory Redis (miniredis): the limit per project per second, separate counters per project, the
key name and 2s TTL, a fresh window the next second, and a stopped Redis failing in under a second.
Auth: the dev key's hash matches the seed and the 60s cache. The integration test runs the real
`api_keys` query for an active, a revoked and an unknown key.

Verified by hand against compose with `INGEST_RATE_LIMIT_RPS=5`: 20 concurrent calls got exactly 5
accepted and 15 rate-limited, over both HTTP (`429`, `Retry-After: 1`) and gRPC (`RESOURCE_EXHAUSTED`,
`retry-after: 1`). With Redis stopped, all 20 were accepted, `ingest_rate_limit_errors_total` rose by
20 and `/readyz` stayed `200`. The limit came back as soon as Redis did. SIGTERM exited 0.

Burst (integration step 9): `examples/burst.py --count 500` fires 500 requests at once from 500 SDK
instances. At the default 200/s it got 400 `429`s, every SDK retried after `Retry-After`, and all 500
traces were in `tracewell.traces` after 2.5s.
