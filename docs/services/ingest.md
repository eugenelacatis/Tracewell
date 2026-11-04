# ingest-svc

Track A. The front door for trace data. It authenticates the SDK's API key, stamps the batch with
the key's `project_id`, and writes it to the Kafka topic `traces.raw`. Request and response shapes are
in `docs/contracts/http.md`.

## Endpoints

| Path | Notes |
|---|---|
| `POST /v1/ingest` | One `IngestBatch`, `application/json` (protojson) or `application/x-protobuf`. `202 {"accepted": <traces>}` once Kafka has it. |
| `GET /healthz` | `200 ok` while the process is up. |
| `GET /readyz` | `200` when Kafka and Postgres both answer within 2s, else `503`. |
| `GET /metrics` | Prometheus. `ingest_http_request_duration_seconds{status}`, `ingest_traces_accepted_total`, Go runtime metrics. |

Responses, in the order they're checked:

| Status | When |
|---|---|
| `401 unauthorized` | No `Bearer` key, or the key is unknown or revoked. |
| `503 unavailable` | Postgres failed during the key lookup. |
| `413 payload_too_large` | Body over 5 MB. |
| `400 invalid_argument` | Body didn't parse, `Content-Type` is neither of the two above, or `idempotencyKey` is empty. Unknown JSON fields are ignored, so a newer SDK isn't rejected. |
| `503 unavailable` | Kafka didn't ack within 10s. |
| `202` | Kafka acked with `acks=all`. |

Behavior the contract leaves open:

- The key is checked before the body is read, so an unauthenticated client can't make the server
  parse 5 MB.
- `project_id` is overwritten on the batch and on every trace inside it.
- Nothing else in the batch is validated (trace or span ids, timestamps). The writer stores what it
  gets.
- Kafka's record size limit has to match the 5 MB body limit. The producer allows records up to
  6 MiB, but the broker's default `max.message.bytes` is about 1 MB (compressed). Until the topic
  config is raised, a batch that is still over 1 MB after compression gets `503` and the SDK
  eventually drops it. Compressible text usually fits.
- A valid key is cached in memory for 60s, so a revoked key keeps working for up to a minute. Unknown
  keys are not cached.

## How it works

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

SIGTERM: the HTTP server stops accepting connections and waits up to 20s for in-flight requests,
each of which is waiting on its own Kafka ack. Then it flushes the producer and exits.

## Env vars

| Var | Default |
|---|---|
| `INGEST_HTTP_ADDR` | `:8081` |
| `INGEST_KAFKA_BROKERS` | `localhost:9092` (comma-separated) |
| `INGEST_POSTGRES_DSN` | `postgres://tracewell:tracewell@localhost:5432/tracewell?sslmode=disable` |

The defaults target compose from the host. Inside compose, use `kafka:29092` and the `postgres`
host from `docs/contracts/README.md`. `INGEST_GRPC_ADDR` and `INGEST_REDIS_ADDR` arrive in round 2.

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

Unit tests: the exact request body from `docs/contracts/http.md` is accepted and produced as binary
protobuf with the right key; `project_id` overwrite on the batch and every trace; 401 for a missing
or unknown key; 503 when Postgres or Kafka fails; 400 for bad JSON, unknown fields, bad protobuf,
wrong content type and a missing idempotency key; 413 over 5 MB; the dev key's hash matches the
seed; the 60s cache. The integration test runs the real `api_keys` query for an active, a revoked
and an unknown key.
