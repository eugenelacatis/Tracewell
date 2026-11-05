# alerter

Track C. Stream processing alerting service written in Go. Consumes raw trace batches from Kafka,
evaluates windowed aggregation rules against stream metrics, tracks cooldown suppression, logs alerts or
dispatches webhooks, and records fired alerts into Postgres for display in the dashboard.

## Endpoints

Port 8083 (`ALERTER_HTTP_ADDR`).

| Path | Description |
|---|---|
| `GET /healthz` | `200 ok` while the process is running. |
| `GET /readyz` | `200 ready` when both Postgres and Kafka are reachable, else `503`. |
| `GET /metrics` | Prometheus metrics (`alerter_spans_processed_total`, `alerter_error_spans_total`, `alerter_alerts_fired_total`, `alerter_rules_active`). |

## Rules and Metrics (Round 1)

In Round 1, the alerter supports the **`error_rate`** metric:
- **Definition**: Per project, the ratio of spans with `STATUS_ERROR` divided by total spans observed over
  the rule's configured `window_seconds`.
- **Condition**: Triggers when `error_rate > threshold`.
- **Cooldown**: Once triggered, a rule enters a cooldown period (`cooldown_seconds`). Subsequent threshold
  violations within this period are suppressed to prevent alert flooding.
- **Delivery**:
  - `log`: An `slog` JSON line containing `rule_id`, `rule_name`, `project_id`, `metric`, `value`, and `threshold`.
  - `webhook`: HTTP POST JSON payload to `target` URL with timeout (5s).
- **Audit event**: Every fired alert is durably inserted into `alert_events` in Postgres.

Rules are loaded at startup and reloaded every 30 seconds from `alert_rules` (`WHERE enabled = true`).

## Window State & Restart Behavior

Window aggregations are tracked in-memory using an efficient time-window tracker that aggregates span records
per `project_id` and prunes data older than 15 minutes.

### Restart behavior (Round 1 / Round 2)
Because window state and cooldown records live in memory:
- **On restart**, in-memory window counters (`total` spans and `error` spans) and the `lastFired` timestamps
  reset.
- When new spans arrive after restart, window counts begin accumulating from zero.
- If a project experiences sustained errors across a restart, the rule will evaluate against the new post-restart
  spans and may fire once (starting a new cooldown period).
- In Round 3, persistent window state checkpoints (or state recovery from compacted Kafka topics / Redis) will
  be investigated to maintain cross-restart continuity.

## Env vars

| Var | Default | Description |
|---|---|---|
| `ALERTER_HTTP_ADDR` | `:8083` | HTTP server address for health and metrics |
| `ALERTER_KAFKA_BROKERS` | `kafka:29092` | Kafka broker bootstrap list (`localhost:9092` on host) |
| `ALERTER_KAFKA_GROUP_ID` | `alerter` | Kafka consumer group ID |
| `ALERTER_TOPIC` | `traces.raw` | Topic containing raw trace batches |
| `ALERTER_POSTGRES_DSN` | `postgres://tracewell:tracewell@postgres:5432/tracewell?sslmode=disable` | Postgres DSN for rules and events |

## Run locally

```bash
# Ensure Postgres and Kafka are running
docker compose up -d postgres kafka

# Run alerter pointing to localhost ports
cd services/alerter
ALERTER_KAFKA_BROKERS=localhost:9092 \
ALERTER_POSTGRES_DSN="postgres://tracewell:tracewell@localhost:5432/tracewell?sslmode=disable" \
go run ./cmd/alerter
```

Docker build from repo root:

```bash
docker build -f services/alerter/Dockerfile -t tracewell/alerter .
```

## Test

```bash
cd services/alerter
go test -v ./...
```

Unit tests cover:
- **Window math**: Exact span error rate computation, window boundary exclusion, and old span pruning.
- **Threshold and cooldown**: Alert firing on threshold crossing, cooldown suppression during cooldown interval,
  and re-firing after cooldown expiration.
- **Rule reload**: Dynamic synchronization of active rules from Postgres, omitting disabled rules and removing deleted rules.
- **Delivery**: Delivery to `log` and `webhook` (tested via in-memory mock transport).
- **Health/Readiness**: Correct HTTP status codes on `/healthz` and `/readyz`.
