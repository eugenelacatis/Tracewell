# alerter

Track C. Stream processing alerting service written in Go. Consumes raw trace batches from Kafka,
evaluates windowed aggregation rules against stream metrics, tracks cooldown suppression, delivers
alerts via logs, webhooks, or Slack with trace links, and records fired alerts into Postgres for display
in the dashboard.

## Endpoints

Port 8083 (`ALERTER_HTTP_ADDR`).

| Path | Description |
|---|---|
| `GET /healthz` | `200 ok` while the process is running. |
| `GET /readyz` | `200 ready` when both Postgres and Kafka are reachable, else `503`. |
| `GET /metrics` | Prometheus metrics (`alerter_rules_loaded`, `alerter_alerts_fired_total{metric, channel}`, `alerter_delivery_errors_total{channel}`). |

## Rules and Metrics (Round 2)

The alerter supports three project-scoped metrics evaluated over a sliding window:

| Metric | Calculation | Threshold condition | Minimum sample guard |
|---|---|---|---|
| `error_rate` | ERROR spans / all spans (0.0 - 1.0) | `value > threshold` | Requires at least 5 spans in the window |
| `cost_usd` | Sum of generation `costUsd` | `value > threshold` | No sample guard (fires immediately) |
| `p95_latency_ms` | 95th percentile of trace duration (ms) | `value > threshold` | Requires at least 5 traces in the window |

### Sliding Windows
- Rules are evaluated every 10 seconds over the last `windowSeconds` (not tumbling buckets).
- In-memory window tracker stores raw trace durations to compute exact nearest-rank p95 percentiles.
- Windows older than 1 hour are automatically pruned to prevent memory growth.

### Cooldown
- Once triggered, a rule enters a cooldown period (`cooldown_seconds`). Subsequent threshold violations
  within this period are suppressed to prevent alert flooding.
- Cooldown timer starts only after the alert event has been durably persisted in Postgres.

### Rule Synchronization & Soft Deletes
- Rules are refreshed from Postgres every 30 seconds.
- Filter query: `SELECT ... FROM alert_rules WHERE enabled = true AND deleted_at IS NULL`.
- Soft-deleted or disabled rules are omitted from in-memory evaluation and their cooldown state is cleaned up.

## Delivery Channels

1. **`log`**:
   - Structured JSON log containing rule ID, name, project ID, metric, value, and threshold.

2. **`webhook`**:
   - HTTP POST JSON payload to `rule.Target` with fields: `rule_id`, `rule_name`, `project_id`, `metric`, `value`, `threshold`, `message`, `fired_at`.

3. **`slack`**:
   - HTTP POST JSON payload `{"text": ...}` to incoming Slack webhook URL (`rule.Target`).
   - The message names the rule, project ID, current value, threshold, and window, and includes a direct link to the trace view:
     `http://localhost:3000/traces?from=<RFC3339>&to=<RFC3339>`

### Retries & Durability
- HTTP deliveries (`webhook`, `slack`) retry up to 3 times with exponential backoff (100ms, 200ms, 400ms).
- If all attempts fail, `alerter_delivery_errors_total{channel}` is incremented.
- **Audit persistence guarantee**: The alert is durably recorded in the Postgres `alert_events` table **before** delivery is attempted, ensuring alert history is preserved even if the delivery channel is temporarily down.

## Prometheus Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `alerter_rules_loaded` | Gauge | None | Number of active enabled rules loaded in memory |
| `alerter_alerts_fired_total` | Counter | `metric`, `channel` | Total number of alert rules triggered |
| `alerter_delivery_errors_total` | Counter | `channel` | Total count of alert delivery errors |
| `alerter_spans_processed_total` | Counter | None | Total spans processed by the alerter |
| `alerter_error_spans_total` | Counter | None | Total error spans observed |

## Env vars

| Var | Default | Description |
|---|---|---|
| `ALERTER_HTTP_ADDR` | `:8083` | HTTP server address for health and metrics |
| `ALERTER_KAFKA_BROKERS` | `kafka:29092` | Kafka broker bootstrap list (`localhost:9092` on host) |
| `ALERTER_KAFKA_GROUP_ID` | `alerter` | Kafka consumer group ID |
| `ALERTER_TOPIC` | `traces.raw` | Topic containing raw trace batches |
| `ALERTER_POSTGRES_DSN` | `postgres://tracewell:tracewell@postgres:5432/tracewell?sslmode=disable` | Postgres DSN for rules and events |

## Development & Verification

```bash
cd services/alerter

# Run unit tests
go test -v ./...
```
