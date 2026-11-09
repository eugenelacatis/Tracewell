# HTTP contracts (rounds 1-2)

JSON field names are lowerCamelCase everywhere, matching what `protojson` produces. Timestamps are
RFC 3339 strings in UTC (`2026-10-26T17:04:05.123456Z`). Errors always look like:

```json
{ "error": { "code": "unauthorized", "message": "invalid api key" } }
```

## Auth (both services)

`Authorization: Bearer <api key>`. The server computes the lowercase hex sha256 of the key, looks it up
in Postgres `api_keys` where `revoked_at IS NULL`, and takes `project_id` from that row. A missing or
unknown key returns `401 unauthorized`. The client never sends `project_id`, and if a body contains one
the server overwrites it.

Local dev keys: `tw_dev_localkey` (project `proj_dev`), `tw_demo_otherkey` (project `proj_demo`, round 2).

### Session tokens (round 2, query-svc only)

The dashboard logs a user in with `POST /v1/auth/login` and gets a session token that starts with
`tws_`. query-svc tells the two kinds of bearer token apart by prefix:

| Token | Project comes from | Extra header |
|---|---|---|
| `tw_...` API key | the key's `api_keys` row, as above | `X-Tracewell-Project` is ignored |
| `tws_...` session | the `X-Tracewell-Project` header | required on every project-scoped route |

For a session, query-svc checks that the project's org has a `memberships` row for the user. If not,
it returns `404 not_found`, so a user can't probe which project IDs exist. A missing header on a
project-scoped route returns `400 invalid_argument`. ingest-svc accepts only API keys.

Session tokens are 32 random bytes, base64url, with the `tws_` prefix. Only the sha256 is stored
(Redis key in `README.md`). They expire 7 days after login.

### Gateway (round 2)

Every client goes through the Envoy gateway on port 8080 (`docs/contracts/README.md`). Paths don't
change. The gateway routes and limits, but it doesn't authenticate: services still check the key,
because they need the `project_id` that comes with it.

## ingest-svc: `POST /v1/ingest` (port 8081)

Request body: one `IngestBatch`, either
- `Content-Type: application/json`: protojson encoding (what the Python SDK sends), or
- `Content-Type: application/x-protobuf`: binary encoding.

Max body size 5 MB, otherwise `413 payload_too_large`.

```json
{
  "idempotencyKey": "9f2c...:17",
  "sdkInstanceId": "9f2c...",
  "batchSeq": "17",
  "traces": [{
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "sessionId": "chat-42",
    "userId": "user-7",
    "name": "chat turn",
    "tags": ["example"],
    "startTime": "2026-10-26T17:04:05.000000Z",
    "endTime": "2026-10-26T17:04:06.200000Z",
    "spans": [{
      "spanId": "00f067aa0ba902b7",
      "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
      "parentSpanId": "",
      "name": "openai.chat",
      "kind": "SPAN_KIND_GENERATION",
      "startTime": "2026-10-26T17:04:05.100000Z",
      "endTime": "2026-10-26T17:04:06.100000Z",
      "status": "STATUS_OK",
      "generation": {
        "provider": "openai",
        "model": "gpt-4o-mini",
        "prompt": "[{\"role\":\"user\",\"content\":\"hi\"}]",
        "completion": "Hello!",
        "params": { "temperature": 0.2 },
        "usage": { "promptTokens": 9, "completionTokens": 3, "totalTokens": 12, "costUsd": 0.0000045 }
      },
      "metadata": {}
    }]
  }]
}
```

Rules the SDK follows:
- `traceId` is 32 lowercase hex chars, `spanId` is 16 (W3C trace-context sizes).
- A trace is sent **once, when it ends, with all of its spans**. The writer computes trace rollups from
  that one message.
- `idempotencyKey` = `"<sdkInstanceId>:<batchSeq>"`. `sdkInstanceId` is a random UUID4 hex per process.
  `batchSeq` starts at 1 and goes up by 1 per batch. (`uint64` is a string in protojson.)
- A retry resends the **same** batch with the same key and seq.

Responses:

| Status | Meaning | SDK action |
|---|---|---|
| `202` `{"accepted": <trace count>}` | Written to Kafka with `acks=all` | done |
| `400 invalid_argument` | Body didn't parse, or missing `idempotencyKey` | drop the batch and log it (retry won't help) |
| `401 unauthorized` | Bad key | drop and log |
| `413 payload_too_large` | Over 5 MB | split the batch next time |
| `429 rate_limited` | Over the project's limit (round 2, Redis, see below) | retry with backoff, honor `Retry-After` |
| `5xx` / network error | Kafka or Postgres down | retry: 0.5s, 1s, 2s, 4s, 8s with jitter, then drop and log |

`202` means the batch is durable in Kafka, not that it's in ClickHouse yet.

### Rate limit (round 2)

Per project, fixed one-second windows in Redis: `INCR` the key `rl:ingest:<project_id>:<unix second>`
and `EXPIRE` it after 2s. Over `INGEST_RATE_LIMIT_RPS` requests (default 200) in that second, the
response is `429 rate_limited` with `Retry-After: 1`. If Redis is down, ingest lets the request
through and counts it in `ingest_rate_limit_errors_total`, so a Redis outage never drops traces.
The same limit applies to gRPC.

### gRPC: `tracewell.v1.IngestService/Ingest` (port 9091, round 2)

Defined in `proto/trace.proto`. Same auth, same rules, same Kafka write as `POST /v1/ingest`.
- Auth: metadata `authorization: Bearer <api key>`.
- Max message 5 MB.
- Status mapping: `400` -> `INVALID_ARGUMENT`, `401` -> `UNAUTHENTICATED`, `413` ->
  `RESOURCE_EXHAUSTED`, `429` -> `RESOURCE_EXHAUSTED` with a `retry-after` trailer of `1`,
  `5xx` -> `UNAVAILABLE`. The TS SDK can use either transport. The Python SDK stays on HTTP.
- A client tells the two `RESOURCE_EXHAUSTED` cases apart by the trailer. With `retry-after`, it's
  ingest's per-project rate limit: retry after that many seconds. Without it, the batch is too large: don't retry, split it
  next time.
- The gateway's per-IP limit keeps Envoy's default for gRPC calls (`UNAVAILABLE`, so don't set
  `rate_limited_as_resource_exhausted`). Clients retry it like any `5xx`. A `RESOURCE_EXHAUSTED`
  without the trailer would read as "too large" and the batch would be dropped.

## query-svc (port 8082)

All reads are scoped to the key's `project_id`. ClickHouse reads use `FINAL` so redelivered rows never
show twice.

### `GET /v1/traces`

Query params: `limit` (default 50, max 200), `cursor` (opaque, from the last response), `from` and `to`
(RFC 3339, filter on trace `startTime`). Newest first.

```json
{
  "traces": [{
    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
    "name": "chat turn",
    "sessionId": "chat-42",
    "userId": "user-7",
    "tags": ["example"],
    "startTime": "2026-10-26T17:04:05Z",
    "endTime": "2026-10-26T17:04:06.2Z",
    "durationMs": 1200,
    "status": "OK",
    "spanCount": 1,
    "totalTokens": 12,
    "costUsd": 0.0000045
  }],
  "nextCursor": "eyJ0IjoiMjAyNi0xMC0yNlQxNzowNDowNVoiLCJpZCI6IjRiZjkuLi4ifQ"
}
```

`nextCursor` is `null` on the last page. The cursor is keyset pagination on `(start_time, trace_id)`
(base64 JSON), never an offset.

### `GET /v1/traces/{traceId}`

`404 not_found` if the trace isn't in this project.

```json
{
  "trace": { "...": "same object as one item of GET /v1/traces" },
  "spans": [{
    "spanId": "00f067aa0ba902b7",
    "parentSpanId": "",
    "name": "openai.chat",
    "kind": "GENERATION",
    "startTime": "2026-10-26T17:04:05.1Z",
    "endTime": "2026-10-26T17:04:06.1Z",
    "durationMs": 1000,
    "status": "OK",
    "errorMessage": "",
    "generation": {
      "provider": "openai",
      "model": "gpt-4o-mini",
      "prompt": "[{\"role\":\"user\",\"content\":\"hi\"}]",
      "completion": "Hello!",
      "params": { "temperature": 0.2 },
      "usage": { "promptTokens": 9, "completionTokens": 3, "totalTokens": 12, "costUsd": 0.0000045 }
    },
    "metadata": {}
  }],
  "evals": [{
    "spanId": "00f067aa0ba902b7",
    "evaluator": "latency",
    "score": 1.0,
    "label": "ok",
    "reasoning": "1000ms, under 5000ms",
    "createdAt": "2026-10-26T17:04:07Z"
  }]
}
```

`generation` is `null` when `kind` isn't `GENERATION`. Spans are ordered by `startTime`; the client
builds the tree from `parentSpanId`. Enum values here have no prefix (`GENERATION`, `OK`), because that's
how ClickHouse stores them.

## query-svc, round 2 additions

Same error shape, auth, `FINAL` rule and cursor style as round 1. "Project-scoped" means the route
works with an API key, or with a session plus `X-Tracewell-Project`.

### Auth and tenancy

| Route | Body / params | Returns |
|---|---|---|
| `POST /v1/auth/login` | `{"email", "password"}` | `200 {"token": "tws_...", "expiresAt", "user": {"id", "email", "name"}}`. Wrong email or password: `401 unauthorized` with the same message for both. |
| `POST /v1/auth/logout` | session token | `204`, deletes the session |
| `GET /v1/me` | session token | `200 {"user": {...}, "orgs": [{"id", "name", "role", "projects": [{"id", "name"}]}]}` |
| `POST /v1/projects` | session; `{"orgId", "name"}` | `201 {"id", "orgId", "name"}`. Caller must be `owner` of the org. IDs look like `proj_<8 random lowercase alnum>`. |
| `POST /v1/projects/{projectId}/api-keys` | session; `{"name"}` | `201 {"id", "name", "keyPrefix", "key": "tw_..."}`. The plaintext key appears in this response only. |
| `GET /v1/projects/{projectId}/api-keys` | session | `200 {"apiKeys": [{"id", "name", "keyPrefix", "createdAt", "revokedAt"}]}` |
| `DELETE /v1/projects/{projectId}/api-keys/{id}` | session | `204`, sets `revoked_at`. ingest-svc's 60s key cache means a revoked key can work for up to a minute. |

Login is rate-limited per email: 10 attempts per minute (`rl:login:<email>:<unix minute>`), then
`429 rate_limited`.

### Sessions (project-scoped)

A session is every trace sharing a `sessionId` (the SDK's session, not a login session). Traces with
an empty `sessionId` aren't part of any session.

`GET /v1/sessions`: params `limit` (default 50, max 200), `cursor`, `from`, `to`. Newest
`lastSeen` first. Keyset cursor on `(lastSeen, sessionId)`.

```json
{
  "sessions": [{
    "sessionId": "chat-42",
    "userId": "user-7",
    "traceCount": 6,
    "errorCount": 1,
    "firstSeen": "2026-10-26T17:04:05Z",
    "lastSeen": "2026-10-26T17:09:40Z",
    "totalTokens": 1840,
    "costUsd": 0.00071
  }],
  "nextCursor": null
}
```

`GET /v1/sessions/{sessionId}`: `{"session": <same object>, "traces": [<trace list items>]}`, traces
oldest first, max 500. `404 not_found` if no trace in this project has that `sessionId`.

### Metrics (project-scoped)

`GET /v1/metrics`: params `from`, `to` (default: the last hour), `bucket` (`1m`, `5m`, `1h`; default
`1m`), optional `model` (filters to traces with a generation span on that model). Max 500 buckets,
otherwise `400 invalid_argument`. Buckets with no traces are included as zeros so charts don't skip.

```json
{
  "bucket": "1m",
  "points": [{
    "t": "2026-10-26T17:04:00Z",
    "traceCount": 12,
    "errorCount": 1,
    "p50LatencyMs": 840,
    "p95LatencyMs": 2310,
    "totalTokens": 5120,
    "costUsd": 0.0021
  }],
  "models": ["gpt-4o-mini", "claude-haiku-5-5"]
}
```

Latency is trace `duration_ms`. `models` lists every model seen in the range, for the filter dropdown.

`GET /v1/evals/summary`: params `from`, `to`. Returns
`{"evaluators": [{"evaluator", "count", "avgScore", "failCount"}]}`. `failCount` counts scores under 0.5.

### Alert rules (project-scoped)

Rules live in Postgres `alert_rules`. The alerter reloads them every 30s, so an edit takes effect
within 30s.

| Route | Returns |
|---|---|
| `GET /v1/alert-rules` | `200 {"alertRules": [<rule>]}`, excluding soft-deleted rows |
| `POST /v1/alert-rules` | `201 <rule>` |
| `GET /v1/alert-rules/{id}` | `200 <rule>` or `404` |
| `PATCH /v1/alert-rules/{id}` | any subset of the writable fields; `200 <rule>` |
| `DELETE /v1/alert-rules/{id}` | `204`; sets `deleted_at` and `enabled = false` |

```json
{
  "id": "12",
  "name": "cost over $1 per 5 min",
  "metric": "cost_usd",
  "threshold": 1.0,
  "windowSeconds": 300,
  "cooldownSeconds": 600,
  "channel": "slack",
  "target": "https://hooks.slack.com/services/...",
  "enabled": true,
  "createdAt": "...",
  "updatedAt": "..."
}
```

`id` is a string, like every `bigint` in JSON here. Validation (`400 invalid_argument`, message names
the field): `metric` in `error_rate`, `cost_usd`, `p95_latency_ms`; `threshold >= 0`, and `<= 1` for
`error_rate`; `windowSeconds` 10-3600; `cooldownSeconds` >= `windowSeconds`; `target` is an `https://`
URL for `webhook` and `slack`, and empty for `log`.

What the alerter measures, per project, over the last `windowSeconds`, checked every 10s:

| Metric | Value | Fires when |
|---|---|---|
| `error_rate` | ERROR spans / all spans (0-1) | value > threshold, and at least 5 spans in the window |
| `cost_usd` | sum of generation `costUsd` | value > threshold |
| `p95_latency_ms` | p95 of trace duration | value > threshold, and at least 5 traces in the window |

`GET /v1/alert-events`: params `limit` (default 50, max 200), `cursor`, optional `ruleId`. Newest
first.

```json
{
  "alertEvents": [{
    "id": "301", "ruleId": "12", "ruleName": "cost over $1 per 5 min", "metric": "cost_usd",
    "value": 1.42, "threshold": 1.0, "message": "cost_usd 1.42 > 1.00 over 300s",
    "firedAt": "2026-10-26T17:09:40Z"
  }],
  "nextCursor": null
}
```

### Cache

query-svc caches project-scoped `GET` responses in Redis (key format in `README.md`). TTLs: 10s for
trace lists, trace detail, sessions and metrics; no caching for `/v1/me`, alert rules or alert events,
because those change from the dashboard and must show right away. Every cacheable response carries
`X-Cache: HIT` or `X-Cache: MISS`. If Redis is down, query-svc serves from ClickHouse and logs it.

## Every service (Go and Python)

| Path | Returns |
|---|---|
| `GET /healthz` | `200 ok` while the process is up |
| `GET /readyz` | `200` only when its dependencies (Kafka/ClickHouse/Postgres) answer, else `503` |
| `GET /metrics` | Prometheus text format |
