# HTTP contracts (round 1)

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

Local dev key: `tw_dev_localkey` (project `proj_dev`).

Round 2 puts the gateway in front of both services and adds dashboard user login. These paths stay the
same.

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
| `429 rate_limited` | Round 2 (Redis) | retry with backoff, honor `Retry-After` |
| `5xx` / network error | Kafka or Postgres down | retry: 0.5s, 1s, 2s, 4s, 8s with jitter, then drop and log |

`202` means the batch is durable in Kafka, not that it's in ClickHouse yet.

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

## Every service (Go and Python)

| Path | Returns |
|---|---|
| `GET /healthz` | `200 ok` while the process is up |
| `GET /readyz` | `200` only when its dependencies (Kafka/ClickHouse/Postgres) answer, else `503` |
| `GET /metrics` | Prometheus text format |
