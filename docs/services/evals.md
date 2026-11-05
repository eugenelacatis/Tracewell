# eval-workers

Track C. Asynchronous evaluation service that scores LLM generation spans consumed from Kafka.
It consumes `IngestBatch` messages from `traces.raw`, filters for `GENERATION` spans, runs rule-based
evaluators, and publishes `EvalResult` messages to `evals.results` for storage by the writer service (Track A)
and presentation on the dashboard (Track D).

## Endpoints

Port 8085 (`EVALS_HTTP_ADDR`).

| Path | Description |
|---|---|
| `GET /healthz` | `200 ok` while the worker process is running. |
| `GET /readyz` | `200 ready` when Kafka broker metadata is reachable, else `503`. |
| `GET /metrics` | Prometheus text metrics (`evals_messages_processed_total`, `evals_produced_total`, `evals_dead_letters_total`). |

## Evaluators (Round 1)

Evaluators implement the `BaseEvaluator` interface (`name` property and `evaluate(span)` method) to enable
adding Round 2 scorers (LLM-as-judge, toxicity, PII) without modifying the ingestion and consumer pipeline:

1. **`latency`**:
   - Calculates duration from `span.start_time` to `span.end_time`.
   - Score: `1.0` and label `ok` if duration is under 5000 ms.
   - Score: `0.0` and label `slow` if duration is 5000 ms or greater.
   - Reasoning: reports duration (e.g., `1200ms, under 5000ms` or `5500ms, exceeds 5000ms`).

2. **`empty_completion`**:
   - Inspects `span.generation.completion`.
   - Score: `0.0` and label `empty` if completion is blank, empty, or whitespace-only.
   - Score: `1.0` and label `ok` if completion is non-empty.
   - Reasoning: reports presence or absence of completion text.

**Span filtering**: Only spans with `kind == SPAN_KIND_GENERATION` are scored. Non-generation spans
(`SPAN_KIND_TOOL`, `SPAN_KIND_RETRIEVAL`, `SPAN_KIND_CHAIN`, `SPAN_KIND_UNSPECIFIED`) are skipped and
produce no eval results.

## How it works

1. **Consumer group**: Joins consumer group `evals` on `traces.raw` using `confluent-kafka` with `enable.auto.commit = false`.
2. **Batch decoding**: Parses binary protobuf `IngestBatch`. Iterates through traces and spans.
3. **Eval emission**: For each generation span, runs both evaluators and produces one `EvalResult` per
   evaluator to `evals.results`, keyed by `project_id`.
4. **Durable flush & commit**: The producer is flushed to Kafka before committing the consumer offset.
5. **Dead Letter Queue (DLQ)**: If a message fails processing (e.g. malformed payload), the worker retries
   up to 3 attempts. After 3 failed attempts, a `DeadLetter` protobuf is published to `traces.dlq`
   (retaining source topic, partition, offset, payload, error description, and attempt count), producer is flushed,
   and offset is committed to prevent poison pill loops.
6. **Graceful shutdown**: On `SIGTERM` or `SIGINT`, the worker finishes processing the current message in flight,
   flushes the producer, commits the offset, and closes the consumer.

## Env vars

| Var | Default | Description |
|---|---|---|
| `EVALS_HTTP_ADDR` | `:8085` | Address for health and Prometheus metrics HTTP server |
| `EVALS_KAFKA_BROKERS` | `kafka:29092` | Kafka broker bootstrap list (`localhost:9092` on host) |
| `EVALS_KAFKA_GROUP_ID` | `evals` | Kafka consumer group ID |
| `EVALS_RAW_TOPIC` | `traces.raw` | Topic containing raw ingest batches |
| `EVALS_RESULTS_TOPIC` | `evals.results` | Topic receiving scored eval results |
| `EVALS_DLQ_TOPIC` | `traces.dlq` | Dead Letter Queue topic for failed messages |

## Run locally

```bash
# Run standalone with local Kafka
cd workers/evals
EVALS_KAFKA_BROKERS=localhost:9092 uv run python -m tracewell_evals.main
```

Docker build from repo root:

```bash
docker build -f workers/evals/Dockerfile -t tracewell/evals .
```

## Test

```bash
cd workers/evals
uv run pytest
```

Tests cover evaluator scoring thresholds and edge cases, non-generation span skipping, batch evaluation,
DLQ routing after 3 failures with mock Kafka producers/consumers, and health/metrics endpoint handling.
