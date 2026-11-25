# eval-workers

Track C. Asynchronous evaluation service that scores LLM generation spans consumed from Kafka.
It consumes `IngestBatch` messages from `traces.raw`, filters for `GENERATION` spans, runs rule-based,
model-based, and LLM-as-judge evaluators, and publishes `EvalResult` messages to `evals.results` for storage
by the writer service (Track A) and presentation on the dashboard (Track D).

## Endpoints

Port 8085 (`EVALS_HTTP_ADDR`).

| Path | Description |
|---|---|
| `GET /healthz` | `200 ok` while the worker process is running. |
| `GET /readyz` | `200 ready` when Kafka broker metadata is reachable via non-blocking TCP socket check within 1.0s, else `503`. |
| `GET /metrics` | Prometheus text metrics (`evals_scored_total{evaluator}`, `evals_duration_seconds{evaluator}`, `evals_judge_errors_total`, `evals_dlq_total{reason}`). |

## Evaluators (Round 2)

Evaluators implement the `BaseEvaluator` interface (`name` property and `evaluate(span)` method) and run
concurrently via a `ThreadPoolExecutor` so network-bound evaluators (such as the LLM judge) do not stall
Kafka partition consumption:

1. **`llm_judge`**:
   - Uses Anthropic SDK (`claude-haiku-5-5` by default) to evaluate prompt/completion quality.
   - Requests structured JSON `{score, reasoning}`: rates whether the completion answers the prompt helpfully and correctly.
   - Score: `0.0` to `1.0`; label: `pass` if score >= 0.5, else `fail`.
   - Sample rate: configured by `EVALS_JUDGE_SAMPLE_RATE` (0.0 to 1.0).
   - "No key means off": If `EVALS_ANTHROPIC_API_KEY` is not set or empty, the evaluator cleanly disables itself without errors.
   - Retries: 3 attempts with exponential backoff on transient API failures; records `evals_judge_errors_total` on failure.

2. **`toxicity`**:
   - Uses local classification model (`detoxify`) without making external API calls.
   - Model is loaded once into memory at worker startup.
   - Score: `1.0 - toxicity_probability` (higher is better, 1.0 = completely clean, 0.0 = highly toxic).
   - Label: `clean` if score >= 0.5, else `toxic`.

3. **`pii`**:
   - Scans completions for Personally Identifiable Information using strict regular expressions and validation:
     - Email addresses
     - US phone numbers
     - US Social Security Numbers (SSN)
     - Credit card numbers with Luhn checksum validation
   - Score: `1.0` and label `clean` if no PII is detected; score `0.0` and label `pii_detected` if PII is found.
   - Privacy guarantee: **never leaks matched text in reasoning** (only names the detected PII categories).

4. **`latency`**:
   - Calculates duration from `span.start_time` to `span.end_time`.
   - Score: `1.0` and label `ok` if duration is under 5000 ms.
   - Score: `0.0` and label `slow` if duration is 5000 ms or greater.

5. **`empty_completion`**:
   - Inspects `span.generation.completion`.
   - Score: `0.0` and label `empty` if completion is blank or whitespace-only; score `1.0` and label `ok` if present.

**Span filtering**: Only spans with `kind == SPAN_KIND_GENERATION` are scored. Non-generation spans
(`SPAN_KIND_TOOL`, `SPAN_KIND_RETRIEVAL`, `SPAN_KIND_CHAIN`, `SPAN_KIND_UNSPECIFIED`) are skipped and
produce no eval results.

## Concurrency & Performance

Evaluations for generation spans run concurrently across a dedicated worker thread pool (`ThreadPoolExecutor(max_workers=5)`).
This ensures:
- Fast local evaluators (`pii`, `toxicity`, `latency`, `empty_completion`) complete in milliseconds.
- Network calls to the Anthropic API for `llm_judge` run in background worker threads without blocking Kafka consumption loops.
- Partition throughput is maintained evenly across consumers in the `evals` consumer group.

## Dead Letter Queue (DLQ)

If a message fails processing (e.g. corrupted protobuf bytes or uncaught batch errors), the worker retries
up to 3 attempts. After 3 failed attempts, a `DeadLetter` protobuf is published to `traces.dlq`
(retaining source topic, partition, offset, payload, error description, and attempt count), producer is flushed,
`evals_dlq_total{reason}` is incremented, and the Kafka offset is committed to prevent poison pill loops.

## Prometheus Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `evals_scored_total` | Counter | `evaluator` | Total spans scored per evaluator |
| `evals_duration_seconds` | Histogram | `evaluator` | Duration histogram of evaluator scoring runs |
| `evals_judge_errors_total` | Counter | None | Total errors encountered when calling the LLM judge API |
| `evals_dlq_total` | Counter | `reason` | Total poison-pill messages routed to Dead Letter Queue |

## Env vars

| Var | Default | Description |
|---|---|---|
| `EVALS_HTTP_ADDR` | `:8085` | Address for health and Prometheus metrics HTTP server |
| `EVALS_KAFKA_BROKERS` | `kafka:29092` | Kafka broker bootstrap list (`localhost:9092` on host) |
| `EVALS_KAFKA_GROUP_ID` | `evals` | Kafka consumer group ID |
| `EVALS_RAW_TOPIC` | `traces.raw` | Topic containing raw ingest batches |
| `EVALS_RESULTS_TOPIC` | `evals.results` | Topic receiving scored eval results |
| `EVALS_DLQ_TOPIC` | `traces.dlq` | Dead Letter Queue topic for failed messages |
| `EVALS_ANTHROPIC_API_KEY` | `""` | Anthropic API key for `llm_judge` (empty = judge disabled) |
| `EVALS_JUDGE_MODEL` | `claude-haiku-5-5` | Model used for `llm_judge` |
| `EVALS_JUDGE_SAMPLE_RATE` | `1.0` | Probability (0.0 to 1.0) of running `llm_judge` on each generation |

## Development & Verification

```bash
cd workers/evals

# Run linter
uv run ruff check

# Run unit tests
uv run pytest -v
```
