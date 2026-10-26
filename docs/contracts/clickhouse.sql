-- Tracewell ClickHouse schema, round 1 (single node).
-- Contract owned by all tracks. Track B copies this into deploy/migrations/clickhouse/ unchanged.
-- Writer (Track A) inserts. Query (Track D) reads. Round 2 converts these to
-- ReplicatedMergeTree + Distributed for the 2x2 cluster without changing column names or types.
--
-- Dedupe: every table is a ReplacingMergeTree. Kafka redelivery inserts the same row twice, and the
-- duplicate collapses on merge. Readers must use FINAL (or argMax) so duplicates never show.
--
-- Enums are stored without their proto prefix: SPAN_KIND_GENERATION -> 'GENERATION',
-- STATUS_ERROR -> 'ERROR', *_UNSPECIFIED -> 'UNSPECIFIED'.

CREATE DATABASE IF NOT EXISTS tracewell;

-- One row per trace. The SDK sends a trace once, when it ends, with all its spans,
-- so the writer can compute the rollup columns from the spans in the same message.
CREATE TABLE IF NOT EXISTS tracewell.traces
(
    project_id      String,
    trace_id        String,
    session_id      String,
    user_id         String,
    name            String,
    tags            Array(String),
    start_time      DateTime64(6, 'UTC'),
    end_time        DateTime64(6, 'UTC'),
    duration_ms     Float64 MATERIALIZED (toUnixTimestamp64Micro(end_time) - toUnixTimestamp64Micro(start_time)) / 1000,
    status          LowCardinality(String),  -- 'ERROR' if any span is ERROR, else 'OK'
    span_count      UInt32,
    total_tokens    UInt64,                  -- sum of generation usage.total_tokens
    cost_usd        Float64,                 -- sum of generation usage.cost_usd
    idempotency_key String,                  -- from the IngestBatch it arrived in
    ingested_at     DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(start_time)
ORDER BY (project_id, start_time, trace_id);

CREATE TABLE IF NOT EXISTS tracewell.spans
(
    project_id        String,
    trace_id          String,
    span_id           String,
    parent_span_id    String,
    name              String,
    kind              LowCardinality(String),
    start_time        DateTime64(6, 'UTC'),
    end_time          DateTime64(6, 'UTC'),
    duration_ms       Float64 MATERIALIZED (toUnixTimestamp64Micro(end_time) - toUnixTimestamp64Micro(start_time)) / 1000,
    status            LowCardinality(String),
    error_message     String,
    -- Generation fields; empty/zero when kind != 'GENERATION'
    provider          LowCardinality(String),
    model             LowCardinality(String),
    prompt            String,
    completion        String,
    params            String,                -- JSON (protojson of google.protobuf.Struct)
    prompt_tokens     UInt32,
    completion_tokens UInt32,
    total_tokens      UInt32,
    cost_usd          Float64,
    metadata          String,                -- JSON (protojson of google.protobuf.Struct)
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(start_time)
ORDER BY (project_id, trace_id, span_id);

-- Written by the writer from the evals.results topic.
-- Dedupe key matches CLAUDE.md: trace_id + span_id + evaluator.
CREATE TABLE IF NOT EXISTS tracewell.evals
(
    project_id  String,
    trace_id    String,
    span_id     String,                      -- '' for a trace-level eval
    evaluator   LowCardinality(String),
    score       Float64,
    label       String,
    reasoning   String,
    created_at  DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (project_id, trace_id, span_id, evaluator);

-- One row per IngestBatch the writer stored. Inserted last, after traces and spans,
-- so a row here means the whole batch is durable. Feeds the completeness (gap) feature later.
CREATE TABLE IF NOT EXISTS tracewell.ingest_batches
(
    project_id      String,
    sdk_instance_id String,
    batch_seq       UInt64,
    idempotency_key String,
    trace_count     UInt32,
    span_count      UInt32,
    kafka_partition Int32,
    kafka_offset    Int64,
    ingested_at     DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (project_id, sdk_instance_id, batch_seq);
