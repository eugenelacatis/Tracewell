-- Tracewell ClickHouse schema for the 2x2 cluster (round 2, kind/EKS only).
-- Contract owned by all tracks. Track B copies this into deploy/migrations/clickhouse-cluster/.
-- docker-compose keeps the single-node clickhouse.sql. Services don't change between the two:
-- they read and write tracewell.traces, tracewell.spans, tracewell.evals, tracewell.ingest_batches.
--
-- Layout:
--   cluster name     'tracewell' (2 shards x 2 replicas, coordinated by ClickHouse Keeper)
--   <table>_local    ReplicatedReplacingMergeTree, one copy per replica, same columns as clickhouse.sql
--   <table>          Distributed over <table>_local; this is the name every service uses
--
-- Sharding key: sipHash64(trace_id) for traces, spans and evals, so every row of one trace lands
-- on the same shard. That matters for two reasons:
--   1. ReplacingMergeTree only collapses duplicates inside one shard. Redelivered rows hit the same
--      shard as the original, so FINAL still dedupes.
--   2. The trace detail query reads one shard's worth of data per trace.
-- ingest_batches shards by sipHash64(sdk_instance_id) so one SDK's sequence stays together
-- (round 4 gap tracking scans it in order).
--
-- Writer rule: insert into the Distributed table with distributed_foreground_insert = 1
-- (older name: insert_distributed_sync = 1). The insert then returns only after the shard has the
-- rows, which keeps "commit the Kafka offset only after the write is durable" true.
--
-- Column names, types, ORDER BY and PARTITION BY are copied from clickhouse.sql. If that file
-- changes, this one changes in the same PR.

CREATE DATABASE IF NOT EXISTS tracewell ON CLUSTER tracewell;

CREATE TABLE IF NOT EXISTS tracewell.traces_local ON CLUSTER tracewell
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
    status          LowCardinality(String),
    span_count      UInt32,
    total_tokens    UInt64,
    cost_usd        Float64,
    idempotency_key String,
    ingested_at     DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/tracewell/traces', '{replica}', ingested_at)
PARTITION BY toYYYYMM(start_time)
ORDER BY (project_id, start_time, trace_id);

CREATE TABLE IF NOT EXISTS tracewell.traces ON CLUSTER tracewell AS tracewell.traces_local
ENGINE = Distributed(tracewell, tracewell, traces_local, sipHash64(trace_id));

CREATE TABLE IF NOT EXISTS tracewell.spans_local ON CLUSTER tracewell
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
    provider          LowCardinality(String),
    model             LowCardinality(String),
    prompt            String,
    completion        String,
    params            String,
    prompt_tokens     UInt32,
    completion_tokens UInt32,
    total_tokens      UInt32,
    cost_usd          Float64,
    metadata          String,
    ingested_at       DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/tracewell/spans', '{replica}', ingested_at)
PARTITION BY toYYYYMM(start_time)
ORDER BY (project_id, trace_id, span_id);

CREATE TABLE IF NOT EXISTS tracewell.spans ON CLUSTER tracewell AS tracewell.spans_local
ENGINE = Distributed(tracewell, tracewell, spans_local, sipHash64(trace_id));

CREATE TABLE IF NOT EXISTS tracewell.evals_local ON CLUSTER tracewell
(
    project_id  String,
    trace_id    String,
    span_id     String,
    evaluator   LowCardinality(String),
    score       Float64,
    label       String,
    reasoning   String,
    created_at  DateTime64(6, 'UTC'),
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/tracewell/evals', '{replica}', ingested_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (project_id, trace_id, span_id, evaluator);

CREATE TABLE IF NOT EXISTS tracewell.evals ON CLUSTER tracewell AS tracewell.evals_local
ENGINE = Distributed(tracewell, tracewell, evals_local, sipHash64(trace_id));

CREATE TABLE IF NOT EXISTS tracewell.ingest_batches_local ON CLUSTER tracewell
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
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/tracewell/ingest_batches', '{replica}', ingested_at)
ORDER BY (project_id, sdk_instance_id, batch_seq);

CREATE TABLE IF NOT EXISTS tracewell.ingest_batches ON CLUSTER tracewell AS tracewell.ingest_batches_local
ENGINE = Distributed(tracewell, tracewell, ingest_batches_local, sipHash64(sdk_instance_id));
