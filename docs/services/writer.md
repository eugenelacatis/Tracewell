# writer-svc

Track A. Moves data from Kafka into ClickHouse. It consumes `traces.raw` (from ingest-svc) and
`evals.results` (from the eval workers) as consumer group `writer` and inserts into the tables in
`docs/contracts/clickhouse.sql`. It has no API, only health endpoints.

## Endpoints (port 8084)

| Path | Notes |
|---|---|
| `GET /healthz` | `200 ok` while the process is up. |
| `GET /readyz` | `200` when Kafka and ClickHouse both answer within 2s, else `503`. |
| `GET /metrics` | Prometheus. `writer_rows_inserted_total{table}`, `writer_insert_duration_seconds{table}`, `writer_dlq_total{reason}` (`decode` or `insert`), Go runtime metrics. |

## What it writes

For each `IngestBatch` (one Kafka message):

- one `spans` row per span. Enums lose their proto prefix (`SPAN_KIND_GENERATION` -> `GENERATION`,
  `STATUS_ERROR` -> `ERROR`). `params` and `metadata` are stored as protojson, `{}` when unset.
  Generation columns stay empty or zero on spans without a generation. A span is always filed
  under the trace it arrived in, even if its own `trace_id` field says otherwise.
- one `traces` row per trace, with rollups computed from its spans: `status` is `ERROR` if any span
  is `ERROR`, else `OK`; `span_count`; `total_tokens` and `cost_usd` are sums over generation usage.
- one `ingest_batches` row recording `sdk_instance_id`, `batch_seq`, counts and the Kafka
  partition and offset.

For each `EvalResult`: one `evals` row.

## How it works

**Batching.** The writer buffers decoded messages and flushes when the buffer holds 500 rows or the
oldest buffered message is 1s old. A flush runs one `INSERT` per table, in the order spans, traces,
evals, ingest_batches. Because `ingest_batches` goes last, a row there means the whole batch is stored.

**Commit after durable.** Auto-commit is off. Offsets are committed only after a flush succeeds
(or the messages are in the DLQ). If the writer dies anywhere before the commit, Kafka redelivers
from the last commit and the rows are inserted again. That's safe: every table is a
`ReplacingMergeTree`, so identical rows collapse, and readers use `FINAL`. There's no separate dedupe
store. Kafka gives at-least-once delivery, and the table engine turns it into exactly-once at read time.

**Retries and the DLQ.** A failed flush is retried after 2s and then 5s (3 attempts in total).
If all three fail, every message in that flush is sent to `traces.dlq` as a `DeadLetter` (source
topic, partition, offset, original payload, error, `attempts=3`) with its original key, and then
committed. A message that doesn't decode, or has no `project_id`, goes to the DLQ right away with
`attempts=1`, because retrying can't fix it. If producing to the DLQ fails, the writer exits without
committing, so nothing is skipped. On restart it gets the messages again.

The ~7s retry window covers a ClickHouse restart (~3s measured in round 1, when a 1.5s window lost
7 of 100 traces to the DLQ) and a replica pod being replaced on the cluster. While it retries, the
writer stops consuming, so Kafka lag grows but nothing is lost. A longer outage still sends everything
consumed during it to the DLQ, which follows the contract (3 attempts, then DLQ) but needs a replay
afterwards. The replay CLI is round 3.

**Cluster inserts.** The writer always sets `distributed_foreground_insert = 1`. On the 2x2 cluster
the tables are `Distributed`, and without it an `INSERT` returns once the receiving node has queued the
rows, before the shard has them, so a commit could run ahead of durability
(`docs/contracts/clickhouse_cluster.sql`). On a single node the setting does nothing.

**Rebalances.** The client holds off partition reassignment while uncommitted records are buffered
(`BlockRebalanceOnPoll`), and allows it once the buffer is flushed and committed. That way one
instance never commits offsets for a partition another instance has already taken over. A rebalance
waits at most about one flush: ~1s normally, up to ~8s while ClickHouse inserts are being retried.

**Shutdown.** On SIGTERM the writer stops polling, flushes and commits what's buffered, then leaves
the group, so another instance picks up the partitions immediately. If ClickHouse is failing at that
moment, the retries add up to ~7s, inside Kubernetes' 30s grace period.

**Crash restart.** After `kill -9` the dead instance stays in the group until its session times out
(45s, the franz-go default), and its partitions aren't consumed during that time. Nothing is lost.
The restarted writer catches up once the session expires. A graceful restart
(`docker compose restart writer`) has no such delay.

## Env vars

| Var | Default |
|---|---|
| `WRITER_HTTP_ADDR` | `:8084` |
| `WRITER_KAFKA_BROKERS` | `localhost:9092` (comma-separated) |
| `WRITER_CLICKHOUSE_DSN` | `clickhouse://tracewell:tracewell@localhost:9000/tracewell` |

The defaults target compose from the host. Inside compose, use `kafka:29092` and the `clickhouse` host
from `docs/contracts/README.md`.

## Run locally

Needs the ClickHouse tables and the three topics. Track B's migrations create them. Before those
land, apply `docs/contracts/clickhouse.sql` by hand and create the topics as in
`docs/services/ingest.md`:

```
docker compose exec -T clickhouse clickhouse-client -u tracewell --password tracewell \
  --multiquery < docs/contracts/clickhouse.sql
cd services/writer && go run ./cmd/writer
```

End to end, with ingest-svc also running:

```
cd sdk/python   # 8081 is ingest-svc directly; leave TRACEWELL_HOST unset to go through the gateway on 8080
TRACEWELL_HOST=http://localhost:8081 TRACEWELL_API_KEY=tw_dev_localkey uv run python ../../examples/chatbot.py --fake --count 5
curl -s 'localhost:8123/?user=tracewell&password=tracewell' \
  --data "SELECT count(), sum(cost_usd) FROM tracewell.traces FINAL"
```

Docker, from the repo root:

```
docker build -f services/writer/Dockerfile -t tracewell/writer .
```

## Test

```
cd services/writer
go test ./...                                  # unit tests, no dependencies
go test -tags integration ./internal/store/    # needs compose ClickHouse with the tables; skips if it's down
```

Unit tests: enum mapping, rollups (OK and ERROR traces, token and cost sums, zero-span traces),
the `ingest_batches` row, buffering across messages and row counting, retry (2s, 5s) then success, DLQ
after 3 failures with the original payload/partition/offset/key, undecodable and project-less
messages going straight to the DLQ while good ones are still inserted, and a DLQ failure returning
an error so the caller doesn't commit. The integration test inserts every table twice and checks
that `FINAL` shows one row each, with microsecond timestamps, `duration_ms`, tags and cost intact.
`system.query_log` shows those inserts ran with `distributed_foreground_insert = 1`.

Verified by hand (round 1): `kill -9` of the writer while the example sent 100 traces left 23
stored; the restarted writer stored the remaining 77 after the session timeout. Result: 100 in
`FINAL`, 100 raw rows, 100 distinct ids, `batch_seq` 1-14 with no gaps, and `sum(cost_usd)` equal to
what the example printed.
