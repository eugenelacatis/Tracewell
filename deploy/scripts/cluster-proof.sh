#!/usr/bin/env bash
# Proves the ClickHouse 2x2 cluster does what we claim, on kind:
#   1. rows inserted through the Distributed table spread across both shards (sharding)
#   2. both replicas of each shard hold the same rows (replication)
# It inserts PROOF_ROWS rows with project_id 'proof', prints the evidence from system.parts, checks it,
# and deletes the rows again so dashboards and counts stay clean. Needs the cluster from deploy/kind/up.sh
# and the schema from deploy/migrations/clickhouse-cluster/001_cluster.sql.
#
#   PROOF_ROWS=100  PROOF_KEEP=1 (keep the rows)  NS=tracewell  CH_POD=chi-tracewell-tracewell-0-0-0
set -euo pipefail

NS="${NS:-tracewell}"
CH_POD="${CH_POD:-chi-tracewell-tracewell-0-0-0}"
ROWS="${PROOF_ROWS:-100}"

q() {
  kubectl exec -n "$NS" "$CH_POD" -- clickhouse-client -u tracewell --password tracewell --format TSV -q "$1"
}
fail() { echo "FAIL: $1" >&2; exit 1; }

echo "Inserting $ROWS rows through the Distributed table tracewell.traces..."
# distributed_foreground_insert=1: the insert returns only after the shard has the rows (the writer's rule).
q "INSERT INTO tracewell.traces
     (project_id, trace_id, session_id, user_id, name, tags, start_time, end_time, status,
      span_count, total_tokens, cost_usd, idempotency_key)
   SELECT 'proof', concat('proof-', toString(number)), '', '', 'proof', CAST([], 'Array(String)'),
          now64(6), now64(6), 'ok', 1, 0, 0, concat('proof-', toString(number))
   FROM numbers($ROWS)
   SETTINGS distributed_foreground_insert = 1"

# Replication is asynchronous, so wait for every replica to catch up before comparing.
q "SYSTEM SYNC REPLICA ON CLUSTER tracewell tracewell.traces_local" >/dev/null

echo
echo "Rows per replica (from the local tables; shard = which half of the data, replica = which copy):"
q "SELECT _shard_num AS shard, hostName() AS pod, count() AS rows
   FROM clusterAllReplicas('tracewell', tracewell.traces_local)
   WHERE project_id = 'proof'
   GROUP BY shard, pod ORDER BY shard, pod" | column -t

echo
echo "Active parts on disk (system.parts):"
q "SELECT _shard_num AS shard, hostName() AS pod, sum(rows) AS rows, count() AS parts
   FROM clusterAllReplicas('tracewell', system.parts)
   WHERE database = 'tracewell' AND table = 'traces_local' AND active
   GROUP BY shard, pod ORDER BY shard, pod" | column -t

echo
total=$(q "SELECT count() FROM tracewell.traces FINAL WHERE project_id = 'proof'")
shards_with_rows=$(q "SELECT count(DISTINCT _shard_num) FROM clusterAllReplicas('tracewell', tracewell.traces_local) WHERE project_id = 'proof'")
mismatched=$(q "SELECT count() FROM (
                  SELECT _shard_num AS shard, uniqExact(c) AS distinct_counts
                  FROM (SELECT _shard_num, hostName() AS h, count() AS c
                        FROM clusterAllReplicas('tracewell', tracewell.traces_local)
                        WHERE project_id = 'proof' GROUP BY _shard_num, h)
                  GROUP BY shard HAVING distinct_counts > 1)")

echo "Distributed total: $total (expected $ROWS)"
echo "Shards holding rows: $shards_with_rows (expected 2)"
echo "Shards whose replicas disagree: $mismatched (expected 0)"

if [ "${PROOF_KEEP:-0}" != "1" ]; then
  q "ALTER TABLE tracewell.traces_local ON CLUSTER tracewell DELETE WHERE project_id = 'proof'" >/dev/null
  echo "Removed the proof rows (PROOF_KEEP=1 keeps them)."
fi

[ "$total" = "$ROWS" ] || fail "expected $ROWS rows through the Distributed table, got $total"
[ "$shards_with_rows" = "2" ] || fail "rows are on $shards_with_rows shard(s), expected 2"
[ "$mismatched" = "0" ] || fail "replicas of a shard hold different row counts"
echo "PASS: rows are sharded across both shards and replicated within each."
