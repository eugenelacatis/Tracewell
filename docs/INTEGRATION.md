# Integration run

We run this together after every round's PRs are merged. Each step names the track that owns a failure.
Log every failure as a GitHub issue with that track's label, then fill in the assessment at the bottom.

## Round 1: walking skeleton

Run everything from the repo root on an up-to-date `main`.

| # | Step | Expected | Owner if it fails |
|---|---|---|---|
| 1 | `docker compose down -v && docker compose --profile apps up -d --build` | Every container is `healthy` or `exited (0)` for the init jobs | B |
| 2 | `for p in 8081 8082 8083 8084 8085; do curl -fs localhost:$p/readyz && echo " $p ok"; done` | 5 x ok | owner of that port |
| 3 | `cd sdk/python && TRACEWELL_API_KEY=tw_dev_localkey uv run python ../../examples/chatbot.py --fake --count 5` | Exits 0, no warnings | A |
| 4 | `curl -s 'localhost:8123/?user=tracewell&password=tracewell' --data "SELECT count() FROM tracewell.traces FINAL"` | `5`, within ~2s of step 3 | A |
| 5 | Same, with `SELECT evaluator, count() FROM tracewell.evals FINAL GROUP BY evaluator` | `latency 5` and `empty_completion 5` | C, or A if the rows are on `evals.results` but not in the table |
| 6 | `curl -s -H 'Authorization: Bearer tw_dev_localkey' localhost:8082/v1/traces \| jq '.traces \| length'` | `5` | D |
| 7 | Open `localhost:3000/traces`, then click a trace | 5 rows. Detail shows the span tree, prompt and completion, and 2 eval scores | D |
| 8 | Run step 3 with `--error --count 10` | Within ~60s: one alerter log line for `error rate above 50%`, one row in `alert_events` | C |
| 9 | Run step 3 again, then `docker compose restart writer`, then step 4 | Count is exactly 10 more than the last check, no duplicates | A |
| 10 | `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups` | Groups `writer`, `evals`, `alerter` with lag 0 | owner of the lagging group |
| 11 | Open a no-op PR | CI is green | B |

Step 3 is the walking-skeleton milestone. Once it shows up in step 7, the skeleton is done.

## Assessment (copy this block per round)

```
### Round N assessment (date)
Passed steps:
Failed steps -> issue links:
Contract changes we needed (and whether they were merged):
What took longer than expected:
Carry-over into round N+1:
Next round focus per track:
  A:
  B:
  C:
  D:
```

Paste the filled-in block into Claude with "Write the round N+1 contracts and briefs from this." That
updates `docs/contracts/`, `docs/tracks/*.md`, and adds the next round's table above.
