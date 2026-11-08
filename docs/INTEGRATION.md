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

## Round 2: gateway, tenancy, cluster

Run on `main` after all round 2 PRs merge. Steps 1-10 use compose. Steps 11-13 use kind. Everything
goes through the gateway on 8080. Alongside `apps`, start `monitoring` for step 10.

| # | Step | Expected | Owner if it fails |
|---|---|---|---|
| 1 | `docker compose --profile apps --profile monitoring up -d --build` | All healthy, including `gateway` and `web` | B (D for gateway/web health) |
| 2 | `cd sdk/python && TRACEWELL_HOST=http://localhost:8080 TRACEWELL_API_KEY=tw_dev_localkey uv run python ../../examples/chatbot.py --fake --count 5` | Exits 0; 5 new traces | A (D if the gateway drops it) |
| 3 | Same with `examples/chatbot.ts`, over HTTP and then gRPC | 5 more traces each time | A |
| 4 | Send 5 traces with `tw_demo_otherkey` | Rows land with `project_id = 'proj_demo'` | A |
| 5 | Log in at `localhost:3000` as `dev@tracewell.local` / `tracewell` | Sees `proj_dev` traces only. A `proj_demo` trace URL returns 404 | D |
| 6 | Check `evals` grouped by evaluator | `toxicity` and `pii` for every generation span; `llm_judge` if a key is set | C |
| 7 | Overview, sessions and trace pages | Charts have data, session timeline renders, new eval scores show | D |
| 8 | Create a `cost_usd` rule (threshold 0) in `/alerts` with a webhook target, then repeat step 2 | Within ~40s: one delivery, one `alert_events` row, the event shows in `/alerts` | C (D if the rule didn't save) |
| 9 | Burst 500 ingest requests in one second | Some `429`s; every trace still lands after SDK retries | A |
| 10 | Grafana on `localhost:3300` | Dashboard shows ingest rate, lag, latency, cache hits | B |
| 11 | `deploy/kind` setup script, then step 2 against the kind gateway | 5 traces in the Distributed table, visible in the dashboard | B |
| 12 | `system.parts` on each shard and replica | Rows split across 2 shards. Both replicas of a shard match | B |
| 13 | Delete one Kafka broker pod and one ClickHouse replica pod during step 2 with `--count 200` | Count matches 200, dashboard keeps serving | B |

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

### Round 1 assessment (2026-11-06)
Passed steps: none run end to end. The full compose stack (all 6 images plus Kafka and ClickHouse)
took over 10 minutes to build and about 4 GB of RAM, which the laptop couldn't spare. The dashboard
was checked on its own against a fake query-svc: trace list and detail both render correctly.
Failed steps -> issue links: none logged, since nothing was run.
Contract changes we needed (and whether they were merged): none during round 1.
What took longer than expected: running the full stack locally. A repo inside an iCloud-synced
folder (`~/Documents`) made `node_modules` and `git` hang until files were re-downloaded. Keep the
repo outside iCloud.
Carry-over into round 2: run the round 1 steps in CI (Track B's smoke test) instead of on laptops.
web `/healthz` and a real 404 for unknown traces (D). evals `/readyz` hang and `ruff` debt (C).
Next round focus per track:
  A: TS SDK, gRPC ingest, Redis rate limit
  B: kind, Strimzi x3, ClickHouse 2x2, Helm, Prometheus/Grafana, a lighter compose mode
  C: llm_judge, toxicity, pii, sliding-window cost/p95 rules, Slack
  D: gateway, login and tenancy, Redis cache, charts, sessions, alert CRUD, API keys
