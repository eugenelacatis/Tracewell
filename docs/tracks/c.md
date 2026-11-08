# Track C: Evals & alerting

Folders: `workers/evals/`, `services/alerter/`
DS concepts to demo: consumer groups, stream processing.

Round 1 (eval worker with latency and empty-completion scorers, error-rate alerter) is done, merged in
PR #7. Its brief is in git history.

## Round 2: real evaluators, full alerter

Branch: `c/round-2`

### Round 1 debt first
- `workers/evals` `/readyz` hangs instead of answering. Fix it so it returns 200/503 within 2s.
- Add `ruff` as a dev dependency and fix the lint findings, so Track B can make the CI step blocking.

### Build

1. **New evaluators** (`workers/evals`). Names, score direction and labels are fixed in
   `docs/contracts/README.md` (Evaluators).
   - `llm_judge`: ask the judge model (Anthropic SDK, `EVALS_JUDGE_MODEL`) to rate whether the
     completion answers the prompt helpfully and correctly. Ask for JSON `{score, reasoning}`.
     Sampling, retries and "no key means off" follow the contract. Run judge calls concurrently
     (a thread pool or asyncio) so one slow call doesn't stall the partition.
   - `toxicity`: a local model (Detoxify or a small Hugging Face classifier). No API calls. Load it
     once at startup.
   - `pii`: regexes for email, phone, credit card (Luhn check) and US SSN. Presidio is optional.
     Never put the matched text in `reasoning`.
   - Tests: each evaluator on fixed inputs. Mock the judge client.

2. **Alerter, full rules** (`services/alerter`). Semantics in `docs/contracts/http.md` (Alert rules).
   - Add `cost_usd` and `p95_latency_ms` next to `error_rate`.
   - Sliding windows: evaluate every 10s over the last `window_seconds`, not tumbling buckets. For
     p95, keep the raw durations in the window (or a t-digest) so the result is a real p95.
   - Minimum-sample guard (5) for `error_rate` and `p95_latency_ms`.
   - Load rules `WHERE enabled AND deleted_at IS NULL`, still every 30s, so dashboard edits take effect.
   - Slack delivery: POST `{"text": ...}` to the incoming-webhook URL in `target`. The message names
     the rule, project, value, threshold and window, and links to
     `http://localhost:3000/traces?from=...&to=...`.
   - A failed delivery retries 3 times with backoff, is counted in `alerter_delivery_errors_total`,
     and still writes the `alert_events` row.
   - Tests: each metric's window math, min samples, cooldown, the deleted-rule filter, a Slack payload
     against a fake server.

3. **Metrics**: the evals and alerter metrics in `docs/contracts/README.md`.

### Contracts used
`docs/contracts/README.md` (evaluators, env vars, metrics), `docs/contracts/http.md` (alert-rule
semantics), `docs/contracts/postgres_round2.sql` (`deleted_at`), `proto/trace.proto` (`EvalResult`).

### Done when
- `uv run pytest` and `uv run ruff check` pass in `workers/evals`. `go test ./...` passes in
  `services/alerter`.
- `chatbot.py --fake --count 5` produces `toxicity` and `pii` rows for all 5 traces, plus
  `llm_judge` rows when a key is set.
- A `cost_usd` rule with a low threshold fires one Slack message (a test webhook or a local echo
  server works) and one `alert_events` row, then stays quiet through its cooldown.
- Two eval worker containers share the 6 partitions (`kafka-consumer-groups --describe` shows the
  split), and the eval count doesn't change.
- `docs/services/evals.md` and `docs/services/alerter.md` are updated.

### Deferred
Scaling eval workers by partition under load, plus HPA (round 3). Persisting alerter window state
across restarts (round 3).
