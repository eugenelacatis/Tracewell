# Track briefs

Each round, every track gets one brief here (`a.md`, `b.md`, `c.md`, `d.md`). The brief says what to
build this round, which contracts it depends on, and how we'll check that it works.

## The loop

1. **Contracts.** Before a round starts, anything two tracks both touch goes into `docs/contracts/` and
   gets merged. Nobody invents a table, endpoint or env var that another track also uses.
2. **Build.** Each person starts Claude Code from the repo root on a fresh branch and pastes the
   starter prompt below. Commit as you go and open a PR to `main` when your "done when" checks pass.
   Someone from another track reviews it.
3. **Integrate.** Once all four PRs are merged, we run `docs/INTEGRATION.md` together. Anything that
   breaks becomes a GitHub issue labelled with the track that owns the fix.
4. **Assess.** We fill in the assessment at the bottom of `docs/INTEGRATION.md`, then the next round's
   briefs and contracts get written from it.

## Starter prompt

Replace `X` with your track letter and `N` with the round number:

```
I own Track X of Tracewell. Read CLAUDE.md, docs/PLAN.md, everything in docs/contracts/,
and docs/tracks/X.md. Then do Round N of my brief.
Rules: only touch my track's folders. If something I need is missing from a contract or
owned by another track, stop and tell me instead of working around it. Explain what you
build in plain terms, since I'm new to parts of this stack. Commit in small conventional
commits on my track branch.
```

## Rules for every track, every round

- Stay in your folders (table in `CLAUDE.md`). If you need a change elsewhere, open an issue for the
  owning track or raise it in the sync.
- Code against `docs/contracts/`, not against another track's code. If a contract is wrong or missing
  something, propose the change in a PR that touches only `docs/contracts/` or `proto/`.
- Go services: `cmd/<service>/main.go` plus `internal/...`, `log/slog` JSON logs with `trace_id` and
  `project_id` when known, env-var config only, `/healthz` `/readyz` `/metrics`, and a clean SIGTERM
  shutdown (stop taking work, finish in-flight work, flush producers, exit).
- Python: `uv`, type hints on public functions, `ruff` clean.
- Every service gets a `Dockerfile` in its folder, built from the repo root (see
  `docs/contracts/README.md`).
- Write a short `docs/services/<name>.md` for each service you own: what it does, env vars, how to run
  it locally, how to test it. Keep it updated when behavior changes.
- Tests: unit tests for the logic you write (batching, dedupe, scoring, pagination). The integration
  run is the end-to-end test.
