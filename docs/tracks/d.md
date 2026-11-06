# Track D: Read path & product

Folders: `services/query/`, `web/`, `gateway/`
DS concepts to demo: microservices behind a gateway, caching.

Round 1 (query-svc trace list and detail, dashboard trace pages) is done, merged in PR #5. Its brief is
in git history.

## Round 2: gateway, tenancy, the rest of the dashboard

Branch: `d/round-2`

### Round 1 debt first
- `GET /healthz` in `web` (an App Router route handler returning `200 ok`) so compose can health-check it.
- The unknown-trace page returns HTTP 200 with the not-found UI, because the page streams. Return a
  real 404 (call `notFound()` before any `Suspense` boundary streams).

### Build

1. **Gateway** (`gateway/envoy.yaml`)
   - The routes, body limit, `x-request-id` and per-IP local rate limit in `docs/contracts/README.md`
     (Gateway). gRPC to ingest needs an HTTP/2 upstream cluster.
   - Admin on 9901, with its stats scraped by Prometheus.
   - Test: `curl` each route through 8080, and a burst over the local limit gets `429`.

2. **Auth and tenancy** (`services/query`)
   - `POST /v1/auth/login`, `/logout`, `GET /v1/me`, project and API-key routes, as in
     `docs/contracts/http.md`. bcrypt (`golang.org/x/crypto/bcrypt`), Redis sessions, login rate limit.
   - One middleware resolves either an API key or a session plus `X-Tracewell-Project` to a
     `project_id`. Every handler reads the project from that middleware, never from the request.
   - Tests: login good and bad, a session reaching a project outside its org gets 404, a missing
     project header gets 400, an API key ignores the header, a revoked key gets 401.

3. **New read endpoints** (`services/query`): sessions, metrics, eval summary, alert-rule CRUD with
   validation, alert events. All per `docs/contracts/http.md`. Parameterized SQL only.

4. **Redis cache** (`services/query`): key format and TTLs from the contracts, `X-Cache` header, fail
   open. Test: the second identical request is a `HIT`, and a different project never gets the first
   project's cached body.

5. **Dashboard** (`web`). Server-side calls go to `WEB_QUERY_URL` (the gateway). The session token
   sits in an `httpOnly` cookie and never reaches client JS.
   - `/login`, logout, and a project switcher in the header (from `/v1/me`). The selected project goes
     in a cookie and becomes `X-Tracewell-Project`.
   - `/` overview: trace count, error rate, p95 latency and cost charts from `/v1/metrics`, with a
     time-range picker and a model filter. Eval summary cards.
   - `/sessions` and `/sessions/[sessionId]`: a session list, then a timeline of that session's traces.
   - `/alerts`: rule list with create, edit, enable/disable and delete, plus alert history from
     `/v1/alert-events`.
   - `/settings/api-keys`: list, create (show the key once), revoke.
   - Trace detail: show the new evaluators, with the same color rule for every score (1 is good).
   - Design pass on the whole dashboard. The round 1 pages are functional but plain.

### Contracts used
`docs/contracts/http.md` (auth, sessions, metrics, alert rules, cache), `docs/contracts/README.md`
(gateway, Redis keys, metrics, evaluators), `docs/contracts/postgres_round2.sql`.

### Done when
- `go test ./...` passes, and `npm run lint && npm run build` passes in `web`.
- Through `localhost:8080`: log in as `dev@tracewell.local` / `tracewell`, see `proj_dev` traces, and
  never see `proj_demo` (traces sent with `tw_demo_otherkey` don't show up, and a direct URL to one
  returns 404).
- Creating a `cost_usd` rule in `/alerts` makes the alerter fire it within ~40s of the next cost spike.
- A repeated `/v1/metrics` call shows `X-Cache: HIT`.
- `docs/services/query.md`, `docs/services/web.md` and a new `docs/services/gateway.md` are written.

### Deferred
Integrity (completeness) page (round 4). Gateway-level auth (not planned).

Eugene is PM as well as owner of this track, so set time aside for integration and the round 3 briefs.
