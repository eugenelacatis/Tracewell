# Track D: Read path & product

Folders: `services/query/`, `web/`, gateway config
DS concepts to demo: microservices behind a gateway, caching.

## Round 1: walking skeleton

Branch: `d/round-1-skeleton`

Show stored traces in the dashboard.

### Build

1. **query-svc** (`services/query`, Go)
   - `GET /v1/traces` and `GET /v1/traces/{traceId}`, exactly as in `docs/contracts/http.md`: auth via
     `api_keys` (60s in-memory cache), keyset cursor, `from`/`to` filters, `FINAL` on every ClickHouse
     read, the error JSON shape.
   - ClickHouse client: `clickhouse-go/v2`. Postgres: `pgx/v5`. Always pass parameters, never format
     user input into SQL.
   - Tests: cursor encode/decode round trip, limit clamping, enum and generation-null mapping, 401/404.
     Run the SQL against the compose ClickHouse in an integration test, seeding a few rows by hand
     until the writer lands.

2. **Dashboard** (`web`, Next.js App Router, TypeScript strict, npm)
   - The server calls query-svc at `WEB_QUERY_URL` with `Authorization: Bearer $WEB_API_KEY`. The key
     never reaches the browser, so fetch in server components or route handlers.
   - `/traces`: a table with name, start time, duration, status, span count, tokens and cost.
     Next/previous paging via the cursor. Newest first.
   - `/traces/[traceId]`: trace header, a span tree or waterfall (indent by `parentSpanId`, bar width by
     duration), a span detail panel with prompt, completion, params and usage, and eval scores grouped
     by span.
   - Empty, loading and error states, because the first demo will start with an empty table.
   - `npm run lint` and `npm run build` clean.

### Contracts used
`docs/contracts/http.md` (query API, auth, error shape), `docs/contracts/clickhouse.sql` (what you
read), `docs/contracts/postgres.sql` (api_keys), `docs/contracts/README.md` (env vars, ports,
Dockerfile layout).

### Done when
- `go test ./...` passes, and `npm run lint && npm run build` passes in `web`.
- With rows in ClickHouse (from the writer, or seeded by hand), `curl -H 'Authorization: Bearer
  tw_dev_localkey' localhost:8082/v1/traces` returns them, and the same traces show at
  `localhost:3000/traces` with working detail pages.
- Dockerfiles for both build from the repo root.
- `docs/services/query.md` and `docs/services/web.md` are written.

### Deferred to later rounds
Gateway (Envoy or Kong: routing, auth, rate limits), Redis cache in query-svc, user login and
org/project switching, session view, cost and latency charts, alert-rule CRUD and alert history
(round 2). Integrity page for completeness (round 4).

Eugene is PM as well as owner of this track, so set time aside each round for integration and the
next round's briefs.
