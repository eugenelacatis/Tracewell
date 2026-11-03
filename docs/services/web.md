# web (dashboard)

Track D. Next.js 16 App Router, TypeScript strict. It shows traces that query-svc returns.

## Pages

| Route | What it shows |
|---|---|
| `/` | Redirects to `/traces`. |
| `/traces` | Table of name, start time, duration, status, span count, tokens and cost, newest first, 50 per page. |
| `/traces/[traceId]` | Trace header, span waterfall, detail panel for the selected span, and all eval scores grouped by span. |

Waterfall: spans are indented by their depth in the `parentSpanId` tree. Each bar starts at the
span's offset from the trace start, and its width is the span's share of the trace duration. LLM
spans are purple and failed spans are red. A span whose parent is missing shows as a root.

The selected span is in the URL (`?span=<spanId>`), so the detail panel is plain server-rendered
HTML with no client JavaScript, and you can share a link to one span. With no `?span`, the panel
shows the first GENERATION span.

Paging: query-svc only gives a cursor for the next page, so `/traces?c=` keeps every cursor visited
so far, joined with `.`. Next appends one and Previous drops the last.

States: each page streams its data inside a `<Suspense>` boundary, and the skeleton rows are the
fallback while it loads. An empty project shows the SDK command to send a first trace. If query-svc
is unreachable or returns an error, the page shows the message and status code. An unknown trace ID
renders the not-found page.

## API key handling

All calls to query-svc happen in server components (`lib/query.ts`). `WEB_API_KEY` has no
`NEXT_PUBLIC_` prefix, so Next never puts it in browser bundles, and `lib/query.ts` imports
`server-only`, so the build fails if a client component ever imports it.

## Env vars

| Var | Default |
|---|---|
| `WEB_QUERY_URL` | `http://localhost:8082` |
| `WEB_API_KEY` | none; every page shows a config error until it's set |

Both are read per request, so set them on the container at runtime. They are not baked in at build
time.

## Run locally

Start query-svc first (see `docs/services/query.md`), then:

```
cd web
npm install
WEB_API_KEY=tw_dev_localkey npm run dev      # http://localhost:3000/traces
```

Docker, from the repo root:

```
docker build -f web/Dockerfile -t tracewell/web .
docker run -p 3000:3000 -e WEB_QUERY_URL=http://host.docker.internal:8082 -e WEB_API_KEY=tw_dev_localkey tracewell/web
```

The image uses Next's `standalone` output (a `server.js` plus only the packages it needs). The
Dockerfile copies source files one by one, so a local `node_modules/` or `.next/` never ends up in
the image.

## Test

```
cd web
npm run lint && npm run build
```

There are no unit tests yet. The integration run (`docs/INTEGRATION.md` step 7) checks the pages
end to end.
