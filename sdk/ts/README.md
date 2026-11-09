# @tracewell/sdk (TypeScript)

Track A. Records traces, nested spans and LLM generations from a Node program and sends them to
ingest-svc in the background, over HTTP (protojson, the default) or gRPC. Same behavior and wire rules
as the Python SDK (`sdk/python`). Node 20+; no browser support.

## Use

```ts
import { Tracewell } from "@tracewell/sdk";

const tw = new Tracewell(); // TRACEWELL_HOST, TRACEWELL_API_KEY

await tw.trace("chat turn", { sessionId: "chat-42", userId: "user-7", tags: ["prod"] }, async () => {
  await tw.span("retrieve", { kind: "retrieval" }, async () => { /* ... */ });
  await tw.generation(
    "anthropic.messages",
    { provider: "anthropic", model: "claude-opus-5-5", prompt: messages, params: { max_tokens: 1024 } },
    async (gen) => {
      const reply = await callModel(messages);
      gen.setOutput(reply.text, { promptTokens: 9, completionTokens: 3, costUsd: 0.0000045 });
    },
  );
});

const lookup = tw.observe(async function lookup(id: string) { /* ... */ }, { kind: "tool" });
```

- `trace(name, [opts], fn)`, `span(name, [opts], fn)` and `generation(name, opts, fn)` run `fn` and
  return what it returns, sync or async. The trace is sent once, when `fn` finishes, with all of its
  spans.
- Nesting follows the async call chain (`AsyncLocalStorage`), across `await`s and concurrent
  requests. A span opened outside any trace starts a trace named after it.
- An error leaving a span marks it `ERROR` (`"<name>: <message>"`) and is rethrown unchanged.
  `span.setError(msg)` marks it failed without throwing.
- `observe(fn, { name?, kind?, metadata? })` wraps a function so every call runs in a span named after
  it. A wrapped method keeps its `this`.
- Span kinds: `generation`, `retrieval`, `tool`, `chain`, or none.

## Options

| Option | Env var | Default |
|---|---|---|
| `host` | `TRACEWELL_HOST` | `http://localhost:8080` (the gateway; use `http://localhost:8081` for ingest-svc directly) |
| `apiKey` | `TRACEWELL_API_KEY` | `""` |
| `transport` | | `"http"`. `"grpc"` calls `tracewell.v1.IngestService/Ingest` on the same host. A function replaces the sender (tests use it). |
| `flushIntervalMs` | | `1000` |
| `maxBatchTraces` | | `100` |

With `"grpc"` directly against ingest-svc, point `host` at its gRPC port (`http://localhost:9091`).
Through the gateway, HTTP and gRPC share `:8080`. `https://` hosts use TLS.

## Sending, retries and exit

- Finished traces queue in memory (at most 10,000; more are dropped with a warning). A batch goes
  out at 100 traces, at 4 MB (below ingest's 5 MB limit), or 1s after the first queued trace.
  One batch is in flight at a time.
- Each batch gets the next `batchSeq` (from 1) and `idempotencyKey = "<sdkInstanceId>:<batchSeq>"`.
  `sdkInstanceId` is a random UUID per `Tracewell` instance. A retry resends the identical body.
- Retry table from `docs/contracts/http.md`: `202` (any 2xx) done; `400`/`401`/`413` dropped with a warning
  (retrying can't help); `429`, `5xx` and network errors retried after 0.5, 1, 2, 4 and 8s (±20%
  jitter), then dropped. A `429` waits at least `Retry-After` (capped at 30s). Over gRPC,
  `RESOURCE_EXHAUSTED` with a `retry-after` trailer counts as `429` and without one as `413`,
  `UNAVAILABLE` as `503`, and any other code as a network error.
- It never throws into your code because Tracewell is down. Failures are `console.warn` lines starting
  with `tracewell:`. A `bigint` anywhere in a trace is sent as its decimal string. A trace that still
  can't be JSON-encoded (a circular `metadata`, say) is dropped with a warning. A prompt that can't be
  encoded is stored as `String(prompt)`, so your LLM call still runs.
- Exit: the interval timer doesn't keep the process alive. When the event loop empties, the SDK
  flushes on `beforeExit`. `process.exit()` skips that, so `await tw.flush()` first. The SDK doesn't
  install SIGTERM/SIGINT handlers, because that would change how your program responds to Ctrl-C.
  Flush in your own handler.

## Generated code

`src/gen/trace_pb.ts` is generated from `proto/trace.proto` by `cd proto && buf generate` (see
`docs/contracts/README.md`). Never edit it. It needs `@bufbuild/protobuf` at the plugin's version
(2.17.0), which `package.json` pins.

## Develop

```
cd sdk/ts
npm install      # also builds dist/ (prepare)
npm test         # vitest
npm run lint     # tsc --noEmit + eslint
npm run build    # dist/
```

Tests: batching at 100 traces, on the interval and by bytes; sending resumes after the interval
fires on an empty queue; seq once per batch and the same body on
retry; the backoff schedule and giving up; no retry on 400/401/413; `Retry-After` honored and capped;
a throwing transport and an unencodable trace don't break the exporter; bigints become strings; span
nesting across awaits and concurrent traces; spans wait for non-native thenables; error marking; an
unencodable prompt doesn't stop the call; `observe` on sync, async and method functions; generations and
usage totals; output that parses as `IngestBatch` protojson; the HTTP transport against a local
server; the gRPC transport against an in-process grpc-js server, including every status mapping and
the `retry-after` trailer.

## Example

`examples/chatbot.ts` is the TS twin of `examples/chatbot.py` (`--fake`, `--error`, `--count N`,
plus `--grpc`):

```
(cd sdk/ts && npm install) && cd examples && npm install
TRACEWELL_API_KEY=tw_dev_localkey npx tsx chatbot.ts --fake --count 5          # HTTP via the gateway
TRACEWELL_API_KEY=tw_dev_localkey npx tsx chatbot.ts --fake --count 5 --grpc   # gRPC via the gateway
```

Without the gateway, set `TRACEWELL_HOST=http://localhost:8081` (HTTP) or `http://localhost:9091`
(gRPC). With `ANTHROPIC_API_KEY` set and no `--fake`, it calls Claude (`claude-opus-5-5`, with
server-side refusal fallback) through `@anthropic-ai/sdk`.

Verified against compose (ingest and writer on the host, no gateway yet): 5 traces over HTTP and 5
over gRPC landed in `tracewell.traces` with the printed costs; `--error` stored `ERROR` traces;
`tw_demo_otherkey` traces landed under `proj_demo`.
