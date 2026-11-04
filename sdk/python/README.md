# tracewell (Python SDK)

Track A. Records traces, spans and LLM generations in your program and ships them to ingest-svc in
the background. The wire format and retry rules are in `docs/contracts/http.md`.

No runtime dependencies. Python 3.11+.

## Install

From another uv project in this repo:

```
uv add --editable ../../sdk/python
```

## Configure

| Setting | Env var | Default |
|---|---|---|
| `host` | `TRACEWELL_HOST` | `http://localhost:8081` |
| `api_key` | `TRACEWELL_API_KEY` | none (requests get `401` and are dropped) |

Constructor arguments win over env vars: `Tracewell(host="http://...", api_key="tw_...")`.

## Use

```python
from tracewell import Tracewell

tw = Tracewell()  # one per process

with tw.trace("chat turn", session_id="chat-42", user_id="user-7", tags=["example"]):
    with tw.span("handle message", kind="chain"):
        with tw.generation(
            "anthropic.messages",
            provider="anthropic",
            model="claude-haiku-5-5",
            prompt=[{"role": "user", "content": "hi"}],  # str or anything JSON-serializable
            params={"max_tokens": 200},
        ) as gen:
            text = call_the_llm()
            gen.set_output(text, prompt_tokens=9, completion_tokens=3, cost_usd=0.0000045)


@tw.observe(kind="tool")  # also @tw.observe or @tw.observe("name"); async functions work too
def search(q: str) -> list[str]: ...
```

| API | What it does |
|---|---|
| `tw.trace(name, *, session_id, user_id, tags)` | Context manager. Opens a trace. When the block exits, the trace and all its finished spans are queued for sending. |
| `tw.span(name, *, kind, metadata)` | Context manager. Opens a span under the current span. `kind` is `"generation"`, `"retrieval"`, `"tool"`, `"chain"` or `None`. With no open trace, it starts a trace named after itself. |
| `tw.generation(name, *, provider, model, prompt, params, metadata)` | A `generation` span for one LLM call. |
| `span.set_output(completion, *, prompt_tokens, completion_tokens, total_tokens, cost_usd)` | Records the generation's result. `total_tokens` defaults to prompt + completion. |
| `span.set_error(message)` | Marks the span `ERROR` without raising. |
| `@tw.observe(name=None, *, kind=None)` | Runs the function inside a span named after it. |
| `tw.flush()` | Blocks until everything finished so far is sent. Also runs at exit. |

An exception that leaves a span marks it `ERROR` with `"<Type>: <message>"` and is re-raised
unchanged. The current trace and span live in `contextvars`, so nesting follows threads and asyncio
tasks without passing objects around.

## How sending works

- Finished traces go into an in-memory queue (max 10,000; beyond that new traces are dropped with a
  warning). A background thread sends a batch every 1s, or as soon as 100 traces are waiting, or
  when a batch would pass 4 MB (ingest's limit is 5 MB).
- Each process picks a random `sdkInstanceId` (UUID4 hex). Each batch gets the next `batchSeq`
  (1, 2, 3, ...) and `idempotencyKey = "<sdkInstanceId>:<batchSeq>"`. A retry resends the identical
  body, so the seq goes up once per batch, not once per attempt. The writer uses these to dedupe and,
  later, to detect missing batches.
- Responses: `202` done. `400`, `401`, `413` and other `4xx`: drop the batch and log a warning,
  since resending can't help. `5xx` or a network error: retry after 0.5s, 1s, 2s, 4s, 8s (each
  ±20% jitter), then drop and log. `429`: same retries, waiting at least `Retry-After`.
- Tracewell never raises into your code because the server is down. Problems are logged as
  warnings on the `tracewell` logger.
- `flush()` at exit waits for retries, so if ingest is down a short script can take ~15s to exit
  before it gives up.

## Develop

```
cd sdk/python
uv run pytest
uv run ruff check && uv run ruff format --check
```

Tests cover the batch thresholds (count, time, bytes), seq and idempotency key per batch (not per
retry), the 5xx backoff schedule, no retry on `400`/`401`/`413`, `Retry-After`, and the span tree,
error and generation fields in the JSON.
