import { fromJsonString } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import { Tracewell } from "../src/client.js";
import { IngestBatchSchema, SpanKind, Status } from "../src/gen/trace_pb.js";

interface SpanJSON {
  spanId: string;
  traceId: string;
  parentSpanId: string;
  name: string;
  kind: string;
  status: string;
  errorMessage: string;
  metadata: Record<string, unknown>;
  generation?: {
    provider: string;
    model: string;
    prompt: string;
    completion: string;
    params: Record<string, unknown>;
    usage: { promptTokens: number; completionTokens: number; totalTokens: number; costUsd: number };
  };
}
interface TraceJSON {
  traceId: string;
  name: string;
  sessionId: string;
  userId: string;
  tags: string[];
  spans: SpanJSON[];
}

/** A client whose batches are kept in memory instead of sent. */
function newClient() {
  const bodies: string[] = [];
  const tw = new Tracewell({
    flushIntervalMs: 60_000,
    transport: async (body) => {
      bodies.push(body);
      return { status: 202 };
    },
  });
  const traces = async (): Promise<TraceJSON[]> => {
    await tw.flush();
    return bodies.flatMap((b) => (JSON.parse(b) as { traces: TraceJSON[] }).traces);
  };
  return { tw, bodies, traces };
}

const tick = () => new Promise((r) => setTimeout(r, 1));

describe("span tree", () => {
  it("nests spans across awaits and sends the trace once, when it ends", async () => {
    const { tw, traces } = newClient();
    await tw.trace("chat turn", { sessionId: "s1", userId: "u1", tags: ["a"] }, async () => {
      await tw.span("outer", { kind: "chain" }, async () => {
        await tick();
        await tw.span("inner", { kind: "tool", metadata: { k: 1 } }, async () => tick());
      });
      tw.span("sibling", () => {});
    });

    const [t] = await traces();
    expect(t).toMatchObject({ name: "chat turn", sessionId: "s1", userId: "u1", tags: ["a"] });
    expect(t!.traceId).toMatch(/^[0-9a-f]{32}$/);
    const byName = Object.fromEntries(t!.spans.map((s) => [s.name, s]));
    expect(t!.spans.map((s) => s.name)).toEqual(["inner", "outer", "sibling"]); // finish order
    expect(byName.outer!.parentSpanId).toBe("");
    expect(byName.inner!.parentSpanId).toBe(byName.outer!.spanId);
    expect(byName.sibling!.parentSpanId).toBe("");
    expect(byName.inner).toMatchObject({ kind: "SPAN_KIND_TOOL", status: "STATUS_OK", metadata: { k: 1 } });
    expect(byName.outer!.kind).toBe("SPAN_KIND_CHAIN");
    expect(byName.sibling!.kind).toBe("SPAN_KIND_UNSPECIFIED");
    for (const s of t!.spans) {
      expect(s.spanId).toMatch(/^[0-9a-f]{16}$/);
      expect(s.traceId).toBe(t!.traceId);
    }
  });

  it("keeps concurrent traces apart", async () => {
    const { tw, traces } = newClient();
    await Promise.all(
      ["a", "b"].map((name) =>
        tw.trace(name, async () => {
          await tick();
          await tw.span(`${name}.span`, async () => tick());
        }),
      ),
    );
    const sent = await traces();
    for (const t of sent) expect(t.spans.map((s) => s.name)).toEqual([`${t.name}.span`]);
  });

  it("a span with no open trace starts one named after it", async () => {
    const { tw, traces } = newClient();
    tw.span("lonely", () => {});
    const [t] = await traces();
    expect(t!.name).toBe("lonely");
    expect(t!.spans.map((s) => s.name)).toEqual(["lonely"]);
  });

  it("passes return values through, sync and async", async () => {
    const { tw } = newClient();
    expect(tw.trace("t", () => 42)).toBe(42);
    await expect(tw.span("s", async () => "x")).resolves.toBe("x");
  });

  it("waits for a non-native thenable before ending the span", async () => {
    const { tw, bodies, traces } = newClient();
    let resolve!: (v: string) => void;
    const native = new Promise<string>((r) => (resolve = r));
    const thenable: PromiseLike<string> = { then: (ok, bad) => native.then(ok, bad) };
    const result = tw.span("custom", () => thenable);
    await tw.flush();
    expect(bodies).toHaveLength(0); // still open: the trace isn't sent until the thenable settles
    resolve("done");
    await expect(result).resolves.toBe("done");
    const [t] = await traces();
    expect(t!.spans.map((s) => s.name)).toEqual(["custom"]);
  });

  it("rejects an unknown kind", () => {
    const { tw } = newClient();
    expect(() => tw.span("s", { kind: "nope" as "tool" }, () => {})).toThrow(/unknown span kind/);
  });
});

describe("errors", () => {
  it("an error leaving a span marks it ERROR and is rethrown unchanged", async () => {
    const { tw, traces } = newClient();
    const boom = new TypeError("bad input");
    expect(() => tw.trace("t", () => tw.span("sync", () => { throw boom; }))).toThrow(boom);
    await expect(
      tw.trace("t2", () => tw.span("async", async () => { await tick(); throw boom; })),
    ).rejects.toBe(boom);

    const sent = await traces();
    expect(sent).toHaveLength(2); // the traces are still sent
    for (const t of sent) {
      expect(t.spans[0]).toMatchObject({ status: "STATUS_ERROR", errorMessage: "TypeError: bad input" });
    }
  });

  it("setError marks a span failed without throwing", async () => {
    const { tw, traces } = newClient();
    tw.span("s", (s) => s.setError("upstream said no"));
    const [t] = await traces();
    expect(t!.spans[0]).toMatchObject({ status: "STATUS_ERROR", errorMessage: "upstream said no" });
  });
});

describe("generation", () => {
  it("records provider, model, prompt, params, output and usage", async () => {
    const { tw, traces } = newClient();
    tw.generation(
      "llm",
      { provider: "anthropic", model: "claude-opus-5-5", prompt: [{ role: "user", content: "hi" }], params: { max_tokens: 5 } },
      (g) => g.setOutput("Hello!", { promptTokens: 9, completionTokens: 3, costUsd: 0.0000045 }),
    );
    const [t] = await traces();
    expect(t!.spans[0]!.kind).toBe("SPAN_KIND_GENERATION");
    expect(t!.spans[0]!.generation).toEqual({
      provider: "anthropic",
      model: "claude-opus-5-5",
      prompt: '[{"role":"user","content":"hi"}]',
      completion: "Hello!",
      params: { max_tokens: 5 },
      usage: { promptTokens: 9, completionTokens: 3, totalTokens: 12, costUsd: 0.0000045 },
    });
  });

  it("a prompt that can't be JSON-encoded doesn't stop the call", async () => {
    const { tw, traces } = newClient();
    const circular: Record<string, unknown> = { role: "user" };
    circular.self = circular;
    const out = tw.generation("llm", { provider: "p", model: "m", prompt: circular }, () => "called");
    expect(out).toBe("called");
    const [t] = await traces();
    expect(t!.spans[0]!.generation!.prompt).toBe("[object Object]");
  });

  it("setOutput on a plain span is a usage error", () => {
    const { tw } = newClient();
    expect(() => tw.span("s", (s) => s.setOutput("x"))).toThrow(/only for spans opened with/);
  });
});

describe("observe", () => {
  it("wraps sync and async functions in a span and keeps `this`", async () => {
    const { tw, traces } = newClient();
    const add = tw.observe((a: number, b: number) => a + b, { name: "add", kind: "tool" });
    const fetchUpper = tw.observe(async function fetchIt(x: string) {
      await tick();
      return x.toUpperCase();
    });
    const obj = {
      base: 10,
      plus: tw.observe(function plus(this: { base: number }, n: number) {
        return this.base + n;
      }),
    };

    expect(add(1, 2)).toBe(3);
    await expect(fetchUpper("a")).resolves.toBe("A");
    expect(obj.plus(5)).toBe(15);

    const sent = await traces();
    expect(sent.map((t) => t.spans[0]!.name)).toEqual(["add", "fetchIt", "plus"]);
    expect(sent[0]!.spans[0]!.kind).toBe("SPAN_KIND_TOOL");
  });
});

describe("wire format", () => {
  it("is valid protojson for IngestBatch", async () => {
    const { tw, bodies } = newClient();
    await tw.trace("t", { tags: ["x"] }, async () => {
      tw.generation("g", { provider: "p", model: "m", prompt: "hi" }, (g) => g.setOutput("yo", { promptTokens: 1 }));
      tw.span("s", { metadata: { nested: { a: [1, "b", null] } } }, () => {});
    });
    await tw.flush();

    const batch = fromJsonString(IngestBatchSchema, bodies[0]!);
    expect(batch.batchSeq).toBe(1n);
    const [gen, plain] = batch.traces[0]!.spans;
    expect(gen!.kind).toBe(SpanKind.GENERATION);
    expect(gen!.status).toBe(Status.OK);
    expect(gen!.generation!.usage!.totalTokens).toBe(1);
    expect(plain!.metadata).toEqual({ nested: { a: [1, "b", null] } });
    expect(batch.traces[0]!.startTime!.seconds).toBeGreaterThan(0n);
  });
});
