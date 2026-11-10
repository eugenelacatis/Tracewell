import { afterEach, describe, expect, it, vi } from "vitest";

import { Exporter, MAX_RETRY_AFTER_MS, RETRY_DELAYS_MS, type TransportResponse } from "../src/exporter.js";

interface Batch {
  idempotencyKey: string;
  sdkInstanceId: string;
  batchSeq: string;
  traces: { traceId: string }[];
}

/** A transport that records every body it gets and answers from a script (default 202). */
function fakeTransport(responses: TransportResponse[] = []) {
  const bodies: string[] = [];
  const send = async (body: string): Promise<TransportResponse> => {
    bodies.push(body);
    return responses.shift() ?? { status: 202 };
  };
  const batches = () => bodies.map((b) => JSON.parse(b) as Batch);
  return { send, bodies, batches };
}

function recordSleeps() {
  const slept: number[] = [];
  return { slept, sleep: async (ms: number) => void slept.push(ms) };
}

const trace = (i: number) => ({ traceId: `t${i}` });

afterEach(() => vi.restoreAllMocks());

describe("batching", () => {
  it("sends at 100 traces without waiting for the interval", async () => {
    const t = fakeTransport();
    const ex = new Exporter(t.send, { flushIntervalMs: 60_000 });
    for (let i = 0; i < 100; i++) ex.enqueue(trace(i));
    await vi.waitFor(() => expect(t.bodies).toHaveLength(1));
    expect(t.batches()[0]!.traces).toHaveLength(100);
  });

  it("splits into batches of 100 on flush", async () => {
    const t = fakeTransport();
    const ex = new Exporter(t.send, { flushIntervalMs: 60_000 });
    for (let i = 0; i < 250; i++) ex.enqueue(trace(i));
    await ex.flush();
    expect(t.batches().map((b) => b.traces.length)).toEqual([100, 100, 50]);
    expect(t.batches().flatMap((b) => b.traces.map((x) => x.traceId))).toEqual(
      Array.from({ length: 250 }, (_, i) => `t${i}`),
    );
  });

  it("sends a partial batch after the interval", async () => {
    const t = fakeTransport();
    const ex = new Exporter(t.send, { flushIntervalMs: 20 });
    ex.enqueue(trace(1));
    ex.enqueue(trace(2));
    expect(t.bodies).toHaveLength(0);
    await vi.waitFor(() => expect(t.bodies).toHaveLength(1));
    expect(t.batches()[0]!.traces).toHaveLength(2);
  });

  it("splits by bytes without losing traces", async () => {
    const t = fakeTransport();
    const ex = new Exporter(t.send, { flushIntervalMs: 60_000, maxBatchBytes: 250 });
    for (let i = 0; i < 10; i++) ex.enqueue({ traceId: `t${i}`, name: "x".repeat(80) });
    await ex.flush();
    expect(t.bodies.length).toBeGreaterThan(1);
    expect(t.batches().flatMap((b) => b.traces)).toHaveLength(10);
  });

  it("drops a trace that can't be encoded, without throwing", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const t = fakeTransport();
    const ex = new Exporter(t.send, { flushIntervalMs: 60_000 });
    const circular: Record<string, unknown> = { traceId: "bad" };
    circular.self = circular;
    expect(() => ex.enqueue(circular)).not.toThrow();
    ex.enqueue(trace(1));
    await ex.flush();
    expect(t.batches()[0]!.traces.map((x) => x.traceId)).toEqual(["t1"]);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("could not encode trace"));
  });

  it("sends a bigint as a string instead of dropping the trace", async () => {
    const t = fakeTransport();
    const ex = new Exporter(t.send, { flushIntervalMs: 60_000 });
    ex.enqueue({ traceId: "t1", n: 2n ** 70n });
    await ex.flush();
    expect(t.batches()[0]!.traces[0]).toEqual({ traceId: "t1", n: "1180591620717411303424" });
  });

  it("keeps sending after the interval fires with nothing queued", async () => {
    const t = fakeTransport();
    const ex = new Exporter(t.send, { flushIntervalMs: 20 });
    for (let i = 0; i < 100; i++) ex.enqueue(trace(i)); // first one arms the timer; the 100th sends
    await vi.waitFor(() => expect(t.bodies).toHaveLength(1));
    await new Promise((r) => setTimeout(r, 50)); // the timer fires on an empty queue
    expect(ex.pending).toBe(false);
    ex.enqueue(trace(100));
    await vi.waitFor(() => expect(t.bodies).toHaveLength(2));
    await ex.flush();
  });

  it("flush with nothing queued resolves", async () => {
    const t = fakeTransport();
    await new Exporter(t.send).flush();
    expect(t.bodies).toHaveLength(0);
  });
});

describe("wire rules", () => {
  it("gives each batch the next seq and an idempotency key built from it", async () => {
    const t = fakeTransport();
    const ex = new Exporter(t.send, { flushIntervalMs: 60_000, maxBatchTraces: 2 });
    for (let i = 0; i < 5; i++) ex.enqueue(trace(i));
    await ex.flush();
    const batches = t.batches();
    expect(batches.map((b) => b.batchSeq)).toEqual(["1", "2", "3"]);
    for (const b of batches) {
      expect(b.sdkInstanceId).toMatch(/^[0-9a-f]{32}$/);
      expect(b.idempotencyKey).toBe(`${ex.sdkInstanceId}:${b.batchSeq}`);
    }
  });

  it("retries resend the same body, and the seq moves on once", async () => {
    const s = recordSleeps();
    const t = fakeTransport([{ status: 503 }, { status: 0 }]);
    const ex = new Exporter(t.send, { flushIntervalMs: 60_000, sleep: s.sleep });
    ex.enqueue(trace(1));
    await ex.flush();
    ex.enqueue(trace(2));
    await ex.flush();
    expect(t.bodies).toHaveLength(4);
    expect(t.bodies[0]).toBe(t.bodies[1]);
    expect(t.bodies[1]).toBe(t.bodies[2]);
    expect(t.batches().map((b) => b.batchSeq)).toEqual(["1", "1", "1", "2"]);
  });
});

describe("retries", () => {
  it("backs off 0.5s, 1s, 2s, 4s, 8s with jitter on 5xx, then drops and warns", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const s = recordSleeps();
    const t = fakeTransport(Array.from({ length: 6 }, () => ({ status: 500 })));
    const ex = new Exporter(t.send, { sleep: s.sleep });
    ex.enqueue(trace(1));
    await ex.flush();
    expect(t.bodies).toHaveLength(6);
    expect(s.slept).toHaveLength(5);
    s.slept.forEach((ms, i) => {
      expect(ms).toBeGreaterThanOrEqual(RETRY_DELAYS_MS[i]! * 0.8);
      expect(ms).toBeLessThanOrEqual(RETRY_DELAYS_MS[i]! * 1.2);
    });
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("giving up on batch"));
  });

  it.each([400, 401, 413])("doesn't retry %i", async (status) => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const s = recordSleeps();
    const t = fakeTransport([{ status, detail: "nope" }]);
    const ex = new Exporter(t.send, { sleep: s.sleep });
    ex.enqueue(trace(1));
    await ex.flush();
    expect(t.bodies).toHaveLength(1);
    expect(s.slept).toHaveLength(0);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining(`with ${status}`));
  });

  it("honors Retry-After on 429", async () => {
    const s = recordSleeps();
    const t = fakeTransport([{ status: 429, retryAfter: "3" }]);
    const ex = new Exporter(t.send, { sleep: s.sleep });
    ex.enqueue(trace(1));
    await ex.flush();
    expect(t.bodies).toHaveLength(2);
    expect(s.slept).toEqual([3000]);
  });

  it("caps a huge Retry-After", async () => {
    const s = recordSleeps();
    const t = fakeTransport([{ status: 429, retryAfter: "3600" }]);
    const ex = new Exporter(t.send, { sleep: s.sleep });
    ex.enqueue(trace(1));
    await ex.flush();
    expect(s.slept).toEqual([MAX_RETRY_AFTER_MS]);
  });

  it("uses the normal backoff when Retry-After isn't a number", async () => {
    const s = recordSleeps();
    const t = fakeTransport([{ status: 429, retryAfter: "soon" }]);
    const ex = new Exporter(t.send, { sleep: s.sleep });
    ex.enqueue(trace(1));
    await ex.flush();
    expect(s.slept[0]).toBeLessThanOrEqual(RETRY_DELAYS_MS[0]! * 1.2);
  });

  it("a transport that throws is retried like a network error and doesn't stop the exporter", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const s = recordSleeps();
    let calls = 0;
    const ex = new Exporter(
      async () => {
        calls++;
        if (calls === 1) throw new Error("boom");
        return { status: 202 };
      },
      { sleep: s.sleep },
    );
    ex.enqueue(trace(1));
    await ex.flush();
    ex.enqueue(trace(2));
    await ex.flush();
    expect(calls).toBe(3);
    expect(s.slept).toHaveLength(1);
  });
});
