// Tracewell client: traces, nested spans, generations and observe().
//
// The current trace and span live in an AsyncLocalStorage, so nesting follows the async call chain
// (across awaits, timers and promises) without passing objects around. A finished trace is
// serialized to the protojson shape in docs/contracts/http.md and handed to the exporter.

import { AsyncLocalStorage } from "node:async_hooks";
import { randomBytes } from "node:crypto";

import { Exporter, jsonSafe, type Transport } from "./exporter.js";
import { grpcTransport, httpTransport } from "./transport.js";

export type SpanKind = "generation" | "retrieval" | "tool" | "chain";
const SPAN_KINDS: readonly SpanKind[] = ["generation", "retrieval", "tool", "chain"];

export interface TraceOptions {
  sessionId?: string;
  userId?: string;
  tags?: string[];
}

export interface SpanOptions {
  kind?: SpanKind;
  metadata?: Record<string, unknown>;
}

export interface GenerationOptions {
  provider: string;
  model: string;
  /** A string, or anything JSON-serializable (e.g. a list of chat messages). */
  prompt: unknown;
  params?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}

export interface Usage {
  promptTokens?: number;
  completionTokens?: number;
  /** Defaults to promptTokens + completionTokens. */
  totalTokens?: number;
  costUsd?: number;
}

export interface TracewellOptions {
  /** Defaults to TRACEWELL_HOST, then http://localhost:8080 (the gateway). */
  host?: string;
  /** Defaults to TRACEWELL_API_KEY. */
  apiKey?: string;
  /** "http" (protojson, the default), "grpc", or a custom transport (tests use it). */
  transport?: "http" | "grpc" | Transport;
  flushIntervalMs?: number;
  maxBatchTraces?: number;
}

interface Generation {
  provider: string;
  model: string;
  prompt: string;
  completion: string;
  params: Record<string, unknown>;
  usage: { promptTokens: number; completionTokens: number; totalTokens: number; costUsd: number };
}

export class Span {
  readonly spanId = randomBytes(8).toString("hex");
  readonly startTime = new Date();
  endTime: Date | undefined;
  errorMessage: string | undefined;
  generation: Generation | undefined;

  constructor(
    readonly name: string,
    readonly traceId: string,
    readonly parentSpanId: string,
    readonly kind: SpanKind | undefined,
    readonly metadata: Record<string, unknown>,
  ) {}

  /** Mark the span failed without throwing. An error leaving the span does this too. */
  setError(message: string): void {
    this.errorMessage = message;
  }

  /** Record the result of a generation. */
  setOutput(completion: string, usage: Usage = {}): void {
    if (!this.generation) throw new Error("setOutput is only for spans opened with Tracewell.generation()");
    const promptTokens = Math.trunc(usage.promptTokens ?? 0);
    const completionTokens = Math.trunc(usage.completionTokens ?? 0);
    this.generation.completion = completion;
    this.generation.usage = {
      promptTokens,
      completionTokens,
      totalTokens: Math.trunc(usage.totalTokens ?? promptTokens + completionTokens),
      costUsd: usage.costUsd ?? 0,
    };
  }

  toJSON(): object {
    return {
      spanId: this.spanId,
      traceId: this.traceId,
      parentSpanId: this.parentSpanId,
      name: this.name,
      kind: this.kind ? `SPAN_KIND_${this.kind.toUpperCase()}` : "SPAN_KIND_UNSPECIFIED",
      startTime: this.startTime.toISOString(),
      endTime: (this.endTime ?? new Date()).toISOString(),
      status: this.errorMessage !== undefined ? "STATUS_ERROR" : "STATUS_OK",
      errorMessage: this.errorMessage ?? "",
      metadata: this.metadata,
      ...(this.generation && { generation: this.generation }),
    };
  }
}

export class Trace {
  readonly traceId = randomBytes(16).toString("hex");
  readonly startTime = new Date();
  endTime: Date | undefined;
  /** Finished spans only. */
  readonly spans: Span[] = [];

  constructor(
    readonly name: string,
    readonly sessionId: string,
    readonly userId: string,
    readonly tags: string[],
  ) {}

  toJSON(): object {
    return {
      traceId: this.traceId,
      sessionId: this.sessionId,
      userId: this.userId,
      name: this.name,
      tags: this.tags,
      startTime: this.startTime.toISOString(),
      endTime: (this.endTime ?? new Date()).toISOString(),
      spans: this.spans.map((s) => s.toJSON()),
    };
  }
}

interface Current {
  trace: Trace;
  span: Span | undefined;
}

// One beforeExit listener for every instance, instead of one each (Node warns past 10 listeners).
// Node can't wait for async work in "exit", but "beforeExit" fires when the event loop empties and
// lets us schedule more. A program that calls process.exit() should await flush() first.
const exporters = new Set<Exporter>();
process.on("beforeExit", () => {
  for (const ex of exporters) if (ex.pending) void ex.flush();
});

/** The prompt as stored: strings as-is, anything else as JSON, and never a throw into user code. */
function encodePrompt(prompt: unknown): string {
  if (typeof prompt === "string") return prompt;
  try {
    return JSON.stringify(prompt, jsonSafe) ?? "";
  } catch {
    return String(prompt); // circular structures and the like
  }
}

function isThenable(v: unknown): v is PromiseLike<unknown> {
  return (typeof v === "object" || typeof v === "function") && v !== null && typeof (v as PromiseLike<unknown>).then === "function";
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? `${e.name}: ${e.message}` : String(e);
}

/**
 * Run fn inside ctx and call done(error?) when it finishes: right away for a plain value, when it
 * settles for a promise. fn's result or error is passed through unchanged.
 */
function runIn<T>(als: AsyncLocalStorage<Current>, ctx: Current, fn: () => T, done: (err?: unknown) => void): T {
  let result: T;
  try {
    result = als.run(ctx, fn);
  } catch (e) {
    done(e);
    throw e;
  }
  if (isThenable(result)) {
    return Promise.resolve(result).then(
      (v) => {
        done();
        return v;
      },
      (e: unknown) => {
        done(e);
        throw e;
      },
    ) as T;
  }
  done();
  return result;
}

/** Entry point. One instance per process is the normal setup. */
export class Tracewell {
  private readonly exporter: Exporter;
  private readonly als = new AsyncLocalStorage<Current>();

  constructor(opts: TracewellOptions = {}) {
    const host = (opts.host ?? process.env.TRACEWELL_HOST ?? "http://localhost:8080").replace(/\/+$/, "");
    const apiKey = opts.apiKey ?? process.env.TRACEWELL_API_KEY ?? "";
    const transport =
      typeof opts.transport === "function"
        ? opts.transport
        : opts.transport === "grpc"
          ? grpcTransport(host, apiKey)
          : httpTransport(host, apiKey);
    this.exporter = new Exporter(transport, {
      flushIntervalMs: opts.flushIntervalMs,
      maxBatchTraces: opts.maxBatchTraces,
    });
    exporters.add(this.exporter);
  }

  get sdkInstanceId(): string {
    return this.exporter.sdkInstanceId;
  }

  /** Resolves once every finished trace has been sent. Never rejects. */
  flush(): Promise<void> {
    return this.exporter.flush();
  }

  /** Run fn in a new trace. The trace is sent, with all its finished spans, when fn finishes. */
  trace<T>(name: string, fn: (trace: Trace) => T): T;
  trace<T>(name: string, opts: TraceOptions, fn: (trace: Trace) => T): T;
  trace<T>(name: string, a: TraceOptions | ((trace: Trace) => T), b?: (trace: Trace) => T): T {
    const [opts, fn] = typeof a === "function" ? [{}, a] : [a, b!];
    const t = new Trace(name, opts.sessionId ?? "", opts.userId ?? "", [...(opts.tags ?? [])]);
    return runIn(this.als, { trace: t, span: undefined }, () => fn(t), () => {
      t.endTime = new Date();
      this.exporter.enqueue(t);
    });
  }

  /**
   * Run fn in a span under the current span. With no open trace, it starts one named after it.
   * An error leaving fn marks the span ERROR and is rethrown unchanged.
   */
  span<T>(name: string, fn: (span: Span) => T): T;
  span<T>(name: string, opts: SpanOptions, fn: (span: Span) => T): T;
  span<T>(name: string, a: SpanOptions | ((span: Span) => T), b?: (span: Span) => T): T {
    const [opts, fn] = typeof a === "function" ? [{}, a] : [a, b!];
    return this.openSpan(name, opts, fn, undefined);
  }

  /** Run fn in a GENERATION span for one LLM call. Call span.setOutput(...) with the result. */
  generation<T>(name: string, opts: GenerationOptions, fn: (span: Span) => T): T {
    const gen: Generation = {
      provider: opts.provider,
      model: opts.model,
      prompt: encodePrompt(opts.prompt),
      completion: "",
      params: { ...opts.params },
      usage: { promptTokens: 0, completionTokens: 0, totalTokens: 0, costUsd: 0 },
    };
    return this.openSpan(name, { kind: "generation", metadata: opts.metadata }, fn, gen);
  }

  /** Wrap fn so every call runs in a span named opts.name (default: fn's name). Sync or async. */
  observe<A extends unknown[], R>(fn: (...args: A) => R, opts: SpanOptions & { name?: string } = {}): (...args: A) => R {
    const name = opts.name ?? (fn.name || "anonymous");
    const spanOpts: SpanOptions = { kind: opts.kind, metadata: opts.metadata };
    const inSpan = (run: () => R): R => this.span(name, spanOpts, run);
    // A regular function, so a wrapped method still sees its own `this`.
    return function (this: unknown, ...args: A): R {
      return inSpan(() => fn.apply(this, args));
    };
  }

  private openSpan<T>(name: string, opts: SpanOptions, fn: (span: Span) => T, gen: Generation | undefined): T {
    if (opts.kind !== undefined && !SPAN_KINDS.includes(opts.kind)) {
      throw new Error(`unknown span kind ${JSON.stringify(opts.kind)}, expected one of ${SPAN_KINDS.join(", ")}`);
    }
    const cur = this.als.getStore();
    if (!cur) return this.trace(name, () => this.openSpan(name, opts, fn, gen));

    const s = new Span(name, cur.trace.traceId, cur.span?.spanId ?? "", opts.kind, { ...opts.metadata });
    s.generation = gen;
    return runIn(this.als, { trace: cur.trace, span: s }, () => fn(s), (err) => {
      if (err !== undefined) s.setError(errorMessage(err));
      s.endTime = new Date();
      cur.trace.spans.push(s);
    });
  }
}
