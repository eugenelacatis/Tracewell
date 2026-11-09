// Background exporter: buffers finished traces and sends them to ingest-svc in batches.
// Retry and status handling follow docs/contracts/http.md. Nothing in here throws into user code:
// every failure becomes a console warning prefixed "tracewell:".

import { randomUUID } from "node:crypto";

export const FLUSH_INTERVAL_MS = 1000;
export const MAX_BATCH_TRACES = 100;
// ingest-svc rejects bodies over 5 MB with 413. Cap batches below that so a batch of large prompts
// is split before it's sent instead of rejected.
export const MAX_BATCH_BYTES = 4 * 1024 * 1024;
export const MAX_QUEUED_TRACES = 10_000;
export const RETRY_DELAYS_MS = [500, 1000, 2000, 4000, 8000];
// A huge Retry-After must not stall the exporter (or the process exit) for an hour.
export const MAX_RETRY_AFTER_MS = 30_000;

/** What a transport reports back, in HTTP terms. status 0 means no response (network error). */
export interface TransportResponse {
  status: number;
  retryAfter?: string;
  detail?: string;
}

/** Sends one IngestBatch, given as its protojson encoding. Must not throw, but the exporter copes. */
export type Transport = (body: string) => Promise<TransportResponse>;

export interface ExporterOptions {
  flushIntervalMs?: number;
  maxBatchTraces?: number;
  maxBatchBytes?: number;
  /** Waits between retries. Tests replace it. */
  sleep?: (ms: number) => Promise<void>;
}

interface Queued {
  json: string;
  bytes: number;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/** JSON.stringify replacer: a bigint becomes its decimal string instead of throwing. */
export function jsonSafe(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? value.toString() : value;
}

export function warn(message: string): void {
  console.warn(`tracewell: ${message}`);
}

/** Owns the queue, the batch sequence number and the retry loop. */
export class Exporter {
  readonly sdkInstanceId = randomUUID().replaceAll("-", "");
  private seq = 0;
  private queue: Queued[] = [];
  private timer: NodeJS.Timeout | undefined;
  private inflight: Promise<void> | undefined;
  private readonly flushIntervalMs: number;
  private readonly maxBatchTraces: number;
  private readonly maxBatchBytes: number;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(
    private readonly transport: Transport,
    opts: ExporterOptions = {},
  ) {
    this.flushIntervalMs = opts.flushIntervalMs ?? FLUSH_INTERVAL_MS;
    this.maxBatchTraces = opts.maxBatchTraces ?? MAX_BATCH_TRACES;
    this.maxBatchBytes = opts.maxBatchBytes ?? MAX_BATCH_BYTES;
    this.sleep = opts.sleep ?? sleep;
  }

  /** True while traces are queued or a batch is being sent. */
  get pending(): boolean {
    return this.queue.length > 0 || this.inflight !== undefined;
  }

  enqueue(trace: object): void {
    try {
      if (this.queue.length >= MAX_QUEUED_TRACES) {
        warn(`buffer full (${MAX_QUEUED_TRACES} traces), dropping trace`);
        return;
      }
      const json = JSON.stringify(trace, jsonSafe);
      this.queue.push({ json, bytes: Buffer.byteLength(json) });
      if (this.queue.length >= this.maxBatchTraces) {
        void this.pump(false);
      } else {
        this.armTimer();
      }
    } catch (e) {
      warn(`could not encode trace, dropping it: ${String(e)}`);
    }
  }

  /** Resolves once every trace enqueued so far has been sent (or dropped after retries). */
  async flush(): Promise<void> {
    clearTimeout(this.timer);
    this.timer = undefined;
    while (this.pending) {
      await this.pump(true);
    }
  }

  // The interval timer is unref'd so it never keeps the process alive. Exit is handled by flush().
  private armTimer(): void {
    if (this.timer || this.inflight || this.queue.length === 0) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.pump(true);
    }, this.flushIntervalMs);
    this.timer.unref();
  }

  // Sends full batches, or everything when all is true. One send runs at a time, like the Python
  // SDK's single worker thread, so batchSeq order matches send order.
  private pump(all: boolean): Promise<void> {
    if (!this.inflight) {
      const run = async () => {
        while (this.queue.length >= this.maxBatchTraces || (all && this.queue.length > 0)) {
          await this.send(this.takeBatch());
        }
      };
      // Assign first, clear in .finally: a finally callback always runs after this assignment, even
      // when there's nothing to send. Clearing inside run() could run before it and leave a settled
      // promise stuck in inflight, which would stop all sending.
      this.inflight = run().finally(() => {
        this.inflight = undefined;
        this.armTimer();
      });
    }
    return this.inflight;
  }

  // Up to maxBatchTraces traces within maxBatchBytes, and always at least one.
  private takeBatch(): string[] {
    let size = 0;
    let n = 0;
    while (n < this.queue.length && n < this.maxBatchTraces) {
      const bytes = this.queue[n]!.bytes;
      if (n > 0 && size + bytes > this.maxBatchBytes) break;
      size += bytes;
      n++;
    }
    return this.queue.splice(0, n).map((q) => q.json);
  }

  private async send(traces: string[]): Promise<void> {
    // The seq is assigned once per batch. Retries below resend the exact same body, so the server
    // sees the same idempotencyKey and batchSeq every time.
    this.seq += 1;
    const key = `${this.sdkInstanceId}:${this.seq}`;
    // Traces are already JSON, so splice them in instead of re-encoding.
    const body =
      `{"idempotencyKey":${JSON.stringify(key)},"sdkInstanceId":"${this.sdkInstanceId}",` +
      `"batchSeq":"${this.seq}","traces":[${traces.join(",")}]}`;

    let resp: TransportResponse = { status: 0 };
    for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
      try {
        resp = await this.transport(body);
      } catch (e) {
        resp = { status: 0, detail: String(e) };
      }
      if (resp.status >= 200 && resp.status < 300) return; // the contract says 202
      const retryable = resp.status === 0 || resp.status === 429 || resp.status >= 500;
      if (!retryable) {
        // 400 / 401 / 413 and other 4xx: resending the same batch can't succeed.
        warn(`ingest rejected batch ${key} with ${resp.status}, dropping ${traces.length} traces: ${resp.detail ?? ""}`);
        return;
      }
      if (attempt === RETRY_DELAYS_MS.length) break;
      let delay = RETRY_DELAYS_MS[attempt]! * (0.8 + Math.random() * 0.4);
      const retryAfterS = Number(resp.retryAfter);
      if (resp.status === 429 && resp.retryAfter && Number.isFinite(retryAfterS)) {
        delay = Math.max(delay, Math.min(retryAfterS * 1000, MAX_RETRY_AFTER_MS));
      }
      await this.sleep(delay);
    }
    warn(
      `giving up on batch ${key} after ${RETRY_DELAYS_MS.length + 1} attempts ` +
        `(last status ${resp.status}), dropping ${traces.length} traces: ${resp.detail ?? ""}`,
    );
  }
}
