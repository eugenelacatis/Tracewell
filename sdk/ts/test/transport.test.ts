import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as grpc from "@grpc/grpc-js";
import { afterEach, describe, expect, it } from "vitest";

import { type IngestBatch, IngestBatchSchema, IngestResponseSchema } from "../src/gen/trace_pb.js";
import { grpcTransport, httpTransport } from "../src/transport.js";

const BODY = JSON.stringify({ idempotencyKey: "k:1", sdkInstanceId: "k", batchSeq: "1", traces: [{ traceId: "t1" }] });

const closers: (() => Promise<void>)[] = [];
afterEach(async () => {
  await Promise.all(closers.splice(0).map((c) => c()));
});

describe("httpTransport", () => {
  async function serve(handler: Parameters<typeof createServer>[1]): Promise<string> {
    const srv: Server = createServer(handler);
    await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
    closers.push(() => new Promise((r) => srv.close(() => r())));
    return `http://127.0.0.1:${(srv.address() as AddressInfo).port}`;
  }

  it("POSTs the body with the key and reports status and Retry-After", async () => {
    let seen: { auth?: string; type?: string; url?: string; body: string } = { body: "" };
    const host = await serve((req, res) => {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        seen = { auth: req.headers.authorization, type: req.headers["content-type"], url: req.url, body };
        res.writeHead(429, { "Retry-After": "1" }).end('{"error":{"code":"rate_limited"}}');
      });
    });
    const resp = await httpTransport(host, "tw_key")(BODY);
    expect(seen).toEqual({ auth: "Bearer tw_key", type: "application/json", url: "/v1/ingest", body: BODY });
    expect(resp).toMatchObject({ status: 429, retryAfter: "1" });
    expect(resp.detail).toContain("rate_limited");
  });

  it("reports a connection error as status 0", async () => {
    const resp = await httpTransport("http://127.0.0.1:1", "k")(BODY);
    expect(resp.status).toBe(0);
  });
});

describe("grpcTransport", () => {
  type Reply = { code: grpc.status; trailer?: Record<string, string> } | "ok";

  /** An in-process IngestService that records calls and answers with reply. */
  async function serve(reply: Reply) {
    const calls: { auth: string; batch: IngestBatch }[] = [];
    const server = new grpc.Server();
    server.addService(
      {
        Ingest: {
          path: "/tracewell.v1.IngestService/Ingest",
          requestStream: false,
          responseStream: false,
          requestDeserialize: (b: Buffer) => fromBinary(IngestBatchSchema, b),
          requestSerialize: (m: IngestBatch) => Buffer.from(toBinary(IngestBatchSchema, m)),
          responseSerialize: (m: ReturnType<typeof create<typeof IngestResponseSchema>>) =>
            Buffer.from(toBinary(IngestResponseSchema, m)),
          responseDeserialize: (b: Buffer) => fromBinary(IngestResponseSchema, b),
        },
      },
      {
        Ingest: (
          call: grpc.ServerUnaryCall<IngestBatch, unknown>,
          cb: grpc.sendUnaryData<ReturnType<typeof create<typeof IngestResponseSchema>>>,
        ) => {
          calls.push({ auth: String(call.metadata.get("authorization")[0]), batch: call.request });
          if (reply === "ok") return cb(null, create(IngestResponseSchema, { accepted: call.request.traces.length }));
          const metadata = new grpc.Metadata();
          for (const [k, v] of Object.entries(reply.trailer ?? {})) metadata.set(k, v);
          cb({ code: reply.code, details: "nope", metadata });
        },
      },
    );
    const port = await new Promise<number>((resolve, reject) =>
      server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, p) => (err ? reject(err) : resolve(p))),
    );
    closers.push(() => new Promise((r) => server.tryShutdown(() => r())));
    return { host: `http://127.0.0.1:${port}`, calls };
  }

  it("sends the batch as binary protobuf with the key in metadata", async () => {
    const { host, calls } = await serve("ok");
    expect(await grpcTransport(host, "tw_key")(BODY)).toEqual({ status: 202 });
    expect(calls[0]!.auth).toBe("Bearer tw_key");
    expect(calls[0]!.batch).toMatchObject({ idempotencyKey: "k:1", batchSeq: 1n, traces: [{ traceId: "t1" }] });
  });

  it.each([
    ["rate limit", { code: grpc.status.RESOURCE_EXHAUSTED, trailer: { "retry-after": "1" } }, 429, "1"],
    ["too large", { code: grpc.status.RESOURCE_EXHAUSTED }, 413, undefined],
    ["bad key", { code: grpc.status.UNAUTHENTICATED }, 401, undefined],
    ["bad batch", { code: grpc.status.INVALID_ARGUMENT }, 400, undefined],
    ["kafka down", { code: grpc.status.UNAVAILABLE }, 503, undefined],
    ["anything else", { code: grpc.status.INTERNAL }, 0, undefined],
  ] as const)("maps %s to %i", async (_name, reply, status, retryAfter) => {
    const { host } = await serve(reply);
    const resp = await grpcTransport(host, "k")(BODY);
    expect(resp.status).toBe(status);
    expect(resp.retryAfter).toBe(retryAfter);
  });

  it("reports an unreachable server as retryable", async () => {
    const resp = await grpcTransport("http://127.0.0.1:1", "k")(BODY);
    expect([0, 503]).toContain(resp.status);
  });
});
