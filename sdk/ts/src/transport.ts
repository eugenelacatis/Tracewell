// The two ways to reach ingest-svc. Both take an IngestBatch as protojson and report back in HTTP
// terms, so the exporter's retry table (docs/contracts/http.md) is the same for both.

import { fromBinary, fromJsonString, toBinary } from "@bufbuild/protobuf";
import * as grpc from "@grpc/grpc-js";

import type { Transport, TransportResponse } from "./exporter.js";
import { IngestBatchSchema, IngestResponseSchema } from "./gen/trace_pb.js";

const TIMEOUT_MS = 10_000;
const INGEST_METHOD = "/tracewell.v1.IngestService/Ingest";

/** POST /v1/ingest with a protojson body. Never throws. */
export function httpTransport(host: string, apiKey: string): Transport {
  const url = `${host}/v1/ingest`;
  return async (body) => {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${apiKey}` },
        body,
        signal: AbortSignal.timeout(TIMEOUT_MS),
      });
      const text = await res.text(); // read it either way, so the connection can be reused
      return {
        status: res.status,
        retryAfter: res.headers.get("retry-after") ?? undefined,
        detail: res.status === 202 ? "" : text.slice(0, 500),
      };
    } catch (e) {
      return { status: 0, detail: String(e) }; // connection refused, timeout, DNS, ...
    }
  };
}

/**
 * tracewell.v1.IngestService/Ingest. The protojson body is converted to binary with the generated
 * code, and gRPC status codes are mapped back to the HTTP statuses the contract pairs them with.
 * Never throws.
 */
export function grpcTransport(host: string, apiKey: string): Transport {
  const url = new URL(host);
  const tls = url.protocol === "https:";
  const target = `${url.hostname}:${url.port || (tls ? "443" : "80")}`;
  const client = new grpc.Client(target, tls ? grpc.credentials.createSsl() : grpc.credentials.createInsecure());

  return (body) =>
    new Promise<TransportResponse>((resolve) => {
      let bin: Uint8Array;
      try {
        bin = toBinary(IngestBatchSchema, fromJsonString(IngestBatchSchema, body));
      } catch (e) {
        resolve({ status: 400, detail: `could not encode batch: ${String(e)}` });
        return;
      }
      const md = new grpc.Metadata();
      md.set("authorization", `Bearer ${apiKey}`);
      client.makeUnaryRequest(
        INGEST_METHOD,
        (b: Uint8Array) => Buffer.from(b),
        (buf: Buffer) => fromBinary(IngestResponseSchema, buf),
        bin,
        md,
        { deadline: Date.now() + TIMEOUT_MS },
        (err) => resolve(err ? fromGrpcError(err) : { status: 202 }),
      );
    });
}

export function fromGrpcError(err: grpc.ServiceError): TransportResponse {
  const detail = err.details;
  switch (err.code) {
    case grpc.status.INVALID_ARGUMENT:
      return { status: 400, detail };
    case grpc.status.UNAUTHENTICATED:
      return { status: 401, detail };
    case grpc.status.RESOURCE_EXHAUSTED: {
      // With a retry-after trailer it's the rate limit; without one the message was too large.
      const retryAfter = err.metadata?.get("retry-after")[0]?.toString();
      return retryAfter ? { status: 429, retryAfter, detail } : { status: 413, detail };
    }
    case grpc.status.UNAVAILABLE:
      return { status: 503, detail };
    default:
      // DEADLINE_EXCEEDED, connection errors and anything unexpected: retry like a network error.
      return { status: 0, detail: `${grpc.status[err.code] ?? err.code}: ${detail}` };
  }
}
