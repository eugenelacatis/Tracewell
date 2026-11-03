// Server-only client for query-svc. WEB_API_KEY has no NEXT_PUBLIC_ prefix, so Next never ships it
// to the browser, and the "server-only" import makes the build fail if a client component imports
// this file.
import "server-only";

import type { TraceDetail, TraceList } from "./types";

export class QueryError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

async function get<T>(path: string): Promise<T> {
  const base = process.env.WEB_QUERY_URL ?? "http://localhost:8082";
  const key = process.env.WEB_API_KEY;
  if (!key) {
    throw new QueryError(0, "config", "WEB_API_KEY is not set");
  }
  let res: Response;
  try {
    res = await fetch(`${base}${path}`, { headers: { Authorization: `Bearer ${key}` } });
  } catch {
    throw new QueryError(0, "unreachable", `query-svc is not reachable at ${base}`);
  }
  if (!res.ok) {
    const body = (await res.json().catch(() => null)) as {
      error?: { code: string; message: string };
    } | null;
    throw new QueryError(
      res.status,
      body?.error?.code ?? "unknown",
      body?.error?.message ?? `query-svc returned ${res.status}`,
    );
  }
  return (await res.json()) as T;
}

export function listTraces(params: { cursor?: string; limit?: number }): Promise<TraceList> {
  const q = new URLSearchParams();
  if (params.cursor) q.set("cursor", params.cursor);
  if (params.limit) q.set("limit", String(params.limit));
  const qs = q.toString();
  return get<TraceList>(`/v1/traces${qs ? `?${qs}` : ""}`);
}

export function getTrace(traceId: string): Promise<TraceDetail> {
  return get<TraceDetail>(`/v1/traces/${encodeURIComponent(traceId)}`);
}
