// Response shapes from query-svc, as pinned in docs/contracts/http.md.

export type Status = "OK" | "ERROR" | "UNSPECIFIED";
export type SpanKind = "SPAN" | "GENERATION" | "UNSPECIFIED";

export interface Trace {
  traceId: string;
  name: string;
  sessionId: string;
  userId: string;
  tags: string[];
  startTime: string;
  endTime: string;
  durationMs: number;
  status: Status;
  spanCount: number;
  totalTokens: number;
  costUsd: number;
}

export interface Usage {
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  costUsd: number;
}

export interface Generation {
  provider: string;
  model: string;
  prompt: string;
  completion: string;
  params: Record<string, unknown>;
  usage: Usage;
}

export interface Span {
  spanId: string;
  parentSpanId: string;
  name: string;
  kind: SpanKind;
  startTime: string;
  endTime: string;
  durationMs: number;
  status: Status;
  errorMessage: string;
  generation: Generation | null;
  metadata: Record<string, unknown>;
}

export interface Eval {
  spanId: string;
  evaluator: string;
  score: number;
  label: string;
  reasoning: string;
  createdAt: string;
}

export interface TraceList {
  traces: Trace[];
  nextCursor: string | null;
}

export interface TraceDetail {
  trace: Trace;
  spans: Span[];
  evals: Eval[];
}
