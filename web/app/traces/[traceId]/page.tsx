import type { ReactNode } from "react";
import { Suspense } from "react";
import Link from "next/link";
import { notFound } from "next/navigation";
import { getTrace, QueryError } from "@/lib/query";
import type { Eval, Span, TraceDetail } from "@/lib/types";
import { flattenTree } from "@/lib/tree";
import { formatCost, formatDuration, formatNumber, formatTime, prettyJSON } from "@/lib/format";
import { ErrorState, SkeletonRows, StatusBadge } from "@/components/states";

type Props = PageProps<"/traces/[traceId]">;

export default function TracePage({ params, searchParams }: Props): ReactNode {
  return (
    <main>
      <p>
        <Link href="/traces">&larr; All traces</Link>
      </p>
      <Suspense fallback={<SkeletonRows rows={6} cols={3} />}>
        <TraceView params={params} searchParams={searchParams} />
      </Suspense>
    </main>
  );
}

async function TraceView({ params, searchParams }: Props): Promise<ReactNode> {
  const [{ traceId }, { span }] = await Promise.all([params, searchParams]);

  let data: TraceDetail;
  try {
    data = await getTrace(traceId);
  } catch (err) {
    if (err instanceof QueryError && err.status === 404) notFound();
    return <ErrorState error={err} />;
  }

  const { trace, spans, evals } = data;
  // The selected span lives in the URL (?span=<id>), so the page needs no client-side JS and a
  // link to a specific span can be shared. Default: the first LLM call, else the root.
  const selected =
    spans.find((s) => s.spanId === span) ??
    spans.find((s) => s.kind === "GENERATION") ??
    spans[0];

  return (
    <>
      <div className="card trace-header">
        <h1>
          {trace.name || "Untitled trace"} <StatusBadge status={trace.status} />
        </h1>
        <div className="mono muted">{trace.traceId}</div>
        <div className="stats">
          <div>
            <span>Started</span>
            {formatTime(trace.startTime)}
          </div>
          <div>
            <span>Duration</span>
            {formatDuration(trace.durationMs)}
          </div>
          <div>
            <span>Spans</span>
            {trace.spanCount}
          </div>
          <div>
            <span>Tokens</span>
            {formatNumber(trace.totalTokens)}
          </div>
          <div>
            <span>Cost</span>
            {formatCost(trace.costUsd)}
          </div>
          {trace.sessionId && (
            <div>
              <span>Session</span>
              {trace.sessionId}
            </div>
          )}
          {trace.userId && (
            <div>
              <span>User</span>
              {trace.userId}
            </div>
          )}
          {trace.tags.length > 0 && (
            <div>
              <span>Tags</span>
              {trace.tags.join(", ")}
            </div>
          )}
        </div>
      </div>

      {spans.length === 0 ? (
        <div className="card state muted">This trace has no spans.</div>
      ) : (
        <div className="detail-grid">
          <Waterfall
            traceId={trace.traceId}
            traceStart={trace.startTime}
            traceMs={trace.durationMs}
            spans={spans}
            selectedId={selected?.spanId}
          />
          {selected && (
            <SpanPanel span={selected} evals={evals.filter((e) => e.spanId === selected.spanId)} />
          )}
        </div>
      )}

      <EvalSummary traceId={trace.traceId} spans={spans} evals={evals} />
    </>
  );
}

// One row per span, indented by depth in the parent/child tree. The bar's left edge is when the
// span started relative to the trace, and its width is the span's share of the trace's duration.
function Waterfall(props: {
  traceId: string;
  traceStart: string;
  traceMs: number;
  spans: Span[];
  selectedId?: string;
}): ReactNode {
  const t0 = new Date(props.traceStart).getTime();
  const total = Math.max(props.traceMs, 1);
  return (
    <div className="card waterfall">
      {flattenTree(props.spans).map(({ span, depth }) => {
        const left = Math.min(Math.max(((new Date(span.startTime).getTime() - t0) / total) * 100, 0), 100);
        const width = Math.max(Math.min((span.durationMs / total) * 100, 100 - left), 0.5);
        const barCls =
          span.status === "ERROR" ? "error" : span.kind === "GENERATION" ? "generation" : "";
        return (
          <Link
            key={span.spanId}
            href={`/traces/${props.traceId}?span=${span.spanId}`}
            scroll={false}
            className={span.spanId === props.selectedId ? "selected" : ""}
          >
            <span className="span-name" style={{ paddingLeft: depth * 16 }} title={span.name}>
              {span.name}
            </span>
            <span className="track">
              <span className={`bar ${barCls}`} style={{ left: `${left}%`, width: `${width}%` }} />
            </span>
            <span className="num muted">{formatDuration(span.durationMs)}</span>
          </Link>
        );
      })}
    </div>
  );
}

function SpanPanel({ span, evals }: { span: Span; evals: Eval[] }): ReactNode {
  const gen = span.generation;
  return (
    <div className="card panel">
      <section>
        <h2>
          {span.name} <StatusBadge status={span.status} />
        </h2>
        <dl className="kv">
          <dt>Kind</dt>
          <dd>{span.kind}</dd>
          <dt>Span ID</dt>
          <dd className="mono">{span.spanId}</dd>
          <dt>Started</dt>
          <dd>{formatTime(span.startTime)}</dd>
          <dt>Duration</dt>
          <dd>{formatDuration(span.durationMs)}</dd>
          {span.errorMessage && (
            <>
              <dt>Error</dt>
              <dd className="state error" style={{ padding: 0, textAlign: "left" }}>
                {span.errorMessage}
              </dd>
            </>
          )}
        </dl>
      </section>

      {gen && (
        <>
          <section>
            <h2>Model</h2>
            <dl className="kv">
              <dt>Provider</dt>
              <dd>{gen.provider || "unknown"}</dd>
              <dt>Model</dt>
              <dd>{gen.model || "unknown"}</dd>
              <dt>Tokens</dt>
              <dd>
                {formatNumber(gen.usage.promptTokens)} prompt + {formatNumber(gen.usage.completionTokens)}{" "}
                completion = {formatNumber(gen.usage.totalTokens)}
              </dd>
              <dt>Cost</dt>
              <dd>{formatCost(gen.usage.costUsd)}</dd>
            </dl>
          </section>
          <section>
            <h2>Prompt</h2>
            <pre>{prettyJSON(gen.prompt) || "(empty)"}</pre>
          </section>
          <section>
            <h2>Completion</h2>
            <pre>{gen.completion || "(empty)"}</pre>
          </section>
          <section>
            <h2>Params</h2>
            <pre>{prettyJSON(gen.params)}</pre>
          </section>
        </>
      )}

      {Object.keys(span.metadata).length > 0 && (
        <section>
          <h2>Metadata</h2>
          <pre>{prettyJSON(span.metadata)}</pre>
        </section>
      )}

      <section>
        <h2>Evals</h2>
        {evals.length === 0 ? <p className="muted">No eval scores for this span yet.</p> : <EvalRows evals={evals} />}
      </section>
    </div>
  );
}

function EvalRows({ evals }: { evals: Eval[] }): ReactNode {
  return evals.map((e) => (
    <div className="eval-row" key={`${e.spanId}:${e.evaluator}`}>
      <span>{e.evaluator}</span>
      <span className="num">{e.score.toFixed(2)}</span>
      <span>{e.label}</span>
      <span className="muted">{e.reasoning}</span>
    </div>
  ));
}

// All eval scores on the trace, grouped by the span they scored ("" means the whole trace).
function EvalSummary(props: { traceId: string; spans: Span[]; evals: Eval[] }): ReactNode {
  const names = new Map(props.spans.map((s) => [s.spanId, s.name]));
  const groups = new Map<string, Eval[]>();
  for (const e of props.evals) {
    groups.set(e.spanId, [...(groups.get(e.spanId) ?? []), e]);
  }
  return (
    <div className="card evals">
      <h2>Eval scores</h2>
      {groups.size === 0 ? (
        <p className="muted">No eval scores yet. Evals arrive a few seconds after the trace.</p>
      ) : (
        [...groups].map(([spanId, list]) => (
          <div className="eval-group" key={spanId || "trace"}>
            <div>
              {spanId ? (
                <Link href={`/traces/${props.traceId}?span=${spanId}`} scroll={false}>
                  {names.get(spanId) ?? spanId}
                </Link>
              ) : (
                <strong>Whole trace</strong>
              )}
            </div>
            <EvalRows evals={list} />
          </div>
        ))
      )}
    </div>
  );
}
