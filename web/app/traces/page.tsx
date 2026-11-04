import type { ReactNode } from "react";
import { Suspense } from "react";
import Link from "next/link";
import { listTraces } from "@/lib/query";
import type { TraceList } from "@/lib/types";
import { formatCost, formatDuration, formatNumber, formatTime } from "@/lib/format";
import { ErrorState, SkeletonRows, StatusBadge } from "@/components/states";

const PAGE_SIZE = 50;

export default function TracesPage({ searchParams }: PageProps<"/traces">): ReactNode {
  return (
    <main>
      <h1>Traces</h1>
      <Suspense fallback={<SkeletonRows rows={8} cols={7} />}>
        <TraceTable searchParams={searchParams} />
      </Suspense>
    </main>
  );
}

// Paging: query-svc hands back an opaque cursor for the next page and can't go backwards, so the
// URL keeps every cursor visited so far in ?c=, joined with "." (base64url never contains one).
// Next appends nextCursor; Previous drops the last one. Page 1 has no ?c at all.
function pageHref(stack: string[]): string {
  return stack.length ? `/traces?c=${stack.join(".")}` : "/traces";
}

async function TraceTable({
  searchParams,
}: {
  searchParams: PageProps<"/traces">["searchParams"];
}): Promise<ReactNode> {
  const { c } = await searchParams;
  const stack = typeof c === "string" && c ? c.split(".") : [];

  let data: TraceList;
  try {
    data = await listTraces({ cursor: stack.at(-1), limit: PAGE_SIZE });
  } catch (err) {
    return <ErrorState error={err} />;
  }

  if (data.traces.length === 0 && stack.length === 0) {
    return (
      <div className="card state">
        <h2>No traces yet</h2>
        <p className="muted">Send a few with the Python SDK, then refresh:</p>
        <pre className="mono">
          {"cd sdk/python\nTRACEWELL_API_KEY=tw_dev_localkey uv run python ../../examples/chatbot.py --fake --count 5"}
        </pre>
      </div>
    );
  }

  return (
    <>
      <div className="card table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Start time</th>
              <th className="num">Duration</th>
              <th>Status</th>
              <th className="num">Spans</th>
              <th className="num">Tokens</th>
              <th className="num">Cost</th>
            </tr>
          </thead>
          <tbody>
            {data.traces.map((t) => (
              <tr key={t.traceId}>
                <td>
                  <Link href={`/traces/${t.traceId}`}>{t.name || t.traceId}</Link>
                </td>
                <td>{formatTime(t.startTime)}</td>
                <td className="num">{formatDuration(t.durationMs)}</td>
                <td>
                  <StatusBadge status={t.status} />
                </td>
                <td className="num">{t.spanCount}</td>
                <td className="num">{formatNumber(t.totalTokens)}</td>
                <td className="num">{formatCost(t.costUsd)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="pager">
        {stack.length > 0 ? (
          <Link href={pageHref(stack.slice(0, -1))}>Previous</Link>
        ) : (
          <span>Previous</span>
        )}
        {data.nextCursor ? (
          <Link href={pageHref([...stack, data.nextCursor])}>Next</Link>
        ) : (
          <span>Next</span>
        )}
      </div>
    </>
  );
}
