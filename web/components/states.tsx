import type { ReactNode } from "react";
import type { Status } from "@/lib/types";
import { QueryError } from "@/lib/query";

export function StatusBadge({ status }: { status: Status }): ReactNode {
  const cls = status === "OK" ? "ok" : status === "ERROR" ? "error" : "neutral";
  return <span className={`badge ${cls}`}>{status}</span>;
}

export function ErrorState({ error }: { error: unknown }): ReactNode {
  const message =
    error instanceof QueryError
      ? `${error.message}${error.status ? ` (${error.status} ${error.code})` : ""}`
      : "Something went wrong loading this page.";
  return (
    <div className="card state error" role="alert">
      <h2>Couldn&apos;t load data from query-svc</h2>
      <p>{message}</p>
    </div>
  );
}

export function SkeletonRows({ rows, cols }: { rows: number; cols: number }): ReactNode {
  return (
    <div className="card table-wrap" aria-busy="true" aria-label="Loading">
      <table>
        <tbody>
          {Array.from({ length: rows }, (_, r) => (
            <tr key={r}>
              {Array.from({ length: cols }, (_, c) => (
                <td key={c}>
                  <div className="skeleton" />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
