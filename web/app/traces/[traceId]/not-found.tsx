import type { ReactNode } from "react";
import Link from "next/link";

export default function TraceNotFound(): ReactNode {
  return (
    <main>
      <div className="card state">
        <h2>Trace not found</h2>
        <p className="muted">It may not have been written yet, or it belongs to another project.</p>
        <Link href="/traces">Back to traces</Link>
      </div>
    </main>
  );
}
