import type { Span } from "./types";

export interface Row {
  span: Span;
  depth: number;
}

// Orders spans depth-first so each child sits under its parent, and records how deep each one is.
// Children keep the API's startTime order. A span whose parent isn't in the list is treated as a
// root, so a missing parent never hides a span.
export function flattenTree(spans: Span[]): Row[] {
  const ids = new Set(spans.map((s) => s.spanId));
  const children = new Map<string, Span[]>();
  const roots: Span[] = [];
  for (const s of spans) {
    if (s.parentSpanId && ids.has(s.parentSpanId) && s.parentSpanId !== s.spanId) {
      const list = children.get(s.parentSpanId) ?? [];
      list.push(s);
      children.set(s.parentSpanId, list);
    } else {
      roots.push(s);
    }
  }

  const rows: Row[] = [];
  const seen = new Set<string>();
  const walk = (s: Span, depth: number): void => {
    if (seen.has(s.spanId)) return; // guards against a parent cycle in bad data
    seen.add(s.spanId);
    rows.push({ span: s, depth });
    for (const c of children.get(s.spanId) ?? []) walk(c, depth + 1);
  };
  for (const r of roots) walk(r, 0);
  for (const s of spans) walk(s, 0); // anything left was in a cycle; show it as a root
  return rows;
}
