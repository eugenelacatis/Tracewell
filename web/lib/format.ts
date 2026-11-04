export function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)} s`;
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
}

export function formatCost(usd: number): string {
  if (usd === 0) return "$0";
  if (usd >= 0.01) return `$${usd.toFixed(2)}`;
  return `$${usd.toPrecision(2)}`;
}

export function formatTime(iso: string): string {
  return new Date(iso).toLocaleString("en-US", {
    timeZone: "UTC",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }) + " UTC";
}

export function formatNumber(n: number): string {
  return n.toLocaleString("en-US");
}

// Pretty-prints a string that holds JSON (prompts are often a JSON message list); returns it
// unchanged otherwise.
export function prettyJSON(value: string | object): string {
  if (typeof value !== "string") return JSON.stringify(value, null, 2);
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}
