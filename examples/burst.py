"""Burst test for the per-project ingest rate limit (integration step 9).

Fires N ingest requests in the same instant: N SDK instances, one trace each, released together.
Over INGEST_RATE_LIMIT_RPS (default 200) per second, ingest answers 429 with Retry-After: 1, and each
SDK retries until its trace lands. Then it waits until ClickHouse has exactly N traces for this run.

Run from sdk/python so the SDK is importable:

    cd sdk/python
    TRACEWELL_API_KEY=tw_dev_localkey uv run python ../../examples/burst.py --count 500

One SDK instance batches traces (one request per second), so it can't reach the limit on its own.
That's why this uses one instance per request.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import threading
import time
import urllib.request
from collections import Counter

from tracewell import Tracewell
from tracewell.exporter import Response, http_transport

CLICKHOUSE_URL = "http://localhost:8123/?user=tracewell&password=tracewell"


def count_traces(clickhouse_url: str, session_id: str) -> int:
    query = f"SELECT count() FROM tracewell.traces FINAL WHERE session_id = '{session_id}' FORMAT JSON"
    with urllib.request.urlopen(clickhouse_url, data=query.encode(), timeout=5) as resp:
        return int(json.load(resp)["data"][0]["count()"])


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--count", type=int, default=500, help="requests (= traces) to send at once")
    parser.add_argument(
        "--clickhouse-url", default=CLICKHOUSE_URL, help="ClickHouse HTTP URL, with credentials"
    )
    parser.add_argument("--timeout", type=float, default=60, help="seconds to wait for every trace to land")
    args = parser.parse_args()

    host = (os.environ.get("TRACEWELL_HOST") or "http://localhost:8080").rstrip("/")
    send = http_transport(f"{host}/v1/ingest", os.environ.get("TRACEWELL_API_KEY", ""))
    statuses: Counter[int] = Counter()
    lock = threading.Lock()

    def counting(body: bytes) -> Response:
        resp = send(body)
        with lock:
            statuses[resp.status] += 1
        return resp

    session_id = f"burst-{secrets.token_hex(3)}"
    clients = [Tracewell(transport=counting) for _ in range(args.count)]
    start = threading.Barrier(args.count + 1)

    def one(tw: Tracewell) -> None:
        with tw.trace("burst", session_id=session_id, tags=["burst"]):
            pass
        start.wait()
        tw.flush()  # sends now and returns once the trace is accepted or given up on

    threads = [threading.Thread(target=one, args=(tw,)) for tw in clients]
    for t in threads:
        t.start()
    print(f"session={session_id} host={host} sending {args.count} requests at once")
    t0 = time.monotonic()
    start.wait()
    for t in threads:
        t.join()
    print(f"all SDKs done after {time.monotonic() - t0:.1f}s; responses: {dict(sorted(statuses.items()))}")

    deadline = time.monotonic() + args.timeout
    stored = 0
    while time.monotonic() < deadline:
        stored = count_traces(args.clickhouse_url, session_id)
        if stored >= args.count:
            break
        time.sleep(0.5)
    ok = stored == args.count and statuses[429] > 0
    print(f"{stored}/{args.count} traces in ClickHouse; {statuses[429]} rate-limited responses retried")
    print("PASS" if ok else "FAIL: expected some 429s and every trace stored")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
