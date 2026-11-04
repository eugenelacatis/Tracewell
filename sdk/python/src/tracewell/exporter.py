"""Background exporter: buffers finished traces and POSTs them to ingest-svc in batches.

Retry and status handling follow docs/contracts/http.md. Nothing in here raises into user code:
every failure is logged as a warning on the "tracewell" logger.
"""

from __future__ import annotations

import json
import logging
import queue
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from typing import NamedTuple

logger = logging.getLogger("tracewell")

FLUSH_INTERVAL_S = 1.0
MAX_BATCH_TRACES = 100
# ingest-svc rejects bodies over 5 MB with 413. Cap batches below that so a batch of large
# prompts is split before it's sent instead of rejected.
MAX_BATCH_BYTES = 4 * 1024 * 1024
MAX_QUEUED_TRACES = 10_000
RETRY_DELAYS_S = (0.5, 1.0, 2.0, 4.0, 8.0)
MAX_RETRY_AFTER_S = 30.0  # a huge Retry-After must not stall the exporter (or exit) for an hour


class Response(NamedTuple):
    status: int  # 0 means the request never got an HTTP response (network error)
    retry_after: str | None = None
    detail: str = ""


Transport = Callable[[bytes], Response]


def http_transport(url: str, api_key: str, timeout_s: float = 10.0) -> Transport:
    """POST a JSON body to url with the API key. Never raises."""

    def send(body: bytes) -> Response:
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return Response(resp.status)
        except urllib.error.HTTPError as e:
            return Response(e.code, e.headers.get("Retry-After"), e.read(500).decode(errors="replace"))
        except Exception as e:  # connection refused, timeout, DNS, ...
            return Response(0, None, repr(e))

    return send


_FLUSH = object()  # queue marker: send what's buffered now instead of waiting for the interval


class Exporter:
    """Owns the background thread, the batch sequence number and the retry loop."""

    def __init__(
        self,
        transport: Transport,
        *,
        flush_interval_s: float = FLUSH_INTERVAL_S,
        max_batch_traces: int = MAX_BATCH_TRACES,
        max_batch_bytes: int = MAX_BATCH_BYTES,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.sdk_instance_id = uuid.uuid4().hex
        self._seq = 0
        self._transport = transport
        self._flush_interval_s = flush_interval_s
        self._max_batch_traces = max_batch_traces
        self._max_batch_bytes = max_batch_bytes
        self._sleep = sleep
        # Items are JSON-encoded traces (str) or the _FLUSH marker.
        self._queue: queue.Queue[object] = queue.Queue(MAX_QUEUED_TRACES)
        # A trace that didn't fit in the last batch's byte budget. Only the worker thread touches it.
        self._carry: str | None = None
        threading.Thread(target=self._run, name="tracewell-exporter", daemon=True).start()

    def enqueue(self, trace: dict[str, object]) -> None:
        try:
            self._queue.put_nowait(json.dumps(trace, separators=(",", ":"), default=str, allow_nan=False))
        except queue.Full:
            logger.warning("tracewell: buffer full (%d traces), dropping trace", MAX_QUEUED_TRACES)
        except Exception:
            logger.warning("tracewell: could not encode trace, dropping it", exc_info=True)

    def flush(self) -> None:
        """Block until every trace enqueued so far has been sent (or dropped after retries)."""
        self._queue.put(_FLUSH)
        self._queue.join()

    def _run(self) -> None:
        while True:
            batch, done = self._collect()
            try:
                if batch:
                    self._send(batch)
            except Exception:
                logger.warning("tracewell: unexpected error sending batch", exc_info=True)
            finally:
                for _ in range(done):
                    self._queue.task_done()

    def _collect(self) -> tuple[list[str], int]:
        """Wait for the first trace, then gather more until 100 traces, the byte cap, the flush
        interval, or a flush() call. Returns the batch and how many queue items it consumed."""
        batch: list[str] = []
        size = 0
        done = 0
        if self._carry is not None:
            item: object = self._carry
            self._carry = None
        else:
            item = self._queue.get()
            done += 1
        deadline = time.monotonic() + self._flush_interval_s
        while True:
            if item is _FLUSH:
                return batch, done
            assert isinstance(item, str)
            if batch and size + len(item) > self._max_batch_bytes:
                # Doesn't fit: send it first thing in the next batch. Its task_done is counted
                # in this batch, which is fine because join() can't return while _FLUSH is queued.
                self._carry = item
                return batch, done
            batch.append(item)
            size += len(item)
            if len(batch) >= self._max_batch_traces:
                return batch, done
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return batch, done
            try:
                item = self._queue.get(timeout=remaining)
                done += 1
            except queue.Empty:
                return batch, done

    def _send(self, traces: list[str]) -> None:
        # The seq is assigned once per batch. Retries below resend the exact same body, so the
        # server sees the same idempotencyKey and batchSeq every time.
        self._seq += 1
        key = f"{self.sdk_instance_id}:{self._seq}"
        # Traces are already JSON strings, so splice them in rather than decoding and re-encoding.
        body = (
            f'{{"idempotencyKey":{json.dumps(key)},"sdkInstanceId":{json.dumps(self.sdk_instance_id)},'
            f'"batchSeq":"{self._seq}","traces":[{",".join(traces)}]}}'
        ).encode()

        for attempt in range(len(RETRY_DELAYS_S) + 1):
            resp = self._transport(body)
            if resp.status == 202:
                return
            retryable = resp.status == 0 or resp.status == 429 or resp.status >= 500
            if not retryable:
                # 400 / 401 / 413 and other 4xx: resending the same batch can't succeed.
                logger.warning(
                    "tracewell: ingest rejected batch %s with %d, dropping %d traces: %s",
                    key,
                    resp.status,
                    len(traces),
                    resp.detail,
                )
                return
            if attempt == len(RETRY_DELAYS_S):
                break
            delay = RETRY_DELAYS_S[attempt] * random.uniform(0.8, 1.2)
            if resp.status == 429 and resp.retry_after:
                try:
                    delay = max(delay, min(float(resp.retry_after), MAX_RETRY_AFTER_S))
                except ValueError:
                    pass
            self._sleep(delay)
        logger.warning(
            "tracewell: giving up on batch %s after %d attempts (last status %d), dropping %d traces: %s",
            key,
            len(RETRY_DELAYS_S) + 1,
            resp.status,
            len(traces),
            resp.detail,
        )
