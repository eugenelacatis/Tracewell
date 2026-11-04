import json
import threading
import time

import pytest

from tracewell.exporter import RETRY_DELAYS_S, Exporter, Response


class FakeTransport:
    """Records every body sent and answers with scripted statuses (then 202 forever)."""

    def __init__(self, *statuses: int) -> None:
        self.statuses = list(statuses)
        self.bodies: list[dict] = []
        self.lock = threading.Lock()

    def __call__(self, body: bytes) -> Response:
        with self.lock:
            self.bodies.append(json.loads(body))
            return Response(self.statuses.pop(0) if self.statuses else 202)


def trace(i: int, pad: int = 0) -> dict:
    return {"traceId": f"{i:032x}", "name": "t" + "x" * pad, "spans": []}


def make(transport: FakeTransport, sleeps: list[float] | None = None, **kw) -> Exporter:
    # Long interval: only flush() or the size thresholds trigger a send, unless a test overrides it.
    kw.setdefault("flush_interval_s", 60)
    return Exporter(transport, sleep=(sleeps.append if sleeps is not None else lambda _: None), **kw)


def test_flushes_at_100_traces_without_waiting_for_interval():
    t = FakeTransport()
    ex = make(t)
    for i in range(100):
        ex.enqueue(trace(i))
    deadline = time.monotonic() + 2
    while not t.bodies and time.monotonic() < deadline:
        time.sleep(0.01)
    assert [len(b["traces"]) for b in t.bodies] == [100]


def test_splits_into_batches_of_100():
    t = FakeTransport()
    ex = make(t)
    for i in range(250):
        ex.enqueue(trace(i))
    ex.flush()
    assert [len(b["traces"]) for b in t.bodies] == [100, 100, 50]
    sent = [tr["traceId"] for b in t.bodies for tr in b["traces"]]
    assert sent == [f"{i:032x}" for i in range(250)]


def test_flushes_after_interval():
    t = FakeTransport()
    ex = make(t, flush_interval_s=0.1)
    for i in range(3):
        ex.enqueue(trace(i))
    time.sleep(0.4)
    assert [len(b["traces"]) for b in t.bodies] == [3]
    ex.flush()


def test_byte_cap_splits_batches_without_losing_traces():
    t = FakeTransport()
    ex = make(t, max_batch_bytes=1000)
    for i in range(5):
        ex.enqueue(trace(i, pad=400))  # ~460 bytes each, so 2 fit per batch
    ex.flush()
    assert [len(b["traces"]) for b in t.bodies] == [2, 2, 1]


def test_seq_and_idempotency_key_per_batch():
    t = FakeTransport()
    ex = make(t)
    ex.enqueue(trace(1))
    ex.flush()
    ex.enqueue(trace(2))
    ex.flush()
    assert [b["batchSeq"] for b in t.bodies] == ["1", "2"]
    for b in t.bodies:
        assert b["sdkInstanceId"] == ex.sdk_instance_id
        assert b["idempotencyKey"] == f"{ex.sdk_instance_id}:{b['batchSeq']}"


def test_retry_resends_same_batch_and_seq_increments_once():
    t = FakeTransport(503, 0, 202)  # server error, network error, then accepted
    ex = make(t)
    ex.enqueue(trace(1))
    ex.flush()
    ex.enqueue(trace(2))
    ex.flush()
    assert len(t.bodies) == 4
    assert t.bodies[0] == t.bodies[1] == t.bodies[2]
    assert [b["batchSeq"] for b in t.bodies] == ["1", "1", "1", "2"]


def test_backoff_schedule_on_5xx_then_gives_up(caplog):
    t = FakeTransport(*[500] * 10)
    sleeps: list[float] = []
    ex = make(t, sleeps)
    ex.enqueue(trace(1))
    ex.flush()
    assert len(t.bodies) == len(RETRY_DELAYS_S) + 1  # first try + 5 retries
    assert len(sleeps) == len(RETRY_DELAYS_S)
    for got, base in zip(sleeps, RETRY_DELAYS_S, strict=True):
        assert base * 0.8 <= got <= base * 1.2
    assert "giving up" in caplog.text


@pytest.mark.parametrize("status", [400, 401, 413])
def test_no_retry_on_client_errors(status, caplog):
    t = FakeTransport(status)
    sleeps: list[float] = []
    ex = make(t, sleeps)
    ex.enqueue(trace(1))
    ex.flush()
    assert len(t.bodies) == 1
    assert sleeps == []
    assert f"rejected batch {ex.sdk_instance_id}:1 with {status}" in caplog.text


def test_429_honors_retry_after():
    sleeps: list[float] = []

    def transport(body: bytes) -> Response:
        return Response(429, "3") if not sleeps else Response(202)

    ex = Exporter(transport, flush_interval_s=60, sleep=sleeps.append)
    ex.enqueue(trace(1))
    ex.flush()
    assert sleeps == [3.0]


def test_transport_exception_does_not_kill_worker(caplog):
    calls = []

    def transport(body: bytes) -> Response:
        calls.append(body)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return Response(202)

    ex = Exporter(transport, flush_interval_s=60, sleep=lambda _: None)
    ex.enqueue(trace(1))
    ex.flush()
    ex.enqueue(trace(2))
    ex.flush()
    assert len(calls) == 2
    assert "unexpected error" in caplog.text


def test_unencodable_trace_is_dropped_not_raised(caplog):
    t = FakeTransport()
    ex = make(t)
    circular: dict = {}
    circular["self"] = circular
    ex.enqueue(circular)
    ex.enqueue({"x": object()})  # non-JSON values are stringified, not dropped
    ex.flush()
    [body] = t.bodies
    [sent] = body["traces"]
    assert sent["x"].startswith("<object object")
    assert "could not encode" in caplog.text


def test_nan_trace_is_dropped_alone_not_with_its_batch(caplog):
    t = FakeTransport()
    ex = make(t)
    ex.enqueue({"traceId": "bad", "cost": float("nan")})  # NaN isn't valid JSON; ingest would 400 the batch
    ex.enqueue(trace(1))
    ex.flush()
    assert [[tr["traceId"] for tr in b["traces"]] for b in t.bodies] == [[f"{1:032x}"]]
    assert "could not encode" in caplog.text


def test_retry_after_is_capped():
    sleeps: list[float] = []

    def transport(body: bytes) -> Response:
        return Response(429, "3600") if not sleeps else Response(202)

    ex = Exporter(transport, flush_interval_s=60, sleep=sleeps.append)
    ex.enqueue(trace(1))
    ex.flush()
    assert sleeps == [30.0]
