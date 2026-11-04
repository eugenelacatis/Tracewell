import asyncio
import json
import re

import pytest

from tracewell import Tracewell
from tracewell.exporter import Response


class Recorder:
    def __init__(self) -> None:
        self.bodies: list[dict] = []

    def __call__(self, body: bytes) -> Response:
        self.bodies.append(json.loads(body))
        return Response(202)

    def traces(self) -> list[dict]:
        return [t for b in self.bodies for t in b["traces"]]


@pytest.fixture
def rec() -> Recorder:
    return Recorder()


@pytest.fixture
def tw(rec: Recorder) -> Tracewell:
    return Tracewell(api_key="k", transport=rec, flush_interval_s=60)


TS = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$")


def test_trace_with_nested_spans_and_generation(tw: Tracewell, rec: Recorder):
    with tw.trace("chat turn", session_id="s1", user_id="u1", tags=["example"]) as t:
        with tw.span("handle", kind="chain") as parent:
            with tw.generation(
                "openai.chat",
                provider="openai",
                model="gpt-4o-mini",
                prompt=[{"role": "user", "content": "hi"}],
                params={"temperature": 0.2},
            ) as gen:
                gen.set_output("Hello!", prompt_tokens=9, completion_tokens=3, cost_usd=0.0000045)
    tw.flush()

    [sent] = rec.traces()
    assert sent["traceId"] == t.trace_id and len(t.trace_id) == 32
    assert (sent["name"], sent["sessionId"], sent["userId"], sent["tags"]) == (
        "chat turn",
        "s1",
        "u1",
        ["example"],
    )
    assert TS.match(sent["startTime"]) and TS.match(sent["endTime"])

    by_name = {s["name"]: s for s in sent["spans"]}
    p, g = by_name["handle"], by_name["openai.chat"]
    assert p["spanId"] == parent.span_id
    assert len(p["spanId"]) == 16 and p["parentSpanId"] == ""
    assert g["parentSpanId"] == p["spanId"]
    assert g["traceId"] == p["traceId"] == t.trace_id
    assert (p["kind"], g["kind"]) == ("SPAN_KIND_CHAIN", "SPAN_KIND_GENERATION")
    assert g["status"] == "STATUS_OK" and g["errorMessage"] == ""
    assert "generation" not in p
    assert g["generation"] == {
        "provider": "openai",
        "model": "gpt-4o-mini",
        "prompt": '[{"role": "user", "content": "hi"}]',
        "completion": "Hello!",
        "params": {"temperature": 0.2},
        "usage": {"promptTokens": 9, "completionTokens": 3, "totalTokens": 12, "costUsd": 0.0000045},
    }


def test_exception_marks_span_error_and_propagates(tw: Tracewell, rec: Recorder):
    with pytest.raises(KeyError):
        with tw.trace("t"), tw.span("boom"):
            raise KeyError("missing")
    tw.flush()
    [span] = rec.traces()[0]["spans"]
    assert span["status"] == "STATUS_ERROR"
    assert span["errorMessage"] == "KeyError: 'missing'"


def test_set_error_without_raising(tw: Tracewell, rec: Recorder):
    with tw.trace("t"), tw.generation("llm", provider="p", model="m", prompt="x") as g:
        g.set_error("provider returned 500")
    tw.flush()
    [span] = rec.traces()[0]["spans"]
    assert span["status"] == "STATUS_ERROR" and span["errorMessage"] == "provider returned 500"


def test_span_without_trace_starts_one(tw: Tracewell, rec: Recorder):
    with tw.span("standalone", kind="tool"):
        pass
    tw.flush()
    [t] = rec.traces()
    assert t["name"] == "standalone"
    assert [s["name"] for s in t["spans"]] == ["standalone"]


def test_observe_sync_and_async(tw: Tracewell, rec: Recorder):
    @tw.observe
    def plain(x: int) -> int:
        return x + 1

    @tw.observe("fetch", kind="retrieval")
    async def fetch() -> str:
        await asyncio.sleep(0)
        return "doc"

    with tw.trace("t"):
        assert plain(1) == 2
        assert asyncio.run(fetch()) == "doc"
    tw.flush()
    spans = {s["name"]: s for s in rec.traces()[0]["spans"]}
    assert "test_observe_sync_and_async.<locals>.plain" in spans
    assert spans["fetch"]["kind"] == "SPAN_KIND_RETRIEVAL"


def test_unknown_kind_rejected(tw: Tracewell):
    with pytest.raises(ValueError):
        with tw.span("x", kind="banana"):  # type: ignore[arg-type]
            pass


def test_config_from_env(monkeypatch):
    sent = {}

    def fake_http_transport(url: str, api_key: str):
        sent.update(url=url, api_key=api_key)
        return lambda body: Response(202)

    monkeypatch.setenv("TRACEWELL_HOST", "http://ingest:9999/")
    monkeypatch.setenv("TRACEWELL_API_KEY", "tw_env")
    monkeypatch.setattr("tracewell.client.http_transport", fake_http_transport)
    Tracewell()
    assert sent == {"url": "http://ingest:9999/v1/ingest", "api_key": "tw_env"}
    Tracewell(host="http://other", api_key="tw_arg")
    assert sent == {"url": "http://other/v1/ingest", "api_key": "tw_arg"}


def test_cancelled_span_is_error(tw: Tracewell, rec: Recorder):
    async def slow() -> None:
        with tw.trace("t"), tw.span("llm call"):
            await asyncio.sleep(10)

    async def run() -> None:
        task = asyncio.create_task(slow())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())
    tw.flush()
    [span] = rec.traces()[0]["spans"]
    assert span["status"] == "STATUS_ERROR" and span["errorMessage"].startswith("CancelledError")
