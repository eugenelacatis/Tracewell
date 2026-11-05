"""Tracewell client: traces, nested spans, generations and the @observe decorator.

The current trace and span live in contextvars, so nesting works across threads and asyncio tasks
without passing objects around. A finished trace is serialized to the protojson shape in
docs/contracts/http.md and handed to the background exporter.
"""

from __future__ import annotations

import atexit
import functools
import inspect
import json
import os
import secrets
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, TypeVar, get_args

from tracewell.exporter import Exporter, Transport, http_transport

SpanKind = Literal["generation", "retrieval", "tool", "chain"]

_current_trace: ContextVar[Trace | None] = ContextVar("tracewell_trace", default=None)
_current_span: ContextVar[Span | None] = ContextVar("tracewell_span", default=None)

F = TypeVar("F", bound=Callable[..., Any])


def _now() -> datetime:
    return datetime.now(UTC)


def _ts(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@dataclass
class Span:
    name: str
    trace_id: str
    parent_span_id: str
    kind: SpanKind | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    span_id: str = field(default_factory=lambda: secrets.token_hex(8))
    start_time: datetime = field(default_factory=_now)
    end_time: datetime | None = None
    error_message: str | None = None
    generation: dict[str, Any] | None = None

    def set_error(self, message: str) -> None:
        """Mark the span failed without raising. An exception leaving the span does this too."""
        self.error_message = message

    def set_output(
        self,
        completion: str,
        *,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int | None = None,
        cost_usd: float = 0.0,
    ) -> None:
        """Record the result of a generation. total_tokens defaults to prompt + completion."""
        if self.generation is None:
            raise ValueError("set_output is only for spans opened with Tracewell.generation()")
        self.generation["completion"] = completion
        self.generation["usage"] = {
            "promptTokens": int(prompt_tokens),
            "completionTokens": int(completion_tokens),
            "totalTokens": int(prompt_tokens + completion_tokens if total_tokens is None else total_tokens),
            "costUsd": float(cost_usd),
        }

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "spanId": self.span_id,
            "traceId": self.trace_id,
            "parentSpanId": self.parent_span_id,
            "name": self.name,
            "kind": f"SPAN_KIND_{self.kind.upper()}" if self.kind else "SPAN_KIND_UNSPECIFIED",
            "startTime": _ts(self.start_time),
            "endTime": _ts(self.end_time or _now()),
            "status": "STATUS_ERROR" if self.error_message is not None else "STATUS_OK",
            "errorMessage": self.error_message or "",
            "metadata": self.metadata,
        }
        if self.generation is not None:
            d["generation"] = self.generation
        return d


@dataclass
class Trace:
    name: str
    session_id: str = ""
    user_id: str = ""
    tags: list[str] = field(default_factory=list)
    trace_id: str = field(default_factory=lambda: secrets.token_hex(16))
    start_time: datetime = field(default_factory=_now)
    end_time: datetime | None = None
    spans: list[Span] = field(default_factory=list)  # finished spans only

    def to_dict(self) -> dict[str, Any]:
        return {
            "traceId": self.trace_id,
            "sessionId": self.session_id,
            "userId": self.user_id,
            "name": self.name,
            "tags": self.tags,
            "startTime": _ts(self.start_time),
            "endTime": _ts(self.end_time or _now()),
            "spans": [s.to_dict() for s in self.spans],
        }


class Tracewell:
    """Entry point. One instance per process is the normal setup.

    host and api_key default to the TRACEWELL_HOST and TRACEWELL_API_KEY env vars. transport
    replaces the HTTP sender (tests use it). The remaining arguments tune batching.
    """

    def __init__(
        self,
        host: str | None = None,
        api_key: str | None = None,
        *,
        transport: Transport | None = None,
        flush_interval_s: float | None = None,
        max_batch_traces: int | None = None,
    ) -> None:
        host = (host or os.environ.get("TRACEWELL_HOST") or "http://localhost:8081").rstrip("/")
        api_key = api_key or os.environ.get("TRACEWELL_API_KEY", "")
        tuning = {
            k: v
            for k, v in {"flush_interval_s": flush_interval_s, "max_batch_traces": max_batch_traces}.items()
            if v is not None
        }
        self._exporter = Exporter(transport or http_transport(f"{host}/v1/ingest", api_key), **tuning)
        atexit.register(self.flush)

    @property
    def sdk_instance_id(self) -> str:
        return self._exporter.sdk_instance_id

    def flush(self) -> None:
        """Block until every finished trace has been sent. Runs automatically at exit."""
        self._exporter.flush()

    @contextmanager
    def trace(
        self, name: str, *, session_id: str = "", user_id: str = "", tags: list[str] | None = None
    ) -> Iterator[Trace]:
        """Open a trace. It's sent, with all of its finished spans, when the block exits."""
        t = Trace(name=name, session_id=session_id, user_id=user_id, tags=list(tags or []))
        trace_token = _current_trace.set(t)
        span_token = _current_span.set(None)
        try:
            yield t
        finally:
            _current_span.reset(span_token)
            _current_trace.reset(trace_token)
            t.end_time = _now()
            self._exporter.enqueue(t.to_dict())

    @contextmanager
    def span(
        self, name: str, *, kind: SpanKind | None = None, metadata: dict[str, Any] | None = None
    ) -> Iterator[Span]:
        """Open a span under the current span. With no open trace, it starts one named after it.

        An exception leaving the block marks the span ERROR and is re-raised unchanged.
        """
        if kind is not None and kind not in get_args(SpanKind):
            raise ValueError(f"unknown span kind {kind!r}, expected one of {get_args(SpanKind)}")
        t = _current_trace.get()
        if t is None:
            with self.trace(name), self.span(name, kind=kind, metadata=metadata) as s:
                yield s
            return
        parent = _current_span.get()
        s = Span(
            name=name,
            trace_id=t.trace_id,
            parent_span_id=parent.span_id if parent else "",
            kind=kind,
            metadata=dict(metadata or {}),
        )
        token = _current_span.set(s)
        try:
            yield s
        except BaseException as e:  # includes asyncio.CancelledError and KeyboardInterrupt
            s.set_error(f"{type(e).__name__}: {e}")
            raise
        finally:
            _current_span.reset(token)
            s.end_time = _now()
            t.spans.append(s)

    @contextmanager
    def generation(
        self,
        name: str,
        *,
        provider: str,
        model: str,
        prompt: Any,
        params: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Iterator[Span]:
        """Open a GENERATION span for one LLM call. Call span.set_output(...) with the result.

        prompt can be a string or anything JSON-serializable (e.g. a list of chat messages).
        """
        with self.span(name, kind="generation", metadata=metadata) as s:
            s.generation = {
                "provider": provider,
                "model": model,
                "prompt": prompt if isinstance(prompt, str) else json.dumps(prompt, default=str),
                "completion": "",
                "params": dict(params or {}),
                "usage": {"promptTokens": 0, "completionTokens": 0, "totalTokens": 0, "costUsd": 0.0},
            }
            yield s

    def observe(self, name: str | F | None = None, *, kind: SpanKind | None = None) -> Any:
        """Decorator that runs the function inside a span. Works on sync and async functions.

        Use as @tw.observe, @tw.observe() or @tw.observe("name", kind="tool").
        """
        if callable(name):
            return self.observe()(name)

        def decorate(fn: F) -> F:
            span_name = name or fn.__qualname__
            if inspect.iscoroutinefunction(fn):

                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    with self.span(span_name, kind=kind):
                        return await fn(*args, **kwargs)

                return functools.wraps(fn)(async_wrapper)  # type: ignore[return-value]

            def wrapper(*args: Any, **kwargs: Any) -> Any:
                with self.span(span_name, kind=kind):
                    return fn(*args, **kwargs)

            return functools.wraps(fn)(wrapper)  # type: ignore[return-value]

        return decorate
