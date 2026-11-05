"""Unit tests for Round 1 evaluators."""

from google.protobuf.timestamp_pb2 import Timestamp
from tracewell_proto import trace_pb2

from tracewell_evals.evaluators import EmptyCompletionEvaluator, LatencyEvaluator


def make_span(
    duration_ms: float = 1000.0,
    completion: str = "Hello world",
    kind: trace_pb2.SpanKind = trace_pb2.SpanKind.SPAN_KIND_GENERATION,
) -> trace_pb2.Span:
    """Helper to build a test span."""
    start_time = Timestamp()
    start_time.FromMilliseconds(1_000_000)

    end_time = Timestamp()
    end_time.FromMilliseconds(int(1_000_000 + duration_ms))

    generation = trace_pb2.Generation(
        provider="openai",
        model="gpt-4o-mini",
        prompt="Hi",
        completion=completion,
    )

    return trace_pb2.Span(
        span_id="span1",
        trace_id="trace1",
        name="test.span",
        kind=kind,
        start_time=start_time,
        end_time=end_time,
        generation=generation,
    )


def test_latency_evaluator_under_threshold() -> None:
    evaluator = LatencyEvaluator(threshold_ms=5000.0)
    span = make_span(duration_ms=1200.0)
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 1.0
    assert out.label == "ok"
    assert out.reasoning == "1200ms, under 5000ms"


def test_latency_evaluator_exceeds_threshold() -> None:
    evaluator = LatencyEvaluator(threshold_ms=5000.0)
    span = make_span(duration_ms=5500.0)
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.0
    assert out.label == "slow"
    assert out.reasoning == "5500ms, exceeds 5000ms"


def test_latency_evaluator_boundary() -> None:
    evaluator = LatencyEvaluator(threshold_ms=5000.0)
    span = make_span(duration_ms=5000.0)
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.0
    assert out.label == "slow"
    assert out.reasoning == "5000ms, exceeds 5000ms"


def test_empty_completion_evaluator_valid() -> None:
    evaluator = EmptyCompletionEvaluator()
    span = make_span(completion="Here is the response.")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 1.0
    assert out.label == "ok"
    assert out.reasoning == "completion is present"


def test_empty_completion_evaluator_empty_string() -> None:
    evaluator = EmptyCompletionEvaluator()
    span = make_span(completion="")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.0
    assert out.label == "empty"
    assert out.reasoning == "completion is blank"


def test_empty_completion_evaluator_whitespace() -> None:
    evaluator = EmptyCompletionEvaluator()
    span = make_span(completion="   \t\n  ")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.0
    assert out.label == "empty"
    assert out.reasoning == "completion is blank"


def test_empty_completion_evaluator_no_generation() -> None:
    evaluator = EmptyCompletionEvaluator()
    span = trace_pb2.Span(span_id="span_nogener", kind=trace_pb2.SpanKind.SPAN_KIND_GENERATION)
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.0
    assert out.label == "empty"
