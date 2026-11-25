"""Unit tests for Round 1 and Round 2 evaluators."""

from unittest.mock import MagicMock

from google.protobuf.timestamp_pb2 import Timestamp
from tracewell_proto import trace_pb2

from tracewell_evals.evaluators import (
    EmptyCompletionEvaluator,
    LatencyEvaluator,
    LLMJudgeEvaluator,
    PIIEvaluator,
    ToxicityEvaluator,
)


def make_span(
    duration_ms: float = 1000.0,
    completion: str = "Hello world",
    prompt: str = "Hi",
    kind: trace_pb2.SpanKind = trace_pb2.SpanKind.SPAN_KIND_GENERATION,
) -> trace_pb2.Span:
    """Helper to build a test span."""
    start_time = Timestamp()
    start_time.FromMilliseconds(1_000_000)

    end_time = Timestamp()
    end_time.FromMilliseconds(int(1_000_000 + duration_ms))

    generation = trace_pb2.Generation(
        provider="anthropic",
        model="claude-haiku-5-5",
        prompt=prompt,
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


# ---------------------------------------------------------------------------
# Round 1 Evaluator Tests
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Round 2 Evaluator Tests: PII
# ---------------------------------------------------------------------------


def test_pii_evaluator_clean() -> None:
    evaluator = PIIEvaluator()
    span = make_span(completion="The quick brown fox jumps over the lazy dog.")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 1.0
    assert out.label == "none"
    assert out.reasoning == "no PII detected"


def test_pii_evaluator_email() -> None:
    evaluator = PIIEvaluator()
    span = make_span(completion="Please email user.name@example.com for assistance.")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.0
    assert out.label == "email"
    assert out.reasoning == "found 1 email"
    # Never leak the actual PII text
    assert "user.name@example.com" not in out.reasoning


def test_pii_evaluator_phone_and_ssn() -> None:
    evaluator = PIIEvaluator()
    span = make_span(completion="Call 415-555-2671 or SSN 123-45-6789.")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.0
    assert "phone" in out.label
    assert "ssn" in out.label
    assert "415-555-2671" not in out.reasoning
    assert "123-45-6789" not in out.reasoning


def test_pii_evaluator_credit_card_luhn() -> None:
    evaluator = PIIEvaluator()
    # 4111 1111 1111 1111 is a valid standard test Visa number
    span = make_span(completion="Payment card 4111 1111 1111 1111 was billed.")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.0
    assert "credit_card" in out.label
    assert "4111" not in out.reasoning


def test_pii_evaluator_invalid_card_not_flagged() -> None:
    evaluator = PIIEvaluator()
    # 1234567890123456 fails Luhn checksum
    span = make_span(completion="Random tracking number 1234567890123456")
    out = evaluator.evaluate(span)

    assert out is not None
    # Since it fails Luhn, it is not detected as credit card
    assert "credit_card" not in out.label


# ---------------------------------------------------------------------------
# Round 2 Evaluator Tests: Toxicity
# ---------------------------------------------------------------------------


def test_toxicity_evaluator_clean() -> None:
    mock_model = MagicMock()
    mock_model.predict.return_value = {
        "toxicity": 0.02,
        "severe_toxicity": 0.001,
        "insult": 0.01,
    }
    evaluator = ToxicityEvaluator(model=mock_model)
    span = make_span(completion="Hello, have a wonderful day!")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score >= 0.5
    assert out.label == "clean"
    assert "clean" in out.reasoning


def test_toxicity_evaluator_toxic() -> None:
    mock_model = MagicMock()
    mock_model.predict.return_value = {
        "toxicity": 0.95,
        "severe_toxicity": 0.40,
        "insult": 0.88,
    }
    evaluator = ToxicityEvaluator(model=mock_model)
    span = make_span(completion="You are horrible and I hate you.")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score < 0.5
    assert out.label == "toxic"
    assert "tripped" in out.reasoning
    assert "insult" in out.reasoning


def test_toxicity_evaluator_empty_completion() -> None:
    mock_model = MagicMock()
    evaluator = ToxicityEvaluator(model=mock_model)
    span = make_span(completion="")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 1.0
    assert out.label == "clean"
    assert not mock_model.predict.called


# ---------------------------------------------------------------------------
# Round 2 Evaluator Tests: LLM Judge
# ---------------------------------------------------------------------------


def test_llm_judge_no_key_disabled() -> None:
    evaluator = LLMJudgeEvaluator(api_key="")
    span = make_span(completion="Answers helpfully.")
    out = evaluator.evaluate(span)

    assert out is None


def test_llm_judge_sample_rate_zero() -> None:
    mock_client = MagicMock()
    evaluator = LLMJudgeEvaluator(
        api_key="sk-test",
        sample_rate=0.0,
        client=mock_client,
    )
    span = make_span(completion="Sample test")
    out = evaluator.evaluate(span)

    assert out is None
    assert not mock_client.messages.create.called


def test_llm_judge_success_pass() -> None:
    mock_client = MagicMock()
    content_block = MagicMock()
    content_block.type = "text"
    content_block.text = '{"score": 0.9, "reasoning": "The response is accurate and clear."}'
    mock_resp = MagicMock()
    mock_resp.content = [content_block]
    mock_client.messages.create.return_value = mock_resp

    evaluator = LLMJudgeEvaluator(
        api_key="sk-test",
        sample_rate=1.0,
        client=mock_client,
    )
    span = make_span(completion="Accurate answer.", prompt="Question?")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.9
    assert out.label == "pass"
    assert out.reasoning == "The response is accurate and clear."


def test_llm_judge_success_fail() -> None:
    mock_client = MagicMock()
    content_block = MagicMock()
    content_block.type = "text"
    content_block.text = '{"score": 0.2, "reasoning": "The response fails to answer the prompt."}'
    mock_resp = MagicMock()
    mock_resp.content = [content_block]
    mock_client.messages.create.return_value = mock_resp

    evaluator = LLMJudgeEvaluator(
        api_key="sk-test",
        sample_rate=1.0,
        client=mock_client,
    )
    span = make_span(completion="Irrelevant answer.", prompt="Question?")
    out = evaluator.evaluate(span)

    assert out is not None
    assert out.score == 0.2
    assert out.label == "fail"


def test_llm_judge_retries_and_skips_on_exhaustion() -> None:
    mock_client = MagicMock()
    mock_client.messages.create.side_effect = RuntimeError("API unavailable")

    evaluator = LLMJudgeEvaluator(
        api_key="sk-test",
        sample_rate=1.0,
        client=mock_client,
    )
    span = make_span(completion="Test", prompt="Q")
    out = evaluator.evaluate(span)

    # 3 attempts made, then returns None without raising
    assert out is None
    assert mock_client.messages.create.call_count == 3
