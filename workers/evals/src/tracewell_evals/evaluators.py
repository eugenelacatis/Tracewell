"""Evaluators for scoring generation spans asynchronously."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from tracewell_proto import trace_pb2


@dataclass(frozen=True)
class EvalOutput:
    """Output produced by an evaluator."""
    score: float
    label: str
    reasoning: str


class BaseEvaluator(ABC):
    """Small interface for span evaluators (Round 1 rule-based, Round 2 LLM-as-judge)."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the evaluator, e.g. 'latency' or 'empty_completion'."""
        pass

    @abstractmethod
    def evaluate(self, span: trace_pb2.Span) -> Optional[EvalOutput]:
        """Evaluate a generation span. Returns EvalOutput or None if not applicable."""
        pass


class LatencyEvaluator(BaseEvaluator):
    """Evaluates whether generation span duration is under threshold (default 5000ms).

    Score 1.0 with label 'ok' if under 5000 ms, else score 0.0 with label 'slow'.
    Reasoning reports the measured duration.
    """

    def __init__(self, threshold_ms: float = 5000.0) -> None:
        self._threshold_ms = threshold_ms

    @property
    def name(self) -> str:
        return "latency"

    def evaluate(self, span: trace_pb2.Span) -> Optional[EvalOutput]:
        start_ms = (
            span.start_time.seconds * 1000.0
            + span.start_time.nanos / 1_000_000.0
        )
        end_ms = (
            span.end_time.seconds * 1000.0
            + span.end_time.nanos / 1_000_000.0
        )
        duration_ms = max(0.0, end_ms - start_ms)

        if duration_ms < self._threshold_ms:
            return EvalOutput(
                score=1.0,
                label="ok",
                reasoning=f"{int(round(duration_ms))}ms, under {int(self._threshold_ms)}ms",
            )
        else:
            return EvalOutput(
                score=0.0,
                label="slow",
                reasoning=f"{int(round(duration_ms))}ms, exceeds {int(self._threshold_ms)}ms",
            )


class EmptyCompletionEvaluator(BaseEvaluator):
    """Evaluates whether the completion of a generation span is blank.

    Score 0.0 with label 'empty' if blank, else score 1.0 with label 'ok'.
    """

    @property
    def name(self) -> str:
        return "empty_completion"

    def evaluate(self, span: trace_pb2.Span) -> Optional[EvalOutput]:
        completion = ""
        if span.HasField("generation"):
            completion = span.generation.completion

        if not completion or completion.strip() == "":
            return EvalOutput(
                score=0.0,
                label="empty",
                reasoning="completion is blank",
            )
        return EvalOutput(
            score=1.0,
            label="ok",
            reasoning="completion is present",
        )


def get_default_evaluators() -> list[BaseEvaluator]:
    """Return the set of default evaluators for Round 1."""
    return [
        LatencyEvaluator(threshold_ms=5000.0),
        EmptyCompletionEvaluator(),
    ]

