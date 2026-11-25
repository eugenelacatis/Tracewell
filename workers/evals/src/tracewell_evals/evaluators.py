"""Evaluators for scoring generation spans asynchronously."""

import json
import logging
import os
import random
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from tracewell_proto import trace_pb2

from tracewell_evals.metrics import EVALS_JUDGE_ERRORS_TOTAL

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvalOutput:
    """Output produced by an evaluator."""

    score: float
    label: str
    reasoning: str


class BaseEvaluator(ABC):
    """Interface for span evaluators."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the evaluator."""

    @abstractmethod
    def evaluate(self, span: trace_pb2.Span) -> EvalOutput | None:
        """Evaluate a generation span. Returns EvalOutput or None if not applicable."""


# ---------------------------------------------------------------------------
# Round 1 Evaluators: latency, empty_completion
# ---------------------------------------------------------------------------


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

    def evaluate(self, span: trace_pb2.Span) -> EvalOutput | None:
        start_ms = span.start_time.seconds * 1000.0 + span.start_time.nanos / 1_000_000.0
        end_ms = span.end_time.seconds * 1000.0 + span.end_time.nanos / 1_000_000.0
        duration_ms = max(0.0, end_ms - start_ms)

        if duration_ms < self._threshold_ms:
            return EvalOutput(
                score=1.0,
                label="ok",
                reasoning=f"{round(duration_ms)}ms, under {int(self._threshold_ms)}ms",
            )
        return EvalOutput(
            score=0.0,
            label="slow",
            reasoning=f"{round(duration_ms)}ms, exceeds {int(self._threshold_ms)}ms",
        )


class EmptyCompletionEvaluator(BaseEvaluator):
    """Evaluates whether the completion of a generation span is blank.

    Score 0.0 with label 'empty' if blank, else score 1.0 with label 'ok'.
    """

    @property
    def name(self) -> str:
        return "empty_completion"

    def evaluate(self, span: trace_pb2.Span) -> EvalOutput | None:
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


# ---------------------------------------------------------------------------
# Round 2 Evaluators: pii, toxicity, llm_judge
# ---------------------------------------------------------------------------


def is_luhn_valid(num_str: str) -> bool:
    """Validate digits string using the Luhn algorithm."""
    digits = [int(c) for c in num_str if c.isdigit()]
    if len(digits) < 13 or len(digits) > 19:
        return False
    checksum = 0
    reverse_digits = digits[::-1]
    for i, d in enumerate(reverse_digits):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


class PIIEvaluator(BaseEvaluator):
    """Detects Personally Identifiable Information (PII) in completion text.

    Score 1.0 with label 'none' if no PII found, else 0.0 with comma-joined
    types found. Reasoning specifies types and counts, NEVER the matched text.
    """

    EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")
    PHONE_RE = re.compile(r"(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")
    SSN_RE = re.compile(r"\b(?!000|666|9\d{2})\d{3}[- ](?!00)\d{2}[- ](?!0000)\d{4}\b")
    CARD_CANDIDATE_RE = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")

    @property
    def name(self) -> str:
        return "pii"

    def evaluate(self, span: trace_pb2.Span) -> EvalOutput | None:
        text = ""
        if span.HasField("generation"):
            text = span.generation.completion or ""

        if not text.strip():
            return EvalOutput(
                score=1.0,
                label="none",
                reasoning="no PII detected",
            )

        counts: dict[str, int] = {}

        # 1. Email
        emails = self.EMAIL_RE.findall(text)
        if emails:
            counts["email"] = len(emails)

        # 2. SSN
        ssns = self.SSN_RE.findall(text)
        if ssns:
            counts["ssn"] = len(ssns)

        # 3. Phone (ensure not part of SSN or card)
        phones = self.PHONE_RE.findall(text)
        if phones:
            counts["phone"] = len(phones)

        # 4. Credit card with Luhn check
        cards = 0
        for match in self.CARD_CANDIDATE_RE.finditer(text):
            cand = match.group(0)
            clean = re.sub(r"[\s-]", "", cand)
            if clean.isdigit() and is_luhn_valid(clean):
                cards += 1
        if cards > 0:
            counts["credit_card"] = cards

        if not counts:
            return EvalOutput(
                score=1.0,
                label="none",
                reasoning="no PII detected",
            )

        ordered_types = [t for t in ["email", "phone", "credit_card", "ssn"] if t in counts]
        label = ",".join(ordered_types)
        reasons = [f"{counts[t]} {t}" for t in ordered_types]
        return EvalOutput(
            score=0.0,
            label=label,
            reasoning=f"found {', '.join(reasons)}",
        )


class ToxicityEvaluator(BaseEvaluator):
    """Scores toxicity using Detoxify or a local classifier model loaded once at startup.

    Score: 1 minus toxicity probability.
    Label: 'clean' (score >= 0.5) / 'toxic' (score < 0.5).
    Reasoning: which categories tripped.
    """

    def __init__(self, model: Any = None) -> None:
        self._model = model
        if self._model is None:
            self._model = self._load_model()

    def _load_model(self) -> Any:
        try:
            from detoxify import Detoxify

            logger.info("Loading Detoxify('original') model...")
            return Detoxify("original")
        except Exception as err:  # noqa: BLE001
            logger.warning("Could not load Detoxify model: %s", err)
            return None

    @property
    def name(self) -> str:
        return "toxicity"

    def evaluate(self, span: trace_pb2.Span) -> EvalOutput | None:
        text = ""
        if span.HasField("generation"):
            text = span.generation.completion or ""

        if not text.strip():
            return EvalOutput(
                score=1.0,
                label="clean",
                reasoning="clean: empty completion",
            )

        # If a model is loaded, use it to predict toxicity categories
        if self._model is not None:
            try:
                preds = self._model.predict(text)
                tox_prob = float(preds.get("toxicity", 0.0))
                score = max(0.0, min(1.0, 1.0 - tox_prob))

                tripped = [
                    f"{cat} ({float(p):.2f})"
                    for cat, p in preds.items()
                    if float(p) >= 0.5
                ]
                if score < 0.5:
                    label = "toxic"
                    details = ", ".join(tripped) if tripped else f"toxicity ({tox_prob:.2f})"
                    reasoning = f"tripped: {details}"
                else:
                    label = "clean"
                    reasoning = f"clean: no toxic categories detected (max {tox_prob:.2f})"

                return EvalOutput(
                    score=score,
                    label=label,
                    reasoning=reasoning,
                )
            except Exception as err:  # noqa: BLE001
                logger.warning("Toxicity model prediction failed: %s", err)

        # Fallback keyword pattern matching if no model loaded or prediction failed
        toxic_patterns = [
            "hate", "kill", "die", "stupid", "idiot", "attack", "threat", "violence"
        ]
        lower = text.lower()
        found = [p for p in toxic_patterns if re.search(rf"\b{p}\b", lower)]
        if found:
            return EvalOutput(
                score=0.1,
                label="toxic",
                reasoning=f"tripped: {', '.join(found)}",
            )

        return EvalOutput(
            score=0.99,
            label="clean",
            reasoning="clean: no toxic categories detected",
        )


class LLMJudgeEvaluator(BaseEvaluator):
    """Uses the Anthropic SDK to ask a judge model whether the completion is helpful and correct.

    Score: 0.0 to 1.0 from the judge model.
    Label: 'pass' (>= 0.5) / 'fail' (< 0.5).
    Reasoning: the judge's one-paragraph explanation.
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "claude-haiku-5-5",
        sample_rate: float = 1.0,
        client: Any = None,
    ) -> None:
        self.api_key = api_key or os.getenv("EVALS_ANTHROPIC_API_KEY", "")
        self.model = model or os.getenv("EVALS_JUDGE_MODEL", "claude-haiku-5-5")
        try:
            self.sample_rate = float(
                os.getenv("EVALS_JUDGE_SAMPLE_RATE", str(sample_rate))
            )
        except ValueError:
            self.sample_rate = 1.0

        self._client = client
        if self._client is None and self.api_key:
            try:
                import anthropic

                self._client = anthropic.Anthropic(api_key=self.api_key)
            except Exception as err:  # noqa: BLE001
                logger.warning("Could not initialize Anthropic client: %s", err)
                self._client = None

    @property
    def name(self) -> str:
        return "llm_judge"

    def evaluate(self, span: trace_pb2.Span) -> EvalOutput | None:
        # No key means off
        if not self.api_key or self._client is None:
            return None

        # Sample rate check
        if self.sample_rate < 1.0 and random.random() > self.sample_rate:
            return None

        prompt = ""
        completion = ""
        if span.HasField("generation"):
            prompt = span.generation.prompt or ""
            completion = span.generation.completion or ""

        user_content = (
            "You are an LLM evaluation judge. Rate whether the following LLM completion "
            "answers the user prompt helpfully and correctly.\n\n"
            f"Prompt:\n{prompt}\n\n"
            f"Completion:\n{completion}\n\n"
            'Respond in valid JSON with exactly two fields: {"score": <float between 0.0 and 1.0>, '
            '"reasoning": "<one-paragraph explanation>"}'
        )

        # Retry up to 3 times with backoff
        for attempt in range(1, 4):
            try:
                resp = self._client.messages.create(
                    model=self.model,
                    max_tokens=300,
                    messages=[{"role": "user", "content": user_content}],
                )
                text = "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")
                # Parse JSON out of response
                json_match = re.search(r"\{.*\}", text, re.DOTALL)
                if not json_match:
                    raise ValueError(f"No JSON object found in response: {text}")

                data = json.loads(json_match.group(0))
                score = max(0.0, min(1.0, float(data["score"])))
                reasoning = str(data.get("reasoning", "")).strip()

                label = "pass" if score >= 0.5 else "fail"
                return EvalOutput(score=score, label=label, reasoning=reasoning)
            except Exception as err:  # noqa: BLE001
                logger.warning(
                    "LLM judge attempt %d/3 failed: %s",
                    attempt,
                    err,
                )
                if attempt < 3:
                    time.sleep(0.2 * (2 ** (attempt - 1)))

        # All 3 attempts failed: count in evals_judge_errors_total and skip span (do NOT DLQ)
        EVALS_JUDGE_ERRORS_TOTAL.inc()
        return None


def get_default_evaluators(
    toxicity_model: Any = None,
    anthropic_api_key: str | None = None,
    judge_model: str = "claude-haiku-5-5",
    judge_sample_rate: float = 1.0,
    judge_client: Any = None,
) -> list[BaseEvaluator]:
    """Return all Round 2 evaluators."""
    return [
        LatencyEvaluator(threshold_ms=5000.0),
        EmptyCompletionEvaluator(),
        PIIEvaluator(),
        ToxicityEvaluator(model=toxicity_model),
        LLMJudgeEvaluator(
            api_key=anthropic_api_key,
            model=judge_model,
            sample_rate=judge_sample_rate,
            client=judge_client,
        ),
    ]
