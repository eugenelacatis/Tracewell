"""Tracewell example: record chat turns, each a trace with a parent span and one generation span.

Run from sdk/python so the SDK is importable:

    cd sdk/python
    TRACEWELL_API_KEY=tw_dev_localkey uv run python ../../examples/chatbot.py --fake --count 5

--fake      canned replies with realistic token counts (default when ANTHROPIC_API_KEY is unset)
--error     make every turn fail, so both spans are ERROR (used by the alerter integration test)
--count N   number of turns, one trace each

With ANTHROPIC_API_KEY set and no --fake, it calls Claude. That needs the anthropic package:

    uv run --with anthropic python ../../examples/chatbot.py --count 1
"""

from __future__ import annotations

import argparse
import os
import random
import secrets
import sys
import time

from tracewell import Tracewell

MODEL = "claude-opus-5-5"
MAX_TOKENS = 1024
# USD per token (input, output). Claude Opus 5.5 lists at $4 / $20 per million tokens.
PRICE_PER_TOKEN = (4.00e-6, 20.00e-6)

QUESTIONS = [
    "What's a good name for a golden retriever?",
    "Summarize the plot of Hamlet in two sentences.",
    "How do I reverse a list in Python?",
    "What's the difference between TCP and UDP?",
    "Give me a haiku about Kafka consumer groups.",
]
FAKE_ANSWERS = [
    "Sunny, Maple or Biscuit all suit a golden retriever's warm personality.",
    "Prince Hamlet feigns madness while seeking revenge on his uncle, who murdered his father. "
    "His hesitation leads to a chain of deaths, including his own.",
    "Use my_list.reverse() to reverse in place, or my_list[::-1] for a reversed copy.",
    "TCP gives ordered, reliable delivery over a connection. UDP sends independent datagrams "
    "with no delivery guarantee, which makes it faster and lighter.",
    "Partitions divide / each consumer takes its share / offsets march on.",
]


class SimulatedProviderError(Exception):
    """Raised by --error to imitate a failed LLM call."""


def fake_reply(i: int) -> tuple[str, int, int]:
    """Canned answer plus token counts in the range a real model would report."""
    time.sleep(random.uniform(0.05, 0.25))  # so spans have a realistic duration
    answer = FAKE_ANSWERS[i % len(FAKE_ANSWERS)]
    question = QUESTIONS[i % len(QUESTIONS)]
    prompt_tokens = len(question.split()) * 4 // 3 + 8  # +8 for the message framing
    completion_tokens = len(answer.split()) * 4 // 3
    return answer, prompt_tokens, completion_tokens


def claude_reply(client: object, messages: list[dict[str, str]]) -> tuple[str, int, int]:
    """One Messages API call. Returns the text and the billed input/output token counts."""
    response = client.beta.messages.create(  # type: ignore[attr-defined]
        model=MODEL,
        max_tokens=MAX_TOKENS,
        messages=messages,
        # If the model declines on safety grounds, the API reruns the request on a fallback
        # model inside the same call instead of returning an empty refusal.
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
    )
    if response.stop_reason == "refusal":
        category = response.stop_details.category if response.stop_details else None
        raise RuntimeError(f"model refused (category: {category})")
    text = "".join(b.text for b in response.content if b.type == "text")
    return text, response.usage.input_tokens, response.usage.output_tokens


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--fake", action="store_true", help="canned replies, no LLM call")
    parser.add_argument("--error", action="store_true", help="make every turn fail")
    parser.add_argument("--count", type=int, default=1, help="number of turns (traces)")
    args = parser.parse_args()

    live = not args.fake and bool(os.environ.get("ANTHROPIC_API_KEY"))
    client = None
    failures: tuple[type[Exception], ...] = (SimulatedProviderError,)
    if live:
        try:
            import anthropic
        except ImportError:
            print("Live mode needs anthropic: uv run --with anthropic python ...", file=sys.stderr)
            return 2
        client = anthropic.Anthropic()
        failures += (anthropic.APIError, RuntimeError)

    tw = Tracewell()
    session_id = f"chat-{secrets.token_hex(3)}"
    print(f"mode={'live' if live else 'fake'} session={session_id} count={args.count}")

    for i in range(args.count):
        question = QUESTIONS[i % len(QUESTIONS)]
        messages = [{"role": "user", "content": question}]
        try:
            with tw.trace("chat turn", session_id=session_id, user_id="user-7", tags=["example"]) as trace:
                with tw.span("handle message", kind="chain", metadata={"turn": i}):
                    with tw.generation(
                        "anthropic.messages",
                        provider="anthropic",
                        model=MODEL,
                        prompt=messages,
                        params={"max_tokens": MAX_TOKENS},
                        metadata={"fake": not live},
                    ) as gen:
                        if args.error:
                            raise SimulatedProviderError("simulated provider error (--error)")
                        text, prompt_tokens, completion_tokens = (
                            claude_reply(client, messages) if live else fake_reply(i)
                        )
                        cost = prompt_tokens * PRICE_PER_TOKEN[0] + completion_tokens * PRICE_PER_TOKEN[1]
                        gen.set_output(
                            text,
                            prompt_tokens=prompt_tokens,
                            completion_tokens=completion_tokens,
                            cost_usd=cost,
                        )
            print(f"  {trace.trace_id}  ok     ${cost:.6f}  {text[:50]!r}")
        except failures as e:
            # The SDK already marked both spans ERROR as the exception left them.
            print(f"  {trace.trace_id}  error  {e}")

    tw.flush()  # also runs at exit; called here so the summary prints after everything is sent
    print("flushed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
