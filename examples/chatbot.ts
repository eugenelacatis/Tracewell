// Tracewell example, the TS twin of chatbot.py: record chat turns, each a trace with a parent span
// and one generation span.
//
// Setup once (sdk/ts builds itself on install):
//
//     (cd sdk/ts && npm install) && (cd examples && npm install)
//
// Run from examples/:
//
//     TRACEWELL_API_KEY=tw_dev_localkey npx tsx chatbot.ts --fake --count 5
//     TRACEWELL_API_KEY=tw_dev_localkey npx tsx chatbot.ts --fake --count 5 --grpc
//
// --fake      canned replies with realistic token counts (default when ANTHROPIC_API_KEY is unset)
// --error     make every turn fail, so both spans are ERROR (used by the alerter integration test)
// --count N   number of turns, one trace each
// --grpc      send over gRPC instead of HTTP
//
// With ANTHROPIC_API_KEY set and no --fake, it calls Claude.

import { randomBytes } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { parseArgs } from "node:util";

import Anthropic from "@anthropic-ai/sdk";
import { Tracewell } from "@tracewell/sdk";

const MODEL = "claude-opus-5-5";
const MAX_TOKENS = 1024;
// USD per token (input, output). Claude Opus 5.5 lists at $4 / $20 per million tokens.
const PRICE_PER_TOKEN = [4.0e-6, 20.0e-6] as const;

const QUESTIONS = [
  "What's a good name for a golden retriever?",
  "Summarize the plot of Hamlet in two sentences.",
  "How do I reverse a list in Python?",
  "What's the difference between TCP and UDP?",
  "Give me a haiku about Kafka consumer groups.",
];
const FAKE_ANSWERS = [
  "Sunny, Maple or Biscuit all suit a golden retriever's warm personality.",
  "Prince Hamlet feigns madness while seeking revenge on his uncle, who murdered his father. " +
    "His hesitation leads to a chain of deaths, including his own.",
  "Use my_list.reverse() to reverse in place, or my_list[::-1] for a reversed copy.",
  "TCP gives ordered, reliable delivery over a connection. UDP sends independent datagrams " +
    "with no delivery guarantee, which makes it faster and lighter.",
  "Partitions divide / each consumer takes its share / offsets march on.",
];

/** Thrown by --error to imitate a failed LLM call. */
class SimulatedProviderError extends Error {
  override name = "SimulatedProviderError";
}

/** The model declined, and so did the fallback. */
class RefusalError extends Error {
  override name = "RefusalError";
}

type Reply = { text: string; promptTokens: number; completionTokens: number };

const words = (s: string) => s.split(/\s+/).filter(Boolean).length;

/** Canned answer plus token counts in the range a real model would report. */
async function fakeReply(i: number): Promise<Reply> {
  await sleep(50 + Math.random() * 200); // so spans have a realistic duration
  const text = FAKE_ANSWERS[i % FAKE_ANSWERS.length]!;
  const question = QUESTIONS[i % QUESTIONS.length]!;
  return {
    text,
    promptTokens: Math.floor((words(question) * 4) / 3) + 8, // +8 for the message framing
    completionTokens: Math.floor((words(text) * 4) / 3),
  };
}

/** One Messages API call. Returns the text and the billed input/output token counts. */
async function claudeReply(client: Anthropic, messages: Anthropic.Beta.BetaMessageParam[]): Promise<Reply> {
  const response = await client.beta.messages.create({
    model: MODEL,
    max_tokens: MAX_TOKENS,
    messages,
    // If the model declines on safety grounds, the API reruns the request on a fallback model
    // inside the same call instead of returning an empty refusal.
    betas: ["server-side-fallback-2026-07-01"],
    fallbacks: "default",
  });
  if (response.stop_reason === "refusal") {
    throw new RefusalError(`model refused (category: ${response.stop_details?.category ?? null})`);
  }
  const text = response.content.map((b) => (b.type === "text" ? b.text : "")).join("");
  return { text, promptTokens: response.usage.input_tokens, completionTokens: response.usage.output_tokens };
}

async function main(): Promise<number> {
  const { values } = parseArgs({
    options: {
      fake: { type: "boolean", default: false },
      error: { type: "boolean", default: false },
      count: { type: "string", default: "1" },
      grpc: { type: "boolean", default: false },
    },
  });
  const count = Number(values.count);
  if (!Number.isInteger(count) || count < 0) {
    console.error(`--count must be a non-negative integer, got ${values.count}`);
    return 2;
  }

  const live = !values.fake && Boolean(process.env.ANTHROPIC_API_KEY);
  const client = live ? new Anthropic() : undefined;
  const tw = new Tracewell({ transport: values.grpc ? "grpc" : "http" });
  const sessionId = `chat-${randomBytes(3).toString("hex")}`;
  console.log(
    `mode=${live ? "live" : "fake"} transport=${values.grpc ? "grpc" : "http"} session=${sessionId} count=${count}`,
  );

  for (let i = 0; i < count; i++) {
    const question = QUESTIONS[i % QUESTIONS.length]!;
    const messages: Anthropic.Beta.BetaMessageParam[] = [{ role: "user", content: question }];
    let traceId = "";
    try {
      const { text, cost } = await tw.trace("chat turn", { sessionId, userId: "user-7", tags: ["example"] }, (trace) => {
        traceId = trace.traceId;
        return tw.span("handle message", { kind: "chain", metadata: { turn: i } }, () =>
          tw.generation(
            "anthropic.messages",
            {
              provider: "anthropic",
              model: MODEL,
              prompt: messages,
              params: { max_tokens: MAX_TOKENS },
              metadata: { fake: !live },
            },
            async (gen) => {
              if (values.error) throw new SimulatedProviderError("simulated provider error (--error)");
              const r = client ? await claudeReply(client, messages) : await fakeReply(i);
              const cost = r.promptTokens * PRICE_PER_TOKEN[0] + r.completionTokens * PRICE_PER_TOKEN[1];
              gen.setOutput(r.text, { promptTokens: r.promptTokens, completionTokens: r.completionTokens, costUsd: cost });
              return { text: r.text, cost };
            },
          ),
        );
      });
      console.log(`  ${traceId}  ok     $${cost.toFixed(6)}  ${JSON.stringify(text.slice(0, 50))}`);
    } catch (e) {
      if (!(e instanceof SimulatedProviderError || e instanceof RefusalError || e instanceof Anthropic.APIError)) throw e;
      // The SDK already marked both spans ERROR as the error left them.
      console.log(`  ${traceId}  error  ${e.message}`);
    }
  }

  await tw.flush(); // also runs before exit; awaited here so the summary prints after everything is sent
  console.log("flushed");
  return 0;
}

process.exitCode = await main();
