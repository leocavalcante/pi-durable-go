// Fake public SDK. No credentials or network are accessed by this fixture.
import { writeFile } from "node:fs/promises";

const usage = { input: 7, output: 3, cacheRead: 5, cacheWrite: 2,
  totalTokens: 17, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.25 } };

export class ModelRuntime {
  static async create(options) {
    if (options.refreshOnCreate !== false || options.allowModelNetwork !== false || process.env.PI_OFFLINE !== "1") {
      throw new Error("unrelated network refresh was enabled");
    }
    console.log("sensitive SDK diagnostic that must not reach stdout");
    console.error("sensitive stderr that must not reach task records");
    return new ModelRuntime(options);
  }
  constructor(options) { this.options = options; }
  getError() { return undefined; }
  getModel(provider, id) {
    return id === "unknown" ? undefined : { provider, id, api: "fake-api", contextWindow: id === "small-window" ? 4096 : 128000,
      input: id === "text-only" ? ["text"] : ["text", "image"] };
  }
  async *streamSimple(model, context, options) {
    const result = { role: "assistant", api: model.api, provider: model.provider, model: model.id,
      content: [], usage, stopReason: "stop", timestamp: 1000, responseId: "native-response" };
    if (model.id === "fail") throw new Error("super-secret-provider-error");
    if (model.id === "error") {
      yield { type: "error", error: { errorMessage: "super-secret-provider-error" } };
      return;
    }
    const failures = {
      "transient-error": "503 overloaded; super-secret-provider-error",
      "quota-error": "429 insufficient_quota; super-secret-provider-error",
      "overflow-error": "400 prompt is too long; super-secret-provider-error",
    };
    if (failures[model.id] !== undefined) {
      yield { type: "text_delta", delta: "partial " };
      yield { type: "error", reason: "error", error: { ...result, stopReason: "error", errorMessage: failures[model.id] } };
      return;
    }
    if (model.id === "tool-round" && context.messages.some((message) => message.role === "toolResult")) {
      const assistant = context.messages.find((message) => message.role === "assistant");
      if (!assistant?.content.some((block) => block.thoughtSignature === "native-tool-signature")) {
        throw new Error("native replay did not survive durable tool execution");
      }
      result.content = [{ type: "text", text: "complete" }];
    } else if (context.messages.some((message) => message.role === "system" && message.content.startsWith("You are a context summarization assistant"))) {
      if (JSON.stringify(context).includes("pi-assistant-v1")) throw new Error("native data leaked into compaction input");
      result.content = [{ type: "text", text: "summary" }];
    } else if (model.id === "inspect") {
      result.content = [{ type: "text", text: JSON.stringify({ create: this.options, model, context, options }) }];
    } else if (model.id === "hold") {
      yield { type: "text_delta", delta: "started" };
      if (!options.signal.aborted) await new Promise((accept) => options.signal.addEventListener("abort", accept, { once: true }));
      return;
    } else {
      yield { type: "text_delta", delta: "hello " };
      yield { type: "text_delta", delta: "world" };
      result.content = [{ type: "text", text: "hello world", textSignature: "native-text-signature" },
        { type: "thinking", thinking: "plan", thinkingSignature: "native-thinking-signature" },
        { type: "toolCall", id: "call-1", name: "lookup", arguments: { q: "x" }, thoughtSignature: "native-tool-signature" }];
      result.stopReason = "toolUse";
      result.diagnostics = [{ message: "sensitive diagnostic" }];
      if (model.id === "bad-arguments") result.content[2].argumentsError = "bad JSON";
      if (model.id === "length") result.stopReason = "length";
    }
    yield { type: "done", message: result };
  }
  async settleCredentialWrites() {
    if (process.env.PI_DURABLE_TEST_SETTLED) {
      await new Promise((accept) => setTimeout(accept, 50));
      await writeFile(process.env.PI_DURABLE_TEST_SETTLED, "settled");
    }
  }
}
