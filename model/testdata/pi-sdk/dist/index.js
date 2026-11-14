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
  // Deferred answers: "job-ready" answers on the first fetch, "job-later"
  // stays deferred once, "job-fail" fails.
  async fetchDeferred(model, handle, options) {
    if (!options?.signal) throw new Error("no signal");
    const message = { role: "assistant", api: model.api, provider: model.provider, model: model.id, content: [], usage,
      stopReason: "stop", timestamp: 1000 };
    if (handle.id === "job-fail") return { ...message, stopReason: "error", errorMessage: "503 overloaded; super-secret-provider-error" };
    if (handle.id === "job-later") return { ...message, stopReason: "deferred", deferred: { ...handle, id: "job-ready", pollAfterMs: 1, secret: "dropped" } };
    if (handle.id !== "job-ready") throw new Error("super-secret unknown job");
    return { ...message, content: [{ type: "text", text: "deferred answer" }] };
  }
  async cancelDeferred(model, handle, options) {
    if (!options?.signal) throw new Error("no signal");
    if (process.env.PI_DURABLE_TEST_CANCELLED) await writeFile(process.env.PI_DURABLE_TEST_CANCELLED, handle.id);
  }
  getError() { return undefined; }
  // A catalog of a classifier (with credentials in its headers) and an
  // image model, for code mode's models.*.
  catalog() {
    return [
      { id: "judge", provider: "test", api: "test-classifier", type: "classifier", headers: { authorization: "secret" } },
      { id: "painter", provider: "test", api: "test-images", type: "image" },
    ];
  }
  getModelsOfType(type, provider) {
    return this.catalog().filter((model) => model.type === type && (provider === undefined || model.provider === provider));
  }
  async getAvailableOfType(type, provider, options) {
    if (!options?.signal) throw new Error("no signal");
    return this.getModelsOfType(type, provider);
  }
  getModelOfType(type, provider, id) {
    return this.catalog().find((model) => model.type === type && model.provider === provider && model.id === id);
  }
  async classify(model, context, options) {
    if (!options?.signal) throw new Error("no signal");
    if (context.state?.fail) {
      return { api: model.api, provider: model.provider, model: model.id, answers: {}, stopReason: "error",
        errorMessage: "super-secret-provider-error", timestamp: 0 };
    }
    return { api: model.api, provider: model.provider, model: model.id, answers: { good: { value: true } }, usage,
      stopReason: "stop", timestamp: 0, sawHeaders: model.headers !== undefined, context };
  }
  async generateImages(model, context, options) {
    if (!options?.signal) throw new Error("no signal");
    return { api: model.api, provider: model.provider, model: model.id,
      output: [{ type: "image", data: "aGk=", mimeType: "image/png" }], stopReason: "stop", timestamp: 0 };
  }
  getModel(provider, id) {
    return id === "unknown" ? undefined : { provider, id, api: "fake-api", contextWindow: id === "small-window" ? 4096 : 128000,
      input: id === "text-only" ? ["text"] : ["text", "image"],
      ...(id === "small-images" ? { inputLimits: { images: { resize: { maxWidth: 512, maxHeight: 256, maxBytes: 1000 } } } } : {}) };
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
    } else if (model.id === "deferred") {
      if (options.deferred !== true && options.deferred?.window !== "1h") throw new Error("deferred option missing");
      yield { type: "done", reason: "deferred", message: { ...result, stopReason: "deferred",
        deferred: { provider: model.provider, modelId: model.id, api: model.api, id: "job-later", pollAfterMs: 10, data: { row: 1 } } } };
      return;
    } else if (model.id === "events") {
      yield { type: "thinking_delta", contentIndex: 0, delta: "plan" };
      yield { type: "text_delta", contentIndex: 1, delta: "reading" };
      const call = { type: "toolCall", id: "call-9", name: "read", arguments: {} };
      yield { type: "toolcall_start", contentIndex: 2, partial: { ...result, content: [{ type: "thinking", thinking: "plan" }, { type: "text", text: "reading" }, call] } };
      yield { type: "toolcall_delta", contentIndex: 2, delta: '{"path": "a' };
      yield { type: "toolcall_delta", contentIndex: 2, delta: '.txt"}' };
      yield { type: "toolcall_end", contentIndex: 2, toolCall: { ...call, arguments: { path: "a.txt" } } };
      result.content = [{ type: "thinking", thinking: "plan" }, { type: "text", text: "reading" }, { ...call, arguments: { path: "a.txt" } }];
      result.stopReason = "toolUse";
    } else if (model.id === "payload") {
      // The provider body passes through onPayload; the answer is the body
      // that would be sent.
      const built = { model: model.id, messages: context.messages.length };
      const sent = options.onPayload ? (await options.onPayload(built, model)) ?? built : built;
      result.content = [{ type: "text", text: JSON.stringify(sent) }];
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
