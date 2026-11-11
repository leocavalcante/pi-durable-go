// Optional subprocess adapter for the public Pi coding-agent ModelRuntime API.
// Authentication and provider protocols stay in Pi, not in this bridge.
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

// No catalog downloads or unrelated provider availability checks. Explicit
// generation (and its required OAuth refresh) is the only network work here.
process.env.PI_OFFLINE = "1";
// stdout is a private JSONL protocol, never a diagnostic channel.
console.log = console.info = console.debug = () => {};

const controller = new AbortController();
const lines = createInterface({ input: process.stdin, terminal: false });
const input = new Promise((accept) => {
  lines.once("line", accept);
  lines.once("close", () => {
    controller.abort();
    accept("");
  });
});
process.on("SIGTERM", () => controller.abort());

async function emit(value) {
  await new Promise((accept, reject) => {
    process.stdout.write(`${JSON.stringify(value)}\n`, (error) => error ? reject(error) : accept());
  });
}

function zeroUsage() {
  return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
}

function project(message) {
  let content = "", thinking = "";
  const toolCalls = [];
  for (const block of message.content) {
    switch (block.type) {
      case "text": content += block.text; break;
      case "thinking": thinking += block.thinking; break;
      case "toolCall":
        // pi-ai may replace invalid JSON with {} and attach argumentsError.
        // Never execute those repaired arguments as a successful tool request.
        if (block.argumentsError) throw new Error("invalid tool arguments");
        toolCalls.push({ id: block.id, name: block.name, arguments: block.arguments });
        break;
      default: throw new Error("unsupported assistant content");
    }
  }
  return { content, thinking, toolCalls };
}

// Text, then images, as pi-ai content blocks; pi-ai downgrades them for a
// model without image input.
function withImages(message) {
  const blocks = message.content ? [{ type: "text", text: message.content }] : [];
  for (const image of message.images) {
    blocks.push({ type: "image", data: String(image.data ?? ""), mimeType: String(image.mimeType ?? "") });
  }
  return blocks;
}

function toMessage(message, model) {
  const timestamp = 0; // Root messages do not carry provider timestamps.
  switch (message.role) {
    case "system":
    case "developer":
      return { role: "system", content: message.content, timestamp };
    case "user":
      if (Array.isArray(message.images) && message.images.length > 0) {
        return { role: "user", content: withImages(message), timestamp };
      }
      return { role: "user", content: message.content, timestamp };
    case "tool":
      return { role: "toolResult", toolCallId: message.toolCallId, toolName: message.toolName ?? "",
        content: Array.isArray(message.images) && message.images.length > 0 ? withImages(message) : [{ type: "text", text: message.content }],
        isError: message.isError === true, timestamp };
    case "assistant": {
      // Retain signed/encrypted thinking and provider-native text/tool IDs across
      // tool rounds, forks, and restarts. Never let stale data override a hook or
      // application edit to the visible assistant message.
      const native = message.providerData;
      const visible = { content: message.content, thinking: message.thinking ?? "", toolCalls: message.toolCalls ?? [] };
      if (native?.format === "pi-assistant-v1" && native.message?.role === "assistant" &&
          isDeepStrictEqual(project(native.message), visible)) {
        return native.message;
      }
      const content = [];
      // Unsigned thinking from another adapter cannot be replayed as a signed
      // provider block. Keep it as text, as pi-ai's cross-model conversion does.
      if (message.thinking) content.push({ type: "text", text: message.thinking });
      if (message.content) content.push({ type: "text", text: message.content });
      for (const call of message.toolCalls ?? []) {
        content.push({ type: "toolCall", id: call.id, name: call.name, arguments: call.arguments ?? {} });
      }
      return { role: "assistant", content, api: model.api, provider: model.provider, model: model.id,
        usage: zeroUsage(), stopReason: message.toolCalls?.length ? "toolUse" : "stop", timestamp };
    }
    default: throw new Error("unsupported message role");
  }
}

function toResponse(message) {
  const visible = project(message);
  const usage = message.usage ?? zeroUsage();
  // Explicit allowlist: do not persist SDK diagnostics, errors, headers, or auth
  // resolution objects. These fields are transcript/continuation data only.
  const replay = {};
  for (const key of ["role", "content", "api", "provider", "model", "responseModel", "responseId",
    "providerThinkingLevel", "thinkingLevel", "usage", "stopReason", "timestamp"]) {
    if (message[key] !== undefined) replay[key] = message[key];
  }
  return { ...visible, stopReason: message.stopReason,
    usage: { inputTokens: usage.input, outputTokens: usage.output, cacheReadTokens: usage.cacheRead,
      cacheWriteTokens: usage.cacheWrite, cost: usage.cost?.total ?? 0 },
    providerData: { format: "pi-assistant-v1", message: replay } };
}

// pi-ai's own classifiers, resolved through the coding agent's dependency tree
// (its index does not re-export them). Without them, nothing is retryable.
async function loadClassifiers(entry) {
  try {
    const require = createRequire(entry);
    for (const dir of require.resolve.paths("@earendil-works/pi-ai") ?? []) {
      const index = join(dir, "@earendil-works", "pi-ai", "dist", "index.js");
      if (!existsSync(index)) continue;
      const ai = await import(pathToFileURL(index).href);
      if (typeof ai.isRetryableAssistantError === "function" && typeof ai.isContextOverflow === "function") return ai;
      return undefined;
    }
  } catch {
    // Classification is optional; an unresolvable pi-ai answers false.
  }
  return undefined;
}

// Classify a provider error message inside Node. Only two booleans cross the
// boundary: the provider's text can quote secrets or response bodies.
function classify(ai, message) {
  const flags = { retryable: false, overflow: false };
  if (!ai || typeof message !== "object" || message === null) return flags;
  const failed = { ...message, stopReason: "error" };
  try { flags.retryable = ai.isRetryableAssistantError(failed) === true; } catch {}
  try { flags.overflow = ai.isContextOverflow(failed) === true; } catch {}
  return flags;
}

async function main() {
  let config;
  try {
    config = JSON.parse(await input);
  } catch {
    return { type: "error", code: "request" };
  }
  let sdk, entry;
  try {
    entry = config.packageDir
      ? pathToFileURL(join(resolve(config.packageDir), "dist", "index.js")).href
      : import.meta.resolve("@earendil-works/pi-coding-agent");
    sdk = await import(entry);
    if (typeof sdk.ModelRuntime?.create !== "function") throw new Error("ModelRuntime is required");
  } catch {
    return { type: "error", code: "sdk" };
  }
  let runtime;
  try {
    const paths = config.agentDir ? {
      authPath: join(resolve(config.agentDir), "auth.json"),
      modelsPath: join(resolve(config.agentDir), "models.json"),
    } : {};
    runtime = await sdk.ModelRuntime.create({ ...paths, refreshOnCreate: false, allowModelNetwork: false,
      signal: controller.signal });
    if (runtime.getError()) return { type: "error", code: "config" };
    const request = config.request;
    const model = runtime.getModel(request.model.provider, request.model.id);
    if (!model) return { type: "error", code: "model" };
    // A describe request reads the catalog entry only; no provider call.
    if (config.describe) {
      const window = Number.isFinite(model.contextWindow) && model.contextWindow > 0 ? Math.floor(model.contextWindow) : 0;
      const images = Array.isArray(model.input) ? model.input.includes("image") : true;
      return { type: "model", contextWindow: window, images };
    }
    let context;
    try {
      const reasoning = request.thinkingLevel;
      if (reasoning && !["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(reasoning)) {
        throw new Error("invalid thinking level");
      }
      context = { messages: (request.messages ?? []).map((message) => toMessage(message, model)), tools: request.tools ?? [] };
    } catch {
      return { type: "error", code: "request" };
    }
    const stream = runtime.streamSimple(model, context, {
      sessionId: request.sessionId,
      reasoning: request.thinkingLevel === "off" ? undefined : request.thinkingLevel,
      signal: controller.signal,
      // Durable's scheduler owns retry policy. Do not hide extra model attempts
      // in provider SDK retries (OAuth refresh remains Pi's responsibility).
      maxRetries: 0,
      transport: "sse",
    });
    let result;
    for await (const event of stream) {
      if (event.type === "text_delta") await emit({ type: "delta", delta: event.delta });
      else if (event.type === "done") result = event.message;
      else if (event.type === "error") {
        if (controller.signal.aborted) return { type: "error", code: "provider" };
        return { type: "error", code: "provider", ...classify(await loadClassifiers(entry), event.error) };
      }
    }
    if (!result || controller.signal.aborted) return { type: "error", code: "provider" };
    // A length stop is an answer cut off at its output limit, as upstream
    // Durable treats it; its tool calls never run.
    if (!["stop", "toolUse", "length"].includes(result.stopReason)) return { type: "error", code: "provider" };
    return { type: "result", response: toResponse(result) };
  } catch {
    // Upstream errors can quote secrets, URLs, command output, or response bodies.
    return { type: "error", code: "provider" };
  } finally {
    // Refresh-token rotation must reach Pi's locked store before normal exit.
    await runtime?.settleCredentialWrites();
  }
}

try {
  await emit(await main());
} catch {
  await emit({ type: "error", code: "provider" });
}
// SDK HTTP pools and the cancellation input must not keep a finished call alive.
// stdout has flushed and credential writes have settled before this point.
process.exit(0);
