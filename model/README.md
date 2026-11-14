# Model adapters

Both adapters implement `durable.Model` and `durable.EventModel`: they stream text, thinking (pi-ai's events, or the `reasoning_content`/`reasoning`/`reasoning_text` fields Chat Completions endpoints send), and tool-call argument text, so partial answers show them. Go continues to own commits, scheduling, tool execution, and recovery.

## Pi coding-agent authentication and providers

`model.Pi` uses an **installed, trusted Pi coding-agent SDK** rather than reimplementing its authentication. Sign in once with `/login` in Pi, then use the same provider and model ID in Go:

```go
adapter := model.Pi{}
// Pass adapter as HarnessOptions.Model.
// Select Agent.Model with the provider/ID shown by Pi's /model picker.
```

This reuses Pi's `auth.json`, environment credentials, secret-manager commands, `models.json`, OAuth refresh, cross-process credential locking, and provider-specific transports. A ChatGPT/Codex OAuth token is not sent to the Chat Completions endpoint as an API key. Sign-in, logout, and credential management remain in Pi; this package never starts an interactive login or copies credentials into durable state. Normal token refresh can update Pi's credential file using Pi's own storage implementation.

### Requirements and discovery

- Node.js **22.19+** and `@earendil-works/pi-coding-agent` **1.5.2**, the tested SDK baseline. The bridge requires its public `ModelRuntime` API; older SDKs without it are unsupported. Other SDK versions are not guaranteed compatible. The Go Durable runtime has its own, separately documented compatibility reference in [COMPATIBILITY.md](../COMPATIBILITY.md).
- `Pi.NodePath` overrides Node discovery. Otherwise it uses `node` on `PATH`, then Pi's managed Node installation under `$XDG_DATA_HOME/pi-node/current/bin` (default `~/.local/share`).
- `Pi.PackageDir` selects the installed npm package root (the directory containing `package.json` and `dist/index.js`). Otherwise discovery checks `PI_PACKAGE_DIR`, `PI_MANAGED_INSTALL_ROOT`, and the `pi` executable on `PATH`, including the managed installer layout. Finally it tries Node package resolution from the working directory. No automatic installation, `npx`, or network dependency resolution occurs.
- `Pi.AgentDir` selects the directory containing **both** `auth.json` and `models.json`. Empty uses Pi's default: `PI_CODING_AGENT_DIR` or `~/.pi/agent`.

For an explicit SDK installation independent of the CLI:

```sh
npm install --ignore-scripts --prefix "$HOME/.local/share/pi-durable-sdk" @earendil-works/pi-coding-agent@1.5.2
```

Set `PackageDir` to `$HOME/.local/share/pi-durable-sdk/node_modules/@earendil-works/pi-coding-agent`. Keep the existing Pi agent directory to reuse `/login` credentials. No npm dependency is needed for the pure-Go adapter or runtime.

The chat example defaults to this adapter:

```sh
go run ./examples/chat -provider openai-codex -model YOUR_PI_MODEL_ID -prompt 'Hello'
# Custom auth/models directory:
go run ./examples/chat -pi-agent-dir /private/pi-agent -provider anthropic -model YOUR_PI_MODEL_ID -prompt 'Hello'
```

The model must be in Pi's bundled catalog or `models.json`; `Pi.HasModel` (`durable.ModelLookup`) reads the catalog, so a run for a model it lacks ends `no_model` as upstream's does. The bridge does not refresh remote catalogs or perform unrelated provider availability checks. It loads no `AgentSession`, project settings, extensions, tools, MCP servers, skills, or Pi sessions. Extension-registered providers are therefore unavailable; configure a compatible provider in `models.json` when possible. Pi's UI settings/default model are not imported. Configure `Agent.Model` and `Agent.ThinkingLevel` explicitly. As before, reopening an existing root preserves its stored Agent rather than reconfiguring it from example flags; use `Conversation.Configure` or a new state directory to change it.

### Mapping and limits

When the request has `OnPayload` (the Harness sets it for `Hooks.BeforeProviderRequest`), the bridge passes pi-ai's `onPayload`. It sends each provider body pi-ai builds to Go on stdout, with the model's API, and sends the body Go answers on stdin. A failed hook fails that provider request, and without `OnPayload` no bodies cross. `OpenAI` runs `OnPayload` on its own Chat Completions body (`openai-completions`).

The bridge maps text, system/developer messages, tools/results (with pi-ai's `argumentsError` for malformed arguments, which validation reports to the model), thinking levels, provider session IDs, and usage (including cache counts and catalog-estimated cost). It streams text, thinking, and tool-call events (`durable.EventModel`). SDK client retries are disabled so the durable scheduler controls model retries; provider-internal behavior can still differ. Token-limit and failed/truncated responses are not accepted as completed answers. Deferred answers go through `ModelRuntime.fetchDeferred` and `cancelDeferred` (`durable.DeferredModel`), with only pi-ai's `DeferredHandle` fields crossing the boundary. Virtual models are not exposed by the Go boundary. For code mode's `models.*`, `Pi` is a `codemode.Models`: `ModelsOfType`, `AvailableOfType`, and `ModelOfType` read the SDK's catalog, with catalog entries crossing without their `headers`; `Classify` and `GenerateImages` run classifier and image models through `ModelRuntime.classify` and `generateImages`. They resolve the model by provider and ID only, and return pi-ai's result object with any `errorMessage` replaced by the sanitized provider error.

Assistant `ProviderData` preserves Pi-native content blocks, signatures, and continuation IDs across tools and storage reopen. It is adapter-owned transcript data, **not credentials**; treat it as sensitive like message content. Pi performs cross-provider history conversion. If an application/hook edits visible text, thinking, or tool calls, stale native blocks are not replayed. Native data is excluded from compaction summary input. This is not Pi session/file-format compatibility.

A fresh Node process starts per request, adding startup overhead and losing in-process provider caches, HTTP pools, and secret-command caches between requests. This favors straightforward ownership and isolation over performance. `MaxResponseBytes` caps JSONL output, including duplicate streamed text and native replay data (8 MiB default). SDK stderr, HTTP error bodies, and upstream error messages are not included in Go errors/task records. Instead, the bridge classifies a provider error with the installed pi-ai's own `isRetryableAssistantError` and `isContextOverflow`, resolved through the coding agent's dependency tree, and passes only those two answers to Go: the returned error implements `Retryable()` and `ContextOverflow()`, which Durable's retry policy reads. If pi-ai cannot be resolved, both answers are false. Local bridge failures are never retryable.

Cancellation closes a control pipe, aborts the SDK operation, and allows up to three seconds for Pi's credential writes to settle before forcibly terminating Node. A crash/forced termination can still interrupt credential refresh; no stronger token durability or exactly-once model/effect guarantee is made. Credential commands execute with the host's permissions and may spawn other processes; this is not a sandbox. Trust the selected npm installation, working-directory package resolution, auth/model configuration, and environment. For stricter deployments set `PackageDir`, `NodePath`, and `AgentDir` explicitly and isolate the host.

### Tests

Normal Go tests use a fake SDK when Node is available (bridge tests skip without Node). To additionally test the installed SDK's real credential resolution and concurrent OAuth refresh with fake tokens and **local HTTP only**:

```sh
PI_DURABLE_TEST_PI_PACKAGE_DIR=/path/to/node_modules/@earendil-works/pi-coding-agent go test ./model -run TestPiInstalled -count=1
```

These tests use temporary auth/model files and block non-local fetches. They do not read your credentials or make paid model calls. OAuth coverage currently exercises the Codex refresh path, not every Pi provider/login flow.

## Pure-Go OpenAI-compatible Chat Completions

`model.OpenAI` uses streaming Chat Completions without Node. Configure `BaseURL` for OpenAI or a local endpoint, `APIKey` when needed, `HTTPClient` for transport customization, and `MaxResponseBytes` to cap the stream (8 MiB default). It sends messages, tool schemas, streaming mode, and `stream_options.include_usage`; it maps streamed content, fragmented tool arguments, and input/output token counts.

```sh
OPENAI_API_KEY=... go run ./examples/chat -adapter openai -model YOUR_MODEL_ID -prompt 'Hello'
```

This adapter has no auth resolution/OAuth, provider catalog, provider-specific conversions, or cost accounting. Malformed tool-call arguments do not fail the response: as pi-ai's adapter does, the call carries an `argumentsError` (`durable.ParseToolCallArguments`), which tool validation reports to the model. For a reasoning model (`Reasoning`, with pi-ai's `ThinkingLevelMap`), it sends the request's thinking level, clamped and mapped, in the endpoint's thinking format as pi-ai does: detected from the base URL (DeepSeek's `thinking`, Z.ai's `thinking` with `clear_thinking`, Together's and OpenRouter's `reasoning`, Ant Ling's mapped `reasoning`, and `reasoning_effort` elsewhere), or set in `Compat`. As upstream, the off level sends no reasoning option, so it is never clamped. An answer's thinking is replayed in the field it came in (`reasoning_content`, `reasoning`, or `reasoning_text`, kept in its `ProviderData`) to the model that gave it only, and DeepSeek reasoning models get an empty `reasoning_content` otherwise, as pi-ai sends them. OpenRouter's `reasoning_details` (text, summary, and encrypted reasoning, merged as they stream) are kept with the answer and sent back to the same model in place of the reasoning field. `Compat` (`OpenAICompat`) ports pi-ai's `model.compat`: each field set replaces what pi-ai's `detectCompat` derives from the provider (the request's `ModelRef.Provider`) and the base URL. It covers the store, developer-role, reasoning-effort, usage-in-streaming, and finish-reason support; the max-tokens field; tool-result names, the assistant message bridging tool results and a user message, non-null assistant content, `tools: []` with tool history, thinking replayed as text, and empty `reasoning_content`; every thinking format, with `chat-template` and `baseten` values (`$var` objects take the request's thinking), and a thinking-token budget field; OpenRouter and Vercel gateway routing, Z.ai `tool_stream`, vLLM priority, Anthropic-style `cache_control`, the prompt-cache-key mode, and long cache retention. `SamplingParams` and `SamplingParamsByThinkingLevel` are sent last, over the named fields. Requests carry pi-ai's headers: the identity's User-Agent and provider attribution (OpenRouter's referer, title, and categories, NVIDIA's billing origin, OpenCode's client), and session-affinity headers where the compat asks for them (detected for OpenRouter; `SendSessionAffinityHeaders`, `SessionAffinityFormat`), with `StreamOptions.Headers` over them. As pi-ai's `transformMessages` does, an answer from another model gets its tool call IDs normalized (pipe-separated Responses IDs, OpenAI's 40-character limit) with its tool results following, failed and aborted answers are not replayed, and a tool call left without a result gets a synthetic `No result provided` error result. With `SupportsMidConvoSystemMessages`, later system messages stay in place as prompt updates (`Updated system prompt section …`), held after the results of tool calls they interrupt; with `SupportsMidConvoToolAdditions` too, tools added later are declared in place and the request offers the tools as of the last removal or redefinition. With `SupportsOpenAIGrammarTools`, a tool whose `ConstrainedSampling` is a grammar (`openai_lark` or `openai_regex`) is sent as an OpenAI custom tool: its calls are replayed as input text, and streamed input fills its one required string property, as pi-ai does. Tool calls in a stream are found by index or, without one, by ID, keeping the first name and ID. By default it sends the fields pi-ai derives:
- `store: false` except on the endpoints pi-ai treats as non-standard;
- the session ID as `prompt_cache_key` on OpenAI's endpoint, or with long cache retention where supported, which also sends `prompt_cache_retention: "24h"`;
- with `MaxTokens` (pi-ai's `model.maxTokens`), the output limit as `max_completion_tokens`, or `max_tokens` where pi-ai sends that. It is clamped to fit `Window` with pi-ai's margins.

A reasoning model's instructions go out as a `developer` message where the endpoint takes that role, as pi-ai sends them. For a conversation with system sections, images, tools, and tool results, the body equals pi-ai 1.5.2's as JSON, with and without reasoning; `make crosscheck` compares them. Responses are read as pi-ai reads them:
- usage puts cache reads and writes apart from the input, from whichever field the provider uses;
- a finish reason other than stop, end, length, or tool calls ends the answer as an error, `Provider finish_reason: <reason>`;
- the answer ends with the stream, `[DONE]` or not: without a finish reason it is pi-ai's transient error `Stream ended without finish_reason`, or with `SupportsFinishReason` false a stop, or a tool use when it has tool calls.

Provider error messages stay sanitized. It sends user images as `image_url` parts and tool-result images in a user message after the tool messages, like pi-ai; set `NoImages` for a model without image input, which then sees pi-ai's placeholders. Like pi-ai, no `strict` field is sent unless the endpoint supports strict mode (`StrictMode`, pi-ai's `compat.supportsStrictMode`, off unless a model enables it). With it, tools whose `ConstrainedSampling` asks for JSON-schema sampling are sent with `strict: true` and their schema converted to the strict subset, and others with `strict: false`. Without it, a tool that requires constrained sampling fails the request. Grammar sampling is a Responses API feature and is not sent. HTTP errors expose only status codes, not response bodies. Before discarding the body, the adapter classifies the text pi-ai would report (the status with the body's `error` object, or an SSE error event's message) with Durable's ported pi-ai patterns; the error implements `Retryable()` and `ContextOverflow()`. Transport failures, dropped streams, and streams that end before `[DONE]` are retryable; configuration, encoding, invalid-event, and size-limit errors are neither. Implement root `durable.Model` for other integrations.
