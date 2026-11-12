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

The model must be in Pi's bundled catalog or `models.json`. The bridge does not refresh remote catalogs or perform unrelated provider availability checks. It loads no `AgentSession`, project settings, extensions, tools, MCP servers, skills, or Pi sessions. Extension-registered providers are therefore unavailable; configure a compatible provider in `models.json` when possible. Pi's UI settings/default model are not imported. Configure `Agent.Model` and `Agent.ThinkingLevel` explicitly. As before, reopening an existing root preserves its stored Agent rather than reconfiguring it from example flags; use `Conversation.Configure` or a new state directory to change it.

### Mapping and limits

The bridge maps text, system/developer messages, tools/results, thinking levels, provider session IDs, and usage (including cache counts and catalog-estimated cost). It streams text through the normal commit-before-publication callback. SDK client retries are disabled so the durable scheduler controls model retries; provider-internal behavior can still differ. Token-limit and failed/truncated responses are not accepted as completed answers. Multimodal input/output, image/classifier models, deferred responses, virtual models, and positional prompt/tool updates are not exposed by the Go boundary.

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

This adapter has no auth resolution/OAuth, provider catalog, provider-specific conversions, reasoning mapping, cost accounting, or session affinity fields. It sends user images as `image_url` parts and tool-result images in a user message after the tool messages, like pi-ai; set `NoImages` for a model without image input, which then sees pi-ai's placeholders. Tools whose `ConstrainedSampling` asks for JSON-schema sampling are sent with `strict: true` and their schema converted to the strict subset, as pi-ai does (others with `strict: false`); set `NoStrictMode` for an endpoint without strict tools. Grammar sampling is a Responses API feature and is not sent. HTTP errors expose only status codes, not response bodies. Before discarding the body, the adapter classifies the text pi-ai would report (the status with the body's `error` object, or an SSE error event's message) with Durable's ported pi-ai patterns; the error implements `Retryable()` and `ContextOverflow()`. Transport failures, dropped streams, and streams that end before `[DONE]` are retryable; configuration, encoding, invalid-event, and size-limit errors are neither. Implement root `durable.Model` for other integrations.
