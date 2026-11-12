# Plan 0005: Remaining parity roadmap

| | |
|---|---|
| Status | Living roadmap; updated 2026-11-12 |
| Compatibility target | Pi Durable **v1.5.2** (tag `v1.5.2`, commit `b4afdddf44e059c67ccf5097e5317d913521b470`) |
| Supersedes | Plan 0001 §13 and plan 0002 §13 follow-up lists |

## 1. Done since plan 0001

| Item | Where |
|---|---|
| A: crash-loop guard, retry policy, interrupted requests | [Plan 0002](0002-crash-loop-guard-and-interrupted-requests.md) |
| B: wait-cycle validation | `wait_graph.go` |
| C: run control in `pi.live`, boundaries, run failures, continue, idle rules, turn limits, a generation per turn | [Plan 0004](0004-run-control-and-run-failures.md) |
| D: durable sleeping, timers instead of polling, `OnWake`, `Wake()` | [Plan 0003](0003-durable-sleeping-and-wakeups.md) |
| E: `extension_error`, `host_error`, idle waits | `idle.go`, `reports.go` |
| F (most): harness tool results with diagnostics, `AfterTool` on every result, sequential rounds, unoffered calls, nested calls, structured output, callers, `ModelTools`, deferred tools, `AddTools`, `tool_search` | `tool_task.go`, `nested.go`, `tools/toolsearch.go` |
| G: upstream compaction (`select`/`summarize`/`retry`), threshold and overflow compaction, policy layers, `BeforeCompact`, context windows | `compaction.go`, `compaction_policy.go` |
| H (part): throttled partials in `pi.live.generation.message` | `generation.go` |
| J (most): stop reasons, error entries, thinking levels, length answers, `AfterRun` with `finish`/`drain`, handoff continuation | `generation.go` |
| Read-only open, `SettingsFunc`, `SubmitTx`, `TaskRuntime.Entries`, `pi.inbox`, tool output limits | `session.go`, `harness.go`, `conversation.go`, `inbox.go`, `tool_task.go` |
| Entry paging, view windows, client transcript, task graph, telemetry spans, events, and metrics, JSONL compaction | `conversation.go`, `view.go`, `transcript.go`, `task_graph.go`, `telemetry.go`, `storage/jsonl.go` |
| Tool output throttling and dropped counters; upstream read (streaming), edit (fuzzy matching, diff details), and bash (timeout, spill, exit codes) results; the PowerShell tool | `tool_runtime.go`, `tools/read_text.go`, `tools/edit_diff.go`, `tools/diff.go`, `tools/coding.go` |
| Positional system messages and context edits (spec §7.4) | `prompt.go`, `context.go` |
| `settings.stream` and `settings.extensions`, `ModelFunc`, runtime helpers (`Agent`, `Task`, `WaitTask`, `Context`, `Diagnostic`, `RetainedOutput`), submission abort results, document families, nested calls as a family, read-only SQLite, trace roots and status categories | `harness.go`, `runtime_api.go`, `documents.go`, `nested.go`, `storage/sqlite.go`, `telemetry.go` |
| Incremental commits and shared reads ([plan 0006](0006-incremental-commits-and-record-storage.md)); commit observers; read-only JSONL; images end to end with a processor and catalog limits; identity and tool model access; structured tool output; `Tx.Configure` | `cow.go`, `delta.go`, `storage/`, `tools/images.go`, `harness.go` |
| Final storage errors, group commit, deferred progress commits, upstream section rendering ([plan 0007](0007-upstream-commit-pipeline.md)) | `session.go`, `lifecycle.go`, `delta.go`, `storage/`, `generation.go` |

## 2. Remaining work, in proposed order

1. **`AfterResponse` on failed attempts.** Upstream's `classify` runs `afterResponse` for every response, failed ones included; Go runs it on success only.
2. **Section render inputs.** Upstream renders sections from `PromptInput{conversationId, agent, env, shown, read}`; Go passes the conversation record and the Harness.
3. **Reading a rewindable document as of an entry** (`snapshotAsOf`), family members included.
4. **`TaskRuntime.Model()`**, as `ToolRuntime.Model()` already is.
5. **Relative agent changes**: `{add, remove}` selections of extensions and tools against the defaults.
6. **Watch end reasons and synchronous observers**: stopped, cancelled, ended, failed; `observeConversation`, `observeTaskGraph`, `observeDoc`.
7. **Richer partials**: thinking and tool-call arguments in `pi.live`, which needs a richer streaming delta API than `func(string) error`.
8. **Deferred model responses**: the `poll` phase for `stopReason: "deferred"`.
9. **Image order.** Images interleaved with text in one message; Go puts a message's images after its text.
10. **Environment parity.** The `env` package's bounded binary and directory readers and file watching. Hosts can use the Go standard library directly; tools need none of them today.
11. **Bounded memory.** Upstream keeps loaded conversations and tails in bounded caches; Go keeps the whole State in memory.

## 3. Not planned

- **Code mode.** It runs model-written scripts in pi-codemode's QuickJS sandbox with deterministic replay; Go would need an embedded engine and a port of pi-codemode.
- **Upstream storage formats and Chord.** Go keeps its own backends; no file-format compatibility is claimed.
- **Provider payload hooks (`beforeProviderRequest`).** Go's `Model` boundary builds provider payloads in adapters, not in the Harness.
- **IndexedDB and Cloudflare adapters.** Platform-specific to JavaScript hosts.
