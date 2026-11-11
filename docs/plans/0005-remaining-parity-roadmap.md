# Plan 0005: Remaining parity roadmap

| | |
|---|---|
| Status | Living roadmap; updated 2026-11-11 |
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

## 2. Remaining work, in proposed order

1. **Tool output throttling.** `progress.outputIntervalMs` and dropped-size counters on running output. (Result bounds with `OutputLimits` and `truncated` diagnostics are done.)
2. **Transcript reads.** Conversation view windows, `ConversationTranscript`, and the task-graph watch. (`Conversation.ScanEntries` pages raw history.)
3. **Telemetry.** Spans and events through an optional host adapter, with `host_error` reports for a failing adapter.
4. **I: storage.** Indexed records instead of whole snapshots, group commit, and deferred commits (`persistWithinMs`). This is the largest change: whole-snapshot commits cost grows with history.
5. **Environment parity.** The `env` package's bounded binary and directory readers, file watching, and PowerShell, in `tools`.

## 3. Not planned

- **Code mode.** It runs model-written scripts in a JavaScript sandbox; Go would need an embedded engine.
- **Upstream storage formats and Chord.** Go keeps its own backends; no file-format compatibility is claimed.
- **Provider payload hooks (`beforeProviderRequest`).** Go's `Model` boundary builds provider payloads in adapters, not in the Harness.
- **IndexedDB and Cloudflare adapters.** Platform-specific to JavaScript hosts.
