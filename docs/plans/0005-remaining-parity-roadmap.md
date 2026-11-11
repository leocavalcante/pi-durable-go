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
| Entry paging, view windows, client transcript, task graph, telemetry spans, events, and metrics, JSONL compaction | `conversation.go`, `view.go`, `transcript.go`, `task_graph.go`, `telemetry.go`, `storage/jsonl.go` |
| Tool output throttling and dropped counters; upstream read (streaming), edit (fuzzy matching, diff details), and bash (timeout, spill, exit codes) results; the PowerShell tool | `tool_runtime.go`, `tools/read_text.go`, `tools/edit_diff.go`, `tools/diff.go`, `tools/coding.go` |

## 2. Remaining work, in proposed order

1. **I: storage.** Indexed records instead of whole snapshots, and group commit. This is the largest change: commit cost grows with history because every commit clones and stores the whole State. It needs copy-on-write transactions (every record mutation through a cloning accessor), a delta-aware `Storage` interface, and record tables in SQLite; group commit also needs batch failure semantics in the Session. It should get its own plan.
2. **Multimodal content.** Images in messages and tool results (upstream's read tool returns images), across `Message`, the adapters, and the bridge.
3. **Environment parity.** The `env` package's bounded binary and directory readers and file watching. Hosts can use the Go standard library directly; tools need none of them today.

## 3. Not planned

- **Deferred commits (`persistWithinMs`).** Upstream publishes progress to watches before storing it; this port's rule that visible state is committed before publication (AGENTS.md) forbids that. Progress commits are throttled instead.

- **Code mode.** It runs model-written scripts in a JavaScript sandbox; Go would need an embedded engine.
- **Upstream storage formats and Chord.** Go keeps its own backends; no file-format compatibility is claimed.
- **Provider payload hooks (`beforeProviderRequest`).** Go's `Model` boundary builds provider payloads in adapters, not in the Harness.
- **IndexedDB and Cloudflare adapters.** Platform-specific to JavaScript hosts.
