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

| Incremental commits and shared reads ([plan 0006](0006-incremental-commits-and-record-storage.md)); commit observers; read-only JSONL; images end to end with a processor and catalog limits; identity and tool model access; structured tool output; `Tx.Configure` | `cow.go`, `delta.go`, `storage/`, `tools/images.go`, `harness.go` |

## 2. Remaining work, in proposed order

1. **Positional system updates (spec §7.4).** Upstream records prompt sections and tool declarations as positional `pi.system` messages (PR #9548 `SystemMessage` patches with `toolsAdded`/`toolsRemoved`, rebaselined after a head marker with `ContextEdit` omissions), replays them to know what the model saw, and lets pi-ai send them positionally or fold them. Go renders one leading system message per request and journals the result. Porting needs a system-message model, context-edit omissions, replay in context derivation, and both adapters; it changes what providers cache, not what a run does.
2. **Group commit.** Deferred in [plan 0006](0006-incremental-commits-and-record-storage.md): it would replace per-commit storage cancellation semantics that the Session's failure rules and tests encode, for throughput only.
3. **Image order.** Images interleaved with text in one message.
4. **Environment parity.** The `env` package's bounded binary and directory readers and file watching. Hosts can use the Go standard library directly; tools need none of them today.

## 3. Not planned

- **Deferred commits (`persistWithinMs`).** Upstream publishes progress to watches before storing it; this port's rule that visible state is committed before publication (AGENTS.md) forbids that. Progress commits are throttled instead.
- **Code mode.** It runs model-written scripts in pi-codemode's QuickJS sandbox with deterministic replay; Go would need an embedded engine and a port of pi-codemode.
- **Upstream storage formats and Chord.** Go keeps its own backends; no file-format compatibility is claimed.
- **Provider payload hooks (`beforeProviderRequest`).** Go's `Model` boundary builds provider payloads in adapters, not in the Harness.
- **IndexedDB and Cloudflare adapters.** Platform-specific to JavaScript hosts.
