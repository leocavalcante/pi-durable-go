# pi-durable-go

An experimental Go port of [Pi Durable v1.5.2](https://github.com/earendil-works/pi/tree/v1.5.2/packages/durable): persistent agent conversations, checkpointed tasks, transactional application state, and restart recovery.

**This is an independent project, not the Pi coding-agent CLI or a complete upstream-compatible replacement.** Its current compatibility reference is Pi Durable v1.5.2; the original Go port base remains v1.1.0. The Go implementation supports a subset of upstream contracts. Read [COMPATIBILITY.md](COMPATIBILITY.md) before relying on it.

## Requirements

Go 1.24 or newer. SQLite uses a pure-Go driver; CGO is not required.

```sh
go get github.com/leocavalcante/pi-durable-go
```

## Quick start

```go
package main

import (
    "context"
    "fmt"

    durable "github.com/leocavalcante/pi-durable-go"
)

type Echo struct{}

func (Echo) Generate(ctx context.Context, req durable.ModelRequest,
    delta func(string) error) (durable.ModelResponse, error) {
    if err := ctx.Err(); err != nil {
        return durable.ModelResponse{}, err
    }
    return durable.ModelResponse{Content: "Hello from a durable agent"}, nil
}

func main() {
    ctx := context.Background()
    h, err := durable.Open(ctx, durable.NewMemoryStorage(),
        durable.HarnessOptions{Model: Echo{}})
    if err != nil { panic(err) }
    defer h.Close(ctx)

    root, err := h.Root(ctx, durable.ConversationOptions{
        Agent: durable.Agent{Model: durable.ModelRef{Provider: "example", ID: "echo"}},
    })
    if err != nil { panic(err) }
    submission, err := root.Submit(ctx, durable.SubmissionRequest{
        Content: "Hello", RequestID: "hello-1",
    })
    if err != nil { panic(err) }
    result, err := submission.Wait(ctx)
    if err != nil { panic(err) }
    if result.Status != "done" { panic(result.Reason) }
    state, err := h.Snapshot(ctx)
    if err != nil { panic(err) }
    fmt.Println(state.Entries[result.Answer].Message.Content)
}
```

The runnable [basic example](examples/basic/main.go) makes no network requests:

```sh
go run ./examples/basic
```

## Persistence and recovery

Replace memory storage with SQLite or JSONL:

```go
store, err := storage.OpenSQLite(ctx, "agent.sqlite")
// Or: storage.OpenJSONL(ctx, "agent-state")
if err != nil { return err }
h, err := durable.Open(ctx, store, options)
if err != nil { store.Close(); return err }
h.Resume()
```

`HarnessOptions.ReadOnly` opens a viewer of a session another process drives: it recovers nothing, never schedules, and refuses commits with writes with `ErrReadOnly`. Import `github.com/leocavalcante/pi-durable-go/storage`. Reinstall application extensions on each reopen; `Open` restores built-in definitions. `Resume`, submission, or a task/submission wait starts scheduling; opening alone does not.

Submissions follow upstream's admission rules. An `input` starts a run in an idle conversation or, while one runs, queues as a follow-up (default), joins the run at its next tool boundary (`WhenBusy: "steer"`), or is refused (`"reject"`, `ErrBusy`); `WhenIdle: "reject"` refuses input to an idle conversation (`ErrIdle`). A `write` appends an entry, at once or at the next boundary. A `continue` starts a run on the context as it is, for example after a failed run; a busy conversation refuses it. Statuses are `queued`, `placed` (with its `Entry`), `done` (with its `Answer`), and `unanswered` (with a `Reason` code and optional JSON `Detail`). Boundaries place queued items one at a time or all at once (`Settings.SteeringMode`, `FollowUpMode`).

Each run is in `pi.live` (`LiveState`, readable with `ReadDocument`): `Run` while busy, the current `Generation` attempt and retry, the round's tool slots, compaction statuses, and `RunFailure` after a run ended without an answer, with reason `model_error`, `no_model`, `nothing_to_answer`, `turn_limit` (`Agent.MaxTurns` tool rounds), `reset`, `aborted`, `faulted`, `orphaned`, `interrupted`, or `restart_limit`. A generation runs one turn: a tool round's end hands the run to the next generation, and a tool's `Handoff` continues the run in a new context. A streamed partial answer is presentation in `pi.live.generation.message`, committed at most every `Settings.Progress.PartialIntervalMs` (default 100 ms); `Conversation.Live` reads the conversation's `pi.live`. Assistant messages record their `StopReason` and requested `ThinkingLevel`; an answer cut off at its output limit (`length`) is an answer whose cut-off tool calls never run, and a failed attempt is kept as an error entry (`StopReason: "error"`, `ErrorMessage`) that never reaches model context. `ModelResponse.StopReason` reports `length`, and both bundled adapters set it. `Settings.AfterFailedRun` keeps (default), continues with, or withdraws (`run_failed`) the inputs queued behind a failed run. `Hooks.AfterRun` runs once a run answers (or every call of its round asks to terminate): the run settles its inputs, creates the tasks the hooks ask for (`RunEndTask`), and ends once they are terminal, keeping queued items queued meanwhile.

A submission's `RequestID` deduplicates admission within one conversation. Retrying it returns the original even if the content changed; reusing it for another submission type is rejected. Persist `submission.ID()` for reconnection with `h.Submission(ctx, id)`. Inputs may use `Entry` to attach a custom non-`pi.*` kind and JSON data to the user message. Display-only write submissions settle immediately during a run unless a reset is queued.

Tool results the Harness writes itself carry an error `ToolResult.Diagnostics` entry with upstream's code (`tool_unavailable`, `invalid_arguments`, `blocked`, `interrupted`, `restart_limit`, `aborted`, `abandoned`), shown to the model in a `<harness>` block after any output; every result passes `AfterTool` hooks. A failing hook is reported as `extension_error` and skipped. `Settings.ToolExecution: "sequential"`, or a called tool's `ExecutionMode: "sequential"`, runs a round one call at a time; a call to a tool the request did not offer settles `tool_unavailable` without running. A running tool can call another through `ToolRuntime.ExecuteTool`, which runs it as an owned task and returns a `NestedToolResult` (`StructuredOutput` is the tool's validated structured output, or its content as a JSON string); `ToolDefinition.Callers` limits a tool to the model or to tools. `Agent.ModelTools` selects which model-callable tools a request offers; `ToolDefinition.Deferred` tools are offered only once loaded, which a result's `AddTools` does from the next request on.

**Admission deduplication is not exactly-once external execution.** A crash between an external effect and the commit recording its result leaves uncertainty. Use service-side idempotency keys, queryable receipts, or reconciliation. Interrupted tools replay only when both their stored and current policies declare `ReplaySafe`. Coding tools default to unsafe.

### Crashes, clean closes, and the restart limit

- **Counting.** `Open` rewrites surviving `running` tasks, including running abort handlers, to `pending` and increments their optional `TaskRecord.Restarts`. Other statuses and stored backoff deadlines are unchanged. A clean `Close` joins local invocations and writes the tasks it stopped back to `pending` without counting them; a failure shutdown writes nothing, so its tasks count as crashed.
- **Resets.** A task's changed checkpoint, yield, wait, outcome, or new or upgraded abort mark resets its count. Structurally identical checkpoints, memos, document writes, and a tool's `Output`/`Details` (stored only in its `pi.live` slot) do not. The count is scheduler-owned.
- **Limit.** `Settings.RestartLimit` (default 5; values below 1 and NaN select the default, fractions round down, and `math.Inf(1)` disables it) ends crash loops. At reservation, before definition resolution, an abort handler or a run of any non-built-in kind cut off that many times in a row ends `faulted` with `ErrorDetail` `{"reason":"restart_limit"}`, reported as `task_faulted` with reason `restart_limit`. Blocked and would-be orphaned tasks end too. A generation whose abort handler is cut off ends its run `aborted` with the fault message as `Detail`, without converting its partial.
- **Built-in endings.** Built-in tasks check the limit when each phase starts. A generation settles its inputs `unanswered` with reason `restart_limit` and `The generation was interrupted <count> in its <phase> phase` as `Detail`, keeping a committed partial as an aborted entry at the limit but dropping it past the limit. A tool call ends with the error result `Tool <name> was interrupted <count>; it is not run again`, followed by its committed output, even for a replay-unsafe tool, and its run continues. A compaction fails with `Compaction was interrupted <count>`. Each fails with `ErrorDetail` `{"reason":"restart_limit"}`.
- **Interrupted requests.** A model request in flight when the host crashed is charged as a failed attempt rather than resent: its partial becomes an aborted entry and, while the retry policy allows that attempt, the generation backs off and prepares the next attempt. Otherwise the run ends `unanswered` with reason `interrupted` and `The host stopped while the model was answering, on attempt <n>`, and the task fails with `ErrorDetail` `{"reason":"interrupted"}`. A request stopped by a clean `Close` is resent; a generation backing off or waiting for its tools is not charged.
- **Reports.** A `failed` outcome whose `ErrorDetail` reason is `restart_limit` or `interrupted` is reported as `task_interrupted`, with `Report.Restarts` holding the count before that commit. To recognize crash loops in stored outcomes, decode `ErrorDetail` of `faulted` outcomes (`restart_limit`) and `failed` outcomes (`interrupted` or `restart_limit`).

Every task running when the host dies is counted, not only the one that caused the crash, and any checkpoint change counts as progress. Keep throttled progress that should not reset the count in documents or memos.

### Model retries

`Settings.Retry` is Pi Durable's retry policy. Its `RetrySettings` pointer fields merge one by one over the default: enabled, 3 retries after the first attempt, a 2,000 ms base delay that doubles per attempt, and a 60,000 ms cap per delay. A nil field keeps the default; an explicit `false` or `0` is honored, and the numbers follow JavaScript's (`math.Inf(1)` retries without bound).

A generation retries a failed model attempt only when the error is transient and not a context overflow. `durable.IsRetryableModelError` and `durable.IsContextOverflowError` port pi-ai v1.5.2's classifiers; errors implementing `Retryable()` or `ContextOverflow()`, as both bundled adapters' sanitized errors do, answer for themselves. The failed attempt's partial is kept as an aborted entry, and the generation sleeps in a `retry` phase until the backoff ends. Afterwards it prepares the request again, so agent, section, and settings changes made meanwhile apply, without counting another turn. Compaction summaries retry under the same policy and classification, without the overflow check, resending their pinned request.

### Compaction

`Conversation.Compact` and automatic compaction follow upstream's policy (`Settings.Compaction`, overridden per conversation by `Agent.Compaction` and per model by `ModelOverrides`; defaults: enabled, 16,384 reserve tokens, 20,000 recent tokens kept, 32,768 background tokens). A compaction selects an old prefix of the model context that leaves `KeepRecentTokens` after it, summarizes it with upstream's prompt, and places the summary, which later context starts with. When the model reports its context window (`ContextWindowModel`), preparing a request above `window − ReserveTokens` first runs a blocking compaction, and above the background threshold starts a background one. A context-overflow error compacts and prepares again instead of failing, unless compaction is disabled or nothing can be compacted. Small conversations may have nothing to compact; such a compaction completes without a summary.

### Upgrading

- `Settings.RetryMax` and `RetryDelay` are removed, and retries are now on by default. Replace `RetryMax: n, RetryDelay: d` with `Retry: durable.RetrySettings{MaxRetries: ptr(float64(n)), BaseDelayMs: ptr(float64(d.Milliseconds()))}` using a local `func ptr[T any](v T) *T { return &v }`; delays now double up to `MaxAgentDelayMs`. Set `Enabled: ptr(false)` to disable retries, for example for test fakes whose error text looks transient (such as "timeout" or "503").
- `pi.generation` and `pi.compaction` are version 3, and `pi.tool` is version 2; each migrates older records at reservation. Older builds leave newer records blocked as `task_too_old`, so do not downgrade with runs live.
- State version 3 stores the run model: opening an older store upgrades it in memory (`running` submissions become `placed`, a busy run moves to `pi.live.run`) and the next commit persists it. Older binaries cannot reopen a version-3 store. `Settings.MaxTurns` is removed; bound tool rounds with `Agent.MaxTurns`.
- Restart accounting adds no storage-version change. On the first open after upgrading, tasks an older build's clean close left `running` are counted once; with the default policy an interrupted request is then retried after the backoff. An older State-v2 build can still open the store, but its snapshot writes drop the optional counter.

## Tools and models

- `tools.Coding()` supplies `read`, `write`, `edit`, and `bash`.
- `tools.NewLocalEnv(cwd)` supplies local filesystem and process execution. **A working directory is not a sandbox.**
- `model.Pi` reuses an installed Pi coding-agent SDK's authentication and provider transports, including existing `/login` OAuth credentials and refresh. Requires Node.js 22.19+ and the tested Pi SDK 1.5.2 baseline; no Pi agent/tool loop is started.
- `model.OpenAI` supplies a pure-Go streaming OpenAI-compatible Chat Completions adapter with an explicit API key.
- Implement `durable.Model` for other integrations. Multimodal APIs and a native Go provider/OAuth catalog are not implemented. See [model/README.md](model/README.md) for setup, credential discovery, and limits.

The [chat example](examples/chat/main.go) wires these together with SQLite and defaults to Pi authentication:

```sh
# First sign in using /login in Pi. Use a provider/model from Pi's /model picker.
go run ./examples/chat -provider openai-codex -model YOUR_PI_MODEL_ID -prompt 'Hello'
# Or keep the standalone pure-Go adapter:
OPENAI_API_KEY=... go run ./examples/chat -adapter openai -model YOUR_MODEL_ID -prompt 'Hello'
```

It makes real model requests only when you run it. Review tools and isolate the process before giving it untrusted input or credentials. Pi credential commands and the selected SDK execute with host permissions; they are not sandboxed.

## Runtime model

A Harness owns one Session and scheduler. A conversation is a transcript, not a goroutine. Entries, tasks, submissions, and JSON documents change on one serialized atomic commit line. Storage succeeds before snapshots or progress are published.

Tasks are checkpointed state machines, not persisted Go stacks. Define their initial checkpoint, phase handler, and optional abort/migration handlers. Task records include start/end times; assistant and tool-result messages include per-attempt durations. Owned child tasks and conversations support cancellation trees. `TaskRuntime.OwnedTasks(ctx)` lists live direct task children, including those from earlier invocations; `AbortOwned(ctx, id)` durably marks a direct child and waits for it to settle. These calls belong outside commit callbacks. Background work can outlive an ordinary conversation abort.

### Task outcomes and definition lifecycle

Task code may commit `completed`, `failed`, or `aborted` outcomes. `failed` requires a non-empty error; `ErrorDetail` is optional JSON. `aborted` may include a `Reason`; `orphaned` requires its blocked `Reason`, while `faulted` carries an error. `faulted` and `orphaned` are scheduler-owned: handler errors or panics, and a phase handler that returns still `running` without checkpoint progress, become `faulted`; aborting a task blocked by missing or incompatible code settles it as `orphaned`. A task can read terminal receipts with `TaskRuntime.Outcomes(ctx, ids)`. Update outcome switches that previously treated all `failed` receipts as execution faults: application-declared failure remains `failed`, while scheduler faults now have their own status. Text diagnostics generated by the runtime are bounded to 4 KiB of valid UTF-8; panic stacks are report-only.

If a handler leaves its task `running`, a changed checkpoint lets `Run` continue into another phase in the same invocation; without that progress it faults. A committed `pending` transition yields, `waiting` waits on tasks, `sleeping` (or `TaskRuntime.SleepUntil`) sleeps until a time on the Harness clock (`WakeAt`), and a terminal transition ends the invocation. A sleeping task holds no invocation, is not counted by a crash, and resumes at its checkpoint once due; an abort mark ends a sleep at once, and abort handlers cannot sleep. The scheduler runs passes on commits, invocation ends, registry changes, `Resume`, and one timer for the earliest sleep (`HarnessOptions.SetTimer`, default `time.AfterFunc`); there is no polling. A host that wakes the process itself sets `HarnessOptions.OnWake`, which receives `Wake{Active, At}` instead of a timer being armed, and calls `Resume` when its alarm fires; `Harness.Wake()` answers the same question. `TaskRuntime.SetTimer` arms a non-durable pacing timer that the invocation's end disarms, and `TaskRuntime.Now` reads the Harness clock. Earlier Go sleeps written as `pending` or `waiting` with a deadline and nothing to wait on are stored as `sleeping`; a wait with targets and a deadline is rejected. Outcomes held in `completing` while ordinary owned work drains drop task memos immediately. A runtime remains valid across continuing phases of the same invocation, as upstream; join phase-owned goroutines before returning when their writes must count in that phase's progress decision.

`TaskDefinition.Migrate` now receives `(fromVersion, input, checkpoint)` and returns migrated input and checkpoint together. It runs under the Session commit mutex at reservation and may also run as an abort/orphan eligibility probe, so keep it pure, deterministic, bounded, and free of Session/registry calls and external effects. A successful reservation commits migrated values atomically; a successful probe is not persisted and can be evaluated again. Missing or failed migrations leave the task live and blocked; failures are cached per task/definition identity for the open Harness (reopening permits retry). `Inspect` derives task states and submissions from one snapshot without running migrations or handlers. `AbortTask(ctx, id)` now returns `AbortMarked` (`"marked"`) or `AbortTerminal` (`"terminal"`) and joins only a captured active run; its join exits on caller cancellation or Session admission sealing, not `Done()`, and cancellation does not undo the durable mark. Do not synchronously abort a task from its own handler or abort a conversation scope containing that handler.

Task-definition identity is per successful extension install. Replacing or reinstalling an extension creates new identities for its task definitions; unrelated installs do not. Reinstalling an unchanged extension is therefore an intentional migration-retry/handover boundary. Harness built-ins (`pi.generation`, `pi.tool`, and `pi.compaction`) take precedence over extension definitions. `CompactionExtension()` is deprecated and installing it is unnecessary.

`HarnessOptions.OnReport` receives best-effort FIFO reports (`storage_failed`, `task_faulted`, `migration_failed`, `task_outdated`, `task_error`, `task_interrupted`, `extension_error`, and `host_error`) on a separate worker. `Harness.WaitForIdle` and `Conversation.WaitForIdle` resolve when the ordinary ownership scope has no live non-background task. A task that commits a `failed` outcome whose `ErrorDetail` reason is `restart_limit` or `interrupted` is reported as `task_interrupted`, with `Report.Restarts` holding its count before that commit. Task reports carry copied task metadata and identify the installing extension where applicable. Enqueueing does not wait for host code; callback panics are recovered and dropped so later reports can still be delivered. Delivery is not durable; already queued reports drain asynchronously and are not joined by `Done()` or `WaitClosed()`, so a handler may run after Session shutdown. `TaskRuntime.Report` is also nonblocking and does not prove that writes in the current commit callback succeeded. A phase sees one `TaskRuntime.Registry()` snapshot; replacement definitions take over at a phase boundary when they can reserve the task.

**Persistence/API migration:** State v1 remains readable, but `Open` commits State v2 during reconciliation; older binaries cannot safely reopen that upgraded store. `SessionEnd.Reason` was renamed to `SessionEnd.Kind`. Scheduler bookkeeping failures, including certified rejected writes, fail the Session; the `RejectedError` exemption remains for ordinary caller commits and caller-cancelled scheduler writes certified unapplied. See [COMPATIBILITY.md](COMPATIBILITY.md) for the full gap list and intentional Go differences.

Set `TaskOptions.AbandonOnRestart` for work whose creator only waits in memory and cannot resume after a restart. A later Harness marks still-live opted-in tasks with `AbortReason: "restart"` at its first scheduling pass, cascading to ordinary owned work before any run handler can replay. Inspection-only opens do not mark them, and newly created tasks in the current Harness are unaffected. Abort handlers can use the reason for cleanup; restart-marked tasks with missing/incompatible definitions remain blocked until code is installed. An explicit abort request replaces a restart reason and may orphan a blocked task, while `AbortOwned` preserves the restart mark. Neither abandonment nor admission deduplication makes external effects exactly once.

Application documents have explicit versions, scopes, and fork policies. Use `UpdateDocument[T]` inside a commit and `ReadDocument[T]` outside it. Reads do not initialize absent state. Rewindable `asOf` documents fork at the entry's entire commit.

`Conversation.Watch` and `WatchDocument` deliver current snapshots with bounded latest-state buffering. They are not audit logs. Retain important facts as immutable entries.

## Safety and limits

- One owning Session per storage. Database transaction safety does not coordinate multiple schedulers.
- SQLite uses WAL and `synchronous=FULL`; JSONL fsyncs complete records. Initial file/directory creation is not directory-fsynced, and guarantees depend on the OS/filesystem/device. Do not assume full power-loss durability.
- Storage currently serializes whole snapshots. This is simple but unsuitable for very large transcripts or high-frequency progress without further work.
- An uncertain storage error or scheduler bookkeeping failure poisons and automatically closes the Session: observers end, pending waits fail, and running invocations are signalled. The failing call returns the original error; later operations return `SessionFailedError`, matching `ErrPoisoned` and unwrapping to the first cause. Ordinary caller commits retain the certified `RejectedError` rollback exemption. Never continue mutating a failed Session; reopen only after shutdown settles.
- `Done()` and `WaitClosed(ctx)` on a Harness or Session expose shutdown completion (`SessionEnd{Kind: "closed"}` or `"failed"` with `Err`). `Err()` reports sealed admission/failure immediately. Waiting does not initiate shutdown; cancelling a wait does not cancel cleanup.
- `Close` stops local invocations without durably cancelling pending work, then writes stopped `running` tasks back to `pending`, so a stopped model request is resent rather than charged as a crash. A write-back failure fails the Session and reports `storage_failed`; `Close` still returns only the backend close error, so inspect `WaitClosed` for the shutdown result. An uncertain write may or may not have applied; reopening reads the durable truth. `AbortTask` requests persisted cancellation and joins the active run. Cancellation is cooperative: task code that ignores it keeps shutdown pending and storage open. A timed-out `Close(ctx)` seals admission and continues cleanup, including write-back; call again or use `WaitClosed` to join it.
- Never make network/model calls, wait for approval, or nest a commit inside a commit callback.
- History and files can contain sensitive information. Do not publish runtime databases or sessions.

## Development

```sh
make check
```

Runs formatting checks, `go vet`, unit/integration tests, and the race detector. Tests use fake models, temporary files, and local HTTP servers. No provider credentials or paid calls are required. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License and provenance

[MIT](LICENSE), the same license as Pi and Pi Durable. The original upstream copyright notice is preserved. See [NOTICE](NOTICE) for the original v1.1.0 port base and current v1.5.2 reference, and [DEPENDENCIES.md](DEPENDENCIES.md) for dependency licenses.
