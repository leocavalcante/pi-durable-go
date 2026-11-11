# Plan 0001: Task outcomes and definition lifecycle

| | |
|---|---|
| Status | Implemented; see implementation record below |
| Date | 2026-11-10 |
| Compatibility target | Pi **v1.5.2**: `@earendil-works/pi-durable` 1.5.2 and `@earendil-works/pi-coding-agent` 1.5.2 |
| Replaces reference | Pi Durable v1.3.0 `main`, commit `4954d760504c11df9b2cbdc040346107b6a8153d` |
| Go baseline | `main` at `537c833` |
| Main code areas | `scheduler.go`, `task_runtime.go`, `registry.go`, `harness.go`, `types.go`, `session.go`, `lifecycle.go`, `generation.go`, `compaction.go`, `conversation.go` |

## 1. Summary

Port Pi Durable 1.5.2's task failure and definition lifecycle. The scheduler will write `faulted` and `orphaned` outcomes. Task invocations will follow the per-phase step rules. The scheduler will derive blocked reasons and migrate tasks during reservation, caching failures. Running tasks will hand over to replacement definitions, hosts will receive typed reports, and `abortTask` will follow the upstream contract.

First, update the compatibility documents from the stale v1.3.0 reference to Pi v1.5.2, the latest release.

After this work, task bugs get durable `faulted` receipts and host reports instead of appearing as application `failed` outcomes. Missing or incompatible definitions leave tasks live and blocked until code is installed or an abort settles them as `orphaned`; migration failures are reported without changing the task record. Deliberate Go differences, including install-based definition identity and queued report delivery, remain documented.

## Implementation record

Milestones 0, 1, 2, 2a, 3, 4, and 5 are implemented against the pinned v1.5.2 reference. The `AGENTS.md` reference edit is deferred because milestone 0 explicitly requires separate maintainer approval; its original port-base instructions remain unchanged. Follow-ups A–J remain outside this implementation.

- Scheduler code is split across `scheduler.go`, `task_resolution.go`, `task_step.go`, `task_abort.go`, and `inspection.go`.
- `reports.go`, `diagnostic.go`, and `json_equal.go` implement queued reporting, bounded persisted diagnostics, and exact structural checkpoint comparison.
- Internal commit errors are classified and sealed on the Session line, before another caller can publish. Expected abort validation failures remain read-only decisions, not Session failures. Abort/orphan bookkeeping uses the internal fail-closed policy.
- Independent review's admission-sealing and direct-orphan/cache findings are addressed by those on-line failure and post-adoption cleanup paths. Its suggested per-phase runtime revocation was not adopted: upstream v1.5.2 creates one runtime in `#run` before the loop, and `#gated` checks invocation end, not phase identity. Continuing phases retain that runtime; only an ending step revokes it. A regression test and the usage documentation make this lifetime explicit.
- Migration-cache retirement happens only after durable adoption. A cancelled, certified-unapplied orphan write therefore retains its cached failure and does not repeat migration or reporting.
- Fault cleanup salvages independently decodable generation fields rather than failing the Session again on a malformed checkpoint. Cancelled open-time reconciliation starts cleanup without forcing the cancelled caller to join backend closure.
- Task/submission waits bind observation to a cancellable context. Registration and watcher cleanup may finish after an admitted storage call settles, but neither caller cancellation nor admission sealing joins that blocked call.
- `README.md`, `COMPATIBILITY.md`, and `NOTICE` record the current reference, API/storage migration, implemented behavior, deliberate differences, and remaining gaps.

Validation on Go 1.24.3:

- `make check` (formatting, vet, all tests, and all race tests): passed.
- Expanded targeted race suite (`Test(Task|ContextToolRound|Owned|AbortOwned|Abandon|Restart|SessionFailure|HarnessFailure|ReportQueue)`), with `-count=20`: passed.
- Installed Pi coding-agent 1.5.2 SDK regression tests with `-race` and fake credentials/loopback servers: passed.
- `git diff --check`: passed. No ignored `s.h.Commit` calls remain in `scheduler.go`.
- Rechecked before publication on 2026-11-11: npm `latest` for Durable, coding-agent, and pi-ai and GitHub **Latest** remain at v1.5.2 (`b4afdddf44e059c67ccf5097e5317d913521b470`).

## 2. Compatibility target: latest Pi release

Verified on 2026-11-10 from npm `latest` dist-tags and the GitHub release marked **Latest**:

| Component | Version | Source of truth | Used for |
|---|---|---|---|
| Pi monorepo release | `v1.5.2` (2026-11-10) | tag `v1.5.2` → commit `b4afdddf44e059c67ccf5097e5317d913521b470` | Single version for every package below |
| `@earendil-works/pi-durable` | 1.5.2 | `packages/durable` at the tag: `docs/spec.md`, `src/`, `test/`, `CHANGELOG.md` | Normative runtime semantics |
| `@earendil-works/pi-coding-agent` | 1.5.2 (Node.js ≥ 22.19.0) | npm `latest` | `model.Pi` bridge baseline, already 1.5.2 |
| `@earendil-works/pi-ai`, `@earendil-works/pi-agent-core` | 1.5.2 | npm `latest` | Transitive SDK dependencies; not imported by Go |

- The npm `pi-durable` tarball ships only `dist`, `README.md`, and `CHANGELOG.md`. Read the normative specification from the Git tag.
- The previous Go reference, `4954d760…`, is two commits after the `v1.3.0` tag (`71023aec…`). Tag `v1.5.2` is 782 commits ahead of it and includes Durable 1.4.0, 1.5.0, and 1.5.1. Durable 1.5.2 has no changelog entries.
- The Pi SDK bridge already uses the latest release. Only the Durable runtime reference is stale.

Targeting policy:

1. Each milestone targets the newest stable Pi release, using the npm `latest` dist-tag and the GitHub **Latest** release. Re-check the target before merging.
2. If a newer release appears, read its `packages/durable/CHANGELOG.md` and the specification sections cited here. Update this plan before continuing.
3. Cite the specification and source from the pinned tag, not from `main`.

Manual drift check. These commands use the network, so do not run them from tests:

```sh
npm view @earendil-works/pi-durable dist-tags --json
npm view @earendil-works/pi-coding-agent dist-tags engines --json
gh release list -R earendil-works/pi --limit 3
gh api repos/earendil-works/pi/commits/v1.5.2 --jq .sha
```

Appendix A highlights and groups Durable compatibility changes between the old reference and v1.5.2; it is not an exhaustive changelog or conformance claim.

## 3. Why this feature is next

The following behavior was observed at `537c833`:

| Area | Go today | Pi Durable 1.5.2 |
|---|---|---|
| Aborting a task whose definition is missing or incompatible | Stays blocked indefinitely. `Conversation.Abort` waits until its context ends. | Settles as `orphaned` with the blocked reason (§5.4). |
| Error returned by `Run` | Recorded as `failed`, which looks like an application decision. | The scheduler writes `faulted` and reports `task_faulted` (§5.1, rule 4). |
| Panic in task code | Kills the host process because the task goroutine has no `recover`. | A thrown exception becomes `faulted`; a Go panic is the closest equivalent. |
| Handler returns while the task is still `running` | `failed`: “task handler returned without a durable transition”. | If the checkpoint changed, the next phase runs in the same invocation. Otherwise, the task faults (§5.1, rules 5 and 6). |
| Migration | Committed separately before reservation, and only the checkpoint changes. A failing `Migrate` reruns on every 20 ms pass. | Applied atomically with reservation to both input and checkpoint. A failure is reported once and retried only for a different definition (§5.4). |
| Definition replaced while a task runs | The old code runs until the next `pending` transition, and nothing is reported. | Handover at the next phase boundary, or a `task_outdated` report (§5.4). |
| Blocked reasons | `missing_task`, `host_definition_too_old`, and `missing_migration`, in inspection only. | `missing_task`, `task_too_old`, and `migration_failed` (§5.4; `TaskInspection`). |
| Host reports | None. | `onReport(report)` with typed kinds (§10.1). |
| Scheduler bookkeeping errors | Ignored with `_ = s.h.Commit(...)`, which can leave tasks stuck in `running` or `completing`. | Fail the Session (§10.1). |
| `pi.tool` task ending without a result entry | The next model request contains an unanswered tool call, which providers reject. | Context derivation adds an error result (§2.1, rule 8). |

`COMPATIBILITY.md` already lists missing orphaned receipts, handover, and migration-failure reporting.

Later gaps depend on these semantics:

- The crash-loop guard (`restarts`, `restartLimit`, `task_interrupted`) ends extension tasks as `faulted` and reports them.
- `pi.live.runFailure` includes the `faulted` and `orphaned` reasons.
- v1.2 nested tool calls create owned tasks whose callers must understand fault and orphan receipts.
- Compaction failure state must handle faulted and orphaned compaction tasks.

## 4. Scope

### In scope

1. **Outcome taxonomy:** add `faulted`, `orphaned`, `Reason`, and `ErrorDetail`. Only the scheduler may write `faulted` and `orphaned`. Persist the change as State version 2.
2. **Reports:** add `HarnessOptions.OnReport` and `Report`. Emit `storage_failed`, `task_faulted` (`threw`), `migration_failed`, `task_outdated`, and `task_error`.
3. **Definition identity:** track each definition's identity and installing extension. Resolve `pi.generation`, `pi.tool`, and `pi.compaction` as Harness built-ins.
4. **Blocked tasks:** derive blocked reasons and expose the v1.5.2 inspection states.
5. **Migration:** migrate input and checkpoint during reservation, in one atomic commit, and cache failures.
6. **Invocation steps:** implement rules 1–6, abort-handler rules, panic containment, and `TaskRuntime.Registry()`.
7. **Scheduler commits:** make failures in the scheduler's own commits fail the Session.
8. **Built-in cleanup:** clean up scheduler-written outcomes for built-in tasks. Add `SubmissionRecord.Detail`.
9. **Orphaning:** orphan in the marking commit or in a later scheduler pass. Tasks marked for restart wait for their definition. `AbortTask` returns `marked` or `terminal` and joins the active run.
10. **Handover:** hand over to replacement definitions and emit `task_outdated` reports.
11. **Tool-round context:** apply §2.1 rules 7 and 8.
12. **Outcome reads:** add `TaskRuntime.Outcomes`.
13. **Compatibility baseline:** re-baseline the documents to v1.5.2 in milestone 0; rename `SessionEnd.Reason` to `SessionEnd.Kind` with the API changes in milestone 1.

### Out of scope

Section 13 orders the follow-up work. In particular, this plan excludes:

- crash-loop counting and `restartLimit`;
- `pi.live.runFailure` and the `afterFailedRun` setting;
- a durable `sleeping` status and wake API;
- nested and structured tools;
- automatic compaction;
- handoff continuation and assistant `thinkingLevel` persistence (follow-up J);
- the Chord observation model;
- indexed storage and group commit;
- reports for hooks, sections, environment builders, and listeners (`extension_error`, `host_error`, and `listener_error`).

## 5. Upstream rules to satisfy

References are to [`packages/durable/docs/spec.md` at `v1.5.2`](https://github.com/earendil-works/pi/blob/v1.5.2/packages/durable/docs/spec.md), unless another file is named. Source and test references use that same tag.

1. **Outcomes.** The outcomes are `completed{result}`, `failed{error{message, detail?}, result?}`, `aborted{reason?, result?}`, `orphaned{reason}`, and `faulted{error}`. Only the scheduler writes `faulted` and `orphaned` (§5.1; `src/types.ts` `TaskOutcome`).
2. **Steps.** Before every phase, a step runs on the Session line. It applies the first matching rule:
   1. terminal, `completing`, `waiting`, or `sleeping`: stop;
   2. Session closing: stop;
   3. run invocation with an abort mark: stop;
   4. uncaught error: fault;
   5. checkpoint changed: continue or hand over;
   6. checkpoint unchanged: fault.

   Before the first phase, only rules 1–3 apply. If an owner has cancellation intent, the invocation ends without a fault, and the scheduler schedules the cascade (§5.1; `src/harness/scheduler.ts` `#rules`).
3. **Abort invocations.** An abort invocation runs its handler once. Afterward, rules 1, 2, and 4 apply. If the handler returns without a terminal outcome, the task faults (§5.1; `#runAbort`).
4. **Held outcomes.** If ordinary owned work is still live, a fault is held as `completing`. A held outcome other than `completed` is cancellation intent. The Harness cleanup runs in the final commit (§5.4; §5.5, rules 1–4).
5. **Run resolution.** At the start of a run invocation:
   - a missing definition makes the task blocked: `missing_task`;
   - a stored version newer than the definition makes it blocked: `task_too_old`;
   - a stored version older than the definition calls `migrate(input, checkpoint, fromVersion)`;
   - a migration failure makes it blocked: `migration_failed`;
   - a successful migration commits the migrated record and `running` atomically (§5.4).
6. **Migration failures.** Report a failed migration once as `migration_failed`. Retry it only after the registry resolves a different definition object. A newer definition without `migrate` is a migration failure (§5.4; `#resolve`).
7. **Derived blocked state.** Never store blocked state. Missing code alone never terminalizes a task. Blocked tasks remain live and are reconsidered when the registry changes. Running tasks, tasks with an abort-request mark, and held tasks are never blocked. A task whose mark has `abortReason: "restart"` can be blocked (§5.4; `blockedReason`).
8. **Orphaning.** Aborting a blocked task settles it as `orphaned`, using the blocked reason.
   - The marking commit settles it immediately when no invocation of either mode is active, no ordinary owned work is live, and the current registry cannot take the task, including after a migration attempt in that commit.
   - Otherwise, the scheduler settles it where it would reserve the abort invocation. An orphaned outcome therefore never holds.
   - A restart mark alone never orphans a task. An abort request replaces a restart mark. `abortOwned` keeps the restart mark (§5.4; `abort()`).
9. **Built-in cleanup.** For built-in run, tool, and compaction kinds, a scheduler-written outcome triggers cleanup (§5.4):
   - convert a committed generation partial into an aborted assistant entry;
   - mark the run inputs `unanswered` with `faulted` and the error detail, or with the blocked reason;
   - clear run control;
   - mark a tool slot `done` without an entry;
   - remove compaction status.
10. **Handover.** At each phase boundary, refresh the registry snapshot. If a different definition object can reserve the task, write the task back as `pending` and end the invocation. Otherwise, keep running and report `task_outdated` once for each resolved definition (§5.4; `#progress`).
11. **Abort result.** `abortTask()` returns `marked` after the mark commits and any active run joins, or after orphan settlement. It returns `terminal` for an already terminal task and rejects unknown IDs (§2.2).
12. **Inspection.** `inspect()` writes nothing and runs no task code. It reports each task as `running`, `pending{migrates}`, `waiting{on}`, `sleeping{until}`, `completing`, or `blocked{reason, error?}`, and includes submissions (§2.2; `src/harness/types.ts` `TaskInspection`).
13. **Scheduler bookkeeping.** A failure in the scheduler's own commit fails the Session. A throwing `migrate` blocks only its task (§10.1).
14. **Reports.** Task reports carry `task` (ID, kind, and conversation). They name the extension that installed the definition the task ran, as of reservation. Report `storage_failed` once after the Session is sealed. A failure inside `onReport` is dropped (§10.1).
15. **Tool-round context.** Move tool results directly after their assistant message, in call order. Add an error result for each call that has none, and drop results that have no preceding call (§2.1, rules 7 and 8).

## 6. Design

### 6.1 Outcome model and persistence

```go
const (
	OutcomeCompleted = "completed"
	OutcomeFailed    = "failed"
	OutcomeAborted   = "aborted"
	OutcomeOrphaned  = "orphaned" // scheduler only
	OutcomeFaulted   = "faulted"  // scheduler only
)

type Outcome struct {
	Status      string          `json:"status"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`       // failed and faulted message
	ErrorDetail json.RawMessage `json:"errorDetail,omitempty"` // optional structured diagnostic
	Reason      string          `json:"reason,omitempty"`      // aborted (optional), orphaned (required)
}
```

`applyTransition` validates outcomes committed by tasks:

| Status | `Error` | `ErrorDetail` | `Reason` | `Result` |
|---|---|---|---|---|
| `completed` | Forbidden | Forbidden | Forbidden | Optional |
| `failed` | Required, non-empty | Optional JSON | Forbidden | Optional |
| `aborted` | Forbidden | Forbidden | Optional | Optional |
| `orphaned` or `faulted` | Rejected with `ErrInvalid` because only the scheduler writes them | | | |

Validation applies only to new transitions. Existing records load unchanged.

Normalize built-in outcomes as follows:

- `settleGenerationAbort` writes `aborted` without `Error`.
- `pi.tool` writes a bounded, non-empty `Error` message for `failed`.
- Compaction commits an explicit `failed` outcome for a missing model or model error. These cases would otherwise become faults under rule 4.
- A shared diagnostic helper bounds scheduler- and built-in-generated persisted error messages to 4 KiB of valid UTF-8, including any truncation marker. An empty `err.Error()` gets a non-empty fallback such as “task failed without an error message”. Apply this to generation failures, tool failures, compaction failures, faults, and formatted panic values. Report errors retain the original cause and, for panics, the stack; stacks are never persisted. This bound does not silently truncate task-authored results or structured detail.

`applyTransition` drops memos whenever a task-written outcome becomes `completing` or `terminal`, not only on terminalization. Scheduler-written outcomes follow the same rule (§6.7). Retain the checkpoint through a hold for Go's generation-partial cleanup.

**State version 2.** `NewSession` accepts versions 1 and 2. It upgrades version 1 in memory and refuses later versions. The next commit stores version 2.

This prevents downgrades from losing data:

- SQLite and JSONL loads decode with `json.Unmarshal`, which ignores unknown fields.
- The next whole-snapshot commit would therefore delete `Reason`, `ErrorDetail`, and `Detail` without warning.
- Older code also does not understand the new statuses.

Document that downgrading is refused, as upstream 1.4.0 did for its SQLite schema.

### 6.2 Reports

```go
type ReportKind string

const (
	ReportStorageFailed   ReportKind = "storage_failed"
	ReportTaskFaulted     ReportKind = "task_faulted"
	ReportMigrationFailed ReportKind = "migration_failed"
	ReportTaskOutdated    ReportKind = "task_outdated"
	ReportTaskError       ReportKind = "task_error"
	// task_interrupted, extension_error, host_error, and listener_error are reserved for follow-ups.
)

type ReportTask struct {
	ID           TaskID
	Kind         string
	Conversation ConversationID
}

type Report struct {
	Kind      ReportKind
	Err       error
	Task      *ReportTask
	Extension string // installer of the definition the task ran; empty for built-ins
	Reason    string // task_faulted: "threw"; task_outdated: missing_task|task_too_old|migration_failed
}

// HarnessOptions.OnReport func(Report)
// TaskRuntime.Report(err error) emits task_error.
```

Delivery rules:

- Use one serial report worker per Harness, with a process-local FIFO. Producers only enqueue; they never wait for `OnReport`. The queue mutex protects enqueue/dequeue only and is released before invoking the handler. Copy task metadata instead of retaining transaction records.
- Call `OnReport` on that worker, never inline in a transaction, task invocation, scheduler pass, or shutdown path. The worker holds neither the Session mutex nor `Scheduler.mu`. Recover and drop handler panics and continue with later reports.
- Delivery order is enqueue order, not a deterministic global order between concurrent producers. Reports are best-effort diagnostics, not durable events: no retry or cross-process delivery guarantee is made.
- The worker is not part of the Session's shutdown join. A handler may call `Err()`, `Snapshot()`, `Inspect()`, `Close()`, or `WaitClosed()` without creating a shutdown dependency cycle. Normal sealed-admission rules still apply. Close report admission only after backend closure and its possible `storage_failed` enqueue; drain already queued reports asynchronously. `Done()` and `WaitClosed()` do not wait for report delivery.
- Handlers should return promptly: a blocked handler delays all later reports and retains the queued diagnostics, although it cannot stall scheduling or backend closure. A blocked handler may outlive Session shutdown.
- `TaskRuntime.Report` is a nonblocking enqueue, including when called from a commit callback. It conveys no claim that that callback's writes committed; callers reporting a durable change must call it after their commit succeeds. The callback must not wait for report delivery.
- Enqueue durable facts only after the commit succeeds. Migration and outdated reports describe in-memory resolution decisions; they require no task-record write and are enqueued after leaving the Session line. A failure hook only enqueues and cannot run host code.

| Kind | Enqueue point |
|---|---|
| `storage_failed` | Exactly once for the first failure, after sealing, when an uncertain write, scheduler bookkeeping failure, or failing `Storage.Close` fails the Session. A plain Session without a Harness drops it. |
| `task_faulted` | After the fault commit, including a hold, for errors, panics, no progress, and abort handlers that return without an outcome. |
| `migration_failed` | Once for each task and effective definition identity, including a newer definition without `Migrate`; inspection alone never enqueues it. |
| `task_outdated` | After the phase-boundary decision, once for each invocation and newly resolved identity, including a missing definition. |
| `task_error` | When `TaskRuntime.Report` is called; delivery is asynchronous. |

Queued delivery is a deliberate Go difference from upstream's inline `onReport`; document its ordering and shutdown behavior.

### 6.3 Definition identity and resolution

- Each successful `Install` assigns a new monotonically increasing identity to the definitions in that installed extension, including a same-name replacement. Unrelated installs leave existing definitions' identities unchanged. Snapshots preserve identities; Go function values cannot be compared.
- This is an explicit approximation, not upstream object identity: reinstalling the same unchanged extension is a new identity in Go, so it can trigger handover and retry cached migration failures. Upstream reusing the same task object need not do either. Document reinstall as an intentional retry boundary.
- `RegistrySnapshot` keeps `resolvedTask{Definition, Identity, Extension}` for each kind. `Task(kind)` keeps its public signature.
- `Harness.resolve(kind, snapshot)` resolves `pi.generation`, `pi.tool`, and `pi.compaction` first. These built-ins have fixed identities. Other kinds come from the snapshot. Built-in resolution always wins over a colliding extension kind. For this milestone, retain acceptance of legacy installs, including `CompactionExtension()`, rather than adding a separate registration rejection; document that extensions cannot replace Harness built-ins.
- `Open`, `Compact`, and `examples/chat/main.go` stop installing `CompactionExtension()`. Keep the function exported, deprecate its unnecessary registration, and document that installing it has no effect on built-in resolution.
- `fit(record, resolved)` is read-only: it runs no task code, changes no cache, and emits no report. It returns one of:
  - `ready`;
  - `ready + migrates`, including when the newer definition has no `Migrate` yet;
  - `blocked missing_task`;
  - `blocked task_too_old`;
  - `blocked migration_failed`, only when a failure is cached for this task and identity.
- Inspection additionally derives `migration_failed` for `ready + migrates` without `Migrate`, but does not cache or report it. Handover uses a read-only `canReserve` check: equal version, or higher version with `Migrate`. The migration-capable `resolveForStart` below is used only when considering a reservation or direct orphaning.

### 6.4 Reservation and migration

The dispatch snapshot is only a candidate list. Resolve and migrate on the Session line, inside the reservation commit, rather than running a migration concurrently with commits. This matches upstream's decision-time migration and avoids stale-snapshot retries.

1. Reread the task and recheck eligibility in the reservation callback: live runnable status, invocation absence, fixed invocation mode, and owner cancellation intent. An unmarked task requires satisfied waits and a due `WakeAt`; an abort-marked task ignores both and waits only for ordinary owned work to drain. Use the current registry snapshot, not the earlier dispatch snapshot. A stale candidate returns `errNoChange`; it reserves nothing.
2. Call `resolveForStart(record, snapshot)`, which uses `fit` and, when migration is needed, invokes:

   ```go
   type MigratedTask struct{ Input, Checkpoint json.RawMessage }
   Migrate func(fromVersion int, input, checkpoint json.RawMessage) (MigratedTask, error)
   ```

   - `Migrate` runs under the Session mutex, but never under `Scheduler.mu`. Pass detached copies of input and checkpoint, recover panics, validate both returned values as JSON, and detach them before retaining them.
   - A missing `Migrate`, an error, a panic, or invalid JSON is a migration failure. Cache `{identity, err}` for the task and enqueue `migration_failed` once after leaving the Session line. Inspection does not perform this attempt.
   - A failure leaves the task record unchanged, including a satisfied `waiting` record. If the task has an abort-request mark and its ordinary owned work has drained, use the same failure reason to orphan it instead (§6.8). A restart mark alone never orphans it.
   - A missing definition, an older definition, or a cached migration failure follows the same blocked/orphan decision, without calling task code.
3. For a ready task, write migrated version, input, checkpoint, `running`, and first `StartedAt` together in one commit. There is no intermediate migration or `waiting` → `pending` commit. Register the fixed-mode invocation on the line so marks, inspection, and later reservations see it; start its handler only after storage succeeds. If reservation fails or the Session seals before the handler starts, finish and remove the unstarted invocation without running a handler.
4. Protect the migration cache with `Scheduler.mu`. Read, insert, and remove entries in short critical sections; release it before `Migrate`. Clear entries when tasks become terminal or resolve to another identity. The cache contains only live tasks. Inspection captures records, invocation state, cache state, and registry resolution on the Session line (§6.10).

`Migrate` must be pure, deterministic, bounded, and free of external effects or Session/registry calls. It blocks the Session line and scheduler while it runs. A successful migration can be evaluated again when an abort marking commit only probes eligibility (§6.8), or after reopen; it is not a memoized effect. A failed migration is attempted/reported only once per task and effective identity in that open Harness.

Registry listeners already wake the scheduler. Unrelated installs preserve the failed definition's identity, so they neither retry its migration nor repeat the report. Reinstalling its extension creates a new identity and permits a retry.

### 6.5 Invocation step loop

`Scheduler.start` keeps one invocation, its fixed mode, and its `done` channel. The run loop becomes:

```text
resolved := reservation resolution
snapshot := registry snapshot captured at reservation
previous := none
loop:
    step on the Session line (commitInternal), rereading the committed task:
      rule 1  terminal | completing | waiting | pending (Go yield) -> end, no write
      rule 2  scheduler closing or Session sealed                  -> end, no write
      rule 3  run mode and abort mark                              -> end; abort invocation later
              owner has cancellation intent                        -> end; schedule cascade
      before the first phase: continue; do not apply rules 4–6
      rule 4  previous.err != nil                                  -> write faulted (hold if owned work live)
      rule 5  checkpoint differs from previous.checkpoint           -> refresh snapshot; hand over (6.9) or continue
      rule 6  checkpoint unchanged                                 -> write faulted "returned without durable progress"
    after the step succeeds: enqueue staged reports
    if ended: return
    record := detached committed task returned by the step
    previous.checkpoint := a separate copy of record.Checkpoint
    previous.err := callPhase(resolved.Run, ctx, runtime(snapshot), record) // recovers panics
```

When a step ends the invocation, close the runtime gate inside that step, before releasing the Session line, even when it writes nothing. Keep the invocation's join channel open until the goroutine has finished its handler and final bookkeeping. A queued runtime commit either precedes an ending step and counts, or reaches the line afterward and rejects. A continuing step retains the same invocation runtime, as in upstream v1.5.2's `#run`/`#runtime`/`#gated`; phase-owned goroutines must be joined before returning if their writes must precede that phase's progress decision.

- Use structural JSON equality for checkpoint comparison: ignore object-key order and compare numbers by value (`1`, `1.0`, and `1e0` are equal), without rounding distinct large integers through `float64`. `UseNumber` plus raw `reflect.DeepEqual` is insufficient because it compares number spellings. Preserve a separate pre-handler checkpoint so mutations to the handler's detached argument cannot change the progress baseline.
- Memos, documents, and recommitting the same checkpoint do not count as progress.
- **Go-specific yield.** Keep the task-committed `pending` transition. Built-in phase changes and retry backoff (`WakeAt`) use it. The step treats it like `waiting`.
- **Abort invocations.** Run the handler once, using a default `aborted` outcome when `Abort` is nil, then apply rules 1, 2, and 4.
  - A handler that returns without `terminal` or `completing` faults with “abort handler returned without a terminal outcome”.
  - Abort-mode `TaskRuntime.Commit` accepts only `running` and `terminal`. It rejects `waiting` and `pending`, as upstream disallows waits in abort handlers.
- **Panic containment.** Contain panics in `Run`, `Abort`, `Migrate`, and `OnReport`.
  - Format the fault message as `panic: <value>`, using the bounded diagnostic helper (§6.1).
  - Do not persist a stack trace.
  - Attach the stack to the report error.
- **Runtime snapshot.** `TaskRuntime.Registry()` returns the current phase's snapshot. Built-in tasks use it instead of `r.h.registry.Snapshot()`, so one phase sees one consistent registry.

### 6.6 Scheduler-internal commits

Add `Scheduler.commitInternal(ctx context.Context, fn func(*Tx) error) (committed bool, err error)` in milestone 1, before reservation changes depend on it. Use it for these scheduler commits:

- reconcile and abandonment;
- cascade marks and `failFast` marks;
- wakes from waiting;
- reservation;
- step, fault, and handover writes;
- orphan settlement;
- `completing` finalization;
- writing `pending` back after cancellation.

Handle results as follows:

- **Callback `errNoChange`:** a decision not to write; skip storage and return `(false, nil)`. Returning `nil` from the callback means a real commit. Callers retain any read-only decision separately and must never infer a reservation from `err == nil` alone.
- **Successful write:** return `(true, nil)` only after storage succeeds. A failed write returns `committed == false`, even if its outcome is uncertain; that uncertainty fails the Session.
- **Admission already sealed:** return its `ErrClosed` or existing `SessionFailedError` without failing the Session again. Do not exempt an error merely because it wraps `ErrClosed` or `ErrPoisoned` while the Session is healthy.
- **Caller cancellation:** cancellation before storage admission, or a context-cancellation error that `RejectedError` certifies was unapplied, ends only that call when its `ctx` is cancelled. In particular, cancelled open-time reconciliation is not a bookkeeping failure. Do not exempt an uncertain storage error just because it wraps a context error; `Session.Commit` must still poison the Session.
- **Any other error, including a certified `RejectedError` from a scheduler write:** call `Session.fail(err)` to seal the Session with a first cause and enqueue `storage_failed`. As upstream says, “running the commit again fixes none of them.” A caught migration failure is a blocked resolution, not an error returned from this wrapper.

Replace every `_ = s.h.Commit(...)` in `scheduler.go`. Existing callbacks that return `ErrConflict` as a stale-decision sentinel change to `errNoChange`; real revision/storage conflicts are not silently retried.

This narrows the documented Go rejection exemption: ordinary caller commits retain the `RejectedError` rollback-without-poisoning contract, but scheduler bookkeeping writes fail closed even when storage certifies that nothing was applied (apart from the caller-cancellation case above). Update `RejectedError` documentation, README, and `COMPATIBILITY.md` in milestone 1.

### 6.7 Scheduler-written outcomes and Harness cleanup

Write a fault through `commitInternal`, without the runtime gate:

- `StartedAt` stays set.
- If ordinary owned work is live, write `completing{faulted}` and mark owned work for cancellation.
- Otherwise, terminalize the task, set `EndedAt`, retire task documents, and run the Harness cleanup in the same commit.
- `orphaned` never holds.
- The `completing` branch of `dispatch` runs cleanup for scheduler-written `faulted` or `orphaned` outcomes in the final commit. Normal orphaning never creates a hold, but finalization must not omit cleanup for a loaded held scheduler outcome.
- Drop memos at every hold, including task-written `completed`, `failed`, and `aborted` outcomes (§6.1). Keep the checkpoint until terminalization because Go stores generation partials there; upstream keeps them in `pi.live`.

`Harness.settleSchedulerOutcome(tx, task)` cleans up these built-in kinds and ignores all others:

| Kind | Cleanup in the terminal commit |
|---|---|
| `pi.generation` | If `Busy == task.ID`:<br>• convert the checkpoint `Partial` to an aborted `pi.assistant` entry;<br>• mark each input submission still `running` as `unanswered`;<br>• use reason `faulted` with `Detail` set to the message, or the blocked reason for an orphan;<br>• clear `Busy`;<br>• leave the inbox unchanged; queued items stay queued, matching upstream's default `afterFailedRun: keep`. |
| `pi.tool` | Remove the task's `pi.live` slot without appending a result entry. §6.11 adds the missing result to the model context. Go removes the slot rather than retaining upstream's `done` slot; document this observation difference. |
| `pi.compaction` | Nothing to clear; Go has no compaction status. |

Add `SubmissionRecord.Detail json.RawMessage` as an optional field in State version 2. This plan does not normalize other run-failure reasons such as `model_error` and `no_model`; plan C covers them.

### 6.8 Abort and orphaning

New signature: `Harness.AbortTask(ctx, id) (AbortResult, error)`. `AbortResult` is `AbortMarked` or `AbortTerminal`.

The marking commit runs before this call enables scheduling, so aborting a paused task cannot start its `Run` handler first:

1. Reject an unknown ID with `ErrNotFound`.
2. Return `terminal` without a write for an already terminal task.
3. Capture the active invocation, if any, and its immutable mode on the Session line. Direct orphaning requires no invocation of either mode and a task that is not `completing`. Lock order is Session mutex, then `Scheduler.mu`; release `Scheduler.mu` before migration.
4. When no ordinary owned work is live either, call `resolveForStart` with the current registry snapshot. This may run `Migrate` in the marking commit. A failed or missing migration is cached/reported under §6.4 and orphans the task with `migration_failed` in this same commit, just as missing or too-old code does with its reason. Terminalization, document retirement, and built-in cleanup are atomic.
5. Otherwise, set or upgrade the mark; an abort request replaces a restart reason. A successful migration probe does not persist migrated values here: only reservation stores them with `running`, so the pure migration may be evaluated again.

After the commit:

- enable scheduling, cancel the captured run invocation if any, and kick the scheduler;
- join only that captured run, not an already active or newly started abort invocation;
- select on its `done`, the caller context, and the Session's internal `closing` channel, which closes at admission sealing. On sealing, return `Err()` immediately; recheck `Err()` before returning a successful join. Never use public `Done()` for this: it waits for all invocations and backend closure, so non-cooperative task code can keep it open forever.

The scheduler also orphans tasks. At the point it would reserve an abort invocation, use `resolveForStart`; if it is blocked and owned work has drained, settle `orphaned` rather than running any handler. This covers a definition that disappeared during a run and a reopened marked task. A restart mark alone keeps waiting for its definition.

`TaskRuntime.AbortOwned` keeps the restart reason. If the child already has a restart mark, it waits without upgrading or orphaning it. Otherwise, it uses the same direct orphan rule and waits for terminal settlement, bound to the invocation, caller context, and Session seal.

`Conversation.Abort` marks its selected work and waits for terminal settlement; blocked tasks now settle instead of hanging. Document that a task must not synchronously call `Harness.AbortTask` on itself or `Conversation.Abort` on a scope containing itself: the join would wait for its own handler to return. It should commit its outcome and return, or let a host request the abort. A context deadline stops only the join, not its durable mark.

### 6.9 Handover

At rule 5, resolve the task's kind from a fresh snapshot.

- **Same identity:** continue.
- **Different identity that can reserve the task:** the definition has the same version, or a higher version with `Migrate`. Write the task as `pending` in the step commit with the checkpoint, memos, and abort mark unchanged. End the invocation. The next reservation applies §6.4.
- **Missing or unusable replacement:** keep running the old definition. Report `task_outdated` with `missing_task`, `task_too_old`, or `migration_failed` once for each newly resolved identity.

The new invocation cannot overlap the old one because `Scheduler.start` refuses to start while an invocation exists. Runtime commits queued by the old invocation reject because the record is no longer `running`, or because the invocation ended.

### 6.10 Inspection

```go
type TaskInspection struct {
	Record TaskRecord
	State  TaskInspectionState
}

type TaskInspectionState struct {
	Kind     string     // running, pending, waiting, sleeping, completing, blocked
	Migrates bool       // pending
	On       []TaskID   // waiting: live part of WaitOn, or live ordinary owned work when abort-marked
	Until    *time.Time // sleeping: future WakeAt with nothing else awaited
	Reason   string     // blocked
	Err      error      // blocked: cached migration error, or "has no migration from N"
}

// HarnessInspection gains Submissions: queued and running submissions in ID order.
```

Capture the task records, scheduling/invocation state, migration cache, and one registry snapshot at a single point on the Session line, taking `Scheduler.mu` only for short reads. Do not combine a detached Session snapshot with later scheduler/cache reads that could describe another reservation.

Derive inspection state in upstream `#inspectTask` order, adapting Go's `WakeAt` encoding:

1. active invocation: `running`;
2. `completing`;
3. `waiting` when the relevant live wait set is non-empty (`WaitOn` normally, ordinary owned work when abort-marked);
4. `sleeping` for a future `WakeAt` only when not abort-marked;
5. `blocked` under the read-only checks in §6.3, except an abort-request mark is pending for abort reservation/orphaning; a restart mark can still be blocked;
6. `pending`, with `migrates` when applicable.

Every abort mark ignores `WakeAt`, including a restart mark. Never call `Migrate`, populate its failure cache, emit reports, or run handlers during inspection. Remove `Ready` and `Blocked`.

### 6.11 Context for incomplete tool rounds

After existing reset/compaction reduction and exclusion of aborted assistant messages, normalize the derived messages as follows (`src/harness/context.ts` `orderToolResults`):

- Each assistant message starts a new result-matching window, ending immediately before the next assistant message or at the transcript tail. Results outside that window cannot satisfy its calls.
- Match results by `ToolCallID` only within that window. The first result for each call ID wins; discard later duplicates, results for unknown IDs, and tool messages preceding any matching assistant call.
- Emit the assistant, then one result per call in call order, ahead of intervening user or system messages. A result after another assistant message never moves back to an earlier round, even when an older round used the same call ID.
- For each missing result, synthesize:

  ```go
  Message{
      Role:       "tool",
      ToolCallID: call.ID,
      ToolName:   call.Name,
      IsError:    true,
      Content:    "Tool result unavailable: history ends before this call completed.",
  }
  ```

- Leave stored entries unchanged. Apply the same normalization to normal model requests, `Conversation.Context`, and compaction input. Go's `Message` has no tool-result details field, so upstream's structured `{reason: "missing_result"}` diagnostic remains unexposed; the IDs, name, error flag, and text are preserved.

This is an independent change and lands in milestone 2a before scheduler-written tool faults become possible.

### 6.12 Concurrency rules

- **Lock order:** Session mutex before `Scheduler.mu`. The latter protects invocations, scheduling flags, and the migration cache. Never call Session APIs, take registry snapshots, or invoke callbacks while holding `Scheduler.mu`.
- **Transactional callbacks:** `Migrate` is deliberately synchronous under the Session mutex, as are existing `Initial`, document, `ConversationCreated`, and commit callbacks. They must be bounded and must not call Session APIs, wait for reports/tasks, mutate the registry, or perform external effects. The blanket ban on user code under the Session mutex does not apply to these transaction-scoped callbacks.
- **Off-line callbacks:** run/abort handlers execute off the Session line; `OnReport` executes only on its independent worker. No scheduler/report mutex is held while calling them. Enqueue never waits for delivery, and the report queue mutex is never held while acquiring Session or scheduler locks.
- **Invocation mode and end:** fix the mode before publication; close the runtime gate on the Session line when a step ends it. Keep the join channel/map entry until its goroutine finishes, preventing handler overlap.
- **Waits:** invocation joins and task waits observe caller cancellation and admission sealing, not shutdown-completion `Done()`. A failed or closing Session ends the wait even if task code ignores cancellation; a healthy Session still requires a caller deadline to bound a non-cooperative run. Backend closure itself must continue joining admitted work.

## 7. Public API changes

| API | Before | After | Breaking |
|---|---|---|---|
| `Outcome` | `completed`, `failed`, `aborted`; `Error` | Adds `faulted` and `orphaned`, plus `Reason` and `ErrorDetail`. Validation is stricter. | Yes: `failed` requires `Error`; tasks cannot write `faulted` or `orphaned`; `aborted` rejects `Error`. |
| `TaskDefinition.Migrate` | `func(int, json.RawMessage) (json.RawMessage, error)` | `func(fromVersion int, input, checkpoint json.RawMessage) (MigratedTask, error)` | Yes |
| `HarnessOptions.OnReport`, `Report`, `ReportKind`, `ReportTask` | None | New | No |
| `Harness.AbortTask` | `(ctx, id) error` | `(ctx, id) (AbortResult, error)`; joins the active run | Yes |
| `TaskInspection` | `Ready`, `Blocked` | `State TaskInspectionState` | Yes |
| `HarnessInspection` | `Scheduling`, `Tasks` | Adds `Submissions` | No |
| `TaskRuntime` | No outcome-read, report, or registry-snapshot methods | Adds `Outcomes(ctx, ids)`, nonblocking queued `Report(err)`, and `Registry()` | No |
| `SubmissionRecord` | None | Adds `Detail json.RawMessage` | No |
| `SessionEnd` | `Reason` | `Kind`, matching v1.5 tagged `kind` | Yes; milestone 1 |
| `CompactionExtension()` | Installed by `Open` and `Compact` | Deprecated optional registration; built-in resolution needs no install | No |
| Stored `State.Version` | 1 | 2; reads 1 | Downgrade refused |

Behavioral breaks also need migration notes:

- Checkpoint progress calls `Run` again in the same invocation instead of failing; returning an error or making no progress now yields `faulted`, not `failed`.
- Abort-mode `Commit` rejects `waiting` and `pending`. Every outcome hold drops memos.
- `AbortTask` joins the active run and can block until caller cancellation or a Session seal; a task must not synchronously abort itself or its own scope (§6.8).
- Scheduler bookkeeping errors, including certified rejected writes, fail the Session; ordinary caller commits retain their rejection exemption (§6.6).
- Reinstalling an unchanged extension creates new task-definition identities, potentially retrying migration and causing handover (§6.3).
- Reports are queued and may arrive after `Done()` or `WaitClosed()`; shutdown never waits for `OnReport` (§6.2).

The package is experimental. Document each break and deliberate upstream difference in the README, `COMPATIBILITY.md`, and the pull request description.

## 8. Milestones

Each milestone is one pull request and must pass `make check` before dependent work starts. Milestone 2a is independent and can land earlier, but must precede milestone 3.

### Milestone 0: Re-baseline compatibility to Pi v1.5.2

- Update `COMPATIBILITY.md`:
  - set the reference to `v1.5.2` (`b4afdddf…`, 2026-11-10);
  - pin reference/specification/provenance links to `v1.5.2`, not `tree/main` or `blob/main`;
  - replace “Upstream changes not ported” with the Appendix A triage, describing this plan's items as proposed, not already implemented;
  - keep the v1.2 and v1.3 items.
- Update `NOTICE` with the current reference.
- In `README.md`, change “last compared against” to v1.5.2.
- In `AGENTS.md`, the pinned reference still names v1.1.0. Keep v1.1.0 as the original port base and add v1.5.2 as the current reference. This file contains repository instructions, so get the maintainer's approval first.
- Verify that the SDK baseline text in `model/README.md`, `DEPENDENCIES.md`, and `README.md` still says 1.5.2 and Node.js 22.19 or later. No change is expected.
- Optionally add `scripts/upstream-versions.sh`, using the §2 commands manually. Never run it from `make check`.

**Exit criteria:** current-reference documents name v1.5.2 while preserving the original v1.1.0 port base and historical references, `make check` passes, and there are no runtime or public-API changes. Any `AGENTS.md` update requires separate maintainer approval.

### Milestone 1: Outcomes, State version 2, and reports

- Change `types.go`: outcome constants and fields, `SubmissionRecord.Detail`, and the `Report` types.
- Make `applyTransition` enforce the validation table and drop memos on task-written holds.
- Normalize the built-in outcomes and add the bounded diagnostic helper listed in §6.1.
- Change `session.go` to accept State version 1 or 2, write 2, and refuse later versions.
- Rename `SessionEnd.Reason` to `Kind` and update tests, README, and `COMPATIBILITY.md` in this API-changing milestone.
- Add `Session.fail(err)` and a failure hook that only enqueues `storage_failed`, once and after sealing. Update `SessionFailedError` documentation: its first cause may now be a bookkeeping error, not only an uncertain storage write.
- Add the independent FIFO report worker, `HarnessOptions.OnReport` panic containment, and nonblocking `TaskRuntime.Report`. Document that Session shutdown does not join report delivery.
- Add `commitInternal`, its explicit `committed` result, and the cancellation/sealed-admission distinctions from §6.6. Replace ignored scheduler commits now, before migration and step-loop changes depend on it.
- Update `RejectedError` documentation, README, and `COMPATIBILITY.md` for the scheduler-only fail-closed policy.
- Add `task_outcome_test.go` and `task_reports_test.go`, plus scheduler bookkeeping-failure coverage, with the M1 rows of §9.

**Exit criteria:** existing tests pass after updating them for the stricter validation, and the new tests pass under `-race`.

### Milestone 2: Definition resolution, blocked tasks, migration, and inspection

- Add per-install Registry identities, `resolvedTask`, and built-in resolution. Remove unnecessary `CompactionExtension` installs from `Open`, `Compact`, and `examples/chat/main.go`; document legacy install precedence and the identity approximation.
- Implement read-only `fit`, migration-capable `resolveForStart`, the scheduler-locked migration cache, and atomic on-line migration/reservation (§6.4). Missing `Migrate` must reach the cached/reported failure path, not be silently skipped by `fit`.
- Change the `Migrate` signature and update `task_contracts_test.go`.
- Implement on-line inspection and the new inspection shape (§6.10).
- Add `task_blocked_test.go` with the M2 rows.

**Exit criteria:** each failed migration is attempted/reported once per task and effective identity, inspection calls no task code, and unrelated installs change neither identity nor failure counts. Reinstall permits a retry, even for unchanged Go values.

### Milestone 2a: Tool-round context, independent PR

- Apply context rules 7 and 8 as specified in §6.11.
- Add `context_tool_rounds_test.go` with the M2a rows, including duplicate/cross-round results and complete synthesized call identity.
- This PR has no dependency on outcomes, reports, or migrations and may land earlier. It must land before milestone 3 so scheduler-written tool faults cannot produce unanswered provider calls.

**Exit criteria:** derived model and compaction context has one correctly identified result per call in call order; stored entries are unchanged.

### Milestone 3: Steps, faults, cleanup, and handover

Each item below can be a separate commit:

1. Build on milestone 1's `commitInternal`; add the first-phase gate and the subsequent step loop, panic containment, and abort-handler rules.
2. Write faults, holds, and final-commit cleanup using the shared outcome/diagnostic rules.
3. Add handover and `task_outdated`, closing the runtime gate on the Session line.
4. Add `TaskRuntime.Registry()` and use the fixed phase snapshot throughout built-in handlers.

Add `task_step_test.go` and `task_handover_test.go` with the M3 rows. Reuse milestone 2a's context tests for the tool-fault regression.

**Exit criteria:**

- No task code runs in parallel with itself.
- Fault terminalization and cleanup are atomic; held faults defer cleanup to finalization. Handover atomically preserves checkpoint, memos, and marks without terminal cleanup.
- A targeted 20-iteration race loop passes.

### Milestone 4: Orphaning, abort API, and outcome reads

- Change `AbortTask` to mark before enabling scheduling, return a result, and join only the captured active run. End joins at admission sealing, not public `Done()`.
- Add direct and scheduled orphaning through `resolveForStart`, including previously untried and missing migrations in the marking commit; keep restart marks in `AbortOwned`.
- Update `Conversation.Abort` and document self-abort/scope-join hazards.
- Add `TaskRuntime.Outcomes`.
- Add `task_orphan_test.go` with the M4 rows, including SQLite reopen.

**Exit criteria:** every blocked-task abort scenario in §9 settles before the test deadline.

### Milestone 5: Documentation and compatibility

- Update the README task section: outcomes, queued reports and their shutdown lifetime, atomic inspection, the on-line/pure migration contract, install-based identities, panic policy, the yield transition, and abort joins.
- Document the scheduler-only rejection exemption change and legacy built-in registration precedence. Keep handoff continuation and assistant `thinkingLevel` persistence explicitly unimplemented (follow-up J).
- In `COMPATIBILITY.md`, move the implemented items. Keep these gaps explicit:
  - no restart limit;
  - no `runFailure`;
  - no `sleeping` status;
  - no wait-cycle validation;
  - partial report kinds;
  - no idle-wait API.
- State that the Go test suite is independent and that the port does not claim conformance.

**Exit criteria:** the §10 checklist passes.

## 9. Test plan

All tests use fake models, in-memory storage, and SQLite or JSONL files in a temporary directory. They make no network calls. Upstream behavior names from `v1.5.2` guide the scenarios. Write the Go tests independently.

| M | Scenario | Upstream reference |
|---|---|---|
| 1 | Tasks cannot commit `faulted` or `orphaned`; `failed` needs `Error`; old records load | `src/types.ts` `TaskOutcome` |
| 1 | Task-written `completed`, `failed`, and `aborted` holds drop memos while retaining the checkpoint until terminalization | §5.1; Go checkpoint retention |
| 1, 3 | Empty built-in error strings get a fallback; large error/panic messages are UTF-8 bounded; stacks reach reports but not storage | Go diagnostic policy |
| 1 | A version 1 snapshot opens and is stored as version 2; version 3 is refused; SQLite and JSONL round-trip new fields | Go-specific |
| 1 | `storage_failed` is enqueued once, after sealing, for write/bookkeeping/close failures; `OnReport` sees `Err() != nil`, and a handler panic does not prevent later delivery | `harness-reports`: “tell … from a failed Session” |
| 1 | A report handler can call `Close`/`WaitClosed` without blocking task/scheduler shutdown; `Done` does not join a blocked handler; queued reports drain after close | Go queued-delivery policy |
| 1 | `TaskRuntime.Report` in a commit callback never invokes `OnReport` inline; its handler can read snapshots/inspection without deadlocking the producer | Go reentrancy regression |
| 1 | A rejected scheduler bookkeeping write fails the Session, while an ordinary rejected caller commit does not; cancelled reconcile/no-op commits reserve nothing and do not fail the Session; uncertain cancelled writes still poison it | §10.1; Go rejection/cancellation policy |
| 1, 2, 3 | `task_error`, then `task_faulted{threw}`, carry `ReportTask` and the installing extension, even after replacement; add identity-sensitive assertions with milestone 2 | `harness-reports`: first two tests |
| 2 | A missing definition stays `pending`, remains live in `Inspect`, and completes after install | `harness-tasks-recovery`: “keeps a task with a missing definition pending …” |
| 2 | A task stored by a newer version stays blocked as `task_too_old` until a fitting definition is installed | “keeps a task stored by a newer version pending …” |
| 2 | Migration happens atomically at reservation: one commit writes version, input, checkpoint, and `running`, as observed with a storage spy | “migrates at reservation …” |
| 2 | A failed migration leaves the record unchanged, reports once, is not retried after an unrelated install, and is retried after the definition is reinstalled | Same test |
| 2 | A newer definition without `Migrate` shows blocked in inspection without caching/reporting; the scheduler's first attempt reports once and leaves the record unchanged | “blocks an older record whose newer definition has no migration” |
| 2 | Inspection derives every state without running `Migrate` or handlers, captures record/invocation/cache consistently, and ignores future `WakeAt` on all abort marks | `harness-inspect`: “derives every live task's state …”; Go `WakeAt` adaptation |
| 2 | Unrelated installs and repeated snapshots preserve task identities; reinstalling the same unchanged extension deliberately changes identity and retries a cached failure; legacy compaction installs do not replace built-ins | Go install-identity/registration policy |
| 2a | Context derivation orders results by call, takes only the first matching result before the next assistant, drops duplicates/unmatched/cross-round results, and fills missing results with the exact call ID/name/error flag; resets and compaction use the same reduction without changing stored entries | §2.1, rules 7 and 8 |
| 3 | One invocation continues through checkpoint progress, with one reservation and multiple `Run` calls | `harness-tasks`: “continues one invocation through checkpoint progress …” |
| 3 | The first-phase step prevents `Run` after a racing abort, owner cancellation, or seal; no-op steps do not write snapshots or imply reservation | §5.1, rules 1–3 |
| 3 | Object-key order and equivalent number spellings do not count as progress; distinct large integers do; memo/document writes and handler argument mutation cannot fake checkpoint progress | §5.1, rules 5 and 6; Go JSON handling |
| 3 | A phase uses one registry snapshot despite concurrent replacement; the next phase refreshes it; held scheduler outcomes drop memos and finalization runs cleanup | §5.1; §5.4 |
| 3 | A phase without progress and a phase that returns an error both fault; a panic faults and the process survives | “faults a phase without durable progress and a throwing phase” |
| 3 | A committed terminal outcome stays when the handler errors afterward, for both run and abort handlers | “keeps a committed terminal outcome …” (both) |
| 3 | Abort handlers that error or settle nothing fault | “… faults abort handlers that throw or settle nothing” |
| 3 | An abort mark wins over a fault that races it | “lets an abort mark win over a fault that races it” |
| 3 | Close during a step writes no fault, and the task resumes after reopen | “writes no fault when close seals …”, “starts no next phase when close seals …” |
| 3 | A failed fault write or other scheduler-commit error fails the Session, and the task does not run again | “fails the Harness instead of running a task again …” |
| 3 | A fault with live owned work holds as `completing`, cascades aborts, and runs cleanup only in the final commit | §5.5, rule 4 |
| 3 | A generation fault converts the partial, settles inputs as `unanswered`/`faulted` with `Detail`, clears `Busy`, and keeps queued inputs | §5.4 Harness hook |
| 3 | A tool-task fault leaves no result entry, and the next request contains the added error result | §2.1, rule 8 |
| 3 | A same-version replacement takes over at the next boundary with no overlap and keeps memos | “hands over … without overlap”, “keeps the memos across a handover” |
| 3 | A newer replacement with `Migrate` takes over; one whose migration fails leaves the task blocked | “hands over to a newer version …” (both) |
| 3 | A missing or too-old replacement leaves the old definition running, with `task_outdated` reported once per identity | “keeps running under the old definition …”, “reports a replacement older …” |
| 3 | After handover, a runtime commit from the old invocation is rejected; an abort mark racing handover runs the new definition's `Abort` | “rejects a runtime commit of the old invocation …”, “preserves an abort mark …” |
| 4 | Aborting a blocked task before `Resume` returns `marked`, settles it as `orphaned{missing_task}`, and retires task documents | “settles an aborted blocked task as orphaned …” |
| 4 | Direct abort attempts an untried migration (including a missing `Migrate`), reports its failure once, and atomically orphans with cleanup in the marking commit; a successful probe persists migration only at abort reservation | §5.4; `#resolve` and `abort()` |
| 4 | Aborting a paused runnable task marks before scheduling, so no `Run` starts; an active abort invocation is not joined by `AbortTask` | §2.2; Go admission-order regression |
| 4 | If the definition disappears while a run is active, the abort settles `orphaned` after the run ends, and `Abort` is not called | “orphans a marked task whose definition disappeared …” |
| 4 | A reopened marked task without a definition is orphaned after `Resume`, using SQLite | “orphans a reopened abort-marked task …” |
| 4 | A blocked task marked for restart waits; an abort request upgrades and orphans it; `AbortOwned` keeps the restart mark | §5.4 abandonment rules |
| 4 | Cascade proceeds from an owner the scheduler orphans; an orphaned outcome never holds | `harness-ownership`: “cascades from an owner the scheduler orphans” |
| 4 | `AbortTask` returns `terminal` for terminal tasks, `ErrNotFound` for unknown IDs, joins a cooperative run, and respects context deadlines for non-cooperative runs without undoing their marks | §2.2 |
| 4 | A Session failure or clean seal ends an `AbortTask` join even while a non-cooperative run keeps public `Done` open; release the test handler afterward so cleanup can finish | §10.1; Go seal-channel regression |
| 4 | `Conversation.Abort` with a blocked task returns, and the task is orphaned | Go regression |
| 4 | `Outcomes` rejects a task that is not terminal and returns outcomes in input order | §5.1 |

## 10. Validation checklist

Run after every milestone. `AGENTS.md` requires every check before publication.

```sh
make check                      # gofmt, go vet ./..., go test ./..., go test -race ./...
go test -race -count=20 -timeout=300s \
  -run 'Test(TaskStep|TaskFault|TaskHandover|TaskOrphan|TaskBlocked|TaskMigration|TaskReport|ContextToolRound|Owned|AbortOwned|Restart|SessionFailure|HarnessFailure)' .
git diff --check
```

Optional SDK regression check when an installed SDK is available (the bridge is unchanged by this plan; local HTTP and fake credentials only):

```sh
PI_DURABLE_TEST_PI_PACKAGE_DIR=$HOME/.pi/agent/install/releases/1.5.2/node_modules/@earendil-works/pi-coding-agent \
  go test -race ./model -run TestPiInstalled -count=1 -timeout=60s
```

Also complete these checks:

- From milestone 1 onward, grep for `_ = s.h.Commit`; none should remain in `scheduler.go`.
- Run the drift check from §2 before merging. If the latest release is no longer v1.5.2, update Appendix A first.

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Hosts that match `failed` miss bugs that are now `faulted` | Add outcome constants and README migration notes. Faults also produce `task_faulted` reports. |
| The step loop reruns `Run` where it previously failed, so a task that always changes its checkpoint can loop | Upstream has the same semantics. Document that each changed checkpoint is progress. The follow-up crash-loop guard does not bound this case, so document it as the handler's responsibility. |
| Failing the Session on scheduler bookkeeping errors exposes latent bugs | Upstream requires it. Fail-closed behavior is better than stuck tasks. Add tests for each internal commit path. |
| Reading `Scheduler.mu` inside a commit introduces deadlocks | Use the fixed lock order from §6.12, run race loops, and release it before any callback, migration, or report delivery. |
| An on-line migration blocks commits and scheduling, or is evaluated again after an abort eligibility probe/reopen | Require pure, deterministic, bounded migration with no Session/registry calls or effects. Recheck eligibility on-line and atomically reserve migrated values; cache failures per task/identity. |
| Recovering panics hides programming errors | Faults are durable and reported. The report error includes the stack, and outcomes store only the message. |
| State version 2 prevents rolling back to older builds | This is intentional to prevent silent field loss. Document it in the README and `COMPATIBILITY.md`. |
| Context rules 7 and 8 change requests for existing transcripts | Only invalid or reordered tool rounds change; previously they failed at providers. Add regression tests. |
| A slow `OnReport` delays later diagnostics and retains the FIFO | Require prompt handlers, document asynchronous best-effort delivery, release report references after delivery, and never join the report worker during Session shutdown. Test reentrant reads and shutdown waits. |
| A non-cooperative run keeps shutdown pending | End abort joins on admission sealing or caller cancellation, not public `Done`; backend closure still waits for the run. |
| Reinstalling unchanged Go definitions causes unnecessary retries/handovers | Document per-install identity as a Go deviation; preserve identities across unrelated installs and snapshots. |

## 12. Decisions and open questions

1. Should the Go-specific task-committed `pending` yield remain, or be deprecated in favor of `running` progress and a future `sleeping` status? Proposal: keep it until plan D.
2. Should `TaskDefinition.Abort` remain optional, with nil meaning `aborted`, although upstream requires it? Proposal: keep it optional and document the difference.
3. **Decided after review:** deliver `OnReport` through one independent serial worker (§6.2), not inline. FIFO means enqueue order only; delivery is not joined by Session shutdown.
4. Should `Ready` and `Blocked` remain on `TaskInspection` as deprecated fields for one release? Proposal: remove them because the package is experimental.
5. Should Registry installation reject built-in task-kind collisions? For this milestone, preserve legacy installs such as `CompactionExtension()` and make Harness-built-in precedence explicit. Rejecting collisions would be a separate API change after the deprecated registration path is removed.

## 13. Follow-up roadmap

Each follow-up targets the latest Pi release when it starts.

- **A. Crash-loop guard:** `TaskRecord.Restarts` and `settings.RestartLimit`.
  - Open counts surviving `running` tasks.
  - Close writes stopped tasks back to `pending` without counting a restart.
  - Built-in tasks end themselves at the limit; extension tasks end `faulted{restart_limit}`.
  - Report `task_interrupted`.
  - An interrupted model request counts as a failed retry attempt.
  - References: §5.4 Restarts; 1.4.0 and 1.5.0.
- **B. Wait-cycle validation:** validate the full wait-for graph, including owned-work edges, reject the wait, and name the cycle. A failing first task held by `failFast` also marks the rest (§5.5; 1.2.0).
- **C. Run failure state:**
  - `pi.live.runFailure`, with reasons `model_error`, `no_model`, `aborted`, `reset`, `faulted`, `orphaned`, and later reasons;
  - `afterFailedRun`;
  - continue submissions;
  - `whenIdle: "reject"`;
  - per-conversation `maxTurns`.

  References: §8.2 and §6; 1.4.0 and 1.5.0.
- **D. Durable sleeping and wakeups:**
  - `sleeping` status, `SleepUntil`, and runtime timers;
  - `Harness.Wake()` and `OnWake`;
  - timer-driven scheduling instead of 20 ms polling.

  References: §5.6; 1.4.0.
- **E. Report completeness:** `extension_error` for hooks, sections, and wrappers; `host_error`; `listener_error`; and an idle-wait API (§10.1).
- **F. v1.2 tool suite:**
  - tagged tool inputs and nested calls;
  - structured output;
  - `callers` and `modelTools`;
  - deferred tools and `tool_search`;
  - `afterTool` on every result;
  - tasks for unoffered calls.

  References: §7.3; 1.2.0 and 1.5.0.
- **G. Automatic compaction:** threshold, background, and overflow compaction; model overrides; per-conversation settings; `compactionFailure` (§8.7; 1.4.0–1.5.1).
- **H. Observation model:**
  - typed watch ends;
  - synchronous observers;
  - windowed `ConversationView`;
  - deferred progress persistence.

  References: §4.1 and §9; 1.5.0.
- **I. Storage:** indexed records, document deltas, and the group-commit contract, with benchmarks (§4 and §10; `docs/group-commit.md`; 1.5.0 and 1.5.1).
- **J. Generation and replay parity:** a tool `handoff` resets context and continues the same run rather than settling its inputs on the tool-calling answer; persist each assistant attempt's requested `thinkingLevel` for replay (§8.3 and §8.5; 1.5.0).

## Appendix A: Durable changes since the previous reference

The table highlights compatibility changes from the old reference `4954d760` through `v1.5.2`, grouping related changelog items. It is not an exhaustive accounting of every fix, optimization, or commit. Source: [`packages/durable/CHANGELOG.md` at `v1.5.2`](https://github.com/earendil-works/pi/blob/v1.5.2/packages/durable/CHANGELOG.md). Durable 1.5.2 has no entries. Statuses below describe the intended result after this plan, not current implementation.

This plan also closes gaps that predate the old reference and are already listed in `COMPATIBILITY.md`: `faulted` and `orphaned` outcomes, step rules, handover, and migration semantics.

| Upstream change | Release | Go status after this plan | Where |
|---|---|---|---|
| Typed `onReport(report)` | 1.4.0, 1.5.0 | Subset: `storage_failed`, `task_faulted`, `migration_failed`, `task_outdated`, and `task_error`; Go delivery is queued, not inline | This plan, then E |
| Blocked reasons `missing_task`, `task_too_old`, and `migration_failed`; inspection `pending{migrates}` | 1.4.0, 1.5.0 | Implemented | This plan |
| Phase faults reported as `task_faulted` | 1.4.0 | Implemented | This plan |
| Crash-loop guard (`restarts`, `restartLimit`); restart-limited tasks end; `task_interrupted` | 1.4.0, 1.5.0 | Missing | A |
| Interrupted model request counted as a failed attempt instead of resent | 1.5.0 | Missing; Go resends | A |
| Durable `sleeping`, `sleepUntil`, `setTimer`, `wake()`/`onWake`; `TaskRuntime.sleep()` removed | 1.4.0 | Partial: `WakeAt` on `pending` or `waiting`, with 20 ms polling | D |
| `settings.afterFailedRun` | 1.4.0 | Missing; Go behaves like `keep` | C |
| `pi.live.runFailure`, continue submissions, `whenIdle: "reject"`, and `maxTurns` | 1.5.0 | Missing | C |
| `Conversation.submit(tx, draft)`, `afterRun` run-end tasks, `beforeProviderRequest`, and hook or task `entries()` | 1.5.0 | Missing | Later |
| Tool `handoff` continues the same run after resetting context | 1.5.0 | Missing; Go settles the run on the tool-calling answer (`generation.go`) | J |
| Assistant messages persist the request's `thinkingLevel` | 1.5.0 | Missing; Go `Message` has no such field | J |
| `afterTool` on every result; unoffered calls get `pi.tool` tasks; tool schemas use pi-schema | 1.5.0 | Missing; the v1.2 tool suite is absent | F |
| Group-commit `Storage.commit(batches, receive)`; deferred commits; `progress.persistWithinMs` | 1.5.0 | Missing; Go uses whole snapshots, one per commit | H, I |
| Chord watch handles and observers; tagged `WatchEnd`; windowed `ConversationView`; `ConversationTranscript`; `watchEvents()` removed | 1.5.0 | Missing; Go channels deliver the latest snapshot | H |
| `SessionEnd` tagged by `kind` | 1.5.0 | Renamed to `SessionEnd.Kind` | Milestone 1 |
| `HarnessOptions.readOnly`; read-only JSONL open | 1.5.0, 1.5.1 | Missing | Later |
| Compaction `modelOverrides`; per-conversation compaction settings | 1.4.0, 1.5.1 | Missing; no automatic compaction | G |
| `HarnessOptions.identity`; function-valued `models` and `settings`; telemetry | 1.4.0 | Missing | Later |
| Documents require `checkpointWhen` | 1.4.0 | Missing; no delta checkpoints | I |
| `Tx.submission(id)` | 1.5.1 | Already present as `Tx.Submission` | None |
| Scheduler pass race fix for `waiting` and `completing` notifications | 1.5.1 | Different design; Go rescans snapshots every pass, so no direct port is needed | None |
| IndexedDB, Chord `StoreStorage`, SQLite schema v2 and statement batching, Cloudflare adapters | 1.4.0–1.5.1 | Not applicable to the Go backends | None |
| `tool_search` and Code Mode fixes | 1.5.0, 1.5.1 | Not applicable while those features are absent | F |
