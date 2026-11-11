# Plan 0002: Crash-loop guard, interrupted requests, and retry policy

| | |
|---|---|
| Status | In progress: M0–M4 complete; M5–M6 pending (§12.1) |
| Date | 2026-11-11 |
| Compatibility target | Pi **v1.5.2**: `@earendil-works/pi-durable` and `@earendil-works/pi-ai` 1.5.2, tag `v1.5.2`, commit `b4afdddf44e059c67ccf5097e5317d913521b470` (unchanged from plan 0001) |
| Depends on | [Plan 0001](0001-task-outcomes-and-definition-lifecycle.md), merged to `main` |
| Go baseline | Plan 0001 implemented on `main` at `fb700b4` |
| Roadmap item | Plan 0001 §13, follow-up **A**, together with the retry policy that interrupted requests are charged to |
| Main code areas | `scheduler.go`, `task_step.go`, `task_runtime.go`, `ownership.go`, `session.go`, `lifecycle.go`, `harness.go`, `generation.go`, `tool_runtime.go`, `compaction.go`, `reports.go`, `types.go`, a new `retry.go`; `model/pi.go`, `model/pi_bridge.mjs`, `model/openai.go` |

## 1. Summary

Port Pi Durable 1.5.2's crash-loop guard (upstream issue #11139), together with the generation and compaction retry policy that interrupted requests are charged to.

This is a port: every rule follows the TypeScript implementation at the pinned tag. Where Go still lacks an upstream subsystem, §12.2 names the remaining difference and the follow-up that ports it.

- **Restart counting.** Each task record counts how many times in a row a crash cut it off (`Restarts`). Open counts surviving `running` tasks. A clean close writes the tasks it stopped back to `pending` without counting them. The count starts over when the task commits a new state, gets an abort mark, or records an outcome.
- **Restart limit.** At `Settings.RestartLimit` (default 5; `math.Inf(1)` disables it, as upstream's `Infinity` does), work ends visibly instead of running again:
  - the scheduler faults an extension task, or any abort handler, with `ErrorDetail` `{"reason":"restart_limit"}`;
  - built-in generation, tool, and compaction tasks end themselves with a `restart_limit` failure.
- **Retry policy.** `Settings.Retry` merges field by field over Pi Durable's default:
  - retries are enabled, up to 3 after the first attempt;
  - the delay starts at 2,000 ms, doubles per attempt, and is capped at 60,000 ms;
  - only transient errors retry, as classified by pi-ai's `isRetryableAssistantError`, and a context overflow never does;
  - after the backoff, the generation prepares again with the next attempt;
  - compaction summaries retry under the same policy.
- **Interrupted requests.** A model request in flight when the host died counts as a failed attempt under that policy, not a resend. With no retry left, the run ends `interrupted`. A clean close still resends.
- **Reports.** Hosts receive `task_interrupted` reports, and `task_faulted` reports with reason `restart_limit`.
- **Persistence.** As upstream does, `Restarts` is an optional record field, with no change to the storage format. When this plan changes a built-in task's stored checkpoint, that built-in definition gets a new version with a migration. An older build then leaves such tasks blocked as `task_too_old` instead of misreading them.

## Implementation record

### M0: Preconditions

Plan 0001 was published on `main` as `fb700b4`; this accepted plan followed as `41a7f2e`. Formatting, vet, all tests, all race tests, the expanded 20-repeat task race suite, and the installed Pi SDK tests passed before that push. The manual npm/GitHub drift check still targets v1.5.2. `AGENTS.md` remains unchanged pending its separately required approval.

### M1: Restart accounting

Implemented as `e348805` and merged through [PR #1](https://github.com/leocavalcante/pi-durable-go/pull/1) into `main` as `fbe0cce` on 2026-11-11. The maintainer authorized direct commits and pushes to `main` for the remaining milestones, without branches or pull requests. No dependent milestone has started.

- `TaskRecord.Restarts` is optional and scheduler-owned. Reconciliation counts every surviving `running` task; State version 2 and built-in definition versions are unchanged.
- Runtime commits compare detached state captured before the callback, including mutable checkpoint/wait slices and deadlines. New task state, outcomes (including held outcomes), and new/upgraded abort marks reset counts. Memo/document/entry writes and scheduler reservation/migration/handover/cancellation releases preserve them.
- Close joins invocations and privately writes stopped `running` tasks back to `pending` after admitted commits, keeping their other fields and publishing nothing. Capture in `finishInvocation` covers the seal/join window. Failure shutdown and cancelled-open cleanup write nothing.
- Write-back errors fail the Session and report `storage_failed`, including certified rejection and errors wrapping the internal no-change sentinel. `Close` still returns only the backend close error. Tests cover both unapplied and uncertain-but-applied writes; the failed Session never adopts an uncertain candidate.
- `close_write_back_test.go`, `task_restart_accounting_test.go`, and `storage/task_restart_test.go` cover accounting, resets, shutdown races, repeated opens, legacy optional fields, and SQLite/JSONL persistence. Existing clean-close generation/tool recovery tests now assert zero restart counts before scheduling.
- The fake-SDK recovery integration test now registers cleanup before its first submission, so a failure cannot leak its first Harness or Node process. Its deadline is a 60-second hang bound for several Node starts and SQLite reopen, not a response-latency assertion.
- `README.md` and `COMPATIBILITY.md` document M1 and explicitly retain the missing limit, interruption-report, retry-policy, and charge behavior for M2–M5.

Validation on Go 1.24.3:

- `make check` passed with `GOMAXPROCS=2 GOFLAGS=-p=1` to limit package concurrency on the loaded host. Earlier parallel runs hit existing wall-clock deadlines; the focused failures passed in isolation, and the final full check and five-repeat storage/model race runs passed after the integration-test cleanup/budget fix above.
- The §10 targeted race suite passed with `-count=20`; storage and model race suites passed with `-count=5`.
- Independent read-only review found no actionable M1 correctness issues against the pinned TypeScript source.
- `git diff --check` passed; the non-test counter writes were audited against §6.4.

### M2: Enforcement and reports

- `Settings.RestartLimit float64` is normalized like upstream `restartLimit()` (`restarts.go`): values of 1 or more round down, `math.Inf(1)` never ends a task, and anything else, including NaN, selects 5.
- `Scheduler.reserve` checks the limit after eligibility and before `resolveForStart`. An abort-marked task, or a run of a non-built-in kind, at the limit ends through the new `Scheduler.terminate`, which `fault` now shares: upstream's run or abort-handler message, `ErrorDetail` `{"reason":"restart_limit"}`, the usual hold/cascade/cleanup path, and a `task_faulted` report with reason `restart_limit` and the current installer, queued only after the write is durable. Blocked, abandoned, and would-be orphaned tasks at the limit fault, as upstream's table orders it.
- `settleSchedulerOutcome` ends a generation whose abort handler was cut off as `aborted`, with the fault message as JSON-string `Detail`, without converting its partial; `Busy` is cleared and queued inputs stay.
- `TaskRuntime.Commit` reports `task_interrupted` after a successful terminal commit whose failed outcome's `ErrorDetail` reason is `restart_limit` or `interrupted`, with the count captured before the callback in `Report.Restarts` (held outcomes at commit).
- `crash_loop_test.go` adds a crash-simulation host wrapper (uncertain commit failures over a shared backend, optional crash points) and covers real crash loops of an extension run, an extension abort handler, and an aborted generation; held-fault cascades; limit-before-resolution ordering; limit settings and reopening with a higher limit; generic interruption reports, including held outcomes; and fault-write failures that fail the Session without a task report. M1 tests that seed large counts now opt out of enforcement with `math.Inf(1)`.

### M3: Retry policy and classification

Four commits, as planned; `RetryMax` and `RetryDelay` were removed with the generation commit rather than the first, so that each commit builds and passes on its own.

1. `retry.go`: `RetrySettings` and its field-by-field resolution, `retryDelayMs` with pi-ai's safe-integer replacement and cap, saturating deadlines, and the exported `IsRetryableModelError`/`IsContextOverflowError` with the optional `Retryable()`/`ContextOverflow()` interfaces (`RetryableError`, `ContextOverflowError`). The pi-ai v1.5.2 pattern lists are verbatim; two lookaround patterns are rewritten for RE2, and JavaScript's `\s`/`\S` become explicit classes. `retry_test.go` checks every classification vector of pi-ai's `retry.test.ts` and every error-text vector of `overflow.test.ts`.
2. Adapters: `model.OpenAI` classifies the status and the `error` object pi-ai's openai SDK path would report (or an SSE error event's message) before sanitizing; transport failures, dropped streams, and streams ending before `[DONE]` are retryable; local configuration, encoding, invalid-event, and size-limit errors are explicitly neither. The Pi bridge resolves pi-ai through the coding agent's dependency tree and returns only pi-ai's own two answers. A fake pi-ai fixture and an installed-SDK loopback test (503, 429 quota, 400 overflow) cover the bridge.
3. `pi.generation` 2: a 1-based `attempt`, a `retry` phase with `until` in Unix milliseconds, `attempt = retry + 1` migration, preparing again after the backoff without counting a turn, and a retry phase that backs off again before its deadline.
4. `pi.compaction` 2: the same policy without the overflow check; `retry` resends the pinned request as the next attempt; version-1 records migrate unchanged.

### M4: Built-in tasks end themselves

1. Tool progress (separate commit): `ToolRuntime.Output` appends to the `pi.live` slot only (trailing 50 KiB, cut at a rune boundary) and commits the unchanged checkpoint; `finishTool` reads committed output from the slot with a version-1 checkpoint fallback; a safe rerun first commits a cleared slot (and drops a legacy checkpoint output); `pi.tool` 2 has an identity migration. The unsafe interruption result now uses upstream's message, `Tool <name> was interrupted and may have partially run`.
2. Limit endings: generation phases `prepare`, `request`, `retry`, and `tools` end through `endGenerationCutOff` and the new `settleRunFailure` (code in `Reason`, text as JSON-string `Detail`, `ErrorDetail` `{"reason":"restart_limit"}`), converting a partial only while `salvageable`; tool `call`/`execute` end through `finishToolEnding` with an explicit outcome error and detail, before the replay decision; compaction `request`/`retry`/`publish` fail with `Compaction was interrupted <count>`. `TaskRuntime.Commit` reports each as `task_interrupted`.

`tool_progress_test.go` and `builtin_limit_test.go` cover output that does not reset counts, safe/unsafe recovery and legacy output, real crash loops of a streaming tool, an unsafe tool, a blocked prepare section, and a compaction request, plus seeded cut-offs of every other phase with salvage at and past the limit.

M5–M6 remain unimplemented: crashed model requests still resend.

## 2. Compatibility target

Rechecked before M1 merge on 2026-11-11; `make check` also passed again before merging:

- npm `latest` for `@earendil-works/pi-durable`, `@earendil-works/pi-coding-agent`, and `@earendil-works/pi-ai` is 1.5.2;
- the GitHub **Latest** release is `v1.5.2` (2026-11-10), commit `b4afdddf…`.

Plan 0001's targeting policy applies unchanged: cite the pinned tag, rerun its §2 drift check before merging each milestone, and update this plan first if a newer release appears.

References are to the monorepo at tag `v1.5.2`:

- `packages/durable`: [`docs/spec.md`](https://github.com/earendil-works/pi/blob/v1.5.2/packages/durable/docs/spec.md), [`CHANGELOG.md`](https://github.com/earendil-works/pi/blob/v1.5.2/packages/durable/CHANGELOG.md), `src/harness/scheduler.ts`, `live.ts`, `generation.ts`, `tool.ts`, `compaction.ts`, `agent.ts`, `src/session/session.ts`, and `test/harness-restart-failure.test.ts`, `harness-generation-recovery.test.ts`, and `harness-tools-recovery.test.ts`;
- `packages/ai`: [`src/utils/retry.ts`](https://github.com/earendil-works/pi/blob/v1.5.2/packages/ai/src/utils/retry.ts), [`src/utils/overflow.ts`](https://github.com/earendil-works/pi/blob/v1.5.2/packages/ai/src/utils/overflow.ts), `test/retry.test.ts`, and `test/overflow.test.ts`. Their pattern lists are identical to those in the installed `@earendil-works/pi-ai` 1.5.2 package.

Upstream shipped the guard in two steps:

- **1.4.0:** counting *parked* a task at the limit, using a `restart_limit` blocked reason and `Harness.retryTask()`.
- **1.5.0:** a visible ending replaced parking. Interrupted requests became charged attempts, `task_interrupted` was added, and `retryTask()` was removed.

This plan targets only the 1.5.x semantics (Appendix A). The retry policy is older than the previous Go reference. It has no changelog entry after it, apart from the 1.4.0 change that made backoffs sleep.

The v1.5.2 specification still says, in §9.4, that a task-graph node's `blocked` state depends on the settings and `restarts`. The v1.5.2 source does not do this (`blockedReason`, `#inspectTask`), and the 1.5.0 changelog removed that blocked reason. Follow the source.

## 3. Why this feature is next

Observed in the plan 0001 working tree:

| Area | Go today | Pi Durable 1.5.2 |
|---|---|---|
| Host dies during a model request | Open rewrites the `running` generation to `pending` (`Harness.reconcile`). Its `request` phase converts the committed partial into an aborted entry and resends the same request. If the request kills the host, this repeats forever, appending an aborted entry each time. | The attempt failed. It is charged to the retry policy, and the run ends `interrupted` when no retry is left (§8.3). |
| A task or abort handler kills the host every time | It runs again on every open, with no limit and nothing reported. | `restarts` counts it. At `restartLimit`, the task ends visibly (§5.4). |
| Clean close compared with a crash | Indistinguishable. Close writes nothing, because `runInvocation` skips its release once admission is sealed. Open resets every `running` task. | Close writes stopped tasks back as `pending`. Only a crash leaves `running` records, which open counts (§5.4). |
| Model error retries | `Settings.RetryMax` defaults to 0. When it is set, every error retries after a constant `RetryDelay`, and the retry resends the pinned request. Compaction never retries. | The default policy allows 3 retries, with backoff starting at 2,000 ms, doubling, and capped at 60,000 ms. Only transient errors retry, and a retry prepares the request again. Compaction summaries also retry (§2.2, §8.3, §8.7). |
| Reports | None. | `task_interrupted`, and `task_faulted` with reason `restart_limit` (§10.1). |
| Tool output | `ToolRuntime.Output` rewrites the task checkpoint, so each chunk would count as progress. | Output lives in the `pi.live` slot; document writes are not progress (§8.2, §8.4). |

This plan implements roadmap item A, the first follow-up to plan 0001, and builds on what plan 0001 added:

- the scheduler-only `faulted` outcome with `ErrorDetail`;
- held faults and built-in cleanup;
- `SubmissionRecord.Detail`;
- the report queue and fail-closed internal commits;
- the definition-resolution, migration, and blocked-state machinery.

Upstream treated the crash loop as a bug (1.5.0 changelog, line 203). The retry policy belongs in this plan because the charge is defined in its terms, and the maintainer chose upstream's default. Durable sleeping (D) is larger and mainly about efficiency. The run-failure state (C) needs the endings added here.

## 4. Scope

### In scope

1. A persistent, optional `TaskRecord.Restarts`.
2. Counting at open, write-back at a clean close, and resets on progress, abort marks, and outcomes.
3. `Settings.RestartLimit`, with upstream's number semantics.
4. Scheduler enforcement for extension kinds and every abort invocation, with `task_faulted` reports carrying reason `restart_limit`, and built-in cleanup for cut-off abort handlers.
5. Upstream's retry policy:
   - the default and its field-by-field merge;
   - `retryDelayMs`;
   - transient-error and context-overflow classification;
   - generation and compaction `retry` phases;
   - a retry that prepares again.
6. Retry and overflow classification in `model.Pi` and `model.OpenAI`, without exposing provider text.
7. Built-in generation, tool, and compaction tasks ending themselves at the limit.
8. Tool output and details stored only in the `pi.live` slot. A safe rerun clears the interrupted attempt's progress first.
9. Interrupted model requests charged as failed attempts, and `interrupted` run endings.
10. `task_interrupted` reports and `Report.Restarts`.
11. Version 2 of the `pi.generation`, `pi.compaction`, and `pi.tool` definitions, each migrating version-1 records.

### Out of scope

These are remaining differences, listed with their follow-ups in §12.2:

- the `pi.live` run state, `runFailure`, and compaction state;
- error entries and stop reasons;
- deferred polls and the `afterRun` phases;
- tool diagnostics, and `afterTool` on results the Harness writes;
- automatic and overflow compaction;
- function-valued settings, read-only open, durable sleeping and wakeups, and telemetry.

## 5. Upstream rules to satisfy

References are to the v1.5.2 specification unless another file is named.

1. **Count.** `TaskRecord.restarts` counts the times in a row a crash cut the task off without a state change since. An absent value means zero, and a close does not count (§5.1 `TaskRecord`; §5.4 "Restarts").
2. **Open.** Open rewrites every surviving `running` task to `pending` with `restarts` one higher, whichever build wrote it, and dispatches nothing. Other statuses are unchanged (§5.4; `scheduler.ts` `open()`).
3. **Close.** After joining the invocations it stopped, close writes the tasks still `running` back to `pending` without counting them. It does this on the Session line, after commits admitted before the seal, and a failed Session writes nothing. If that write fails:
   - if the write was not applied, the tasks stay `running`, so the next open counts them; an uncertain error may instead leave the write applied, and only reopening establishes which;
   - `join()` swallows the error;
   - the storage guard fails the Session and reports `storage_failed`;
   - `close()` still resolves unless the backend's own close fails, and `closed` settles `{ kind: "failed" }`.

   See §5.4, `join()`, and `session.ts` `writeAtClose()` and `close()`.
4. **Progress.** A state that the task commits and that differs from its current state removes the count: a new checkpoint, a wait, a sleep, or an outcome. These do not:
   - committing the same checkpoint again;
   - memo writes;
   - document writes.

   Held (`completing`) and terminal outcomes drop the count. Every abort mark starts the count over, whether it comes from a request, an abandonment, a cascade, or an upgrade (§5.4; `#commitState`, `progressed`, `withState`, `withAbortMark`).
5. **Limit.** `settings.restartLimit` is a number. A value of 1 or more is rounded down; anything else selects the default of 5. `Infinity` never ends a task (§2.2 `HarnessSettings`; `agent.ts` `restartLimit()`).
6. **Scheduler.** At reservation, before resolving the definition, a task with `restarts >= restartLimit` ends `faulted` with `error.detail = { reason: "restart_limit" }` when either:
   - it would run an abort handler; or
   - its kind does not end itself at the limit, which means any kind that is not built in.

   The fault follows the normal hold, terminal, and cleanup path, and is reported as `task_faulted` with reason `restart_limit` (§5.4 resolution table; `#reserve`).
7. **Built-ins.** Built-in kinds check the count when each phase starts; a tool checks after reading its call. At the limit:
   - a tool call ends with an error result built from its slot and fails with detail `restart_limit`;
   - a compaction fails with that detail;
   - a generation ends its run `unanswered` with reason `restart_limit` and fails with that detail.

   At the limit, a task still does its usual cleanup, such as converting a partial. Past the limit, it skips that cleanup (§5.4 table; `live.ts` `cutOffTooOften`, `salvageable`, and `cutOffCount`; §8.3, §8.4, §8.7).
8. **Cut-off abort handlers.** A built-in task faulted at the limit from its abort handler keeps the meaning of the abort:
   - a generation's run ends `aborted`, with the fault message as its detail, and its partial is not converted;
   - a compaction's status is removed without recording a failure;
   - a tool's slot ends without a result entry.

   See §5.4 and `live.ts` `settleSchedulerOutcome`.
9. **Retry policy.** `settings.retry` is a `Partial<ConversationRetryPolicy>`. It is merged over `{ enabled: true, maxRetries: 3, baseDelayMs: 2000, maxAgentDelayMs: 60000 }` and read when an attempt's result is classified (§2.2 `Settings`; `agent.ts` `DEFAULT_RETRY_POLICY` and `resolveSettings`).
10. **Delay.** `retryDelayMs(policy, attempt)` is the smaller of `baseDelayMs × 2^max(0, attempt − 1)` and `maxAgentDelayMs ?? 60000`. A product that is not a safe integer is first replaced by `Number.MAX_SAFE_INTEGER` (pi-ai `retryDelayMs`).
11. **Generation retries.** A generation retries an error response only when all of these hold:
    - the error is not a context overflow;
    - `isRetryableAssistantError` accepts it;
    - the policy is enabled;
    - `attempt <= maxRetries`.

    That classification uses transient provider and transport patterns, and excludes quota, billing, and subscription limits. When retrying, the generation appends the failed attempt's entry, which context derivation excludes, and sleeps at `retry` until `now + retryDelayMs(policy, attempt)`. Otherwise it fails the run with `model_error`.

    A `retry` phase that runs before `until` sleeps again. Otherwise it returns to `prepare` with `attempt + 1`, so agent and settings changes made during the backoff apply (§8.3; `classify` and the `retry` phase; pi-ai `isRetryableAssistantError` and `isContextOverflow`).
12. **Compaction retries.** A summary error retries under the same policy and classification, without the overflow check. Its `retry` phase returns to the summary request, with the next attempt and the same pinned request (§8.7).
13. **Interrupted requests.** A generation whose `request` phase runs with `restarts > 0` was cut off while its request was in flight. After the limit check, that attempt counts as failed:
    - its partial becomes an aborted entry;
    - if `enabled && attempt <= maxRetries`, it sleeps at `retry`. A crash is not an error message, so nothing is classified;
    - otherwise its inputs settle `unanswered` with reason `interrupted` and the text, and the task fails with detail `{ reason: "interrupted" }`.

    A close is not charged; after one, the request is resent. A generation that is backing off or waiting for its tools is not running and is never charged (§8.3; `generation.ts` `interrupted()`).
14. **Compaction requests.** After a crash, the summary request is resent until the limit ends the task (§8.7).
15. **Reports.** When a task commits a `failed` outcome whose detail reason is `restart_limit` or `interrupted`, it is reported as `task_interrupted`. The report carries that reason, the task, and its `restarts` value from before the commit (§10.1; `#runtime().commit` and `interruptionOf`).
16. **Tool progress.** A running tool's output and details live in its `pi.live` slot and are not task progress. A safe rerun first clears what the interrupted attempt published (§8.2, §8.4; `harness-tools-recovery`: "clears the interrupted attempt's progress before a safe rerun").
17. **Built-in versions.** When an older Harness would mishandle a newer record of a built-in kind, the definition's version is raised and a `migrate` accepts older records. The older Harness then leaves newer records blocked as `task_too_old` (§8.3 for `pi.generation` 2; §8.4 for `pi.tool` 3).

## 6. Design

### 6.1 Persistence and built-in definition versions

```go
type TaskRecord struct {
    // ...existing fields
    // Restarts counts consecutive crashes that cut the task off without a
    // state change since. It is scheduler-owned: a clean Close does not
    // count, and a new committed state, an abort mark, or an outcome resets it.
    Restarts int `json:"restarts,omitempty"`
}
```

Upstream adds `restarts` as an optional record field without changing any storage format, and Go does the same:

- `State.Version` stays 2, and no load or open path treats older stores specially.
- Open counts every surviving `running` record, including records left by an older build's clean close, which did not write tasks back. Upstream had the same one-time effect when 1.4.0 introduced the write-back. With the default retry policy, such a request is charged once and retried after the backoff (§6.12).
- An older Go build can still open the store. Its whole-snapshot commits drop `restarts`, which only starts the counts over. It also leaves stopped tasks `running`, which a newer build's open then counts once.
- `Inspect`, views, `Task`, and `WaitTask` expose the count in the record. Inspection states are unchanged.
- `Restarts`, like `Status`, is owned by the scheduler. Mutating it through `Tx.Task` is unsupported.

Following rule 17, this plan raises the version of each built-in whose stored form it changes. Plan 0001's resolution, migration, and blocked-state machinery already covers Go built-ins:

| Built-in | Version | Stored change | Migration from version 1 | What an older build would otherwise do |
|---|---|---|---|---|
| `pi.generation` | 1 → 2 | A `retry` phase; a 1-based `attempt` and `until` replace the `retry` count (§6.8) | Set `attempt = retry + 1` and keep the other fields | Fault on the unknown `retry` phase |
| `pi.compaction` | 1 → 2 | A `retry` phase, `attempt`, and `until` (§6.8) | Identity; a missing `attempt` means 1 | Fault on the unknown `retry` phase |
| `pi.tool` | 1 → 2 | Output and details stored only in the `pi.live` slot (§6.11) | Identity; a legacy checkpoint `output` stays readable | Drop committed output from interrupted and aborted results |

Go numbers its own built-in versions. Go's stored built-in formats differ from upstream's, where `pi.generation` is version 2, `pi.tool` is 3, and `pi.compaction` is 1. Migrations are pure and run at reservation, so in-flight records continue after an upgrade.

### 6.2 Counting at open

`Harness.reconcile` already rewrites `running` tasks to `pending` in its internal commit. It now also increments `Restarts`. It changes nothing else:

- `pending`, `waiting`, and `completing` tasks were not running and are not counted. That includes tasks backing off as `pending` with a future `WakeAt`, whose deadlines are also unchanged.
- Abandonment capture through `AbandonOnRestart` is unchanged. The first scheduling pass then sets a `restart` mark, which resets the count (§6.4).

Opening and closing without resuming counts each surviving task once. Those tasks are `pending` afterward, so later opens do not count them again. A future read-only open must not count, because upstream's read-only opens write nothing.

### 6.3 A clean close writes stopped tasks back

Before M1, `Close` sealed admission and `Scheduler.close` cancelled invocations. `runInvocation` then skipped its release write because the Session was sealed, so the stopped tasks stayed `running`.

**Stopped set.** When the Session is sealed or failed (`h.Err() != nil`), `finishInvocation` records the task ID in `Scheduler.stopped`, under `Scheduler.mu`. Sealing prevents new reservation admission; already-admitted commits settle before close joins invocations. This captures upstream's set: every invocation stopped by the close. It also captures an invocation that ends between the seal and `Scheduler.close`. Concurrent goroutines make that window real, so capturing only at `Scheduler.close`, as upstream's single-threaded `join()` does, would miss such invocations. A cancelled `Open` has no invocations, so it writes nothing and cannot erase crash evidence that its reconciliation did not count.

**Write.** `beforeClose` runs, in order: `scheduler.close()`, `<-scheduler.done`, `scheduler.writeBackStopped()`, and `stopRegistry()`. `writeBackStopped` calls a new private `Session.writeAtClose(fn)`, which:

- holds the commit mutex, so it follows every commit admitted before the seal. For example, it keeps an abort mark committed just before close;
- writes nothing if the Session has failed;
- rewrites each stopped task still `running` to `pending`, keeping `Restarts`, the checkpoint, marks, and memos;
- writes nothing (`errNoChange`) when no stopped task is still `running`;
- bypasses the sealed-admission check, but runs no user callback and publishes nothing, since every watch has already ended;
- uses `context.Background()`, and fails the Session through `failLocked` on any storage error, including a certified rejection;
- runs `afterCommit` on success.

**Result of a failed write-back, as upstream:** the Session fails, a `storage_failed` report is queued, and the backend still closes. `Close` returns only the backend close error, so it returns nil when only the write-back failed. `WaitClosed` and `Done` show `SessionEnd{Kind: "failed"}` with the cause. If the write did not apply, the tasks stay `running` for the next open to count. An uncertain write may have applied; the failed Session never adopts a guess, and only reopening establishes the durable record.

A failure shutdown writes nothing, so its remaining `running` tasks are counted at the next open, as after a crash. A `Close(ctx)` that times out keeps cleaning up. If the process exits before the write-back lands, its tasks count as cut off.

### 6.4 What starts the count over

| Event | `Restarts` |
|---|---|
| Opening with a `running` task | Incremented by 1; the task becomes `pending` |
| Opening with any other status | Unchanged |
| Clean close of a stopped `running` task | Unchanged; the task becomes `pending` |
| Failure shutdown | Unchanged; the task stays `running` |
| A task commit that changes the checkpoint, status (such as a `pending` yield or `waiting`), wait set, or `WakeAt` | Reset to 0 |
| A task commit that keeps the same state; a memo, document, or entry write; a tool output or details write | Unchanged |
| A scheduler write: reservation, migration, handover, or the abort-cancellation release to `pending` | Unchanged |
| Setting an abort mark, or changing its reason | Reset to 0 |
| An outcome held as `completing`, or a terminal outcome | Reset to 0 |

Implementation:

- `TaskRuntime.Commit` captures the record's status, checkpoint, wait set, wait policy, `WakeAt`, and `Restarts` *before* calling `change`, because the callback can mutate the record through `Tx.Task`. After `applyTransition`, it clears `Restarts` if the committed state differs. Checkpoints are compared with `jsonEqual`. A nil transition checkpoint means the checkpoint is unchanged.
- `applyTransition` clears `Restarts` together with memos when it records a terminal or `completing` outcome. `Scheduler.fault` and `terminalizeTask` do the same.
- `markAbortTreeReason` and `markOwned` clear `Restarts` whenever they set `AbortRequested` or change `AbortReason`. Re-marking with the same reason, as `AbortOwned` does for a restart-marked task, keeps it.

### 6.5 Restart limit

```go
type Settings struct {
    // ...
    // RestartLimit ends a task cut off this many times in a row. It follows
    // Pi Durable's number setting: values below 1, and NaN, select the
    // default of 5; fractions round down; math.Inf(1) never ends a task.
    RestartLimit float64
}
```

`float64` carries the JavaScript number semantics, including `Infinity`. The zero value selects the default, as an absent upstream setting does.

- A task is **cut off** when `Restarts > 0` and `float64(Restarts) >= limit`.
- Its cleanup is **salvageable** while `float64(Restarts) <= limit`.
- Messages describe the count as "once", or as "N times in a row", following `cutOffCount`.

Go settings are fixed for each Harness (§12.2). To let a cut-off task that has not ended yet run, reopen with a higher limit before the task is reserved. A task that has already ended stays ended.

### 6.6 Retry policy

```go
// RetrySettings is Pi Durable's HarnessSettings.retry. Each set field
// overrides the default policy: Enabled true, MaxRetries 3, BaseDelayMs 2000,
// and MaxAgentDelayMs 60000. A nil field keeps the default. Numbers follow
// JavaScript's.
type RetrySettings struct {
    Enabled         *bool
    MaxRetries      *float64 // retries after the first attempt
    BaseDelayMs     *float64 // delay before retry n is BaseDelayMs × 2^(n-1)
    MaxAgentDelayMs *float64 // cap on each delay
}

type Settings struct {
    Retry        RetrySettings
    RestartLimit float64
    MaxTurns     int // Go-specific safeguard, unchanged
}
```

- **Removed fields.** `RetryMax` and `RetryDelay` are removed.
- **Pointers.** Pointer fields are Go's form of `Partial<…>`: a nil field keeps the default, while an explicit value of `false` or `0` is honored.
- **Resolution.** At every classification, resolution merges the set fields over the default, as `{ ...DEFAULT_RETRY_POLICY, ...settings.retry }` does.
- **When a retry is allowed.** A retry is allowed when `Enabled && float64(attempt) <= MaxRetries`, where `attempt` is a 1-based integer. As in JavaScript, `math.Inf(1)` retries without bound, and a negative or NaN value never retries.
- **Delay.** `retryDelayMs` is ported from pi-ai:
  1. Compute `delay = BaseDelayMs × 2^max(0, attempt − 1)`.
  2. If `delay` is not a safe integer, set it to `2^53 − 1`.
  3. Return `min(delay, MaxAgentDelayMs)`.

  The scheduler converts the result to a `time.Duration`, saturating at the maximum. A NaN or negative delay is due at once, matching how JavaScript compares `NaN` and past times.

### 6.7 Model error classification

Upstream classifies the assistant error message with the error-text part of `isContextOverflow` and with pi-ai's `isRetryableAssistantError`. Go's `Model` returns an `error`, so classification works as follows:

- **Optional methods.** An error that implements `Retryable() bool` or `ContextOverflow() bool`, found with `errors.As`, answers that question itself. Adapters that sanitize provider text use these methods, so the raw text is never needed.
- **Text classification.** Otherwise, the error's message is treated as the error text. Port pi-ai v1.5.2's two pattern lists verbatim:
  - non-retryable quota, billing, and subscription limits;
  - retryable load, status, server, gateway, transport, and stream failures.

  Also port the error-text part of `isContextOverflow`: the overflow patterns, the non-overflow exclusions, and the Cerebras check for an error without a body, which needs the request's provider. An empty message is neither retryable nor an overflow.
- **RE2 compatibility.** Go's `regexp` package has no lookaround, so rewrite the two affected patterns under `(?i)`. The rewrites are equivalent for match-anywhere tests:

  | pi-ai pattern | Go pattern |
  |---|---|
  | `(?<![a-z0-9])(?:429\|…)(?![a-z0-9])` | `(?:^\|[^a-z0-9])(?:429\|…)(?:[^a-z0-9]\|$)` |
  | `1308(?![0-9])` | `1308(?:[^0-9]\|$)` |

- **Exported helpers.** Export `IsRetryableModelError(err error) bool` and `IsContextOverflowError(err error, provider string) bool` for adapters and custom models.
- **`model.Pi`.** The bridge resolves pi-ai through the coding agent's dependency tree. Inside Node, it applies pi-ai's own `isRetryableAssistantError` and `isContextOverflow` to a provider error message and sends only two booleans to Go. Go returns the existing sanitized error, which now implements both methods. If pi-ai cannot be resolved, both answers are false.
- **`model.OpenAI`.** Before sanitizing, the adapter classifies the text that pi-ai would carry:
  - an HTTP status code together with a bounded excerpt of the error body;
  - an SSE error event's message.

  Transport failures, and streams that end before `[DONE]`, are retryable; they correspond to pi-ai's fetch, socket, and premature-end patterns. Configuration, request-encoding, invalid-JSON, and size-limit errors are neither retryable nor overflows. The returned error stays sanitized.

### 6.8 Generation and compaction retries

**Generation (`pi.generation` 2).** The checkpoint carries a 1-based `attempt`. A new turn starts at attempt 1: the run's first request, the turn after a tool round, or an `OnYield` continuation.

- **`prepare`.** At attempt 1, `prepare` counts a turn and checks `MaxTurns`. At a later attempt, it prepares the same turn again, so any agent, section, or settings change made during the backoff applies.
- **`request`.** A cancelled invocation returns as it does today. On any other model error, the generation retries if:
  - the error is not an overflow;
  - it is retryable;
  - `Enabled` is true;
  - `attempt <= MaxRetries`.

  The retry commit appends the partial as an aborted assistant entry, as today, and yields `pending` at `{phase: "retry", attempt, until}` with `WakeAt = until = now + retryDelayMs(policy, attempt)`. When the error does not qualify for a retry, the run fails as it does today.
- **`retry`.** After the limit check (§6.10):
  - before `until`, it yields `pending` again with the same checkpoint;
  - otherwise, it commits `running` at `{phase: "prepare", attempt: attempt + 1}`, which continues into `prepare` within the same invocation.

**Compaction (`pi.compaction` 2).**

- **`request`.** A summary error that is retryable, with `Enabled` true and `attempt <= MaxRetries`, yields `pending` at `{phase: "retry", attempt, until}`. As in upstream compaction, overflow is not checked. Otherwise the compaction fails as it does today.
- **`retry`.** It yields again before `until`, then returns to `request` with `attempt + 1` and the same pinned request.

### 6.9 Scheduler enforcement

In `Scheduler.reserve`, the existing eligibility checks run first: status, live waits, a future `WakeAt`, owned work for an abort-marked task, and owner cancellation intent.

Then, before `resolveForStart`, the scheduler checks whether the task is cut off. If it is, and the task is either abort-marked or not a Harness built-in (`pi.generation`, `pi.tool`, or `pi.compaction`), the scheduler:

1. Builds upstream's message:
   - for a run invocation: `Task <id> (<kind>) was cut off <n> times without progress`;
   - for an abort invocation: `Abort handler of task <id> (<kind>) was cut off <n> times`.
2. Writes `Outcome{Status: faulted, Error: message, ErrorDetail: {"reason":"restart_limit"}}` through `Scheduler.fault`, extended to take an `ErrorDetail`. While a run-mode task's ordinary owned work lives, the fault holds as `completing` and starts a cascade. An abort-marked task gets here only when it has no live owned work, so its fault becomes terminal at once. A terminal fault runs `settleSchedulerOutcome`.
3. After the commit succeeds, reports `task_faulted` with reason `restart_limit`, the task, and the installing extension, resolved from the current snapshot. The extension is empty for a missing definition or a built-in.

As in upstream's table, this check comes before resolution. As a result:

- a blocked extension task at the limit faults instead of staying blocked;
- an abort-marked blocked task faults instead of becoming `orphaned`;
- a `restart`-marked abandoned task, which runs as an abort invocation, also faults at the limit.

`settleSchedulerOutcome` gains a cut-off branch for `pi.generation`. When the fault's `ErrorDetail` reason is `restart_limit`, the cut-off happened in the abort handler, so the run ends as aborted:

- its running inputs settle `unanswered`, with reason `aborted` and the fault message as `Detail`;
- the partial is not converted, because that conversion may be what keeps failing;
- `Busy` is cleared;
- queued inputs stay queued.

Tool cleanup is unchanged: the slot is removed and no entry is appended. Go has no compaction status to clear.

### 6.10 Built-in tasks at the limit

| Kind | Checked at the start of | Ending | Cleanup kept at the limit, skipped past it |
|---|---|---|---|
| `pi.generation` | `prepare`, `request`, `retry`, and `tools` | Inputs settle `unanswered` with reason `restart_limit` and detail `The generation was interrupted <count> in its <phase> phase`. `Busy` is cleared and queued inputs are kept. The task ends `failed` with that error and `{"reason":"restart_limit"}`. | In `request`, the committed partial becomes an aborted assistant entry |
| `pi.tool` | `call` and `execute`, before the replay decision | An error result, `Tool <name> was interrupted <count>; it is not run again`, followed by the committed slot output in Go's result form. The slot is removed. The task ends `failed` with the message and `{"reason":"restart_limit"}`. | None; Go does not yet run hooks on results the Harness writes (§12.2) |
| `pi.compaction` | `request`, `retry`, and `publish` | `failed` with `Compaction was interrupted <count>` and `{"reason":"restart_limit"}` | None |

Each ending is one `TaskRuntime.Commit`, reported as `task_interrupted` (§6.13).

In `request`, the checks run in this order:

1. the limit check;
2. the interrupted-request charge (§6.12);
3. the model check.

With this order, the limit ends a charge that keeps killing the host.

Implementation:

- Add shared helpers: `cutOff`, `salvageable`, and `cutOffCount`.
- Add a `settleRun` variant that takes a reason code and detail text, and builds `ErrorDetail`. Like plan 0001's `faulted` settlement, it stores the code in `Reason` and the text, as a JSON string, in `Detail`.
- Give `finishTool` an explicit outcome error and detail, so the task's `Error` is just the limit message rather than the whole result content.

### 6.11 Tool progress is not task progress

Today, `ToolRuntime.Output` appends to `toolCheckpoint.Output` and mirrors it into the `pi.live` slot. Every output chunk therefore changes the checkpoint and would reset `Restarts`, so a replay-safe tool that streams output before killing the host would never reach the limit. Upstream's scenario "ends a tool call with an error result instead of running it again" streams output in exactly this way.

In `pi.tool` version 2:

- `Output` reads the slot's output, appends to it, keeps the trailing 50 KiB, writes the slot, and commits the unchanged checkpoint.
- `finishTool` reads committed output from the slot. If the slot has none, it falls back to a legacy checkpoint `output`. New code never writes that checkpoint field.
- A safe rerun (`execute`, with both the stored and current definitions `ReplaySafe`) first commits a cleared slot `output` and `details`, then executes. An unsafe interrupted result still uses the interrupted attempt's slot output.

Generation partials stay in the checkpoint for now (§12.2). This does not change the guard: a `request` phase that starts with a nonzero count always ends or charges before streaming.

### 6.12 Interrupted model requests

In `request`, after the limit check and before the model check, `Restarts > 0` means the attempt in flight was cut off. The text is `The host stopped while the model was answering, on attempt <attempt>`.

- **A retry remains.** If `Enabled && attempt <= MaxRetries`, one commit appends the partial as an aborted entry and yields `pending` at `{phase: "retry", attempt, until: now + retryDelayMs(policy, attempt)}`. There is no classification, because a crash is not an error message. No usage is recorded. `retry` then prepares attempt `attempt + 1` (§6.8).
- **No retry remains.** One commit:
  - converts the partial;
  - settles the inputs `unanswered` with reason `interrupted` and the text;
  - clears `Busy` and keeps queued inputs;
  - fails the task with `{"reason":"interrupted"}`.
- **No restarts.** When `Restarts == 0` (a first attempt, an attempt after a retry, or an attempt after a clean close), the phase converts any leftover partial and resends the pinned request. This is today's behavior and upstream's behavior after a close.
- **Not charged.** A generation that is backing off, or waiting for its tools, was not running at the crash.
- **With the default policy,** interrupted attempts 1 to 3 are retried after 2, 4, and 8 seconds. If attempt 4 is also cut off, the run ends `interrupted`.

### 6.13 Reports

```go
const ReportTaskInterrupted ReportKind = "task_interrupted"

type Report struct {
    // ...existing fields
    Restarts int // task_interrupted: the task's restart count when it ended
}
```

- **`task_interrupted`.** `TaskRuntime.Commit` captures `Restarts` before the commit. After it successfully commits a terminal transition with a `failed` outcome whose `ErrorDetail` reason is `restart_limit` or `interrupted`, it enqueues a `task_interrupted` report. The report carries:
  - the reason;
  - the captured count;
  - the `ReportTask`;
  - the runtime's extension;
  - `Err`, which wraps the outcome error.

  The rule is generic, as upstream's is: an extension task that ends itself this way is reported too. An outcome held as `completing` is reported when it is committed.
- **`task_faulted`.** Reservation faults report it with reason `restart_limit` (§6.9). Step faults keep reason `threw`.
- **Not reported.** Counting, the close write-back, and individual retries. Delivery remains queued and best-effort.

### 6.14 Concurrency rules

- **Lock order.** It is unchanged: the Session mutex first, then `Scheduler.mu`. `stopped` is guarded by `Scheduler.mu`. The write-back reads it on the Session line, which that order permits.
- **Write-back timing.** The write-back runs after `<-scheduler.done`. By then, the loop and every invocation goroutine have ended, so no handler overlaps the write-back and no reservation can start.
- **`writeAtClose`.** It is private and reachable only from `beforeClose`. It runs no user code, and after a failure it does nothing.
- **Classification and delays.** Both are pure and computed off the Session line, before the commit that records the decision.
- **Commit paths.** Built-in limit, retry, and charge paths use `TaskRuntime.Commit` and inherit plan 0001's runtime gate. Reservation faults use `commitInternal`, so a failed write fails the Session.

## 7. Public API and behavior changes

| API or behavior | Before | After | Breaking |
|---|---|---|---|
| `TaskRecord.Restarts` | None | Crash count owned by the scheduler | No |
| `Settings.Retry` (`RetrySettings`) | `RetryMax`, `RetryDelay` | Upstream's policy, merged over its default | Yes: both fields are removed, and the default changes from no retries to upstream's |
| `Settings.RestartLimit float64` | None | Default 5; `math.Inf(1)` means unlimited | Behavioral |
| `Retryable()` and `ContextOverflow()` on model errors; `IsRetryableModelError` and `IsContextOverflowError` | None | Added | No |
| `ReportTaskInterrupted`, `Report.Restarts`, and `task_faulted` reason `restart_limit` | None | Added | No |
| `pi.generation`, `pi.compaction`, and `pi.tool` versions | 1 | 2; version-1 records migrate | Older builds block newer records as `task_too_old` |
| `State.Version` | 2 | 2 | No |
| `Harness.Close` | Leaves stopped tasks `running` | Writes them back as `pending`, adding one commit when any were stopped | Behavioral |
| Model errors | With `RetryMax > 0`, every error retries after a constant delay and resends the pinned request | Only transient errors retry, with exponential backoff, and the request is prepared again | Behavioral |
| Compaction summary errors | Fail at once | Retried under the policy | Behavioral |
| Crash during a model request | Request resent on reopen | Charged as a failed attempt | Behavioral |
| `ToolRuntime.Output` | Rewrites the checkpoint and the slot | Writes only the slot | Internal layout |
| Safe tool rerun | Appends to the interrupted attempt's output | Starts with cleared output and details | Behavioral |
| Submission reasons | None for crashes | `interrupted` and `restart_limit` with `Detail`; `aborted` with `Detail` for a cut-off abort handler | No; additive |

Migration notes for the README and commit descriptions:

- **Retry settings.** Replace `RetryMax: n, RetryDelay: d` with `Retry: RetrySettings{MaxRetries: ptr(float64(n)), BaseDelayMs: ptr(float64(d.Milliseconds()))}`, where `ptr` is a local helper such as `func ptr[T any](v T) *T { return &v }`. Delays now double per attempt, up to `MaxAgentDelayMs`. To disable retries, set `Enabled: ptr(false)`. In tests, disable retries for fakes whose error text looks transient, such as "timeout" or "503".
- **Upgrading.** The first open after upgrading counts tasks that an older build's clean close left `running`. With the default policy, an interrupted request is retried after the backoff.
- **Downgrading.** Do not downgrade while runs are live: older builds leave version-2 built-in tasks blocked.
- **Recognizing crash loops.** A crash loop shows up as either:
  - a `faulted` outcome whose `ErrorDetail` reason is `restart_limit`; or
  - a `failed` outcome whose `ErrorDetail` reason is `interrupted` or `restart_limit`.

  Decode `ErrorDetail` as JSON.
- **Restart counts.** Every task running when the host dies is counted, not only the one that caused the crash. Use `math.Inf(1)` to keep the old unbounded behavior.
- **Progress.** Checkpoint changes count as progress. Store throttled task progress in documents or memos if it should not reset the count.

## 8. Milestones

Per the maintainer's 2026-11-11 instruction, commit and push each remaining milestone directly to `main`; do not create branches or pull requests. Each milestone must pass `make check` before publication and before dependent work starts. M1's already-open PR #1 was merged before this workflow took effect.

### Milestone 0: Preconditions

- Merge plan 0001's implementation to `main`, with its checks passing.
- Run plan 0001's §2 drift check. If the latest release is newer than v1.5.2, reread its changelog, the Durable specification's §2.2, §5.4, §8.3, §8.4, §8.7, and §10.1, and pi-ai's `retry.ts` and `overflow.ts`, then update this plan.
- This plan needs no `AGENTS.md` change. Plan 0001's v1.5.2 reference update to that file still awaits separate maintainer approval.

**Exit criteria:** the baseline is green, and the target release is confirmed.

### Milestone 1: Restart accounting

- Add `TaskRecord.Restarts` and counting during reconciliation (§6.1, §6.2).
- Add the progress, mark, and outcome resets (§6.4).
- Add the close write-back: `Scheduler.stopped`, capture in `finishInvocation`, `Session.writeAtClose`, and the new `beforeClose` order (§6.3).
- Audit tests that persist `running` records and reopen, including `task_restart_test.go` and `recovery_test.go`.
- Add the M1 rows from §9.

**Exit criteria:**

- Counts are observable, but no task ends because of them.
- Behavior after a clean close is unchanged.
- The storage format is unchanged.

### Milestone 2: Enforcement and reports

- Add `Settings.RestartLimit` and its normalization (§6.5).
- Add the reservation-time fault, and give `Scheduler.fault` an `ErrorDetail` (§6.9).
- Add `task_faulted` reports with reason `restart_limit`, the `task_interrupted` rule, and `Report.Restarts` (§6.13).
- Add the generation cleanup branch for a cut-off abort handler (§6.9).
- Add the M2 rows from §9.

**Exit criteria:** crash loops in extension tasks and abort handlers end at the limit. Built-in run invocations are unchanged.

### Milestone 3: Retry policy and classification

Make each item a separate commit:

1. In `retry.go`, add `RetrySettings` and its resolution, `retryDelayMs`, both classifiers, and the optional methods. Remove `RetryMax` and `RetryDelay` (§6.6, §6.7).
2. Add adapter classification:
   - the `model.Pi` bridge flags, with a fake pi-ai module next to the fake SDK fixture;
   - classification in `model.OpenAI`.
3. Add the generation retry flow and `pi.generation` 2, with its migration (§6.1, §6.8).
4. Add the compaction retry flow and `pi.compaction` 2, with its migration.

Add the M3 rows from §9, and audit tests whose fake model errors look transient.

**Exit criteria:**

- Retries match upstream: the default policy, the merge, the delays, the classification, and preparing again.
- Version-1 records continue after migration.
- Records newer than a built-in definition stay blocked.

### Milestone 4: Built-in tasks end themselves

1. Move tool progress to the `pi.live` slot, raise `pi.tool` to version 2, and add the legacy fallback and the safe-rerun clear (§6.11). Make this a separate commit.
2. Add limit checks, messages, and the salvage rule to every generation, tool, and compaction phase (§6.10).

Add the M4 rows from §9.

**Exit criteria:** every task kind ends at the limit, and tool output commits do not reset counts.

### Milestone 5: Interrupted model requests

- Add the generation charge (§6.12).
- Add the M5 rows from §9.

**Exit criteria:**

- A request cut off by a crash is never resent.
- A request stopped by a clean close is resent.
- The retry policy bounds the number of charges.
- The restart limit bounds a charge that itself crashes the host.

### Milestone 6: Documentation and compatibility

- **`README.md`.** Update these sections:
  - "Persistence and recovery";
  - the `Close` bullet in "Safety and limits";
  - the text on task outcomes and reports.

  Cover crash and close semantics, the restart limit, the retry policy and model-error classification, interrupted requests, `Restarts`, the new reports, the built-in versions, and the migration notes from §7.
- **`COMPATIBILITY.md`.** Mark the crash-loop row of Appendix A implemented, record that the retry policy is ported, and list every difference from §12.2.
- **This plan.** Add an implementation record.

**Exit criteria:** every item in the §10 checklist passes.

## 9. Test plan

All tests use fake models, `httptest` servers, the fake SDK fixture, in-memory storage, or SQLite and JSONL files in a temporary directory. None makes a network call or a paid model call. They are independent Go tests; upstream test names identify each scenario.

**Crash simulation.** A test-only storage wrapper has a `crash()` switch. After the switch, `Commit` returns an uncertain error (never a `RejectedError`), and nothing more is written. `Close` releases only the wrapper:

- for memory storage, the shared `MemoryStorage` stays open;
- for SQLite or JSONL, the file handle closes and the data is unchanged.

A `crash(t, h)` helper simulates the crash:

1. It flips the switch.
2. It forces one commit with `h.Commit(ctx, nil)`, so the Session fails.
3. It waits for `h.Done()`.

A failed Session writes nothing at close, so `running` records survive exactly as a killed process would leave them. The test then reopens a new Harness over the same backend.

Fake models and tools honor cancellation, so the dead Harness's goroutines exit. Use a fixed clock for timing assertions. Wait on channels signalled from inside handlers rather than on sleeps.

| M | Scenario | Upstream reference |
|---|---|---|
| 1 | Open counts only `running` tasks, including tasks running abort handlers. `pending`, `waiting`, `completing`, and backing-off tasks are untouched. Opening and closing without resuming counts once, and later opens do not count again. | §5.4; "does not charge a generation that sleeps in a retry or waits for its tools" |
| 1 | A store written by an earlier Go build has its `running` records counted at the first open, and `State.Version` stays 2. SQLite and JSONL round-trip `restarts`. | §5.4 `open()`; Go storage |
| 1 | After a clean close stops a run, no `running` record remains and nothing is counted. Reopening resends the request; the existing recovery test also asserts `Restarts == 0`. | "resends after a clean close: a close is not a crash" |
| 1 | The write-back keeps an abort mark committed just before close. If the write-back fails in storage, `Close` returns nil while the backend close succeeds; `WaitClosed` reports `SessionEnd{Kind: "failed"}`; `storage_failed` is reported; and unapplied writes leave tasks `running` to be counted at the next open. An uncertain applied write is not adopted by the failed Session; reopening observes the stored `pending` task without incrementing. A failure shutdown writes nothing. | `join()`; `session.ts` `writeAtClose()` and `close()` |
| 1 | An invocation that ends between the seal and `Scheduler.close` is still written back. A cancelled `Open` writes nothing at close. | Go regression |
| 1 | A new checkpoint, a `pending` yield, a wait, or an outcome resets the count. Committing the same checkpoint again, memos, documents, and entries do not. A request, restart, cascade, or upgraded mark resets the count; re-marking with the same reason does not. | §5.4 "Restarts" |
| 1 | Reservation, migration, handover, and the abort-cancellation release keep the count. | §5.4; `withState` |
| 2 | An extension task cut off `RestartLimit` times faults at reservation. The fault carries upstream's message and `ErrorDetail`, and `task_faulted` reports reason `restart_limit` and names the extension. `Run` is not called, and a held fault cascades to owned work. | §5.4 table; `#reserve` |
| 2 | A blocked extension task at the limit, such as one whose definition is missing, faults. An abort-marked blocked task faults instead of becoming `orphaned`. | §5.4 table order |
| 2 | An extension abort handler cut off at the limit faults with the abort message, and `Abort` is not called again. | §5.4 |
| 2 | Limit normalization: by default, a task cut off 4 times runs and one cut off 5 times faults. 0, negative values, and NaN select 5. 2.7 behaves as 2, and `math.Inf(1)` never ends a task. Reopening with a higher limit lets a cut-off task run. | `agent.ts` `restartLimit()`; changelog line 138 |
| 2 | Seed a stored abort-marked generation at the limit. Its run ends `aborted`, with the fault message as `Detail`. No partial is converted, `Busy` is cleared, queued inputs are kept, and the task is `faulted`. | "ends a run someone aborted as aborted when its abort handler is cut off restartLimit times" |
| 2 | An extension task that commits `failed` with `{"reason":"restart_limit"}` or `{"reason":"interrupted"}` produces `task_interrupted` with the count from before the commit. Other failed outcomes produce no such report. | `#runtime().commit`; `interruptionOf` |
| 2 | A rejected or uncertain write of a reservation fault fails the Session and reports nothing for the task. | Plan 0001 §6.6 |
| 3 | Empty `RetrySettings` resolve to enabled, 3, 2000, and 60000. Each set field overrides only itself. Explicit `Enabled: false`, `MaxRetries: 0`, and `MaxAgentDelayMs: 0` are honored. | §2.2 `Settings`; `agent.ts` `resolveSettings` |
| 3 | With the defaults, `retryDelayMs` gives 2,000, 4,000, and 8,000 ms for attempts 1 to 3. Larger results are capped at `MaxAgentDelayMs`. A product that is not a safe integer becomes the cap. NaN and negative delays are due at once. | pi-ai `retryDelayMs`; `retry.test.ts` "caps agent retry delay" |
| 3 | The text classifiers agree with every classification case in pi-ai's `retry.test.ts` and every error-text case in `overflow.test.ts`. These include quota, billing, and subscription limits, and status codes inside longer numbers. | pi-ai tests at `v1.5.2` |
| 3 | `Retryable()` and `ContextOverflow()` override text classification. | Go adapter boundary |
| 3 | A transient generation error with retries left keeps the partial as an aborted entry and backs off at `retry` until `now + delay`. It then prepares again: a section or agent change made during the backoff applies, and no turn is counted. It answers at attempt 2. | §8.3 `classify`; the `retry` phase |
| 3 | A non-transient error fails at once even when retries are left. A transient error fails when `attempt > MaxRetries` or retries are disabled. An overflow never retries. | §8.3 |
| 3 | A `retry` phase that runs before `until`, for example after the clock moved backward, yields again with the same checkpoint. | §8.3: a phase run before `until` sleeps again |
| 3 | A transient compaction error retries with backoff and then places the summary. A non-transient error fails, and so does a transient one once retries are exhausted. | §8.7 |
| 3 | Version-1 generation, compaction, and tool records in every phase migrate at reservation and continue; inspection shows `pending` with `Migrates`. A record whose version is newer than the built-in definition stays blocked as `task_too_old`. | §8.3 versioning; plan 0001 |
| 3 | `model.Pi`: the fake SDK reports transient, overflow, and quota errors. Go receives sanitized errors whose `Retryable()` and `ContextOverflow()` match pi-ai's classification. | Go bridge |
| 3 | `model.OpenAI`, against `httptest`: 503 and 429 are retryable, but 429 with `insufficient_quota` is not. A 400 response with an overflow body is an overflow. Refused connections and streams that end before `[DONE]` are retryable. SSE error events are classified. No provider text is returned. | pi-ai classification |
| 4 | Tool output and details commits leave the checkpoint unchanged and do not reset `Restarts`. A safe rerun clears the slot first. An unsafe interrupted result includes the interrupted attempt's output. A legacy checkpoint `output` still appears in results. | "clears the interrupted attempt's progress before a safe rerun"; Go legacy |
| 4 | With `RestartLimit: 2`, a replay-safe streaming tool runs twice and is cut off both times. It then ends with an error result containing the limit message and its partial output. The task fails with `restart_limit`, `task_interrupted` reports `Restarts: 2`, and the run continues to `done`. | "ends a tool call with an error result instead of running it again" |
| 4 | A replay-unsafe tool at the limit gets the `restart_limit` result, not the interrupted result. | §8.4; changelog line 150 |
| 4 | With limit 1, a compaction cut off once in `request` or `retry` fails with "Compaction was interrupted once" and reports `task_interrupted`. | "fails a compaction into compactionFailure", without the document |
| 4 | With limit 1, a generation cut off in `prepare` (a section that blocks) settles its inputs `unanswered` with `restart_limit` and detail "The generation was interrupted once in its prepare phase". `task_interrupted` reports `Restarts: 1`. Cut-offs in `retry` and `tools` behave the same way. | "ends a run whose generation is cut off restartLimit times outside a request" |
| 4 | A generation in `request` at the limit keeps its partial as an aborted entry and makes no model call. Past the limit, set through a stored count, it drops the partial with "…3 times in a row in its request phase". | "ends a run restart_limit when charging the interrupted request keeps being cut off"; "drops the partial past the limit…" |
| 5 | With the default policy, a crash during streaming is charged. Before resume, the count is 1. The partial becomes an aborted entry, and the generation backs off at `retry` until `now + 2000 ms`. Attempt 2 prepares again and answers without the aborted partial in its context. The charge resets the count. | "charges the attempt: keeps the partial, waits for the retry backoff, and answers on the next attempt"; "sleeps the backoff of `settings.retry` before the next attempt" |
| 5 | With `MaxRetries: 1`, two crashes end the run `interrupted` with "…on attempt 2". Both partials become aborted entries, the task fails with `{"reason":"interrupted"}`, and no request is sent after the second crash. `task_interrupted` reports reason `interrupted`, `Restarts: 1`, and the text. | "ends the run interrupted once no retry is left…"; "reports task_interrupted…" |
| 5 | With `Enabled: false`, the run ends `interrupted` immediately. | "ends the run at once when retries are disabled" |
| 5 | Crash, then open and close cleanly without resuming, then open again: the request is charged exactly once. | "charges a request once when a crash is followed by a clean close before the charge" |
| 5 | A generation that is backing off, or waiting for its tools, at the crash is not counted or charged; a running tool is counted. | "does not charge a generation that sleeps in a retry or waits for its tools" |
| 5 | `TestRestartPreparedModelRequestAndCommittedPartial`, which uses a clean close, still resends and keeps the partial. | "converts a committed partial into an aborted entry and resends the same messages" |

## 10. Validation checklist

Run after every milestone. `AGENTS.md` requires every check before publication.

```sh
make check   # gofmt, go vet ./..., go test ./..., go test -race ./...
go test -race -count=20 -timeout=300s \
  -run 'Test(CrashLoop|CloseWriteBack|InterruptedRequest|ToolProgress|RetryPolicy|ModelErrorClassification|BuiltinVersion|Restart|Abandon|SessionFailure|HarnessFailure|TaskFault|TaskOrphan|TaskScheduler)' .
go test -race -count=5 ./storage ./model
PI_DURABLE_TEST_PI_PACKAGE_DIR=$HOME/.pi/agent/install/releases/1.5.2/node_modules/@earendil-works/pi-coding-agent \
  go test -race ./model -run TestPiInstalled -count=1 -timeout=60s   # required from milestone 3: the bridge changes
git diff --check
```

Name new tests with these prefixes: `TestCrashLoop`, `TestCloseWriteBack`, `TestInterruptedRequest`, `TestToolProgress`, `TestRetryPolicy`, `TestModelErrorClassification`, and `TestBuiltinVersion`.

Also check that:

- `rg -n 'RetryMax|RetryDelay' --glob '*.go'` finds nothing.
- `rg -n 'cp\.Output\s*\+?=' generation.go tool_runtime.go` finds nothing, so no code writes tool output into the checkpoint.
- In `rg -n 'Restarts' --glob '!*_test.go'`, only reconciliation and the §6.4 resets change the count; every other match only reads it.
- The installed-SDK test uses fake credentials and loopback servers only.
- Plan 0001's §2 drift check passes before each milestone is merged.

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Default retries change ordinary error handling: a transient error now retries up to three times over about 14 seconds | This is upstream's default. Document it. Tests disable retries or use non-transient error text. |
| Adapter errors are sanitized, so their text would never classify as retryable | Adapters report `Retryable()` and `ContextOverflow()`. Custom models get text classification. |
| The ported patterns drift from pi-ai, or behave differently because RE2 lacks lookaround | Port the lists verbatim with documented rewrites. Test against pi-ai's own vectors. The drift check rereads `retry.ts` and `overflow.ts`. |
| Older builds block version-2 built-in records, keeping conversations busy | This is upstream's strategy. The migration notes say not to downgrade while runs are live. |
| The first open after upgrading counts records that an older build's clean close left `running` | Upstream had the same one-time effect. Default retries absorb a charged request. |
| Tasks running alongside the culprit are counted and can end after five crashes in a row | This is upstream's behavior. Any progress resets the count; raise the limit or use `math.Inf(1)`. |
| Close gains a storage write that can fail | It fails closed, as upstream does, and the next open counts the tasks. The write happens only when tasks were stopped. |
| `writeAtClose` bypasses sealed admission | It is private, writes only changes the scheduler builds, runs no callbacks, and does nothing after a failure. Tests cover it. |
| Progress detection goes wrong because a callback mutates the record | Capture the state before the callback. Use table-driven tests for each transition kind. |
| Moving tool output changes recovery output | Keep the legacy fallback. Add recovery tests for safe and unsafe replays. |
| Crash-simulation tests are flaky or leak goroutines | Use the crash switch that fails the Session, synchronize on channels, use cooperative fakes, and run race loops with `-count=20`. |
| Frequent checkpoint changes hide crash loops in extension tasks | This is upstream's behavior. Document that progress which should not reset the count belongs in documents or memos. |
| `float64` settings fields are unusual in Go | They mirror JavaScript numbers exactly. Document the normalization next to each field. |

## 12. Decisions and remaining differences

### 12.1 Decisions

The maintainer directed that every open decision follow the original TypeScript implementation, because this is a port. Applied:

1. **Retry default:** upstream's default policy, with its merge, delay, classification, and retry flow (§6.6–6.8).
2. **Persistence strategy:** upstream's. `restarts` is an optional field with no format change, the first open counts whatever is running, and changed built-in stored forms get new definition versions (§6.1).
3. **Restart limit:** upstream's number semantics, with `Infinity` represented as `math.Inf(1)` (§6.5).
4. **Close result:** as upstream. A failed write-back fails the Session and is reported, and `Close` returns only the backend close error (§6.3).

The same rule governs the rest of the design.

### 12.2 Remaining differences after this plan

Each remaining difference needs an upstream subsystem that Go does not have yet. This plan widens none of them.

| Difference | Upstream behavior | Follow-up |
|---|---|---|
| There is no `pi.live` run state: no `runFailure`, no `generation` attempt or retry note, and no `compactions` or `compactionFailure` | These are recorded in `pi.live` | C, G |
| Run failures other than `faulted`, orphan reasons, `interrupted`, `restart_limit`, and cut-off `aborted` keep free-text reasons | Reason codes such as `model_error` and `no_model`, with a detail | C |
| Go messages have no stop reason or error message. A failed attempt is kept as its partial (an aborted entry), and `AfterResponse` sees only successful responses. | Error entries; `afterResponse` observes every terminal message | J |
| One generation task serves a whole run, with phases `prepare`, `request`, `retry`, and `tools`. There is no `poll`, `finish`, or `drain`, and no deferred model handles. | A generation per turn, with those phases | J, C |
| No structured tool diagnostics, and no `afterTool` hooks on results the Harness writes. The `restart_limit` result uses Go's result form. | A diagnostics block; hooks run at the limit but not past it | F |
| No automatic or overflow compaction, so an overflow fails the run | Overflow compaction when compaction is enabled | G |
| Settings are static | Function-valued settings, read at each resolution | Later |
| No durable `sleeping` status or wakeups; backoffs are `pending` with `WakeAt` | `sleeping` and `wake()` | D |
| Generation partials are stored in the checkpoint | `pi.live.generation.message`, written by deferred commits | H |

## 13. Follow-up roadmap after this plan

- **C** (run-failure state) also records `interrupted` and `restart_limit` in `pi.live.runFailure`, records `model_error` and other reason codes, and shows the attempt and retry note in `pi.live.generation`.
- **G** records a compaction's `restart_limit` failure, and its retry status, in `pi.live`.
- **K** (retry-policy parity), proposed in this plan's first draft, is now part of this plan.
- B, D, E, F, H, I, and J are unchanged from plan 0001 §13.

Proposed next: C, since it builds directly on the run endings added here; then D.

## Appendix A: Upstream changes covered

Source: [`packages/durable/CHANGELOG.md` at `v1.5.2`](https://github.com/earendil-works/pi/blob/v1.5.2/packages/durable/CHANGELOG.md). In that file, 1.5.0 spans lines 23–208 and 1.4.0 spans lines 209–249. Statuses describe the intended result of this plan.

| Changelog line | Release | Change | Go status after this plan |
|---|---|---|---|
| 228 | 1.4.0 | Crash-loop guard: counting, close write-back, and resets on progress | Implemented (§6.1–6.4) |
| 217, 231 | 1.4.0 | Parking, the `restart_limit` blocked reason, and `retryTask()` | Superseded in 1.5.0; not ported |
| 239 | 1.4.0 | Retry and poll backoffs sleep instead of waiting in a running phase; a record that resumes early sleeps again | Backoffs are `pending` with `WakeAt`, and `retry` phases re-check `until`; durable sleeping belongs to D |
| 138 | 1.5.0 | Restart-limited tasks end visibly | Implemented, without the `runFailure` and `compactionFailure` documents or the tool diagnostic code |
| 139 | 1.5.0 | Interrupted requests are charged; a close still resends | Implemented; deferred polls do not apply |
| 150 | 1.5.0 | `afterTool` runs on `restart_limit` results and is skipped past the limit; unsafe tools also end with `restart_limit` | The ending is implemented; the hooks wait for F |
| 165 | 1.5.0 | `RunFailureReason` gains `interrupted` and `restart_limit` | Submission reasons only; `runFailure` belongs to C |
| 166 | 1.5.0 | `task_interrupted` reports | Implemented |
| 203 | 1.5.0 | A host that crashed during a request no longer resends it forever (#11139) | Implemented |
| None | Before the previous reference | The `settings.retry` policy and pi-ai classification (spec §2.2, §8.3, §8.7) | Implemented (§6.6–6.8) |
