# Plan 0004: Run control, inbox boundaries, and run failures

| | |
|---|---|
| Status | Implemented on `main` |
| Date | 2026-11-11 |
| Compatibility target | Pi Durable **v1.5.2** (tag `v1.5.2`, commit `b4afdddf44e059c67ccf5097e5317d913521b470`) |
| Depends on | Plans [0002](0002-crash-loop-guard-and-interrupted-requests.md) and [0003](0003-durable-sleeping-and-wakeups.md) |
| Roadmap item | Plan 0001 §13, follow-up **C**, with the parts of **J** it needs (a generation per turn) and the compaction status of **G** |
| Main code areas | `live.go` (new), `boundary.go` (new), `generation.go`, `conversation.go`, `inbox.go`, `compaction.go`, `tool_runtime.go`, `task_step.go`, `session.go`, `types.go` |

## 1. Summary

Port upstream's run model (spec §6, §8.2–8.5; `generation.ts`, `live.ts`, `inbox.ts`, `submissions.ts`):

- **`pi.live`** becomes upstream's `LiveState`: `run {taskId, inputs, turns, first}`, `generation {attempt, retry}`, `tools` slots in call order, `compactions`, `compactionFailure`, and `runFailure {taskId, inputs, reason, detail, first}`. `ConversationRecord.Busy` remains as a mirror of `run.taskId`.
- **A generation per turn.** A tool round's end hands the run to a new generation (`handOver`); an answer, a failure, or an abort ends the run (`endRun`).
- **Boundaries.** Queued submissions are placed at upstream's `final` and `postTools` boundaries with `Settings.SteeringMode` and `FollowUpMode` (`one-at-a-time` by default, or `all`); a queued reset turns a post-tools boundary into a final one and ends the run `reset`.
- **Run failures** carry upstream's reason codes (`model_error`, `no_model`, `nothing_to_answer`, `turn_limit`, `reset`, `aborted`, `faulted`, `orphaned`, `interrupted`, `restart_limit`) in the submissions and in `runFailure`; `Settings.AfterFailedRun` (`keep`, `continue`, `withdraw`) decides what happens to the inputs queued behind a failed run.
- **Submissions.** `placed` replaces Go's `running` status and records the placed `Entry`; `continue` submissions start a run on the current context; `WhenIdle: "reject"` refuses input to an idle conversation (`ErrIdle`); `WhenBusy` uses upstream's `followUp` spelling.
- **Turn limit.** `Agent.MaxTurns` bounds a run's tool rounds and ends it `turn_limit`; Go's global `Settings.MaxTurns` safeguard is removed, as upstream has none.
- **Compaction status.** Compactions list themselves in `pi.live.compactions` with their attempt and retry, and a failure is kept in `compactionFailure`.

## 2. Storage

State version 3. Opening a version-2 store upgrades it in memory, persisted by the next commit: `running` submissions become `placed`, write submissions move their placed entry from `Answer` to `Entry`, `follow-up` becomes `followUp`, a busy conversation's `pi.live.run` is built from its generation's input, and Go's version-1 `pi.live` tool map becomes upstream's slot array. `pi.generation` becomes version 3, so a build that ran whole runs in one generation blocks per-turn records; its version-2 records migrate unchanged.

## 3. Remaining differences

- No `pi.inbox` document: queued submissions stay in the submissions table, selected in ID order.
- No entry `head` targets, so no `stale` writes other than compaction summaries.
- No sequential tool rounds, unoffered calls, `afterRun` hooks, or `finish`/`drain` phases (follow-ups F and J).
- Generation partials stay in the checkpoint (H).

## 4. Milestones

1. **Run model.** Everything in §1, with tests for every reason code, both boundaries and queue modes, every `afterFailedRun` policy, continue and idle rules, handover, turn limits, slots, compaction statuses, and the State v3 upgrade.
2. **Documentation.** README, COMPATIBILITY, and this plan's record.

## Implementation record

`live.go` defines `LiveState` (pi.live version 2 in Go's numbering, migrating Go's version-1 tool map) and `readRunLive`, which adopts a version-2 run whose inputs were in its generation's input. `boundary.go` ports `applyBoundary`, `startRun`, `handOver`, `endRun`, `endFailedRun`, `withdrawQueuedInputs`, and `admitSubmission`. `generation.go` was rewritten as one generation per turn (`prepare`, `request`, `retry`, `tools`), with upstream's `answer`, `startToolRound`, and `finishToolRound` (handoff continuation, `turn_limit`, post-tools reset) and coded failures (`failRun`). The tool task moved to `tool_task.go` and keeps its slot until the round ends. Compaction lists its status, retry, and failure. `buildRequest` reports a failing section or `BeforeRequest` hook as `extension_error` and continues, as upstream's `hooks.each` does; `ReportExtensionError` was added. Generation phases now continue within an invocation (`running`) where Go previously yielded `pending`. State version 3 upgrades older stores in memory. `run_control_test.go` covers handover, every failure reason, post-tools resets, `AfterFailedRun`, admission rules, continues, queue modes, compaction status, and the upgrade.
