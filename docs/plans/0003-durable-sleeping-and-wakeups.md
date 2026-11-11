# Plan 0003: Durable sleeping and wakeups

| | |
|---|---|
| Status | Implemented on `main` |
| Date | 2026-11-11 |
| Compatibility target | Pi Durable **v1.5.2** (tag `v1.5.2`, commit `b4afdddf44e059c67ccf5097e5317d913521b470`) |
| Depends on | [Plan 0002](0002-crash-loop-guard-and-interrupted-requests.md), implemented on `main` |
| Roadmap item | Plan 0001 §13, follow-up **D** |
| Main code areas | `types.go`, `task_runtime.go`, `scheduler.go`, `harness.go`, `inspection.go`, `generation.go`, `compaction.go` |

## 1. Summary

Port upstream's `sleeping` task state and its wakeups, replacing Go's 20 ms polling loop:

- **State.** A task can commit `sleeping` until a time on the Harness clock (`TaskTransition{Status: "sleeping", WakeAt: &until}`, or `TaskRuntime.SleepUntil`). It holds no invocation and resumes at its checkpoint once the clock reaches the deadline. An abort mark ends a sleep at once. An abort handler cannot sleep. A sleeping task was not running, so a crash does not count it.
- **Built-ins.** Generation and compaction retry backoffs sleep, as upstream's do (changelog 1.4.0, line 239); a `retry` phase run early sleeps again.
- **Wakeups.** The scheduler arms one timer for the earliest sleep, through `HarnessOptions.SetTimer` (default `time.AfterFunc`), and rechecks the clock when it fires. With `HarnessOptions.OnWake`, it arms no timer and offers `Wake{Active, At}` after changes instead, for hosts that wake the process themselves. `Harness.Wake()` answers the same question synchronously. `Resume` always starts a pass, so a host woken by its own alarm calls it.
- **No polling.** Every commit, invocation end, registry change, and timer starts a pass. A pass picks up every sleep whose time has come.
- **Pacing timers.** `TaskRuntime.SetTimer` arms a non-durable timer that the invocation's end disarms, for throttling work inside a phase.

## 2. Upstream rules (v1.5.2)

1. `TaskState` `sleeping` has `checkpoint` and `until` (epoch ms on `HarnessOptions.now`); "An abort mark does not wait for it" (`src/types.ts`).
2. `validateSleep`: an abort handler cannot sleep; `until` must be a finite, non-negative time (`scheduler.ts`).
3. `#notDue`: a sleep is not due while `until > now` and the task is not abort-marked. Every reservation pass first makes due sleeps candidates (`#wakeDue`).
4. `wake()`: `active` while an invocation runs, a reconcile pass has work, or a live task can run now; `at` is the earliest not-due sleep of a task without an abort mark.
5. `#scheduleWake`: without `onWake`, arm the built-in timer for `wake().at` while scheduling is enabled; with it, call `onWake` when the value changes, never blocking the line, offering it again after a throw.
6. `TaskRuntime.setTimer`: not durable; the invocation's end disarms it.
7. Generation and compaction `retry` phases sleep until `until`; one run before it sleeps again.

## 3. Go design

- `TaskRecord.Status` gains `sleeping`; `WakeAt` is its `until`. `applyTransition` validates `sleeping` (requires `WakeAt`; abort handlers reject it with upstream's message) and keeps `WaitOn` empty.
- **Compatibility.** Go previously expressed sleeps as `pending` or `waiting` with `WakeAt` and no wait targets. A new transition of that form is stored as `sleeping`. Stored records of that form remain eligible at `WakeAt`, as before. A `waiting` transition with targets and `WakeAt` is rejected: upstream waits have no deadline.
- `TaskRuntime.SleepUntil(ctx, until, checkpoint)` commits the sleep; `TaskRuntime.Now()` reads the Harness clock; `TaskRuntime.SetTimer(callback, delay) (stop func())` paces work.
- The scheduler loop waits on kicks and one wake timer. `Session.afterCommit` kicks after every adopted commit. `HarnessOptions.SetTimer func(callback func(), delay time.Duration) (stop func())` and `HarnessOptions.OnWake func(Wake)` follow upstream; `Wake{Active bool; At *time.Time}`.
- `Inspect` reports `sleeping` with `Until` for any not-due deadline, as today.

## 4. Milestones

1. **Sleeping state.** Status, validation, compatibility forms, `SleepUntil`, `Now`, built-in retry sleeps, inspection, reconcile (not counted). Tests: sleep and resume at the deadline, an early pass sleeping again, an abort mark ending a sleep at once, abort handlers rejected, a crash not counting a sleeping task.
2. **Wakeups.** Timer-driven passes, `SetTimer`, `OnWake`, `Wake()`, `Resume` kicking, commit kicks, and `TaskRuntime.SetTimer`; remove polling. Tests: fake timers and `OnWake` hosts, a timer that fires early, no pass without a kick, and every existing scheduling test passing without polling.
3. **Documentation.** README, COMPATIBILITY appendix row, and this plan's record.

Each milestone passes `make check` and the repeated race suite before it is pushed to `main`.

## Implementation record

- **M1** (`b7ec96e`): `sleeping` status, `SleepUntil`, `Now`, compatibility forms, built-in retry sleeps. `sleep_test.go`.
- **M2**: `wake.go` (`Wake`, `Harness.Wake`, the waker goroutine, `armTimer`, `OnWake` with panics reported as `host_error`), `HarnessOptions.SetTimer`/`OnWake`, `Resume` always kicking, commit kicks through `Session.afterCommit`, invocation-bound `TaskRuntime.SetTimer`, and removal of the 20 ms ticker. `ReportHostError` and `Report.Callback` were added for the `onWake` callback. Tests that move a fake clock now call `Resume`, as upstream hosts do; `wake_test.go` covers fake timers, early fires, `OnWake` hosts, wake values, and pacing timers. The whole root package passed 10 race runs without polling.
- **M3**: README runtime paragraph, COMPATIBILITY rows.

Differences: Go keeps its `pending` yield; `wakeFor` scans the whole snapshot rather than upstream's in-memory indexes; `HarnessOptions.SetTimer` has no separate background/foreground timers (Go has no idle-memory timers to unreference).
