# Persistence and recovery

Every change a Harness makes, from a model answer to a tool result, is committed to storage before anyone sees it. When the process dies, the next one opens the same storage and carries on: this guide covers choosing storage, what happens to work a crash interrupts, and the limits of what recovery can promise.

## Storage

Replace memory storage with SQLite or JSONL:

```go
store, err := storage.OpenSQLite(ctx, "agent.sqlite")
// Or: storage.OpenJSONL(ctx, "agent-state")
if err != nil { return err }
h, err := durable.Open(ctx, store, options)
if err != nil { store.Close(); return err }
h.Resume()
```

Storage writes only what each commit changed (`DeltaStorage`); `storage.OpenJSONLReadOnly` and `storage.OpenSQLiteReadOnly` open a store another process writes without locking, repairing, or writing it, and `Harness.ObserveCommits` streams each commit's changes in order, for replicas. `HarnessOptions.ReadOnly` opens a viewer of a session another process drives: it recovers nothing, never schedules, and refuses commits with writes with `ErrReadOnly`. Import `github.com/leocavalcante/pi-durable-go/storage`. Reinstall application extensions on each reopen; `Open` restores built-in definitions. `Resume`, submission, or a task/submission wait starts scheduling; opening alone does not.

## External effects

**Admission deduplication is not exactly-once external execution.** A crash between an external effect and the commit recording its result leaves uncertainty. Use service-side idempotency keys, queryable receipts, or reconciliation. Interrupted tools replay only when both their stored and current policies declare `ReplaySafe`. Coding tools default to unsafe.

## Crashes, clean closes, and the restart limit

- **Counting.** `Open` rewrites surviving `running` tasks, including running abort handlers, to `pending` and increments their optional `TaskRecord.Restarts`. Other statuses and stored backoff deadlines are unchanged. A clean `Close` joins local invocations and writes the tasks it stopped back to `pending` without counting them; a failure shutdown writes nothing, so its tasks count as crashed.
- **Resets.** A task's changed checkpoint, yield, wait, outcome, or new or upgraded abort mark resets its count. Structurally identical checkpoints, memos, document writes, and a tool's `Output`/`Details` (stored only in its `pi.live` slot) do not. The count is scheduler-owned.
- **Limit.** `Settings.RestartLimit` (default 5; values below 1 and NaN select the default, fractions round down, and `math.Inf(1)` disables it) ends crash loops. At reservation, before definition resolution, an abort handler or a run of any non-built-in kind cut off that many times in a row ends `faulted` with `ErrorDetail` `{"reason":"restart_limit"}`, reported as `task_faulted` with reason `restart_limit`. Blocked and would-be orphaned tasks end too. A generation whose abort handler is cut off ends its run `aborted` with the fault message as `Detail`, without converting its partial.
- **Built-in endings.** Built-in tasks check the limit when each phase starts. A generation settles its inputs `unanswered` with reason `restart_limit` and `The generation was interrupted <count> in its <phase> phase` as `Detail`, keeping a committed partial as an aborted entry at the limit but dropping it past the limit. A tool call ends with the error result `Tool <name> was interrupted <count>; it is not run again`, followed by its committed output, even for a replay-unsafe tool, and its run continues. A compaction fails with `Compaction was interrupted <count>`. Each fails with `ErrorDetail` `{"reason":"restart_limit"}`.
- **Interrupted requests.** A model request in flight when the host crashed is charged as a failed attempt rather than resent: its partial becomes an aborted entry and, while the retry policy allows that attempt, the generation backs off and prepares the next attempt. Otherwise the run ends `unanswered` with reason `interrupted` and `The host stopped while the model was answering, on attempt <n>`, and the task fails with `ErrorDetail` `{"reason":"interrupted"}`. A request stopped by a clean `Close` is resent; a generation backing off or waiting for its tools is not charged.
- **Reports.** A `failed` outcome whose `ErrorDetail` reason is `restart_limit` or `interrupted` is reported as `task_interrupted`, with `Report.Restarts` holding the count before that commit. To recognize crash loops in stored outcomes, decode `ErrorDetail` of `faulted` outcomes (`restart_limit`) and `failed` outcomes (`interrupted` or `restart_limit`).

Every task running when the host dies is counted, not only the one that caused the crash, and any checkpoint change counts as progress. Keep throttled progress that should not reset the count in documents or memos.
