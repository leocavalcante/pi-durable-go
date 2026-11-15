# Upgrading

Changes that need action when moving to a newer pi-durable-go. Each release's changes are in the [changelog](../CHANGELOG.md).

- `Submission.Abort` now returns what it found (`SubmissionAborted`, `SubmissionAlreadyPlaced`, or `SubmissionSettled`) instead of an error for a submission that was not queued.
- `Settings.RetryMax` and `RetryDelay` are removed, and retries are now on by default. Replace `RetryMax: n, RetryDelay: d` with `Retry: durable.RetrySettings{MaxRetries: ptr(float64(n)), BaseDelayMs: ptr(float64(d.Milliseconds()))}` using a local `func ptr[T any](v T) *T { return &v }`; delays now double up to `MaxAgentDelayMs`. Set `Enabled: ptr(false)` to disable retries, for example for test fakes whose error text looks transient (such as "timeout" or "503").
- `pi.generation` and `pi.compaction` are version 3, and `pi.tool` is version 2; each migrates older records at reservation. Older builds leave newer records blocked as `task_too_old`, so do not downgrade with runs live.
- State version 4 keeps rewindable documents' revisions as records of their own (opening an older store moves them, and its next commit stores the whole upgraded State); version 3 stored the run model: opening an older store upgrades it in memory (`running` submissions become `placed`, a busy run moves to `pi.live.run`) and the next commit persists it. Older binaries cannot reopen a version-3 store. `Settings.MaxTurns` is removed; bound tool rounds with `Agent.MaxTurns`.
- Restart accounting adds no storage-version change. On the first open after upgrading, tasks an older build's clean close left `running` are counted once; with the default policy an interrupted request is then retried after the backoff. An older State-v2 build can still open the store, but its snapshot writes drop the optional counter.

## Storage versions and APIs

**Persistence/API migration:** Stores of earlier state versions remain readable: `Open` upgrades them in memory and commits the current version (4) during reconciliation; older binaries cannot safely reopen an upgraded store. `SessionEnd.Reason` was renamed to `SessionEnd.Kind`. Scheduler bookkeeping failures, including certified rejected writes, fail the Session; the `RejectedError` exemption remains for ordinary caller commits and caller-cancelled scheduler writes certified unapplied. See [COMPATIBILITY.md](../COMPATIBILITY.md) for the full gap list and intentional Go differences.
