# Plan 0007: Upstream commit pipeline

| | |
|---|---|
| Status | In progress: M1–M3 done |
| Compatibility target | Pi Durable **v1.5.2** (tag `v1.5.2`, commit `b4afdddf44e059c67ccf5097e5317d913521b470`) |
| Supersedes | The deferrals in [plan 0006](0006-incremental-commits-and-record-storage.md) §5 (group commit) and plan 0005 §3 (deferred commits) |
| Main code areas | `session.go`, `lifecycle.go`, `delta.go`, `storage/`, `generation.go`, `tool_runtime.go`, `prompt.go` |

## 1. Decision

The port follows upstream semantics. Earlier Go choices that differ from upstream's commit pipeline are replaced:

- **Storage errors are final.** Upstream fails the Session on the first error a Storage commit returns ("Storage failures are final"). Go let a certified rejection (`RejectedError`) or a cancelled caller fail one public commit and keep the Session; that exemption goes.
- **Cancellation is checked when a job starts.** Once a commit's callback ran and it was admitted, it is stored and its caller gets its outcome.
- **Group commit.** The line runs one callback at a time; a commit with writes joins the writer's pending batch and the line goes on. The writer stores every commit admitted while no store call is in flight in one call, off the line, and publishes them in line order, each with its own sequence number.
- **Deferred commits.** A commit may ask to be deferred (`persistence: deferred`, `withinMs`, default `settings.progress.persistWithinMs`, 1,000 ms). When it changes only existing conversation documents with `latest` history and the `initial` fork, it is adopted and shown at once and its documents are held: they are stored with the next stored commit, at their deadline, or at close; a crash loses them. Otherwise it is stored like any commit. Upstream's progress commits are deferred: partial answers, running tool output, a slot's running status, and a generation's attempt.
- **Prompt sections.** A section renders an empty string as an empty section; omitting one is explicit.

## 2. Milestones

| Milestone | Content |
|---|---|
| M1 | Final storage errors; cancellation checked at job start |
| M2 | Group commit: writer, batched delta storage (`CommitDeltas`), in-order publication |
| M3 | Deferred commits: `CommitOptions`, held documents, deadlines, close; progress commits deferred |
| M4 | Section omission and instructions as upstream renders them |

Each milestone passes `make check` before it is pushed. Tests that encoded the earlier Go semantics are rewritten to upstream's.

## 3. Implementation record

- **M1** (`18486ed`): `commitLocked` fails the Session on any storage commit error and stores an admitted commit with a context that its caller's cancellation does not reach (`context.WithoutCancel`). Tests that encoded the certified-rejection and caller-cancellation exemptions now assert upstream's: a rejected write fails the Session, a storage error wrapping a cancellation is final, and a callback that cancels its own context still has its commit stored.
- **M2**: `admitLocked` runs a callback on `Session.head`, the newest admitted State, and admits a commit with writes to `pending` with the next sequence; a commit without writes never reaches storage, except the first after an in-memory upgrade. `writeLoop` stores every pending commit in one call (`BatchDeltaStorage.CommitDeltas`: one SQLite transaction, one JSONL write and sync; memory applies them in order), then publishes them in line order. A failed store fails the Session; the commits of the call get its error, later ones `SessionFailed`. Close drains the writer before closing storage. Tests that forced storage with an empty commit now write a document (`touchStorage`).
- **M3**: `CommitOptions{Deferred, WithinMs}` on `Harness.CommitWith` and `TaskRuntime.CommitWith`, with `Settings.Progress.PersistWithinMs` (default 1,000 ms; 0 stores at once; `math.Inf(1)` waits for a stored commit or close). A deferred commit whose writes are only existing conversation documents with latest history and the initial fork (records it touched without changing them are trimmed first) is adopted without a sequence and published at once, or after the commits admitted before it; its documents are held until the next stored commit carries them, the earliest deadline (`time.AfterFunc`), or close. Partial answers and tool output commits are deferred. `newCrashHosts` sets `PersistWithinMs` 0, as upstream's restart tests do. Differences: held documents ride any stored commit, deadlines use the wall clock, and the attempt marker is not deferred.
