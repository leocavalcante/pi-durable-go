# Plan 0006: Incremental commits and record storage

| | |
|---|---|
| Status | M1 and M2 done; M3 (group commit) superseded by [plan 0007](0007-upstream-commit-pipeline.md) M2 |
| Compatibility target | Pi Durable **v1.5.2** (tag `v1.5.2`, commit `b4afdddf44e059c67ccf5097e5317d913521b470`) |
| Roadmap item | I (plan 0005 §2) |
| Main code areas | `session.go`, `tx.go`, `documents.go`, `ownership.go`, `types.go`, `memory.go`, `storage/sqlite.go`, `storage/jsonl.go` |

## 1. Problem

Upstream stores indexed records: a commit writes the records it changed, and readers read what they need. The Go port copies the whole State through JSON for every commit (twice), for every internal read (`Snapshot`), and for every watcher, and its backends write the whole State per commit. Cost grows with history: a long conversation makes every partial-answer commit, scheduler pass, and task read slower, until throttled progress alone saturates a core.

## 2. Goal

Commit and read cost proportional to what changes, not to history, without weakening any guarantee:

- Visible state is committed before publication.
- Uncertain storage errors poison the Session; certified rejections roll back.
- A transaction sees its own writes; a failed transaction leaves no trace.
- Values a host receives or hands in are detached from committed state.
- One owning Session per storage.

Upstream file formats remain out of scope.

## 3. Design

### 3.1 Immutable committed state (M1)

The committed `*State` is never mutated after adoption. A transaction starts from a *shallow* candidate: new maps holding the committed record pointers. A record is cloned into the candidate on first mutable access (`Tx.Conversation`, `Tx.Entry`, `Tx.Task`, `Tx.Submission`, document writes, and internal helpers), so the committed record stays untouched. A failed transaction drops its candidate.

At the end of the callback, the records whose pointers differ from the committed ones (new or cloned) are round-tripped through strict JSON one by one, which validates and detaches them, as `cloneState` did for the whole State. Read-only Harnesses refuse a commit whose candidate differs in scalars, keys, or changed-record JSON.

Internal readers read the committed State itself (no copy): they must treat it as read-only. Public APIs that return `*State` or records (`Session.Snapshot`, `Harness.Snapshot`, `Session.Watch`, record getters) keep returning detached copies.

A test-only check guards the invariant: with `verifyCommittedState` set by the package tests, the Session records the committed State's JSON after each adoption and panics if it changes before the next commit, so any write that bypasses copy-on-write fails the suite.

### 3.2 Delta storage (M2)

A backend may implement `DeltaStorage`:

```go
// StateDelta carries the header fields (Version, Sequence, NextID, Root),
// the changed records of each table, and the keys each table removed.
type DeltaStorage interface {
    Storage
    CommitDelta(ctx context.Context, expected uint64, delta *StateDelta) error
}
```

The Session sends the delta when the backend supports it, and the whole State otherwise, so third-party `Storage` implementations keep working. Rejection and uncertainty rules are unchanged.

- **Memory**: applies the delta to its own detached copy.
- **SQLite**: one row per record (`durable_records(kind, id, value)`) plus a header row, updated in one transaction. An existing single-row `durable_state` database is converted on open, in one transaction.
- **JSONL**: appends delta lines after a full snapshot line, and compacts to a fresh snapshot once the deltas outweigh it; recovery replays the snapshot and its complete deltas.

### 3.3 Group commit (M3)

Commits that arrive while a storage write is in flight are applied in memory in order and stored together in the next write, then published together. If the shared write is rejected, every commit in the group is rolled back and fails with the rejection; if it is uncertain, the Session fails as before. Each logical commit keeps its own sequence number for entries and revisions; storage receives the last.

## 4. Milestones

| Milestone | Content | Exit criteria |
|---|---|---|
| M1 | Shallow candidates, copy-on-write accessors, per-record detachment, shared internal reads, test-only invariant check | All tests pass with the check on; benchmark shows commit and internal read cost independent of history |
| M2 | `StateDelta`, `DeltaStorage`, memory, SQLite record tables with conversion, JSONL deltas with compaction | Backends round-trip deltas; reopen equals the committed State; old SQLite files convert |
| M3 | Group commit | Concurrent commits share writes; rejection and failure semantics hold per member |

Each milestone passes `make check` before it is pushed.

## 5. Implementation record

- **M1** (`cow.go`): shallow candidates, `mut*` accessors behind every `Tx` accessor and internal write, per-record strict JSON detachment, `Harness.read` for internal reads, detached `Entry`/`Message` copies from `entriesFromState`, a read-only `cascadePending`, and `verifyCommittedState` on for every package test (`verify_main_test.go`). With 5,000 one-KiB entries in memory storage, an internal read went from 217 ms to 152 ns and a one-entry commit from 761 ms to 160 ms; the rest is `MemoryStorage` copying the whole State, which M2 removes.

- **M2** (`delta.go`): `StateDelta` (header fields, changed records, sorted removed keys) and `DeltaStorage`, collected while detaching. Memory storage applies a cloned delta; SQLite keeps `durable_header` and `durable_records` (one row per record) and converts an earlier single-row database inside its first write's transaction, so opening never writes, and reports errors before `COMMIT` as rejections; JSONL appends `{"delta": ...}` lines after a whole-State line, recovers them in order, and compacts once they outweigh it and 1 MiB. A State upgraded in memory at open is written whole by the first commit, which the test-only check caught. The check also compares memory storage with the committed State after every commit. With 5,000 one-KiB entries, a one-entry commit takes 1.2 ms in memory storage (761 ms before plan 0006) and 3.1 ms in SQLite (1.1 s before).

- **Shared watches**: waits and derived watches (`Conversation.Watch` and `View`, `WatchDocument`, `WatchWindow`, `WatchTaskGraph`, `Submission.Wait`, idle and task waits) and `Inspect` read the committed State through `watchShared` and copy only the records they return; `Snapshot` and `Session.Watch` keep whole copies.
- **M3 deferred**: with deltas, a commit costs about one storage sync, and grouping would relax the storage sequence contract (one write for several sequence numbers) that third-party backends rely on. Group commit stays open until a workload needs it. (Superseded: plan 0007 M2 ports upstream's group commit, with `BatchDeltaStorage` keeping one sequence per commit.)

## 6. Risks

- **A write that bypasses copy-on-write** corrupts the committed State and survives a rollback. Mitigation: the test-only invariant check over the whole suite, and record mutation only through `Tx` accessors.
- **Internal readers that mutate a shared snapshot.** Same check; public APIs keep detached copies.
- **SQLite conversion of existing files.** One transaction; the old table is dropped only after the records are written.
