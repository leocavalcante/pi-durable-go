# Plan 0010: Entry heads and attribution

| | |
|---|---|
| Status | Done |
| Compatibility target | Pi Durable **v1.5.2** (commit `b4afdddf44e059c67ccf5097e5317d913521b470`) |
| Upstream | `src/types.ts` (`EntryRecord.head`, `byTaskId`, `EntryDraft.head: "self"`), `src/session/transaction.ts` (`appendEntry`), `src/harness/context.ts`, `src/harness/inbox.ts` (`isDisplayOnly`, `hasQueuedReset`, `isStale`), `src/harness/compaction.ts` (`placeSummary`), `src/entries.ts` |

## 1. Upstream behavior

Any entry can start a new context. `EntryRecord.head` names the first entry of the active context it selects, and an entry draft's `head: "self"` starts the context at the new entry. The newest visible entry with a `head` is the head marker. The context is that marker followed by the entries from its `head` through the tail, other head markers excepted.

Built-in entries use this:
- `pi.reset` has `head: "self"`, with no message for a plain reset or the handoff text.
- `pi.compaction` has `head` set to the first kept entry, the summary as its user message, and `{ reason }` as its data.

Hosts and extensions can write their own head entries.

Writes follow heads too:
- A write is display-only, placed at once even while busy, when it has no message, head, or edits.
- A queued write with `head: "self"` is a reset.
- A write whose numeric head lies before the active head is stale.

Entries appended by a task's commit carry `byTaskId`.

## 2. Go today

- Head markers are the `pi.reset` and `pi.compaction` kinds. A compaction keeps the entries after `data.cut`, and its data also holds `summary` and `text`.
- A display-only write is any kind but those two, without a message.
- A queued reset is a `pi.reset` write.
- A compaction is stale after a later reset or a compaction with a later or equal cut.
- Entries carry no task attribution.

## 3. Design

- `Entry.Head` (`head`) and `Entry.ByTask` (`byTaskId`). A draft's `Head: HeadSelf` resolves to the new entry's ID. `TaskRuntime` commits stamp `ByTask`.
- Context derivation, bounded memory, and write placement read `Head`. Entries stored before it keep working by kind:
  - a `pi.reset` without a head starts at itself;
  - a `pi.compaction` without a head keeps the entries after its `data.cut` and contributes its stored summary.
- Write placement follows upstream's `isDisplayOnly`, `hasQueuedReset`, and `isStale`.
- New `pi.reset` entries take `HeadSelf`. New `pi.compaction` entries take the first kept entry as their head, the summary as their message, and `{reason}` as their data.

## 4. Implementation record

- `Entry.Head`, `Entry.ByTask`, and `HeadSelf`. `Tx.Append` resolves `HeadSelf` and stamps the commit's task, set by `TaskRuntime.CommitWith`.
- `isHeadMarker`, `markerBound`, and `deriveContext` read heads. `legacySummary` keeps derivation working for summaries stored before heads.
- Placement follows upstream: `isDisplayOnlyWrite`, `isResetWrite`, and the head check in `placeWrite`.
- Writers: `Conversation.Reset` and a handoff take `HeadSelf`. `summaryEntry` builds upstream's compaction entry, for both the current path and version-2 tasks, whose staleness now uses the active range's start.
- Tests: `entry_heads_test.go`, and the `custom-head` scenario. In that scenario a host's head entry and an empty marker start the context, equal to upstream; with kind-based markers it differs.
