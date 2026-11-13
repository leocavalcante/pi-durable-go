# Plan 0008: Bounded Session memory

| | |
|---|---|
| Status | Done (M1–M5, and revisions apart from their documents) |
| Compatibility target | Pi Durable **v1.5.2** (commit `b4afdddf44e059c67ccf5097e5317d913521b470`) |
| Upstream | `src/session/cached-storage.ts`, `src/session/conversation-tails.ts`, `src/session/session.ts` (document cache), `src/harness/context.ts` (context ranges), the `Storage` interface in `src/types.ts` |
| Supersedes | Plan 0005 §2 "Bounded memory" |

## 1. Upstream behavior

Upstream's `Storage` is record-level: a Session never loads everything. It reads records on demand and keeps:

- **Tasks**: every live (non-terminal) task until it ends; the ended tasks a waiting task waits on, until it moved on; and an LRU of 256 ended tasks (`CachedStorage`).
- **Submissions**: an LRU of 256 written submissions; open ones are read as needed.
- **Conversations**: an LRU of 4096.
- **Entries**: the newest entry ID of each recently used conversation (LRU 4096) and its newest head marker (LRU 256) (`ConversationTails`), and per active conversation a context range from its newest head marker on (`ContextRange`, expiring when idle). Everything else is read from storage when asked for.
- **Documents**: loaded on first use and evicted once idle for `documentRetentionMs`, unless an observer is attached or held changes pin them.

A normal agent run makes no storage reads. Memory is bounded by live work and the caches, not by history. Upstream's JSONL storage keeps its own full in-memory copy (a `MemoryStorage`); only a record-level backend such as SQLite bounds the process.

## 2. Go today

The Session holds the whole `State`: every conversation, entry, task, submission, and document, including revision histories, and copies its table maps on each commit. `Storage.Load` reads everything; JSONL storage holds a second full copy.

## 3. Design

- **`RecordStorage`**, an optional interface over `DeltaStorage`: `LoadResident` loads the header and the resident records; `Task`, `Submission`, `SubmissionByRequest`, `Entry`, and scans read the rest. Memory, SQLite, and JSONL storage implement it; third-party storages without it keep today's whole-State behavior.
- **Resident State**: the Session's `State` holds the resident records only. A record leaves it at the publication of the commit that made it cold, never through a delta, so storage keeps it. Reads by ID fall back to an LRU cache, then to storage, on the line.
- **No whole-State writes in bounded mode**: a store that needs an in-memory upgrade, or a storage without `RecordStorage`, opens in full mode.
- **Whole-State APIs**: `Snapshot` and the public `Session.Watch` overlay the resident records on `Storage.Load`, which is costly in bounded mode; views, scans, and observers are the bounded way to read.

## 4. Milestones

Each milestone keeps `make check` green and is pushed on its own.

- **M1. Record reads.** `RecordStorage` and its implementations (memory, SQLite with indexes, JSONL from its own copy), with conformance tests. No Session change.
- **M2. Resident tasks and submissions.** Live tasks and open submissions stay; terminal tasks and settled submissions leave at publication into an LRU of 256 each, and the ended tasks a live task waits on are kept until it moves on. Reads by ID fall back to the cache and storage; request-ID deduplication uses `SubmissionByRequest`. Loops over tasks or submissions see live work only, which is what they need.
- **M3. Resident entries.** Each conversation keeps its context range (from its newest head marker) and its tail; transcript reads, views, forks, and as-of reads page older entries from storage.
- **M4. Documents.** Idle documents leave after a retention; observed and held ones stay. Rewindable revisions are stored apart from the current value.
- **M5. Conversations.** An LRU of conversation records.

## 5. Implementation record
- **M1**: `RecordStorage` (record_storage.go) with `LoadResident` (header, live tasks, open submissions), lookups of conversations, entries, tasks, submissions (also by request ID), and documents, and scans by ID (`ScanCursor`, `ScanPage`; entries follow fork lineage; documents by scope and key). `StateRecords` serves them from a State kept whole in memory, for `MemoryStorage` and JSONL storage; SQLite queries `durable_records` with JSON expressions and partial expression indexes, and reads a legacy snapshot until its first write converts it. `TestRecordStoragesReadAlike` checks all three against `StateRecords`.
- **M2**: over a `RecordStorage` (not read-only, not just upgraded) the Session keeps live tasks, the ended tasks a live task's `WaitOn` names, and queued or placed submissions (cold.go). Every other task and submission is evicted from a commit's State just before it is published, so no delta removes it and no reader sees a map change, into an LRU of 256 (`coldRecords`). `State.task`, `State.submission`, and `State.submissionByRequest` fall back to it and to storage, with a read error failing the Session; `Tx.Task` and `Tx.Submission` return detached copies of ended records; `Harness.Task`, `WaitTask`, `TaskRuntime.Outcomes`, tool round results, `Submission` handles, and request-ID deduplication use them; `Tx.ScanTasks` merges stored ended tasks. `Snapshot` and `Session.Watch` add the stored ended records to a whole-State copy. The test-only storage check compares the resident records; `crashRecordStorage` lets crash tests run bounded. The Session still loads the whole State at open (M3 switches to `LoadResident`).
- **M3**: entries (entries.go). Each conversation's context range starts at the bound of its newest head marker (a reset itself, a compaction's cut, which context derivation looks up), or 0; `coldRecords.keepEntries` recomputes the starts a published commit changed (head markers appended, conversations created) and evicts each conversation's own entries below the smallest start of itself and the conversations whose lineage passes through it. `walkBack` walks a lineage newest first and continues in storage (`ScanEntries`, a page at a time) where the State stops; `entriesFromState` reads the whole lineage, `rangeEntries` the context range (current context derivation and `messagesWith`), and window views walk back only as far as they need. `Tx.Entry` returns a detached copy of an evicted entry (entries never change once appended, as upstream's); `Snapshot` adds stored entries. Opening loads only what the Session keeps (`loadResidentState`): `LoadResident`, every conversation, the documents of the session, each conversation, and each live task, and each conversation's context range; a store an older version wrote is loaded whole and upgraded in full mode.
- **M3 (retention)**: context ranges follow upstream's `contextRetentionMs` (`Settings.ContextRetentionMs`, default ten minutes): opening loads no entries; a published commit that touches a conversation loads its range (`coldRecords.load`, into the head State too when that is later) and notes the time; a conversation idle (not busy) for the retention is unloaded, by the next publication or by a sweep timer that replaces the published State with a copy without its entries. The retention is read off the line, since `SettingsFunc` is host code.
- **M4**: conversation documents follow their conversation (`coldRecords.load` reads them in with the range; `evictEntries` drops those of unloaded conversations, except held ones, whose changes exist only in memory). Each published State carries the loaded set (`State.loaded`), so readers know whether its documents are all there. `State.document` falls back to a cache of 256 and storage; `State.conversationDocuments` merges storage's for an unloaded conversation (views, windows, forks); `mutStoredDocument` loads a document into the candidate before it changes, keeping its stored revisions; retiring a document the State does not hold removes it through the delta (`State.removedDocuments`); `syncInboxes` also visits the conversations whose submissions a commit changed. Session and live tasks' documents stay; opening loads only those. Upstream evicts each document after its own idle time; Go's follow their conversation. Rewindable revisions still live in their document record.
- **M4 (fix)**: a conversation loaded at one publication is read into every State admitted after it too (the rest of the writer's batch and the pending commits), except the documents those commits retired, so each published State that marks a conversation loaded holds all of its documents.
- **Review fixes**: a cold read of a mutable record (document, conversation) caches its result only if no invalidation happened while it read storage; a bounded `Snapshot` waits, publication by publication, until storage is not ahead, and a public `Session.Watch` skips a publication made while it is.
- **M5**: conversation records (`coldRecords.evictConversations`): the root, loaded and busy conversations, those of live tasks and open submissions, and those live tasks own stay; others leave at publication (or by the sweep) into an LRU of 4096, upstream's count. `State.conversation` falls back to it and storage; `Tx.Conversation` loads a record into the candidate before it changes (so `Append`, `PutDocument`, and `Configure` reach any conversation); `ConversationsByOwner`, `Tx.ScanConversations`, and `Harness.Usage` merge storage's; opening loads only the conversations in use.

## 6. Differences from upstream

- Upstream bounds conversation records by an LRU count alone; Go keeps the ones in use and caches the others.
- **Revisions apart** (State version 4): a rewindable document's revisions are records of their own (`State.Revisions`, keyed `docKey@<20-digit sequence>`), so a write stores one revision, not the whole history, and a retirement writes a `Retired` revision: as-of reads (`ReadDocumentAsOf`, asOf forks) find a retired document as it was before, as upstream's `snapshotAsOf` finds an incarnation alive at a point. `revisionAt` reads the State's own revisions and `RecordStorage.DocumentRevision` (SQLite: one key-range query); a bounded Session never keeps revisions in memory. Opening a version-3 store moves the revisions out of their documents.
- **Pre-image overlay**: while a group commit is stored but not all published, storage is ahead of the published State. Each commit carries the values, before it, of the records it changed that the Session did not keep (`preImages`: the stored documents and conversation records its transaction loaded, absent for documents it created); the writer sets them as an overlay before storing a group (the earliest commit's value of each record winning), narrows it as each commit publishes, and clears it after. Cold reads of documents and conversation records, and the storage part of a conversation's documents, read through it, so no read sees a value a commit wrote before it is published; upstream makes such reads wait for the commit's publication instead.
- **Retired documents in asOf forks**: revisions record their document's fork policy, and `RecordStorage.DocumentRevisions` returns the newest revision per document under a key prefix, so an asOf fork also copies documents alive at the fork entry and retired since, as upstream forks the incarnations alive at the point.
- **Document retention** (`Settings.DocumentRetentionMs`, upstream's `documentRetentionMs`, default ten minutes): a document leaves memory once unwritten and unloaded for the retention, unless held or observed (`ObserveDocument`, `WatchDocument`, or a view or observer of its conversation pins it, as upstream keeps observed documents); a loaded conversation that lost one is marked incomplete in the published loaded set, so listings merge storage's documents for it until it loads again. Conversations still load and drop all their documents with their context ranges.
- **Review fixes** (`pipeline-review-2`):
  - A cold read checks the pre-image overlay again after its storage read, so a group stored meanwhile is never seen before it publishes.
  - A conversation created by an unpublished commit has an absent pre-image.
  - Unloading a conversation keeps its observed documents.
  - Close stops the held-deadline and expiry timers.
  - The concern that a held value is stored without a pre-image first looked unreachable: `holdable` requires the document in the head State, and eviction skipped held keys. Since held documents travel with the commits that read them, a commit can take one out of the held set before it is stored. Eviction at an earlier commit's publication could then drop the document, and reads found the older stored value after the newer one was shown. A test reproduced this.
  - Fix: eviction at a publication now keeps every document whose newest value is not published: held ones, and those the later commits of the batch and the queued commits change (`unpublishedDocumentsLocked`).
