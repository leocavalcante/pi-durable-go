# Plan 0008: Bounded Session memory

| | |
|---|---|
| Status | In progress |
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
