# Upstream compatibility

Reference reviewed: [Pi Durable `main`](https://github.com/earendil-works/pi/tree/main/packages/durable), package v1.3.0 at commit `4954d760504c11df9b2cbdc040346107b6a8153d` (2026-10-15).

This is a semantic Go port of the central runtime, not a line-by-line translation or an upstream-conformance claim. Upstream's TypeScript APIs and Chord protocol are not interchangeable with this package. The Go implementation covers selected contracts through the v1.3.0 reference; it does not include the full API or conformance suite.

## Implemented foundation

- Serialized commits across transcripts, submissions, tasks, and JSON documents.
- Memory, SQLite, and append-only JSONL persistence.
- Detached snapshots and commit-before-publication observation.
- Fatal/poisoned Sessions after uncertain storage writes; rejected transactions roll back. Fatal writes end observers and waits, signal task invocations, and automatically close storage after admitted work drains. `Err`, `Done`, and `WaitClosed` expose the first cause and shutdown completion, including close failures.
- ID-based conversations, root creation, explicit reconfiguration, transcript forks, and per-conversation provider session IDs.
- Input/write submissions, including custom non-`pi.*` input-entry kinds and data, request-ID admission deduplication, waiting, and queued-input withdrawal. Display-only writes settle immediately during a run unless a reset is queued.
- Model generation and tool tasks with persisted intent and replay-safety policies.
- Custom task checkpoints, timers, waiting, ownership, cancellation, and background work. `abandonOnRestart` marks old live work on scheduling, not inspection; reasons cascade through ordinary ownership while respecting background boundaries. `OwnedTasks` and invocation-bound `AbortOwned` expose direct task children across attempts/restarts.
- Versioned documents with conversation/session/task scope; conversation `latest`/`rewindable` history and `initial`/`current`/`asOf` forks.
- Ordered extension selections, same-name replacement, tool wrappers, and hook interfaces.
- Latest-snapshot conversation/document observation, resets, and explicit durable compaction. Task start/end timestamps and per-attempt model/tool response durations are recorded.
- Local coding tools, a pure-Go OpenAI-compatible streaming adapter, and an optional installed Pi SDK adapter for shared authentication and provider transports.
- Assistant-native replay data persisted across tool rounds/restarts, excluded from compaction summary input.

The test suite is an independent Go suite. It does not run upstream's TypeScript conformance suite.

## Deliberate differences and remaining work

### Storage and state

Go backends persist a versioned whole-State JSON snapshot per commit. Upstream uses indexed table records and Chord document bases/deltas. Neither JSONL nor SQLite files are compatible between projects. Snapshot commits copy all retained history; memory and storage costs grow with history. The Go `RejectedError` contract also remains: any backend write can opt out of poisoning by certifying it was not applied. Upstream v1.2 instead makes storage failures fatal to the Session except for `StorageRequestError` invalid-request failures. Implementing indexed/delta storage and performance benchmarks is future work.

Go transaction methods reject access after callback completion, and committed values are detached from escaped pointers. Go cannot revoke an already-returned pointer like a JavaScript proxy. Do not retain transaction references. Upstream's table read-after-write restriction is not reproduced.

The Go observer coalesces to the latest full snapshot rather than buffering up to 100 exact Chord frames. There is no Chord operation stream, RPC/service/facet layer, coding-agent event compatibility layer, or offline multi-writer protocol. Views expose application documents as well as built-in state. Go exposes Session shutdown through `Done()`/`WaitClosed(ctx)` rather than upstream's `closed` promise. `SessionFailedError` wraps the first failure and matches `ErrPoisoned`; `SessionEnd` distinguishes clean/failed shutdown after backend closure. Per-watch terminal results (`stopped`, `cancelled`, `session_failed`, etc.) are not exposed: legacy watch APIs close their channels, and hosts inspect `Harness.Err()` for Session failure. There is no `onReport` callback or general internal-bookkeeping failure hook.

Document families, incarnation-specific observation, retirement/recreation history, and configurable document checkpoint policies are not implemented. `Configure` replaces a complete Agent rather than applying upstream's incremental selection-patch API; `modelTools` and deferred tool-selection updates are also absent.

### Agent execution

The pure-Go `model.OpenAI` adapter supports text and tools over OpenAI-compatible Chat Completions with an explicit API key. The optional `model.Pi` adapter delegates to an installed Pi coding-agent SDK (tested baseline **1.5.2**, requiring `ModelRuntime` and Node.js 22.19+). This reuses Pi credential resolution, OAuth refresh/locking, its bundled/configured model catalog, provider conversions, thinking levels, session IDs, and usage/cost reports. Login/logout remain in Pi. This integration is separate from the Durable runtime reference; it is not a native Go port of pi-ai or a guarantee for every SDK/provider version.

The Pi bridge starts one Node process per request, loads no AgentSession/project resources/extensions, does not refresh remote catalogs, and loses process-local caches between requests. Provider-specific native assistant blocks/signatures are persisted for replay, without importing Pi session files. The Go boundary still does not expose multimodal content, image/classifier APIs, virtual models, deferred model handles, or positional prompt/tool updates. See [model/README.md](model/README.md) for setup, cancellation/credential-write limits, and local-only SDK authentication tests.

The v1.2+ tool APIs are also absent: tagged model/nested tool-task inputs, nested tool calls, structured tool output, tool caller restrictions, model-tool selection, deferred tools/search, and codemode. Task restart abandonment and direct owned-task APIs are implemented. Task outcomes remain Go's `completed`/`failed`/`aborted` subset: explicit aborts of missing/incompatible definitions still wait for code rather than yielding upstream's `orphaned` receipt. Definition handover, migration-failure reporting/caching, comprehensive wait-cycle validation, and invocation-bound conversation handles are not implemented.

Explicit compaction is checkpointed, but automatic token-threshold/background compaction and provider-overflow recovery are not implemented. `Open` reinstalls the built-in compaction definition on every reopen. Compaction requests do not pass through generation hooks. Summaries and resets never delete original entries.

Local environments provide a filesystem and shell, not a VM/container boundary. There is no Cloudflare Durable Object adapter, remote environment protocol, bounded binary/directory reader API, filesystem watcher API, or PowerShell-specific tool. Local shell execution requires `/bin/sh` and is unsupported on Windows; JSONL currently requires Unix advisory locks. Reads load the entire file before bounding displayed output. Atomic file replacement does not preserve hard-link alias semantics. Shell execution uses the platform-specific implementation documented by the tools package.

Streaming progress is committed directly rather than using upstream's default 100 ms throttle. Prompt/tool choices are journaled, but provider-native positional system/tool updates and their caching behavior are not reproduced. The Pi adapter forwards session IDs for best-effort provider affinity, without retaining process-local continuation caches. Usage is best-effort from adapter/tool reports; interrupted streams without usage receipts cannot yield exact billing totals. Custom model adapters and tools must cooperate with context cancellation. No distributed scheduler/lease/fencing implementation is provided.

### Release status

Experimental. API/storage migrations can change as the port evolves. Do not assume upstream version numbers imply Go API stability. No production readiness, benchmark equivalence, exact provider billing totals, or exhaustive crash/power-loss proof is claimed.

## Upstream changes not ported

The reference's v1.2.0 release added nested tools, structured tool results, deferred tools/codemode, restart-abandoned tasks, stricter Session failure reporting, and new environment capabilities. v1.3.0 added SQLite table namespacing, coordinated Cloudflare Durable Object storage, and fixes/performance improvements to compaction, image encoding, document retention, and scheduler indexes. These features and implementation changes are not represented by this Go port unless listed above; the Go test suite remains independent and does not establish upstream conformance.

## Provenance

The [upstream README](https://github.com/earendil-works/pi/blob/main/packages/durable/README.md), [normative specification](https://github.com/earendil-works/pi/blob/main/packages/durable/docs/spec.md), implementation, and runnable examples informed this port. The exact reference commit is recorded in [NOTICE](NOTICE). Both projects use the MIT License. This repository is independently maintained.
