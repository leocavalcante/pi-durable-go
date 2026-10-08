# Upstream compatibility

Target: [Pi Durable v1.1.0](https://github.com/earendil-works/pi/tree/v1.1.0/packages/durable), commit `abe508e1b89912adde45528136c3221eb69acdd7`.

This is a semantic Go port of the central runtime, not a line-by-line translation or an upstream-conformance claim. Upstream's TypeScript APIs and Chord protocol are not interchangeable with this package.

## Implemented foundation

- Serialized commits across transcripts, submissions, tasks, and JSON documents.
- Memory, SQLite, and append-only JSONL persistence.
- Detached snapshots and commit-before-publication observation.
- Fatal/poisoned Sessions after uncertain storage writes; rejected transactions roll back.
- ID-based conversations, root creation, explicit reconfiguration, transcript forks, and per-conversation provider session IDs.
- Input/write submissions, request-ID admission deduplication, waiting, and queued-input withdrawal.
- Model generation and tool tasks with persisted intent and replay-safety policies.
- Custom task checkpoints, timers, waiting, ownership, cancellation, and background work.
- Versioned documents with conversation/session/task scope; conversation `latest`/`rewindable` history and `initial`/`current`/`asOf` forks.
- Ordered extension selections, same-name replacement, tool wrappers, and hook interfaces.
- Latest-snapshot conversation/document observation, resets, and explicit durable compaction.
- Local coding tools and an OpenAI-compatible streaming model adapter.

The test suite is an independent Go suite. It does not run upstream's TypeScript conformance suite.

## Deliberate differences and remaining work

### Storage and state

Go backends persist a versioned whole-State JSON snapshot per commit. Upstream uses indexed table records and Chord document bases/deltas. Neither JSONL nor SQLite files are compatible between projects. Snapshot commits copy all retained history; memory and storage costs grow with history. Implementing indexed/delta storage and performance benchmarks is future work.

Go transaction methods reject access after callback completion, and committed values are detached from escaped pointers. Go cannot revoke an already-returned pointer like a JavaScript proxy. Do not retain transaction references. Upstream's table read-after-write restriction is not reproduced.

The Go observer coalesces to the latest full snapshot rather than buffering up to 100 exact Chord frames. There is no Chord operation stream, RPC/service/facet layer, coding-agent event compatibility layer, or offline multi-writer protocol. Views expose application documents as well as built-in state.

Document families, incarnation-specific observation, retirement/recreation history, and configurable document checkpoint policies are not implemented. `Configure` replaces a complete Agent rather than applying upstream's incremental selection-patch API.

### Agent execution

Only the `durable.Model` boundary is portable across providers. The bundled adapter supports text and tools over OpenAI-compatible Chat Completions. It does not provide pi-ai's catalog, OAuth, model discovery, Responses/Anthropic/Google conversions, images, classifiers, deferred model handles, or provider-specific cache affinity.

Explicit compaction is checkpointed, but automatic token-threshold/background compaction and provider-overflow recovery are not implemented. `Open` reinstalls the built-in compaction definition on every reopen. Compaction requests do not pass through generation hooks. Summaries and resets never delete original entries.

Local environments provide a filesystem and shell, not a VM/container boundary. There is no Cloudflare Durable Object adapter, remote environment protocol, bounded binary/directory reader API, filesystem watcher API, or PowerShell-specific tool. Local shell execution requires `/bin/sh` and is unsupported on Windows; JSONL currently requires Unix advisory locks. Reads load the entire file before bounding displayed output. Atomic file replacement does not preserve hard-link alias semantics. Shell execution uses the platform-specific implementation documented by the tools package.

Streaming progress is committed directly rather than using upstream's default 100 ms throttle. Prompt/tool choices are journaled, but provider-native positional system/tool updates and their caching behavior are not reproduced. Usage is best-effort from adapter/tool reports; interrupted streams without usage receipts cannot yield exact billing totals. Custom model adapters and tools must cooperate with context cancellation. No distributed scheduler/lease/fencing implementation is provided.

### Release status

Experimental. API/storage migrations can change as the port evolves. Do not assume upstream version numbers imply Go API stability. No production readiness, benchmark equivalence, exact provider billing totals, or exhaustive crash/power-loss proof is claimed.

## Provenance

The [upstream README](https://github.com/earendil-works/pi/blob/v1.1.0/packages/durable/README.md), [normative specification](https://github.com/earendil-works/pi/blob/v1.1.0/packages/durable/docs/spec.md), implementation, and runnable examples informed this port. Both projects use the MIT License. This repository is independently maintained.
