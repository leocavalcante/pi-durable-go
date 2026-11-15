# Retries and compaction

Model requests fail and contexts fill up. A Harness retries transient failures with backoff, and compacts long conversations into summaries, as upstream Pi Durable does.

## Model retries

`Settings.Retry` is Pi Durable's retry policy. Its `RetrySettings` pointer fields merge one by one over the default: enabled, 3 retries after the first attempt, a 2,000 ms base delay that doubles per attempt, and a 60,000 ms cap per delay. A nil field keeps the default; an explicit `false` or `0` is honored, and the numbers follow JavaScript's (`math.Inf(1)` retries without bound).

A generation retries a failed model attempt only when the error is transient and not a context overflow. `durable.IsRetryableModelError` and `durable.IsContextOverflowError` port pi-ai v1.5.2's classifiers; errors implementing `Retryable()` or `ContextOverflow()`, as both bundled adapters' sanitized errors do, answer for themselves. The failed attempt's partial is kept as an aborted entry, and the generation sleeps in a `retry` phase until the backoff ends. Afterwards it prepares the request again, so agent, section, and settings changes made meanwhile apply, without counting another turn. Compaction summaries retry under the same policy and classification, without the overflow check, resending their pinned request.

## Compaction

`Conversation.Compact` and automatic compaction follow upstream's policy (`Settings.Compaction`, overridden per conversation by `Agent.Compaction` and per model by `ModelOverrides`; defaults: enabled, 16,384 reserve tokens, 20,000 recent tokens kept, 32,768 background tokens). A compaction selects an old prefix of the model context that leaves `KeepRecentTokens` after it, summarizes it with upstream's prompt, and places the summary, which later context starts with. A run or compaction fails `no_model` when the agent has no model or the Harness's model does not serve it (`ModelLookup`, which `model.Pi` answers from Pi's catalog). When the model reports its context window (`ContextWindowModel`), preparing a request above `window − ReserveTokens` first runs a blocking compaction, and above the background threshold starts a background one. A context-overflow error compacts and prepares again instead of failing, unless compaction is disabled or nothing can be compacted. Small conversations may have nothing to compact; such a compaction completes without a summary.
