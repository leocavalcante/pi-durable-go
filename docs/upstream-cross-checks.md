# Cross-checks against upstream sources

Pure behavior of the port is checked against upstream's own code, run from the pinned sources rather than read. `make crosscheck` runs them. It is not part of `make check`: it needs Node 22.6+ and the upstream sources. Fixed-expectation Go tests keep the results for `make check` too.

## Running

```sh
make crosscheck
```

The target has two steps:
1. `scripts/crosscheck-setup.sh` prepares `.crosscheck/`, which git ignores. It holds three things:
   - a sparse checkout of the pinned release tag (`PI_DURABLE_UPSTREAM_TAG`, default `v1.5.2`; `PI_DURABLE_UPSTREAM` names an existing checkout instead);
   - a `node_modules` linking the dependencies of the installed Pi of the same version (`~/.pi/agent/install/releases/<version>/node_modules`, or `PI_DURABLE_PI_MODULES`);
   - a shim for `@earendil-works/chord`, whose `dist` is not built, mapping its exports to their TypeScript sources.

   The script also writes `prompt.crosscheck.ts`, a copy of upstream's `prompt.ts` with the private `planSections` and `planTools` exported.
2. The Go tests named `TestCrosscheck*`, built with the `crosscheck` tag, send their cases to `internal/crosscheck/oracle.mts`. The oracle runs upstream's functions with Node's type stripping and returns their results, which the tests compare with the port's. Shared case sets are in `internal/crosscheck/testdata`. Without a prepared checkout (`PI_DURABLE_CROSSCHECK_UPSTREAM`), the tests skip.

For another upstream release, set `PI_DURABLE_UPSTREAM_TAG` with Pi of that version installed. The differences found are what the port has to follow.

## Checked

| Area | Upstream | Result |
| --- | --- | --- |
| Tool argument coercion and validation | pi-schema `parse()` with `coerce: true`, pi-ai `validateToolArguments` | Ported; 112 cases equal, including error wording |
| Edit tool | `edit-diff.ts`: fuzzy normalization, edits, diff, patch, errors | Equal (13 cases) |
| Read tool | `read.ts` over a Node environment: 23 files and ranges | Equal except fractional continuation notes |
| Coding tool specs | `createReadTool` and the other coding tools' schemas, output schemas, limits | Ported to equal |
| `tool_search` | `rankDocuments` | Equal (12 queries) |
| Context ordering | `orderToolResults` | Equal (6 sequences) |
| Compaction | `serializeConversation`, `estimateMessageTokens`, `selectCut`, `estimateContext` | Ported to equal |
| Prompt planning | `planSections`, `planTools` | Equal |
| Retry and overflow classification | pi-ai `isRetryableAssistantError`, `isContextOverflow` | Equal (27 messages) |
| Partial tool arguments | pi-ai `parseStreamingJson` | Equal except unterminated numbers such as `1.` |
| Size formatting | `formatSize`, `toFixed` | Ported to equal |
| `model.OpenAI` requests | pi-ai `openai-completions` `streamSimple` payload via `onPayload` | Ported to equal for messages, tools, cache key, store, and output limit |
| `model.OpenAI` reasoning | `reasoning_effort` and `clampThinkingLevel` | Ported to equal (24 combinations) |
| `model.OpenAI` responses | SSE parsing from a local server: usage, stop reasons | Ported to equal; provider error text stays sanitized by design |
| Tool runtime behavior | `tool.ts`: throws, retained output, structured output contract, nested keys | Ported |
| Whole conversations | Upstream's `Harness` with pi-ai's faux provider, against this Harness with a scripted model: 20 scenarios (`internal/crosscheck/testdata/scenarios`) | Equal: model requests, transcripts, and settlements. The first run found that upstream keeps the agent in a `pi.agent` document, not in transcript entries, which is now ported |

## Scenarios

A scenario (`internal/crosscheck/testdata/scenarios/*.json`) is a script that both runtimes play:
- **model:** scripted answers (text, thinking, tool calls, stop reasons, errors);
- **tools:** scripted results, or each call's arguments echoed back;
- **steps:** submissions, waits, holds and releases of a model answer or a tool call, and aborts.

`internal/crosscheck/scenario.mts` plays it on upstream's Harness with pi-ai's faux provider. `TestCrosscheckScenarios` plays it on this one with a scripted model. Both report the messages of each model request, the transcript, and each submission's settlement in one normalized form. Parallel tool calls append their results as they finish in either runtime, so the comparison puts each round's results in call order, as the model sees them. To check another flow, add a scenario file and list it in `index.json`.

## Known remaining differences

- Partial tool arguments: an unterminated number such as `1.` or `1E-` is dropped by pi-ai's parser, while Go shows its digits. This only affects what streaming shows for a moment.
- `read`: with fractional `offset` or `limit`, upstream's continuation note prints fractions. A negative `limit` selects nothing in Go, where upstream's slice counts it from the end.
- `model.OpenAI` sanitizes provider error text that pi-ai passes through, and has no provider-specific thinking formats (DeepSeek, OpenRouter, Z.ai, Qwen, Together) or model catalog.
- Schema coercion keeps no key order for schema objects. Pattern properties apply in sorted order, and RE2 replaces JavaScript regular expressions.
- A schema with a recursive union whose branches all recurse, for example an `anyOf` that includes a reference to itself, is exponential to check against deep arguments. pi-schema fails such a call with a stack overflow after about 12 seconds at depth 10, and Go needs about 3 seconds at depth 10 and a minute at depth 15. Coercion has a work budget (`maxCoercionSteps`), but the JSON Schema library's own check is not bounded.
