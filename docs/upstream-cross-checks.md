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
| Tool argument coercion and validation | pi-schema `parse()` with `coerce: true`, pi-ai `validateToolArguments` | Ported; 147 cases equal, including error wording and ECMAScript patterns |
| Edit tool | `edit-diff.ts`: fuzzy normalization, edits, diff, patch, errors | Equal (13 cases) |
| Read tool | `read.ts` over a Node environment: 37 files and ranges, fractional and negative numbers included | Ported to equal: lines taken as `Array.prototype.slice` takes them and notes printed with JavaScript's numbers |
| Tool paths | `read.ts` path resolution over a Node environment: mentions, Unicode spaces, screenshot names, curly apostrophes, decomposed accents, file URLs | Ported to equal |
| Coding tool specs | `createReadTool` and the other coding tools' schemas, output schemas, limits | Ported to equal |
| `tool_search` | `rankDocuments` | Equal (12 queries) |
| Context ordering | `orderToolResults` | Equal (6 sequences) |
| Compaction | `serializeConversation`, `estimateMessageTokens`, `selectCut`, `estimateContext` | Ported to equal |
| Prompt planning | `planSections`, `planTools` | Equal |
| Retry and overflow classification | pi-ai `isRetryableAssistantError`, `isContextOverflow` | Equal (27 messages) |
| Partial tool arguments | pi-ai `ToolCallArgumentsStream`, which its providers stream arguments through | Ported to equal (about 130 texts). The check used to read the older `parseStreamingJson`; the port now follows the streaming parser, including unterminated numbers, JavaScript number values, key order, and lone surrogates |
| Finished tool arguments | pi-ai `parseToolCallArguments` with its repair, and V8's `JSON.parse` messages it reports | Ported to equal (about 130 texts). Malformed arguments used to fail the response here; upstream reports them to the model as `invalid_arguments` |
| Size formatting | `formatSize`, `toFixed` | Ported to equal |
| `model.OpenAI` requests | pi-ai `openai-completions` `streamSimple` payload via `onPayload` | Ported to equal for messages, tools, cache key, store, and output limit |
| `model.OpenAI` reasoning | `reasoning_effort` and `clampThinkingLevel`; thinking formats detected from the URL or set in compat; thinking replayed to the same model | Ported to equal (about 140 combinations) |
| `model.OpenAI` compat | pi-ai's `model.compat` settings and provider detection, one variant each (38) | Ported to equal |
| `model.OpenAI` headers | Attribution and session-affinity headers a local server receives (9 variants) | Ported to equal |
| `model.OpenAI` message transform | pi-ai's `transformMessages`: other models' tool call IDs, failed and aborted answers, tool calls without results (8 variants) | Ported to equal |
| `model.OpenAI` mid-conversation system messages | Updates in place, tool additions in place, removals, held system messages (7 variants) | Ported to equal |
| `model.OpenAI` reasoning details | OpenRouter's `reasoning_details` read from streams and replayed (2 streams, 3 replays) | Ported to equal |
| `model.OpenAI` grammar tools | Custom tools, replayed custom calls, streamed custom input, and pi-ai's errors (7 requests, 1 stream) | Ported to equal |
| `model.OpenAI` responses | SSE parsing from a local server: usage, stop reasons, malformed and repaired tool arguments, streams ending without `[DONE]` or a finish reason | Ported to equal; provider error text stays sanitized by design |
| Tool runtime behavior | `tool.ts`: throws, retained output, structured output contract, nested keys | Ported |
| Code mode functions | pi-codemode's `toCodemodeIdentifier`, `schemaToType`, `renderToolSample`, and `parseCodemodeSource`; durable's `codemodeDescription`, `codemodeSection`, and `formatResult` (`codemode` op, `codemode.json`) | Ported to equal: 11 identifiers, 43 schemas, 4 samples, 28 sources, 2 descriptions, 6 sections, 28 results. `TestCrosscheckFunctions` records upstream's answers for `TestUpstreamAnswers` in `make check`. Code mode's replay runs on pi-codemode's own VM (`codemode.NodeSandbox`), so its scripts are not compared separately |
| Whole conversations | Upstream's `Harness` with pi-ai's faux provider, against this Harness with a scripted model: 90 scenarios (`internal/crosscheck/testdata/scenarios`) | Equal: model requests, transcripts, and settlements. They found five differences, all ported: upstream keeps the agent in a `pi.agent` document rather than transcript entries, a compaction entry carries its summary as a user message, a nested call's value depends on whether the tool ran or the Harness wrote its result, a line kept in part is not counted as dropped, and only tool results with usage enter the usage ledger |

## Scenarios

A scenario (`internal/crosscheck/testdata/scenarios/*.json`) is a script that both runtimes play:
- **model:** scripted answers (text, thinking, tool calls, stop reasons, errors);
- **tools:** scripted results, or each call's arguments echoed back;
- **steps:** submissions, waits, holds and releases of a model answer or a tool call, aborts, compactions, resets, agent changes, forks (at an entry index; later steps act on the fork until `use` switches back, and each fork's transcript is reported), conversation documents written and retired (each conversation's values are reported), `live` snapshots of `pi.live` (IDs and times replaced by counts and presence), and reopening the Harness over the same storage (JSONL upstream, memory here);
- **extensions:** declarative static sections and hook rules (`beforeRequest` appended messages, `afterResponse` error rewrites, `beforeTool` blocks or rewrites, `afterTool` replacements, `onYield` continues, `beforeCompact` summaries or declines), and an `afterRun` hook asking for a task per run that appends an entry, whose summaries are reported with their entries as transcript positions. Transcripts carry the data of entries outside the `pi.` kinds.

`internal/crosscheck/scenario.mts` plays it on upstream's Harness with pi-ai's faux provider. `TestCrosscheckScenarios` plays it on this one with a scripted model. Both report each model request's model, thinking level, messages, and offered tools, the transcript, the root's usage ledger (tokens per model and per tool, input, output, and cache counts together, since the faux provider counts the prompt as cache reads and writes), and each submission's settlement in one normalized form. Scenarios may register more faux models; the scripted model serves only those (`ModelLookup`). The scripted model reports the usage pi-ai's faux provider estimates (`fauxUsage`), which context estimates prefer over character counts. Summarization requests take their answers from a separate `summaries` queue and are reported apart, so they can run beside the conversation's own requests.

Some outcomes depend on timing in either runtime, and the scenarios avoid relying on them:
- Parallel tool calls append their results as they finish, so the comparison puts each round's results in call order, as the model sees them.
- A background compaction's summary is placed whenever it finishes. Scenarios hold its answer, and `wait: compactions` waits until none is listed.

Two representations differ and are compared as an adapter sends them:
- Upstream's requests carry tools as system-message changes, so its offered tools are pi-ai's `getCurrentTools` fold of them; this port's are `ModelRequest.Tools`.
- A tool result's text blocks are joined by newlines, as pi-ai's OpenAI completions adapter joins them, since this port keeps one text per message; its images are counted apart.

To check another flow, add a scenario file and list it in `index.json`.

## Known remaining differences

- Partial tool arguments: upstream's streaming parser copies its open containers for each snapshot within a work budget, so its preview of a very large open container (thousands of elements) can lag behind the text; every reading here is fresh.
- `model.OpenAI` sanitizes provider error text that pi-ai passes through, and has no model catalog: compat is set by hand.
- Schema coercion keeps no key order for schema objects, so pattern properties apply in sorted order.
- A schema with a recursive union whose branches all recurse, for example an `anyOf` that includes a reference to itself, is exponential to check against deep arguments. pi-schema fails such a call with a stack overflow after about 12 seconds at depth 10, and Go needs about 3 seconds at depth 10 and a minute at depth 15. Coercion has a work budget (`maxCoercionSteps`), but the JSON Schema library's own check is not bounded.
