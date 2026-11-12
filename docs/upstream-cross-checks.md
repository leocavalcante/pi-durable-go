# Cross-checks against upstream sources

Pure behavior of the port is checked against upstream's own code, run from the pinned sources rather than read. This file records how, what was checked, and what still differs. The checks are one-off scripts, not part of `make check`: they need Node and the upstream sources. Their findings are kept as ordinary Go tests whose expectations are upstream's outputs.

## Running upstream's TypeScript

Node 22 strips TypeScript types (`--experimental-strip-types`), so upstream modules run from `src/` as they are:

1. A sparse clone of the release tag at `/tmp/pi-v1.5.2` (`packages/durable`, `packages/ai`, `packages/chord`).
2. A `node_modules` directory (`/tmp/durable-nm`) linking the dependencies of the installed Pi 1.5.2 (`~/.pi/agent/install/releases/1.5.2/node_modules`, including `@earendil-works/pi-ai/node_modules`). It is linked as `node_modules` into `packages/durable`, `packages/ai`, and `packages/chord`.
3. A shim package for `@earendil-works/chord` whose `exports` map each subpath to its `source` entry, `src/*.ts`. Chord's `dist` is not built, and `--conditions=source` would also send installed packages to sources they do not ship.
4. Unexported functions are reached through a temporary copy of the module with `export` added, for example `prompt.ts`'s `planSections` and `planTools`.

A script imports the module (`await import("/tmp/pi-v1.5.2/packages/.../x.ts")`), runs it on a set of cases, and prints JSON. A temporary Go test runs the same cases through the port and compares.

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

## Known remaining differences

- Partial tool arguments: an unterminated number such as `1.` or `1E-` is dropped by pi-ai's parser, while Go shows its digits. This only affects what streaming shows for a moment.
- `read`: with fractional `offset` or `limit`, upstream's continuation note prints fractions. A negative `limit` selects nothing in Go, where upstream's slice counts it from the end.
- `model.OpenAI` sanitizes provider error text that pi-ai passes through, and has no provider-specific thinking formats (DeepSeek, OpenRouter, Z.ai, Qwen, Together) or model catalog.
- Schema coercion keeps no key order for schema objects. Pattern properties apply in sorted order, and RE2 replaces JavaScript regular expressions.
