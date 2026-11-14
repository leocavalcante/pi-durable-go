# Changelog

All notable changes to pi-durable-go, the experimental Go port of Pi Durable.
Commits follow [Conventional Commits](https://www.conventionalcommits.org/);
versions will follow semantic versioning once a release is tagged.

## Unreleased

The first release candidate. It ports upstream Pi Durable's model and harness
with v1.5.2 as the compatibility target. [COMPATIBILITY.md](COMPATIBILITY.md)
lists what differs, and [docs/upstream-cross-checks.md](docs/upstream-cross-checks.md)
says how the port is compared with upstream's own code.

### Highlights

- **Durable sessions.** Conversations, entries, tasks, submissions, and documents
  (rewindable or latest, forked as of an entry or current) are committed through one
  owning Session. Group commits, deferred progress commits, and bounded memory over
  record storage are supported. Any storage error fails the Session. Every
  conversation and fork starts with upstream's built-in documents: `pi.live`,
  `pi.inbox`, `pi.usage`, `pi.provider`, and `pi.agent`.
- **Agent harness.** Ported from upstream:
  - generation runs with retries, steering, follow-ups, turn limits, and failed-run
    policies;
  - tool rounds, with nested calls, structured output, output bounds, and replay
    safety;
  - compaction: manual, threshold, background, and overflow;
  - hooks, prompt sections, and `afterRun` tasks;
  - restart accounting.
- **Storage.** Memory, JSONL, and SQLite. The `storagetest` package checks a storage
  against the conformance suite.
- **Models.**
  - `model.Pi` reaches every pi-ai provider through an installed Pi SDK, using Pi's
    authentication.
  - `model.OpenAI` speaks Chat Completions natively. It follows pi-ai's
    openai-completions adapter: request fields, thinking formats, the full
    `model.compat` (`OpenAICompat`), headers, message transforms, OpenRouter
    reasoning details, and grammar-constrained tools.
- **Code mode.** The `codemode` package ports upstream's `codemode` tool and
  prompt section: model-written JavaScript calls the conversation's tools as
  nested calls and survives a restart by deterministic replay from a journal.
  `codemode.NodeSandbox` runs pi-codemode's QuickJS sandbox from the installed Pi.
- **Coding tools.** `read`, `write`, `edit`, `bash`, `powershell`, and `tool_search`,
  with upstream's schemas, outputs, and notes. Paths resolve as upstream resolves
  them, and `LocalEnv` writes files in place as upstream's Node environment does.
- **Cross-checks.** `make crosscheck` runs upstream's TypeScript sources on the same
  inputs as the Go code:
  - 90 conversation scenarios replayed through upstream's Harness;
  - checks of argument coercion and validation, JSON parsing, partial arguments, the
    read and edit tools, compaction, and OpenAI requests and responses.

### Breaking changes since the first commits on `main`

No release carried the earlier behavior. These matter only to code that followed `main`:

- `feat(session)!`: storage failures are final, and `ErrPoisoned` uses upstream's
  wording.
- `feat(prompt)!`:
  - sections render from a `PromptInput`, and `ErrOmitSection` omits a section;
  - `Agent.Instructions` is a `*string` that renders last.
- `feat(documents)!`: document revisions are stored apart from their documents
  (state version 4). `RecordStorage` gained `DocumentRevision` and
  `DocumentRevisions`.
- `feat(agent)!`: the agent is a rewindable `pi.agent` document, not transcript
  entries. New conversations start with no entries.
- `feat(model)!`: model availability and `model.OpenAI` settings:
  - `OpenAI.StrictMode` replaces `NoStrictMode`;
  - an agent without a model fails runs with `no_model`;
  - `OpenAI.ThinkingFormat` moved to `OpenAI.Compat.ThinkingFormat`.
