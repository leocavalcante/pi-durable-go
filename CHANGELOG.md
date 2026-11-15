# Changelog

All notable changes to pi-durable-go, the experimental Go port of Pi Durable.
Commits follow [Conventional Commits](https://www.conventionalcommits.org/), and
versions follow semantic versioning: before v1, a minor version may change the API.

## v0.1.0 (2026-11-15)

The first release. It ports upstream Pi Durable's model and harness with v1.5.2
as the compatibility target, and needs Go 1.24 or newer.
[COMPATIBILITY.md](COMPATIBILITY.md) lists what differs, and
[docs/upstream-cross-checks.md](docs/upstream-cross-checks.md) says how the port
is compared with upstream's own code.

### Highlights

- **Durable sessions.** Conversations, entries, tasks, submissions, and documents
  (rewindable or latest, forked as of an entry or current) are committed through one
  owning Session. Group commits, deferred progress commits, and bounded memory over
  record storage are supported. Any storage error fails the Session. Every
  conversation and fork starts with upstream's built-in documents: `pi.live`,
  `pi.inbox`, `pi.usage`, `pi.provider`, and `pi.agent`. Any entry can start a new
  context with a head, as upstream's do, entries name the task that appended them, and
  an entry may carry several model messages (`Entry.Messages`).
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
    reasoning details, grammar-constrained tools, and usage priced with a configured
    `Cost` as pi-ai prices it, with OpenRouter's billed costs. `OpenAIModel(provider, id)`
    configures it from pi-ai's catalog of 728 openai-completions models, as pi-ai
    defines them.
  - `Usage` carries every counter and cost part of pi-ai's usage.
- **Code mode.** The `codemode` package ports upstream's `codemode` tool and
  prompt section: model-written JavaScript calls the conversation's tools as
  nested calls and survives a restart by deterministic replay from a journal.
  `codemode.QuickJSSandbox` runs pi-codemode's QuickJS VM in Go through wazero,
  with no Node; `codemode.NodeSandbox` runs pi-codemode itself from the installed
  Pi; and `model.Pi` serves scripts' `models.*` from Pi's catalog.
- **Coding tools.** `read`, `write`, `edit`, `bash`, `powershell`, and `tool_search`,
  with upstream's schemas, outputs, and notes. Paths resolve as upstream resolves
  them, and `LocalEnv` writes files in place, reports file errors with Node's messages,
  and names temporary files as upstream's Node environment does.
- **Execution environment.** `LocalEnv` ports the file system of upstream's
  `NodeExecutionEnv`: metadata, listings, directory, text-line, and binary readers with
  line scans, appends, truncation, flushing, renames, canonical paths, directories, and
  removal, with upstream's `FileError` codes and Node's messages; `Watch`, upstream's
  snapshot watcher (inotify on Linux, polling elsewhere); and `ExecWith` and
  `ExecArgvWith`, upstream's `exec` with its options, spill, and errors. The bash and
  powershell tools take upstream's `commandPrefix` and `prepare`.
- **Cross-checks.** `make crosscheck` runs upstream's TypeScript sources on the same
  inputs as the Go code:
  - 118 conversation scenarios replayed through upstream's Harness, 10 of them
    with code mode, 8 with the coding tools on files, and 3 with tool search;
  - checks of argument coercion and validation, JSON parsing, partial arguments, the
    read and edit tools and the coding tools' errors, compaction, and OpenAI requests
    and responses, those of every catalog model included;
  - the execution environment's file system and commands against upstream's
    `NodeExecutionEnv`;
  - code mode's declarations, source parsing, prompt section, result formatting, and
    `models.*` checks, and scripts run in the Go VM and in pi-codemode's own.

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
- `feat(codemode)!`: `ToolRuntime.ExecuteTool` passes arguments that are not an
  object to the nested call's validation instead of failing with `ErrInvalid`.
- `feat(entries)!`: entries have heads (`Entry.Head`, `HeadSelf`) and task
  attribution (`Entry.ByTask`), as upstream's. A new `pi.compaction` entry's data is
  `{reason}` with the first kept entry as its head, instead of `{summary, cut, text}`.
- `fix(tools)!`: a `pi.tool-result` entry's data is `{"diagnostics": [...]}`, as
  upstream's, and the result's details are on its message (`Message.Details`); it
  held the whole result before.
- `feat(tools)!`: `NewLocalEnvWith(cwd, LocalEnvOptions{...})` configures a local
  environment's shell, base variables, and watching; it replaces
  `LocalEnv.SetWatchOptions`.
- `fix(tools)!`: progress commits are paced as upstream's: the first output after an
  idle period commits at once, an interval of 0 no longer commits synchronously in
  `ToolRuntime.Output`, and `ToolRuntime.Details` is a paced progress commit that
  returns once it is published.
- `fix(tools)!`: running output is kept in the window of the tool's output limits and
  sanitized, as upstream chord's `OutputBuffer` keeps it, rather than as the trailing
  50 KiB, and `bash` and `powershell` return no content: the Harness shows their
  retained output, as upstream's does.
- `feat(model)!`: model availability and `model.OpenAI` settings:
  - `OpenAI.StrictMode` replaces `NoStrictMode`;
  - an agent without a model fails runs with `no_model`;
  - `OpenAI.ThinkingFormat` moved to `OpenAI.Compat.ThinkingFormat`.
