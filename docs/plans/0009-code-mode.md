# Plan 0009: Durable Code Mode

| | |
|---|---|
| Status | Done (M1–M6) |
| Compatibility target | Pi Durable **v1.5.2** (commit `b4afdddf44e059c67ccf5097e5317d913521b470`) |
| Upstream | `src/codemode/{codemode,describe,output,models,index}.ts`, `src/search.ts`; pi-codemode 1.5.2 (`CodemodeSandboxBase.start`, `renderToolSample`, `parseCodemodeSource`, `toCodemodeIdentifier`), installed with Pi |

## 1. Upstream behavior

`createCodemodeExtension({ sandbox, namespaces, inlineBudget, models, globals })` adds the `codemode` tool and a `codemode` prompt section. The tool runs model-written JavaScript in a QuickJS sandbox (pi-codemode) whose only capability is calling the conversation's tools (`tools.<name>(args)`) and a few globals. Each script call of a tool is a nested durable call keyed `c<VM call id>`.

A script survives a restart by deterministic replay. `CodemodeSandboxBase.start(code, { startedAt, seed, tools, globals, store, timeoutMs, onStarted, onCall, onIdle })` runs the script with a virtual clock and seeded randomness. It hands every call to the host, and receives results through `run.deliver(seq, now, id, outcome)` in `seq` order. After each drain it reports `idle(seq, issued)`.

The tool is replay-safe and keeps two documents scoped to its task:
- `pi.codemode.run` holds the start data, written once before the VM starts, the output directory, and the decided outcome.
- `pi.codemode.journal` has one entry per delivered result, in delivery order: the VM call ID, the clock, the calls issued after it was processed, and a global's outcome.

A rerun starts the VM from the start data and delivers the journal in order. Nested calls reattach by key. The rerun then continues live. It refuses with an error result when:
- the module version or the VM fingerprint changed;
- the deadline passed;
- the script diverged: other calls, other arguments, counts that differ, or an end where the live run went on.

A live call launches only once the count of calls issued before it is committed. Scripts keep JSON values with `store()`/`load()` in the conversation's rewindable `pi.codemode.store` document. The result is formatted as the coding agent does: a header with the wall time, text items, console lines, the returned value, and errors with the calls made. Text past the token budget is cut, with the full text and any images saved to temp files.

The optional `models.*` globals use pi-ai's model catalog, classifiers, and image models.

## 2. Go today

No Code Mode. The pieces it builds on exist: nested calls with keys and the nested-call family, task- and conversation-scoped documents, replay-safe tools, grammar-constrained tools, and `tools.RankDocuments`.

## 3. Design

- **Package `codemode`** ports `src/codemode` and the parts of pi-codemode the durable side uses: `ParseSource`, `SourceGrammar`, `Identifier`, `RenderToolSample`, and `SchemaToType`.
- **`Sandbox`**, a Go interface for upstream's `start()`: `Start(code, RunOptions) (Run, error)` with `OnStarted`, `OnCall`, and `OnIdle`. `Run` has `Deliver`, `Abort`, `Finished`, and `Wait`.
- **`NodeSandbox`** runs upstream's own `CodemodeSandbox` (a worker plus QuickJS WASM) in a Node process per run, over a JSON-lines bridge embedded in Go. It finds Node and the Pi installation as `model.Pi` does, through the new `internal/piinstall`. pi-codemode resolves from the coding agent's dependencies. Using the same VM, prelude, and fingerprint makes replay exactly upstream's. A pure-Go VM (wazero with quickjs-wasi's WASM) is a later option.
- **The durable call** is an event loop that mirrors upstream's single-threaded promise flow. Sandbox callbacks and finished I/O post continuations to the loop; commits and nested calls run in goroutines.
- **Root additions:**
  - `ToolRuntime.CommitChange`, a commit that leaves the task running unchanged (upstream `api.commit`);
  - `ToolRuntime.NestedCall`, the call an earlier attempt made under a key (upstream reads `NestedCallDoc` and the task's input);
  - an optional `TempDirEnv` that `tools.LocalEnv` implements (upstream `createTempDir`).
- **`models.*`** is backed by an optional `Models` interface, since Go has no pi-ai catalog. Without it, scripts get no `models` globals and the description leaves them out, as upstream's `models: false`.

## 4. Milestones

Each milestone keeps `make check` green and is pushed on its own. Tests that run real scripts need Node and an installed Pi and are gated like the installed-SDK tests. A scripted Go sandbox tests the durable logic in `make check`.

- **M1. Sandbox.** `codemode.Sandbox`, `NodeSandbox` with its bridge, and `internal/piinstall`. Gated tests: calls and delivery, idle counts, fingerprint, timeouts, aborts, `Close`, syntax errors, deterministic clock and seed.
- **M2. Pure functions.** Source parsing and grammar, identifiers, declarations (`renderToolSample`, `schemaToType`), the description and section (`describe.ts`), and result formatting (`output.ts`). Cross-checked against upstream's functions.
- **M3. The tool.** First runs:
  - start data and journal;
  - nested calls and the outcome mapping (`outcomeOf`);
  - discovery globals and custom globals;
  - `store`/`load`, details rows, and output files;
  - the decided outcome, and the extension with its section.
- **M4. Replay.** Rerun after restart: journal delivery and count checks, divergence, held calls checked against earlier attempts, fingerprint, version and deadline, and sandbox closing. Port upstream's `codemode.test.ts`.
- **M5. Models.** The `Models` interface, the internal model tools, catalog globals, the concurrency limit, and the note about images the script did not show.
- **M6. Cross-checks.** Scenarios with Code Mode against upstream's Harness and sandbox.

## 5. Implementation record

- **M1.** `codemode.Sandbox`, `Run`, and the result types. `NodeSandbox` runs pi-codemode's `CodemodeSandbox` through `codemode/bridge.mjs`, which Node imports from a `data:` URL: worker threads cannot inherit `--input-type=module`. `internal/piinstall` holds the Node and Pi discovery that `model.Pi` used. The tests need `PI_DURABLE_TEST_PI_PACKAGE_DIR`.
- **M2.** `Identifier`, `ParseSource`, `SourceGrammar`, `RenderToolSample`, `SchemaToType`, `Description`, `Section`, and result formatting. The `codemode` oracle op runs upstream's functions on `internal/crosscheck/testdata/codemode.json`. `TestCrosscheckFunctions` compares them and, with `PI_DURABLE_CROSSCHECK_RECORD=1`, records upstream's answers in `codemode/testdata/upstream.json` for `TestUpstreamAnswers` in `make check`. The JavaScript helpers that root kept privately (`JSON.parse` order, `JSON.stringify`, V8 parse messages, trim, UTF-16 length, `toFixed`) move to `internal/js`. Namespaces sort by the root collation, as `localeCompare` does. Images decode as Node's `atob` does, with its messages.
- **M3 and M4.** The tool, the extension, and replay, together.
  - `codemode.go` ports `CodemodeCall` as an event loop. Sandbox callbacks and finished commits, nested calls, and globals post their continuations to the loop, so state changes happen in upstream's order.
  - Root gains `ToolRuntime.CommitChange` (upstream `api.commit`), `ToolRuntime.NestedCall` (the call an attempt admitted under a key), and `durable.TempDirEnv`, which `tools.LocalEnv` implements.
  - `ExecuteTool` now passes arguments that are not an object to the nested call's validation, as upstream does.
  - Upstream's `codemode.test.ts` is ported to `codemode/replay_test.go`, with SQLite restarts and the installed Pi's sandbox. Its timeout test uses 3000 ms instead of 300 ms, since a Node process per run starts more slowly.
  - Not ported: the two tests that need upstream's `ControlledStorage`.
  - `codemode/fake_test.go`'s scripted sandbox tests the durable side in `make check`: first runs, restarts, divergence, fingerprint and version refusals, globals, the store, errors, and the section.
- **M5.** `models.*` over `codemode.Models`: the catalog globals, the internal `pi.codemode.models.classify` and `pi.codemode.models.generateImages` tools (`callers: tools`, hidden from scripts' listings, usage counted by the Harness), at most four model calls at once, and the note about images the script did not show. Upstream's `codemode-models.test.ts` and its output tests are ported against the installed Pi's sandbox. The `codemodeModels` oracle op checks the catalog functions, model resolution, and context checks against a copy of `models.ts` that exports them. `searchTools()` takes a null limit as the default and any positive integer, as `options?.limit ?? 8` does.
- **M6.** Ten code mode scenarios play through upstream's Harness with `createCodemodeExtension` and this Harness with `codemode.Extension`, both on pi-codemode's sandbox from the checkout's dependencies. They cover scripts, parallel calls, errors, source and option errors, the token budget, tool search, the store, the section's budget, output values with images, and invalid nested arguments. Model requests, transcripts, and settlements are equal. The root scenario runner gains a `codemode` field; the external test package installs the extension through `SetScenarioCodemode` (`export_crosscheck_test.go`). The faux usage helper now counts a tool's `constrainedSampling`, as pi-ai's `toToolDeclaration` keeps it.

- **After M6.** `model.Pi` is a `codemode.Models`. A `models` op of its bridge reads the SDK's catalog without headers and runs `ModelRuntime.classify` and `generateImages`, with error text sanitized. `Classify` and `GenerateImages` return pi-ai's result object, from which code mode reads the stop reason, error, and usage.

The Go VM is plan 0011: `codemode.QuickJSSandbox`, the default.
