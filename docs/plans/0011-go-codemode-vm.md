# Plan 0011: A Go code mode VM

| | |
|---|---|
| Status | Done |
| Compatibility target | pi-codemode 1.5.2 with quickjs-wasi 3.6.2, installed with Pi 1.5.2 |
| Upstream | pi-codemode `dist/runtime/{vm,execution,prelude-source}.js`; quickjs-wasi `dist/{index,wasi-shim}.js` and `quickjs.wasm` |

## 1. Why

`codemode.NodeSandbox` runs upstream's sandbox in Node, so code mode needs Node and an installed Pi. pi-codemode's VM is QuickJS-NG compiled to WebAssembly (quickjs-wasi), driven by a small amount of JavaScript. That JavaScript is the deterministic clock and seeded randomness (`vm.js`), the host side of an execution (`execution.js`), and the wrapper over QuickJS's C API (`index.js`).

The same WASM module and prelude can run from Go through wazero, a pure-Go WebAssembly runtime. That makes code mode work without Node. The fingerprint stays upstream's (quickjs-wasi and QuickJS versions, prelude hash, deterministic version, stack, heap, budget). A script started in one sandbox therefore replays in the other.

## 2. Design

- **Package `codemode/quickjs`.** It is kept apart so `codemode` users do not embed the 637 KB module unless they use it. It embeds:
  - `quickjs.wasm` from quickjs-wasi 3.6.2 (MIT);
  - `prelude.js`, pi-codemode 1.5.2's `PRELUDE_SOURCE` (MIT; its FNV-1a hash is `4dce1bde`).
- **The VM.** A wazero module per run, from one compiled module per sandbox, with:
  - `env` host functions: `host_call` for the bridge, `host_interrupt`, a timezone offset of 0, and inert module and rejection hooks;
  - the WASI subset `vm.js` uses: the deterministic clock, the mulberry32 random source, discarded output, and stubs.

  Values, strings (WTF-8), exceptions, and job draining follow quickjs-wasi's wrappers. Each VM runs on its own goroutine.
- **The driver.** It ports `startVm`: the prelude, the bridge, the fingerprint, the `(async (tools, console) => {…})` wrapper, `settle`, `run`, `stalled`, idle counts, `seq`-ordered delivery, and job draining with interrupt polls every 50 jobs.
- **The host.** It ports `Execution`: output, call records, pending calls, deliveries, the timeout, aborts, store writes, `during`, and sandbox errors.
- **Dependency.** `github.com/tetratelabs/wazero` v1.11.0 (Apache-2.0; v1.12 needs Go 1.25).

## 3. Tests

The `NodeSandbox` tests run against both sandboxes. With Node available, a cross-check runs the same scripts in both and compares the events, values, outputs, and fingerprints.

## 4. Implementation record

- `codemode.QuickJSSandbox` lives in `codemode` itself (`quickjs_vm.go`, `quickjs_run.go`), so the durable tests run on it in `make check`. It is the default when `Options.Sandbox` is nil. The module compiles once per process.
- The wrapper's string transfer is WTF-8 both ways. Text read from the VM turns a lone surrogate into one U+FFFD, as Go reads pi-codemode's JSON messages.
- `TestQuickJSSandboxRunsAsPiCodemodeDoes` runs 17 scripts in both sandboxes with equal events and results: tools, globals, clock and randomness, errors and stacks, syntax errors, `exit()`, recursion, rejections, stalls, the store and its limits, images, timeouts, job loops, Unicode, and spread and namespaced globals.
- The sandbox tests run against both sandboxes. The durable tests run on `QuickJSSandbox`, or on Node with `PI_DURABLE_TEST_CODEMODE_SANDBOX=node`.
- Found on the way: the durable call read `Run.Finished` live, so events the VM sent before finishing could be dropped. A script's last un-awaited call lost its details row. The call now marks the run finished after handling those events, as upstream does.
