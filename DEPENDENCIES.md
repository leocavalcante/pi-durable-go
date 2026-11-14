# Dependency licenses

The project itself is MIT licensed, preserving Pi/Pi Durable's original notice.
Dependencies are separate works under their own licenses. Exact versions and
checksums are recorded in `go.mod` and `go.sum`.

## Direct dependencies

| Module | License | Purpose |
|---|---|---|
| [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) | BSD-3-Clause | Pure-Go SQLite driver; SQLite itself is public domain |
| [`github.com/santhosh-tekuri/jsonschema/v6`](https://github.com/santhosh-tekuri/jsonschema) | Apache-2.0 | Local JSON Schema validation for tool arguments |
| [`github.com/dlclark/regexp2`](https://github.com/dlclark/regexp2) | MIT | ECMAScript regular expressions for schema patterns, as pi-schema compiles them |
| [`golang.org/x/text`](https://pkg.go.dev/golang.org/x/text) | BSD-3-Clause | Unicode normalization and segmentation for `tool_search` and edit matching; wording of schema failures pi-schema has no message for; the root collation that orders code mode namespaces as `localeCompare` does |
| [`golang.org/x/image`](https://pkg.go.dev/golang.org/x/image) | BSD-3-Clause | WebP and BMP decoding and image scaling for `tools.StdImages` |
| [`github.com/tetratelabs/wazero`](https://github.com/tetratelabs/wazero) | Apache-2.0 | The pure-Go WebAssembly runtime of `codemode.QuickJSSandbox` |

## Embedded files

`codemode` embeds `quickjs.wasm` from quickjs-wasi 3.6.2 (MIT; QuickJS-NG v0.17.0, MIT) and pi-codemode 1.5.2's prelude (MIT). [codemode/QUICKJS.md](codemode/QUICKJS.md) lists their sources, checksums, and license files.

## Optional Pi SDK runtime

`model.Pi` runs a separately installed Node.js and `@earendil-works/pi-coding-agent` (MIT; tested baseline 1.5.2), using that SDK's own pi-ai/auth/provider dependencies. These are not Go dependencies, vendored code, or bundled binaries. The bridge is original integration code using the public SDK. Install only trusted packages and preserve their licenses/notices if redistributing them. The default Go runtime and `model.OpenAI` do not require Node/npm.

`codemode.NodeSandbox` likewise runs the installed Pi's `@earendil-works/pi-codemode` (MIT) and its `quickjs-wasi` (MIT; QuickJS-NG compiled to WebAssembly) with a separately installed Node.js. They are not Go dependencies or bundled; the bridge, `codemode/bridge.mjs`, is original integration code using pi-codemode's public API.

## Runtime transitive dependencies

- MIT: `github.com/dustin/go-humanize`, `github.com/mattn/go-isatty`, `github.com/ncruces/go-strftime`.
- BSD-3-Clause: `github.com/google/uuid`, `github.com/remyoudompheng/bigfft`, `golang.org/x/exp`, `golang.org/x/sys`, `modernc.org/libc`, `modernc.org/mathutil`, `modernc.org/memory`.

Additional test/build dependencies may appear in the module graph. Use
`go list -m all` for the exact graph and consult each downloaded module's
`LICENSE`/`COPYING` and additional notices before redistributing a binary.
The MIT license on this repository does not relicense dependencies or eliminate
their notice obligations. No dependencies are vendored and no binaries are
published in this initial repository import.
