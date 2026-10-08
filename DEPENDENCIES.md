# Dependency licenses

The project itself is MIT licensed, preserving Pi/Pi Durable's original notice.
Dependencies are separate works under their own licenses. Exact versions and
checksums are recorded in `go.mod` and `go.sum`.

## Direct dependencies

| Module | License | Purpose |
|---|---|---|
| [`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite) | BSD-3-Clause | Pure-Go SQLite driver; SQLite itself is public domain |
| [`github.com/santhosh-tekuri/jsonschema/v6`](https://github.com/santhosh-tekuri/jsonschema) | Apache-2.0 | Local JSON Schema validation for tool arguments |

## Runtime transitive dependencies

- MIT: `github.com/dustin/go-humanize`, `github.com/mattn/go-isatty`, `github.com/ncruces/go-strftime`.
- BSD-3-Clause: `github.com/google/uuid`, `github.com/remyoudompheng/bigfft`, `golang.org/x/exp`, `golang.org/x/sys`, `golang.org/x/text`, `modernc.org/libc`, `modernc.org/mathutil`, `modernc.org/memory`.

Additional test/build dependencies may appear in the module graph. Use
`go list -m all` for the exact graph and consult each downloaded module's
`LICENSE`/`COPYING` and additional notices before redistributing a binary.
The MIT license on this repository does not relicense dependencies or eliminate
their notice obligations. No dependencies are vendored and no binaries are
published in this initial repository import.
