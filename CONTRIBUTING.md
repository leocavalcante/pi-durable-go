# Contributing to pi-durable-go

Thanks for helping. Bug reports, parity fixes, docs, examples, storage backends,
and model adapters are all welcome. Please read the
[Code of Conduct](CODE_OF_CONDUCT.md) first.

## Ground rules

- **Port, don't invent.** pi-durable-go follows upstream
  [Pi Durable](https://github.com/earendil-works/pi/tree/v1.5.2/packages/durable)'s
  behavior. A change to behavior should cite upstream's source, tests, or spec,
  and, where it can, add a cross-check case that compares the two. Deliberate
  differences belong in [COMPATIBILITY.md](COMPATIBILITY.md).
- **No real effects in tests.** Tests must not call paid model providers, use
  real credentials, publish messages, or touch anything outside temporary
  directories. Use fake models, `httptest`, and `t.TempDir()`.
- **Keep the attribution.** The project is MIT, as Pi is; keep upstream's
  copyright notice, and update [NOTICE](NOTICE) and
  [DEPENDENCIES.md](DEPENDENCIES.md) for anything embedded or added.
- **Describe parity accurately.** A passing test is evidence for what it covers,
  not for the whole upstream conformance suite.

## Development

You need Go 1.26 or newer. Everything else is optional.

```sh
git clone https://github.com/leocavalcante/pi-durable-go
cd pi-durable-go
make check       # gofmt, go vet, the tests, and the race detector
```

The cross-checks compare the port with upstream's own TypeScript on the same
inputs. They need Node.js 22.19 or newer and Pi's packages of the pinned
version, which npm installs:

```sh
npm install --prefix ~/.cache/pi @earendil-works/pi-coding-agent@1.5.2
PI_DURABLE_PI_MODULES=~/.cache/pi/node_modules make crosscheck
```

With Pi itself installed at that version, `make crosscheck` finds its packages
on its own. [docs/development.md](docs/development.md) covers the layout, the
test helpers, recording upstream's answers, and CI.

## Project layout

| Path | What it holds |
|---|---|
| `.` (package `durable`) | The Harness: sessions, conversations, tasks, documents, generation, tools, compaction |
| `storage/`, `storagetest/` | Memory, JSONL, and SQLite storage, and the conformance suite for your own |
| `model/` | `model.OpenAI` (pure Go, with pi-ai's catalog) and `model.Pi` (any pi-ai provider through an installed Pi) |
| `tools/` | The coding tools and `LocalEnv`, the local execution environment |
| `codemode/` | Code mode: model-written JavaScript in a QuickJS sandbox |
| `internal/crosscheck/` | The oracle that runs upstream's code, and the shared cases |
| `examples/` | Runnable programs |
| `docs/` | Guides, the cross-check method, and the design records in `docs/plans/` |

## Tests

- Prove a fix: a new test should fail without the change and pass with it.
- Cover restart, cancellation, ownership, and uncertain storage writes where
  they matter; the Harness's promises are about crashes.
- Run a suspected flaky test many times (`go test -count=20 -run Name`) and
  under the race detector.
- Tests that need Node or an installed Pi skip without their environment
  variables, so `make check` never needs them.

## Commits and pull requests

- Commit titles follow [Conventional Commits](https://www.conventionalcommits.org/):
  `feat(tools): ...`, `fix(model): ...`, `docs: ...`. Mark breaking changes with
  `!` and a `BREAKING CHANGE:` footer.
- Sign off every commit (`git commit -s`): by signing off you certify the
  [Developer Certificate of Origin](https://developercertificate.org/).
- Keep a pull request to one change, with its tests and docs. Add a line to
  `CHANGELOG.md` under *Unreleased*.
- CI runs the checks, the race detector, the cross-checks against upstream, and
  `govulncheck` on every pull request.

Dependency changes update both `go.mod` and `go.sum`. The minimum Go version is
deliberate: CI builds on it with `GOTOOLCHAIN=local`, so raise it only on
purpose.

## Releases

Maintainers release by moving the *Unreleased* section of `CHANGELOG.md` under
a `## vX.Y.Z (YYYY-MM-DD)` heading, committing `chore(release): vX.Y.Z`, and
pushing an annotated tag `vX.Y.Z`. The Release workflow verifies the tag,
publishes the GitHub release with that section as its notes, and announces the
version to the Go module proxy.
