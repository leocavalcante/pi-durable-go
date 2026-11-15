# Development

How the repository is built, tested, checked against upstream, and released.
[CONTRIBUTING.md](../CONTRIBUTING.md) has the ground rules.

## Everyday commands

```sh
make check       # gofmt, go vet, the tests, and the race detector
make crosscheck  # the comparisons with upstream's own code (needs Node and Pi)
go run ./examples/recovery -crash && go run ./examples/recovery
```

`make check` needs only Go 1.25 or newer. Tests use fake models, `httptest`,
and temporary directories; none needs credentials or a network.

## Tests

- Tests live next to the code, in the package they test. The root package's
  tests use helpers such as `taskTestHarness`, `toolExtension`, and fake models
  that script their answers.
- `storagetest.Run(t, open)` is the storage conformance suite; memory, JSONL,
  and SQLite run it, and so should any new storage.
- Tests that need Node or an installed Pi skip without their environment
  variables: `PI_DURABLE_TEST_PI_PACKAGE_DIR` (the installed
  `pi-coding-agent` package) runs the `model.Pi` and Node sandbox tests.
- Upstream's own test suites are ported where they apply, under names that
  say so, such as `TestNodeEnv*` for `env-node.test.ts`.

## Cross-checks with upstream

[upstream-cross-checks.md](upstream-cross-checks.md) explains the method: the
same cases go through the port and through the pinned upstream release's
TypeScript, which `internal/crosscheck/oracle.mts` runs with Node, and the
answers must be equal. Whole conversations are scenarios in
`internal/crosscheck/testdata/scenarios`, which both Harnesses play.

To run them:

```sh
npm install --prefix ~/.cache/pi @earendil-works/pi-coding-agent@1.5.2
PI_DURABLE_PI_MODULES=~/.cache/pi/node_modules make crosscheck
```

`scripts/crosscheck-setup.sh` clones upstream at the pinned tag into
`.crosscheck/` and links Pi's packages into it. Cross-check tests are built
with the `crosscheck` tag and named `TestCrosscheck*`. Without an upstream
checkout or Node they skip; `PI_DURABLE_CROSSCHECK_REQUIRED=1`, which CI sets,
makes them fail instead.

Comparisons that matter without Node too are recorded:
`PI_DURABLE_CROSSCHECK_RECORD=1` writes upstream's answers to `testdata`, and
plain tests, which `make check` runs, hold the port to them.

## Continuous integration

| Workflow | Runs | What it does |
|---|---|---|
| CI | pushes, pull requests | gofmt, `go mod tidy`, vet, staticcheck; tests on Linux and macOS with the minimum and the newest Go; the race detector; Windows, as an experiment |
| Cross-check | pushes, pull requests, weekly | `make crosscheck` against upstream, and the tests that need an installed Pi |
| Security | pushes, pull requests, daily | `govulncheck` with the newest Go |
| Upstream drift | weekly | opens an issue when upstream releases past the compatibility target |
| Release | version tags | verifies the tag, publishes the GitHub release, and announces it to the Go module proxy |

CI builds with `GOTOOLCHAIN=local`, so a dependency that needs a newer Go than
`go.mod` says fails instead of quietly raising the minimum. Dependabot opens
weekly updates for Go modules and actions.

## The model catalog

`model/catalog/openai-completions.json` is pi-ai's catalog of the models its
openai-completions adapter serves, generated from an installed pi-ai:

```sh
node scripts/gen-openai-catalog.mjs [pi-ai package directory]
```

`TestCrosscheckOpenAICatalog` compares every catalog model's requests with
pi-ai's.

## A new upstream release

The Upstream drift workflow opens an issue with the checklist: read upstream's
changes, port them with tests and cross-check cases, move the compatibility
target (`scripts/crosscheck-setup.sh`, the README, COMPATIBILITY.md, NOTICE,
and AGENTS.md), and run the cross-checks against the new release.

## Releases

1. Move the *Unreleased* section of `CHANGELOG.md` under
   `## vX.Y.Z (YYYY-MM-DD)`.
2. Commit it as `chore(release): vX.Y.Z` and push.
3. Tag and push: `git tag -a vX.Y.Z -m 'pi-durable-go vX.Y.Z' && git push origin vX.Y.Z`.

The Release workflow runs the tests on the tag, publishes the GitHub release
with the changelog section as its notes, and announces the version to the Go
module proxy, which pkg.go.dev reads. Versions follow semantic versioning;
before v1, a minor version may change the API.

## Design records

[plans/](plans/README.md) holds a record of each larger change: its goal,
upstream's behavior, milestones, and what was done.
