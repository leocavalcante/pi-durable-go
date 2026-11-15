## What and why

<!-- What this changes, and why. Link the issue it closes. -->

## Upstream

<!-- For behavior ported from upstream Pi Durable: the upstream source, tests, or spec it follows. -->

## Checklist

- [ ] `make check` passes (gofmt, vet, tests, race detector).
- [ ] Tests cover the change, without paid model calls or real external effects.
- [ ] `make crosscheck` passes when this ports upstream behavior, and new cases cover it.
- [ ] Docs follow: `CHANGELOG.md` (Unreleased), and `COMPATIBILITY.md` for anything that differs from upstream.
- [ ] The title follows [Conventional Commits](https://www.conventionalcommits.org/), and each commit is signed off (`git commit -s`).
