.PHONY: check fmt vet test race crosscheck

check: fmt vet test race

fmt:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))" || { echo 'Run gofmt on the listed Go files'; gofmt -l $$(find . -name '*.go' -not -path './vendor/*'); exit 1; }

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

# Cross-checks with upstream's own code, run from the pinned release's
# TypeScript sources (docs/upstream-cross-checks.md). Not part of check: it
# needs Node 22.6+, the installed Pi of the pinned version (or
# PI_DURABLE_PI_MODULES), and git access to the upstream repository (or an
# existing checkout in PI_DURABLE_UPSTREAM).
crosscheck:
	go vet -tags crosscheck ./...
	PI_DURABLE_CROSSCHECK_UPSTREAM="$$(./scripts/crosscheck-setup.sh)" go test -tags crosscheck -count=1 -run Crosscheck ./...
