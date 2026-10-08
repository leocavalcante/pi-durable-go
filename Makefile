.PHONY: check fmt vet test race

check: fmt vet test race

fmt:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))" || { echo 'Run gofmt on the listed Go files'; gofmt -l $$(find . -name '*.go' -not -path './vendor/*'); exit 1; }

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...
