# pi-durable-go

Experimental Go port of Pi Durable v1.1.0, pinned upstream commit
`abe508e1b89912adde45528136c3221eb69acdd7`.

- Use Go 1.24+ and idiomatic context.Context cancellation.
- Preserve MIT licensing and upstream copyright notices.
- No paid model calls or real external effects in tests. Use fake models and httptest.
- Visible state must be committed before publication. Uncertain storage errors poison the Session.
- External effects require idempotency or reconciliation; never promise exactly-once effects.
- One owning Session per storage. Do not claim multi-writer or upstream file-format compatibility.
- Run gofmt, go vet ./..., go test ./..., and go test -race ./... before publication.
- Document missing upstream features explicitly. Do not call this a complete/conformant port without evidence.
- Do not commit, push, create releases, or trigger pipelines from a delegated task.
