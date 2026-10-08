# Contributing

Changes should include focused tests and documentation for any changed contract.
Keep Go APIs idiomatic and preserve upstream MIT attribution.

Run `make check` before submitting. Tests must not call paid providers, use real
credentials, publish messages, or perform external side effects. Use fake models,
`httptest`, and temporary directories. Add regression coverage for restart,
cancellation, ownership, and uncertain storage writes when relevant.

Keep generated binaries, runtime sessions, databases, and local credentials out
of commits. Dependency changes must update both `go.mod` and `go.sum`.

Use Conventional Commit titles and a DCO `Signed-off-by` trailer. By signing,
you certify the [Developer Certificate of Origin](https://developercertificate.org/).

Describe upstream parity accurately. An independent test is not evidence that
the entire upstream conformance suite passes.
