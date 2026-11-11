package durable

import (
	"os"
	"testing"
)

// Every test runs with the committed-state check (plan 0006): a write that
// bypasses copy-on-write, or a reader that mutates a shared State, panics.
func TestMain(m *testing.M) {
	verifyCommittedState = true
	os.Exit(m.Run())
}
