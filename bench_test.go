package durable

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// historyHarness opens a Harness whose root conversation holds n entries of
// about 1 KiB each.
func historyHarness(b *testing.B, n int) (*Harness, ConversationID) {
	b.Helper()
	// Measure the Session, not the test-only committed-state check.
	verify := verifyCommittedState
	verifyCommittedState = false
	b.Cleanup(func() { verifyCommittedState = verify })
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = h.Close(context.Background()) })
	root, err := h.Root(ctx, ConversationOptions{})
	if err != nil {
		b.Fatal(err)
	}
	data, _ := json.Marshal(strings.Repeat("x", 1024))
	for done := 0; done < n; done += 500 {
		if err := h.Commit(ctx, func(tx *Tx) error {
			for i := 0; i < 500 && done+i < n; i++ {
				if _, err := tx.Append(root.ID(), Entry{Kind: "app.note", Data: data}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	return h, root.ID()
}

func BenchmarkCommitWithHistory(b *testing.B) {
	h, root := historyHarness(b, 5000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.Commit(ctx, func(tx *Tx) error {
			_, err := tx.Append(root, Entry{Kind: "app.note", Data: json.RawMessage(`1`)})
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInternalReadWithHistory(b *testing.B) {
	h, _ := historyHarness(b, 5000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := h.read(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
