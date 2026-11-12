package durable

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

// Every test runs with the committed-state check (plan 0006): a write that
// bypasses copy-on-write, or a reader that mutates a shared State, panics.
func TestMain(m *testing.M) {
	verifyCommittedState = true
	os.Exit(m.Run())
}

// withDelta returns the stored State with delta applied, detached, for a
// test storage that sees every write as a whole-State Commit.
func (m *MemoryStorage) withDelta(delta *StateDelta) *State {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := &State{}
	if m.state != nil {
		var err error
		if next, err = cloneState(m.state); err != nil {
			panic(err)
		}
	}
	detached, err := delta.Clone()
	if err != nil {
		panic(err)
	}
	next.Sequence = delta.Sequence - 1 // a conflicting sequence is the wrapper's to reject
	if err := detached.ApplyTo(next); err != nil {
		panic(err)
	}
	return next
}

// Test storages that wrap MemoryStorage and override Commit see deltas
// through that Commit, so their fault injection covers every write.

func (s *closeWriteBackStorage) CommitDelta(ctx context.Context, seq uint64, delta *StateDelta) error {
	return s.Commit(ctx, seq, s.MemoryStorage.withDelta(delta))
}

func (s *faultWriteStorage) CommitDelta(ctx context.Context, seq uint64, delta *StateDelta) error {
	return s.Commit(ctx, seq, s.MemoryStorage.withDelta(delta))
}

func (s *lifecycleStorage) CommitDelta(ctx context.Context, seq uint64, delta *StateDelta) error {
	return s.Commit(ctx, seq, s.MemoryStorage.withDelta(delta))
}

func (s *blockedCommitStorage) CommitDelta(ctx context.Context, seq uint64, delta *StateDelta) error {
	return s.Commit(ctx, seq, s.MemoryStorage.withDelta(delta))
}

func (f *failingStorage) CommitDelta(ctx context.Context, seq uint64, delta *StateDelta) error {
	return f.Commit(ctx, seq, nil)
}

func (s *cancelBookkeepingStorage) CommitDelta(ctx context.Context, seq uint64, delta *StateDelta) error {
	return s.Commit(ctx, seq, s.MemoryStorage.withDelta(delta))
}

func (s *taskBlockedStorageSpy) CommitDelta(ctx context.Context, seq uint64, delta *StateDelta) error {
	return s.Commit(ctx, seq, s.MemoryStorage.withDelta(delta))
}

// storedStateJSON shows the stored State to the committed-state check.
func (m *MemoryStorage) storedStateJSON() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return nil
	}
	data, err := json.Marshal(m.state)
	if err != nil {
		panic(err)
	}
	return data
}

// promptText is the system prompt a request's messages fold into.
func promptText(messages []Message) string {
	folded := CollapseSystemMessages(messages)
	if len(folded) == 0 || folded[0].Role != "system" {
		return ""
	}
	return SystemText(folded[0])
}

// withoutSystem drops system messages, for tests about the conversation.
func withoutSystem(messages []Message) []Message {
	var out []Message
	for _, m := range messages {
		if m.Role != "system" {
			out = append(out, m)
		}
	}
	return out
}
