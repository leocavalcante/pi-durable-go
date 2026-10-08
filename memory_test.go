package durable

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStorageDetachedAndCAS(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryStorage()
	next := &State{Version: 1, Sequence: 1, NextID: 1, Conversations: map[ConversationID]*ConversationRecord{}, Entries: map[EntryID]*Entry{}, Tasks: map[TaskID]*TaskRecord{}, Submissions: map[SubmissionID]*SubmissionRecord{}, Documents: map[string]*DocumentRecord{"x": {Kind: "original"}}}
	if err := m.Commit(ctx, 0, next); err != nil {
		t.Fatal(err)
	}
	next.Documents["x"].Kind = "caller mutation"
	loaded, err := m.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Documents["x"].Kind != "original" {
		t.Fatal("backend retained caller-owned pointer")
	}
	loaded.Documents["x"].Kind = "read mutation"
	again, _ := m.Load(ctx)
	if again.Documents["x"].Kind != "original" {
		t.Fatal("Load returned backend-owned pointer")
	}
	err = m.Commit(ctx, 0, next)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}
