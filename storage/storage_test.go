package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

func testState(seq uint64) *durable.State {
	return &durable.State{Version: 1, Sequence: seq, NextID: 1, Conversations: map[durable.ConversationID]*durable.ConversationRecord{}, Entries: map[durable.EntryID]*durable.Entry{}, Tasks: map[durable.TaskID]*durable.TaskRecord{}, Submissions: map[durable.SubmissionID]*durable.SubmissionRecord{}, Documents: map[string]*durable.DocumentRecord{}}
}
func TestJSONLReopenTornTailAndConflict(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := OpenJSONL(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Commit(ctx, 0, testState(1)); err != nil {
		t.Fatal(err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "snapshots.jsonl")
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("{\"incomplete\":")
	_ = f.Close()
	st, err = OpenJSONL(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := st.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Sequence != 1 {
		t.Fatalf("sequence %d", loaded.Sequence)
	}
	if err = st.Commit(ctx, 0, testState(1)); err == nil {
		t.Fatal("expected sequence conflict")
	} else if !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	_ = st.Close()
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 || b[len(b)-1] != '\n' {
		t.Fatal("torn tail was not truncated")
	}
}
func TestJSONLRejectsCompleteCorruption(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "snapshots.jsonl"), []byte("not-json\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJSONL(context.Background(), dir); err == nil {
		t.Fatal("expected complete-line corruption")
	}
}

func TestSQLiteReopenAndSequenceConflict(t *testing.T) {
	ctx := context.Background()
	file := filepath.Join(t.TempDir(), "session.sqlite")
	st, err := OpenSQLite(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Commit(ctx, 0, testState(1)); err != nil {
		t.Fatal(err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = OpenSQLite(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	loaded, err := st.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Sequence != 1 {
		t.Fatalf("sequence %d", loaded.Sequence)
	}
	if err = st.Commit(ctx, 0, testState(1)); err == nil || !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("wanted conflict, got %v", err)
	}
}
