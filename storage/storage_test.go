package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func TestJSONLCompactsToItsLatestSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := OpenJSONL(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", 64*1024)
	state := &durable.State{Version: 3, NextID: 1}
	for i := 0; i < 40; i++ {
		next := *state
		next.Sequence = state.Sequence + 1
		next.Documents = map[string]*durable.DocumentRecord{"d": {Kind: "app.big", Version: 1, Scope: "harness", Value: json.RawMessage(strconv.Quote(big + strconv.Itoa(i)))}}
		if err := st.Commit(ctx, state.Sequence, &next); err != nil {
			t.Fatal(err)
		}
		state = &next
	}
	info, err := os.Stat(filepath.Join(dir, "snapshots.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > compactMinBytes+70*1024 {
		t.Fatalf("log was never compacted: %d bytes", info.Size())
	}
	if _, err := OpenJSONL(ctx, dir); err == nil {
		t.Fatal("a second owner opened the compacted log")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenJSONL(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	loaded, err := reopened.Load(ctx)
	if err != nil || loaded.Sequence != 40 || !strings.Contains(string(loaded.Documents["d"].Value), big+"39") {
		t.Fatalf("loaded sequence=%v error=%v", loaded.Sequence, err)
	}
}
