package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func residentCounts(h *Harness) (tasks, submissions, coldTasks int) {
	h.session.mu.Lock()
	defer h.session.mu.Unlock()
	if h.session.cold != nil {
		h.session.cold.mu.Lock()
		coldTasks = h.session.cold.tasks.len()
		h.session.cold.mu.Unlock()
	}
	return len(h.session.state.Tasks), len(h.session.state.Submissions), coldTasks
}

func TestBoundedSessionKeepsLiveTasksOnly(t *testing.T) {
	// Hundreds of commits: the test-only whole-State check would dominate.
	verify := verifyCommittedState
	verifyCommittedState = false
	t.Cleanup(func() { verifyCommittedState = verify })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	registry := NewRegistry()
	definition := TaskDefinition{Kind: "test.done", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted, Result: task.Input})
	}}
	installTasks(t, registry, "done", definition)
	h := taskTestHarness(t, HarnessOptions{Registry: registry})
	root := testRoot(t, h)
	const count = 300
	var ids []TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		for i := 0; i < count; i++ {
			id, err := tx.CreateTask(root.ID(), definition, json.RawMessage(fmt.Sprintf("%d", i)), TaskOptions{})
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	for _, id := range ids {
		if record, err := h.WaitTask(ctx, id); err != nil || record.Status != "terminal" {
			t.Fatalf("task %d=%+v error=%v", id, record, err)
		}
	}
	// Ended tasks left memory: the State keeps none, the cache at most 256.
	tasks, _, cold := residentCounts(h)
	if tasks != 0 || cold > coldTaskCapacity {
		t.Fatalf("resident tasks=%d cached=%d", tasks, cold)
	}
	// Every read by ID still finds them, the oldest from storage.
	first, err := h.Task(ctx, ids[0])
	if err != nil || first.Outcome == nil || string(first.Outcome.Result) != "0" {
		t.Fatalf("first task=%+v error=%v", first, err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil || len(state.Tasks) != count {
		t.Fatalf("snapshot has %d tasks, error=%v", len(state.Tasks), err)
	}
	found := 0
	if err := h.Commit(ctx, func(tx *Tx) error {
		var cursor *ScanCursor
		for {
			page, err := tx.ScanTasks(TaskQuery{Kind: "test.done", Status: "terminal"}, 64, cursor)
			if err != nil {
				return err
			}
			found += len(page.Items)
			if page.Next == nil {
				return nil
			}
			cursor = page.Next
		}
	}); err != nil {
		t.Fatal(err)
	}
	if found != count {
		t.Fatalf("scanned %d ended tasks, want %d", found, count)
	}
}

func TestBoundedSessionKeepsOpenSubmissionsOnly(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root := testRoot(t, h)
	write := func(requestID string) SubmissionID {
		t.Helper()
		sub, err := root.Submit(ctx, SubmissionRequest{Type: "write", RequestID: requestID, Entry: &Entry{Kind: "app.note", Data: json.RawMessage(`{}`)}})
		if err != nil {
			t.Fatal(err)
		}
		return sub.ID()
	}
	first := write("note-1")
	write("note-2")
	if _, submissions, _ := residentCounts(h); submissions != 0 {
		t.Fatalf("%d settled submissions kept", submissions)
	}
	// A settled submission is still found by its request ID and by ID.
	if again := write("note-1"); again != first {
		t.Fatalf("a repeated request admitted %d, want %d", again, first)
	}
	sub, err := h.Submission(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if record, err := sub.Record(ctx); err != nil || record.Status != "done" {
		t.Fatalf("record=%+v error=%v", record, err)
	}
}

func TestReadOnlySessionsKeepEverything(t *testing.T) {
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if h.session.cold == nil {
		t.Fatal("a Harness over memory storage is not bounded")
	}
	viewer, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer viewer.Close(context.Background())
	if viewer.session.cold != nil {
		t.Fatal("a read-only Harness is bounded")
	}
}

func residentEntries(h *Harness, cid ConversationID) int {
	h.session.mu.Lock()
	defer h.session.mu.Unlock()
	n := 0
	for _, e := range h.session.state.Entries {
		if e.Conversation == cid {
			n++
		}
	}
	return n
}

func TestBoundedSessionKeepsContextRangesOnly(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root := testRoot(t, h)
	old := appendNotes(t, h, root, 40)
	if _, err := root.Reset(ctx, ""); err != nil {
		t.Fatal(err)
	}
	recent := appendNotes(t, h, root, 3)
	// The reset moved the context range: the State keeps the reset and what
	// follows.
	if n := residentEntries(h, root.ID()); n > 5 {
		t.Fatalf("%d entries kept after a reset", n)
	}
	// Whole-history reads still find everything, from storage.
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 40+1+3 || entries[1].ID > old[0] {
		t.Fatalf("history has %d entries", len(entries))
	}
	page, err := root.ScanEntries(ctx, EntryQuery{Order: "ascending", MaxEntryID: old[9]}, 100, nil)
	if err != nil || len(page.Items) == 0 || page.Items[len(page.Items)-1].ID != old[9] {
		t.Fatalf("scan=%+v error=%v", page, err)
	}
	// A window larger than the range reads the older entries.
	view, stop, err := root.WatchWindow(ctx, ConversationWindow{Min: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	select {
	case first := <-view:
		if len(first.Entries) != 10 || first.Entries[9].ID != recent[2] || first.Before == 0 {
			t.Fatalf("window=%d entries, before %d", len(first.Entries), first.Before)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no window")
	}
	// The current context starts at the reset; an earlier one reads storage.
	current, err := root.ContextView(ctx, 0)
	if err != nil || current.Head == nil {
		t.Fatalf("context=%+v error=%v", current, err)
	}
	if _, err := root.ContextView(ctx, old[20]); err != nil {
		t.Fatal(err)
	}
	// A fork at an entry the State no longer keeps reads its lineage.
	fork, err := root.Fork(ctx, old[10], ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	forked, err := fork.Entries(ctx)
	if err != nil || len(forked) < 11 {
		t.Fatalf("fork history=%d error=%v", len(forked), err)
	}
	if _, err := fork.Context(ctx); err != nil {
		t.Fatal(err)
	}
}
