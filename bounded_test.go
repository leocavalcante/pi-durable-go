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
