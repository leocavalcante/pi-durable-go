package durable

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadOnlyHarnessReadsWithoutRecoveringOrWriting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend := NewMemoryStorage()
	hosts := newCrashHosts(t, backend, HarnessOptions{Model: echoModel{}})
	h := hosts.open(nil)
	root := testRoot(t, h)
	definition := TaskDefinition{Kind: "test.read-only", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error { <-ctx.Done(); return ctx.Err() }}
	installTasks(t, h.registry, "viewer", definition)
	id := createTaskForTest(t, h, definition)
	h.Resume()
	waitForTask(t, h, id, func(task TaskRecord) bool { return task.Status == "running" })
	hosts.crash(h)
	before, _ := backend.Load(ctx)
	wakes := 0
	viewer := hosts.openWith(HarnessOptions{Model: echoModel{}, ReadOnly: true, OnWake: func(Wake) { wakes++ }}, nil)
	if task, err := viewer.Task(ctx, id); err != nil || task.Status != "running" || task.Restarts != 0 {
		t.Fatalf("a read-only open recovered the task: %+v error=%v", task, err)
	}
	if err := viewer.Commit(ctx, func(tx *Tx) error { _, err := tx.Conversation(root.ID()); return err }); err != nil {
		t.Fatalf("a commit without writes was refused: %v", err)
	}
	if err := viewer.Commit(ctx, func(tx *Tx) error { tx.state.mutConversation(root.ID()).Agent.Instructions = str("x"); return nil }); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("write error=%v", err)
	}
	viewerRoot, err := viewer.Conversation(ctx, root.ID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := viewerRoot.Submit(ctx, SubmissionRequest{Content: "hi"}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("submit error=%v", err)
	}
	viewer.Resume()
	if w := viewer.Wake(); w.Active || w.At != nil || viewer.Err() != nil {
		t.Fatalf("wake=%+v err=%v", w, viewer.Err())
	}
	if err := viewer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	after, _ := backend.Load(ctx)
	if after.Sequence != before.Sequence || wakes != 0 {
		t.Fatalf("a read-only Harness wrote: %d -> %d, wakes=%d", before.Sequence, after.Sequence, wakes)
	}
	h = hosts.open(nil)
	if task, err := h.Task(ctx, id); err != nil || task.Status != "pending" || task.Restarts != 1 {
		t.Fatalf("a later writer did not count the crash: %+v error=%v", task, err)
	}
}
