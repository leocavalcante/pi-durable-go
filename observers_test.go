package durable

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"testing"
)

// recorder records an observer's calls.
type recorder[T any] struct {
	mu     sync.Mutex
	values []T
	ends   []WatchEnd
}

func (r *recorder[T]) observer() Observer[T] {
	return Observer[T]{
		Advance: func(value T) { r.mu.Lock(); defer r.mu.Unlock(); r.values = append(r.values, value) },
		End:     func(end WatchEnd) { r.mu.Lock(); defer r.mu.Unlock(); r.ends = append(r.ends, end) },
	}
}

func (r *recorder[T]) snapshot() ([]T, []WatchEnd) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]T(nil), r.values...), append([]WatchEnd(nil), r.ends...)
}

func TestObserveDocumentAdvancesOnAdoption(t *testing.T) {
	store := NewMemoryStorage()
	h, cid := deferredHarness(t, store)
	var seen recorder[json.RawMessage]
	initial, _, err := ObserveDocument(h, progressDocument, uint64(cid), seen.observer())
	if err != nil || string(initial) != `{"n":0}` {
		t.Fatalf("initial=%s error=%v", initial, err)
	}
	if err := h.Commit(context.Background(), setProgress(cid, 1)); err != nil {
		t.Fatal(err)
	}
	// A commit that leaves the document alone does not advance it.
	if err := h.Commit(context.Background(), touchStorage); err != nil {
		t.Fatal(err)
	}
	// A deferred commit advances it before it is stored.
	if err := h.CommitWith(context.Background(), deferredWithin(math.Inf(1)), setProgress(cid, 2)); err != nil {
		t.Fatal(err)
	}
	values, ends := seen.snapshot()
	if len(values) != 2 || string(values[0]) != `{"n":1}` || string(values[1]) != `{"n":2}` || len(ends) != 0 {
		t.Fatalf("values=%s ends=%v", values, ends)
	}
	if got := storedProgress(t, store, cid); got != `{"n":1}` {
		t.Fatalf("stored=%s", got)
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, ends = seen.snapshot()
	if len(ends) != 1 || ends[0].Kind != "failed" || !errors.Is(ends[0].Err, ErrClosed) {
		t.Fatalf("ends=%+v", ends)
	}
}

func TestObserverPanicDetachesWithListenerError(t *testing.T) {
	reports := make(chan Report, 4)
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{OnReport: func(r Report) { reports <- r }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	var ends []WatchEnd
	advances := 0
	_, _, err = ObserveDocument(h, touchDocument, 0, Observer[json.RawMessage]{
		Advance: func(json.RawMessage) { advances++; panic("boom") },
		End:     func(end WatchEnd) { ends = append(ends, end) },
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := h.Commit(context.Background(), touchStorage); err != nil {
			t.Fatal(err)
		}
	}
	if advances != 1 || len(ends) != 1 || ends[0].Kind != "listener_error" {
		t.Fatalf("advances=%d ends=%+v", advances, ends)
	}
	if report := <-reports; report.Kind != ReportListenerError {
		t.Fatalf("report=%+v", report)
	}
	if h.Err() != nil {
		t.Fatalf("a listener error failed the Session: %v", h.Err())
	}
}

func TestDetachedObserverGetsNothingMore(t *testing.T) {
	h := testHarness(t)
	var seen recorder[json.RawMessage]
	_, detach, err := ObserveDocument(h, touchDocument, 0, seen.observer())
	if err != nil {
		t.Fatal(err)
	}
	detach()
	detach()
	if err := h.Commit(context.Background(), touchStorage); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if values, ends := seen.snapshot(); len(values)+len(ends) != 0 {
		t.Fatalf("after detach: values=%s ends=%v", values, ends)
	}
}

func TestTaskDocumentObserverEndsWithItsTask(t *testing.T) {
	notes := DocumentDefinition{Kind: "test.task-notes", Version: 1, Scope: "task"}
	release := make(chan struct{})
	definition := TaskDefinition{Kind: "test.noted", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		if err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, tx.PutDocument(notes, uint64(task.ID), json.RawMessage(`{"note":1}`))
		}); err != nil {
			return err
		}
		<-release
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	registry := NewRegistry()
	installTasks(t, registry, "noted", definition)
	h := taskTestHarness(t, HarnessOptions{Registry: registry})
	id := createTaskForTest(t, h, definition)
	var seen recorder[json.RawMessage]
	if _, _, err := ObserveDocument(h, notes, uint64(id), seen.observer()); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	close(release)
	if _, err := h.WaitTask(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	values, ends := seen.snapshot()
	if len(values) == 0 || string(values[0]) != `{"note":1}` || len(ends) != 1 || ends[0].Kind != "ended" {
		t.Fatalf("values=%s ends=%+v", values, ends)
	}
}

func TestObserveTaskGraphFollowsTasksAndTheRegistry(t *testing.T) {
	registry := NewRegistry()
	h := taskTestHarness(t, HarnessOptions{Registry: registry})
	definition := TaskDefinition{Kind: "test.graphed", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	var seen recorder[TaskGraph]
	initial, detach, err := h.ObserveTaskGraph(seen.observer())
	if err != nil || len(initial.Tasks) != 0 {
		t.Fatalf("initial=%+v error=%v", initial, err)
	}
	defer detach()
	// Unregistered, the new task is blocked.
	id := createTaskForTest(t, h, definition)
	values, _ := seen.snapshot()
	if len(values) == 0 || values[len(values)-1].Tasks[id].Blocked == "" {
		t.Fatalf("graph after create=%+v", values)
	}
	// Installing its definition unblocks it without a commit.
	installTasks(t, registry, "graphed", definition)
	values, _ = seen.snapshot()
	if node, ok := values[len(values)-1].Tasks[id]; ok && node.Blocked != "" {
		t.Fatalf("graph after install=%+v", values[len(values)-1])
	}
}

func TestConversationObserveFollowsTheWindow(t *testing.T) {
	h := testHarness(t)
	root, err := h.Root(context.Background(), ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var seen recorder[ConversationView]
	initial, detach, err := root.Observe(ConversationWindow{Min: 2, Max: 3}, seen.observer())
	if err != nil {
		t.Fatal(err)
	}
	defer detach()
	appendNotes(t, h, root, 3)
	values, _ := seen.snapshot()
	if len(values) == 0 {
		t.Fatalf("no advance; initial=%+v", initial)
	}
	last := values[len(values)-1]
	if n := len(last.Entries); n < 2 || n > 3 {
		t.Fatalf("window=%d entries", n)
	}
}
