package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestParentCompletingWaitsForOwnedChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := testHarness(t)
	c := testRoot(t, h)
	started := make(chan struct{}, 1)
	var childID TaskID
	child := TaskDefinition{Kind: "test.child", Version: 1, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"phase":"sleep"}`), nil }, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		if string(task.Checkpoint) == `{"phase":"sleep"}` {
			wake := time.Now().Add(60 * time.Millisecond)
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
				return TaskTransition{Status: "waiting", Checkpoint: json.RawMessage(`{"phase":"finish"}`), WakeAt: &wake}, nil
			})
		}
		return r.Complete(ctx, Outcome{Status: "completed"})
	}}
	parent := TaskDefinition{Kind: "test.parent", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			var err error
			childID, err = tx.CreateTask(task.Conversation, child, nil, TaskOptions{Owner: Owner{Task: r.ID}})
			if err != nil {
				return TaskTransition{}, err
			}
			return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: "completed"}}, nil
		})
		started <- struct{}{}
		return err
	}}
	if err := h.registry.Install(Extension{Name: "ownership", Tasks: []TaskDefinition{parent, child}}); err != nil {
		t.Fatal(err)
	}
	var id TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		id, err = tx.CreateTask(c.id, parent, nil, TaskOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("parent did not run")
	}
	record, err := h.Task(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != "completing" {
		t.Fatalf("parent settled before child: %+v", record)
	}
	record, err = h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	childRecord, err := h.Task(ctx, childID)
	if err != nil {
		t.Fatal(err)
	}
	if childRecord.Status != "terminal" || record.EndedAt.Before(*childRecord.EndedAt) {
		t.Fatal("parent ended before child")
	}
}
func TestAbortRunsOwnedChildrenBeforeParent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := testHarness(t)
	c := testRoot(t, h)
	var mu sync.Mutex
	var order []string
	ready := make(chan struct{}, 1)
	child := TaskDefinition{Kind: "test.abort-child", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		ready <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}, Abort: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		mu.Lock()
		order = append(order, "child")
		mu.Unlock()
		return r.Complete(ctx, Outcome{Status: "aborted"})
	}}
	parent := TaskDefinition{Kind: "test.abort-parent", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			id, err := tx.CreateTask(task.Conversation, child, nil, TaskOptions{Owner: Owner{Task: r.ID}})
			return TaskTransition{Status: "waiting", WaitOn: []TaskID{id}, WaitPolicy: "allSettled"}, err
		})
	}, Abort: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		mu.Lock()
		order = append(order, "parent")
		mu.Unlock()
		return r.Complete(ctx, Outcome{Status: "aborted"})
	}}
	if err := h.registry.Install(Extension{Name: "abort-test", Tasks: []TaskDefinition{parent, child}}); err != nil {
		t.Fatal(err)
	}
	var id TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		id, err = tx.CreateTask(c.id, parent, nil, TaskOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("child did not start")
	}
	if err := h.AbortTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	record, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if record.Outcome.Status != "aborted" {
		t.Fatalf("%+v", record)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "child" || order[1] != "parent" {
		t.Fatalf("abort order %v", order)
	}
}
func TestTaskDocumentRetiresOnlyAtTerminal(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	doc := DocumentDefinition{Kind: "app.output", Version: 1, Scope: "task", Initial: func() any { return map[string]string{"text": "kept while live"} }}
	def := TaskDefinition{Kind: "test.task-doc", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if _, err := tx.Document(doc, uint64(task.ID)); err != nil {
				return TaskTransition{}, err
			}
			return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: "completed"}}, nil
		})
	}}
	if err := h.registry.Install(Extension{Name: "task-doc", Tasks: []TaskDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	var id TaskID
	if err := h.Commit(ctx, func(tx *Tx) error { var err error; id, err = tx.CreateTask(c.id, def, nil, TaskOptions{}); return err }); err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Documents[documentKey(doc, uint64(id))] != nil {
		t.Fatal("terminal task document retained")
	}
}
