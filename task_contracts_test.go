package durable

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackgroundTaskSurvivesOrdinaryConversationAbort(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sleeping := make(chan struct{}, 1)
	def := TaskDefinition{Kind: "test.background", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		wake := time.Now().Add(time.Hour)
		err := r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "waiting", WakeAt: &wake}, nil })
		sleeping <- struct{}{}
		return err
	}}
	if err := h.registry.Install(Extension{Name: "background", Tasks: []TaskDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	var id TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		id, err = tx.CreateTask(c.id, def, nil, TaskOptions{Background: true})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	select {
	case <-sleeping:
	case <-ctx.Done():
		t.Fatal("background task not running")
	}
	if err := c.Abort(ctx, false); err != nil {
		t.Fatal(err)
	}
	task, err := h.Task(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.AbortRequested || task.Status == "terminal" {
		t.Fatal("ordinary abort reached background work")
	}
	if err := c.Abort(ctx, true); err != nil {
		t.Fatal(err)
	}
	task, err = h.Task(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.Outcome.Status != "aborted" {
		t.Fatalf("%+v", task)
	}
}
func TestTaskMigrationBeforeRun(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old := TaskDefinition{Kind: "test.migration", Version: 1, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"old":true}`), nil }, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		return r.Complete(ctx, Outcome{Status: "completed"})
	}}
	var id TaskID
	if err := h.Commit(ctx, func(tx *Tx) error { var err error; id, err = tx.CreateTask(c.id, old, nil, TaskOptions{}); return err }); err != nil {
		t.Fatal(err)
	}
	newer := old
	newer.Version = 2
	newer.Migrate = func(version int, data json.RawMessage) (json.RawMessage, error) {
		if version != 1 {
			t.Errorf("version %d", version)
		}
		return json.RawMessage(`{"new":true}`), nil
	}
	newer.Run = func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		if task.Version != 2 || string(task.Checkpoint) != `{"new":true}` {
			t.Errorf("unmigrated task %+v", task)
		}
		return r.Complete(ctx, Outcome{Status: "completed"})
	}
	if err := h.registry.Install(Extension{Name: "migration", Tasks: []TaskDefinition{newer}}); err != nil {
		t.Fatal(err)
	}
	task, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.Version != 2 {
		t.Fatal("migration not persisted")
	}
}
func TestFailFastAbortsRemainingChildrenThenResumesParent(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var compensated atomic.Int32
	failing := TaskDefinition{Kind: "test.fail", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		return r.Complete(ctx, Outcome{Status: "failed", Error: "declined"})
	}}
	other := TaskDefinition{Kind: "test.other", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		wake := time.Now().Add(time.Hour)
		return r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "waiting", WakeAt: &wake}, nil })
	}, Abort: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		compensated.Add(1)
		return r.Complete(ctx, Outcome{Status: "aborted"})
	}}
	parent := TaskDefinition{Kind: "test.failfast", Version: 1, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"phase":"start"}`), nil }, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		if string(task.Checkpoint) == `{"phase":"start"}` {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				a, err := tx.CreateTask(c.id, failing, nil, TaskOptions{Owner: Owner{Task: r.ID}})
				if err != nil {
					return TaskTransition{}, err
				}
				b, err := tx.CreateTask(c.id, other, nil, TaskOptions{Owner: Owner{Task: r.ID}})
				if err != nil {
					return TaskTransition{}, err
				}
				return TaskTransition{Status: "waiting", WaitOn: []TaskID{a, b}, WaitPolicy: "failFast", Checkpoint: json.RawMessage(`{"phase":"decide"}`)}, nil
			})
		}
		if compensated.Load() != 1 {
			t.Error("parent resumed before child compensation")
		}
		return r.Complete(ctx, Outcome{Status: "completed"})
	}}
	if err := h.registry.Install(Extension{Name: "failfast", Tasks: []TaskDefinition{parent, failing, other}}); err != nil {
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
	task, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.Outcome.Status != "completed" || compensated.Load() != 1 {
		t.Fatalf("parent %+v compensations %d", task, compensated.Load())
	}
}
