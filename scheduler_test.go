package durable

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestSchedulerRunsCustomDurableTask(t *testing.T) {
	h := testHarness(t)
	var runs atomic.Int32
	definition := TaskDefinition{Kind: "test.count", Version: 1, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"phase":"run"}`), nil }, Run: func(ctx context.Context, rt *TaskRuntime, task TaskRecord) error {
		runs.Add(1)
		return rt.Complete(ctx, Outcome{Status: "completed", Result: json.RawMessage(`{"ok":true}`)})
	}}
	if err := h.registry.Install(Extension{Name: "test", Tasks: []TaskDefinition{definition}}); err != nil {
		t.Fatal(err)
	}
	conversation := testRoot(t, h)
	var id TaskID
	err := h.Commit(context.Background(), func(tx *Tx) error {
		var e error
		id, e = tx.CreateTask(conversation.ID(), definition, json.RawMessage(`{}`), TaskOptions{})
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	record, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != "terminal" || record.Outcome == nil || record.Outcome.Status != "completed" {
		t.Fatalf("task outcome: %+v", record)
	}
	if runs.Load() != 1 {
		t.Fatalf("runs=%d", runs.Load())
	}
}

func TestTaskMemoFirstWriterWins(t *testing.T) {
	h := testHarness(t)
	conversation := testRoot(t, h)
	definition := TaskDefinition{Kind: "test.memo", Version: 1, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil }, Run: func(ctx context.Context, rt *TaskRuntime, _ TaskRecord) error {
		winner, err := rt.Memo(ctx, "winner", json.RawMessage(`{"candidate":1}`))
		if err != nil {
			return err
		}
		if string(winner) != `{"candidate":1}` {
			t.Errorf("winner=%s", winner)
		}
		return rt.Complete(ctx, Outcome{Status: "completed"})
	}}
	if err := h.registry.Install(Extension{Name: "memo", Tasks: []TaskDefinition{definition}}); err != nil {
		t.Fatal(err)
	}
	var id TaskID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var e error
		id, e = tx.CreateTask(conversation.ID(), definition, json.RawMessage(`{}`), TaskOptions{})
		return e
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.WaitTask(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryReplacePreservesPositionAndSnapshot(t *testing.T) {
	r := NewRegistry()
	if err := r.Install(Extension{Name: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Install(Extension{Name: "b"}); err != nil {
		t.Fatal(err)
	}
	old := r.Snapshot()
	if err := r.Install(Extension{Name: "a", Sections: []Section{{Name: "context"}}}); err != nil {
		t.Fatal(err)
	}
	got := r.Snapshot().Extensions()
	if got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("order: %+v", got)
	}
	if len(old.Extensions()[0].Sections) != 0 {
		t.Fatal("published snapshot mutated")
	}
}

func TestAbortTaskRunsAbortHandlerAfterCooperativeCancellation(t *testing.T) {
	h := testHarness(t)
	conversation := testRoot(t, h)
	started := make(chan struct{})
	var aborted atomic.Int32
	definition := TaskDefinition{Kind: "test.abort", Version: 1, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil }, Run: func(ctx context.Context, _ *TaskRuntime, _ TaskRecord) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, Abort: func(ctx context.Context, rt *TaskRuntime, _ TaskRecord) error {
		aborted.Add(1)
		return rt.Complete(ctx, Outcome{Status: "aborted"})
	}}
	if err := h.registry.Install(Extension{Name: "abort", Tasks: []TaskDefinition{definition}}); err != nil {
		t.Fatal(err)
	}
	var id TaskID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		id, err = tx.CreateTask(conversation.ID(), definition, json.RawMessage(`{}`), TaskOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	select {
	case <-started:
	case <-time.After(time.Second * 3):
		t.Fatal("task did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.AbortTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	record, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if record.Outcome == nil || record.Outcome.Status != "aborted" || aborted.Load() != 1 {
		t.Fatalf("abort outcome=%+v calls=%d", record.Outcome, aborted.Load())
	}
}

func TestSchedulerWaitsForOwnedChildren(t *testing.T) {
	h := testHarness(t)
	conversation := testRoot(t, h)
	child := TaskDefinition{Kind: "test.child", Version: 1, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{}`), nil }, Run: func(ctx context.Context, rt *TaskRuntime, _ TaskRecord) error {
		return rt.Complete(ctx, Outcome{Status: "completed"})
	}}
	parent := TaskDefinition{Kind: "test.parent", Version: 1, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"child":0}`), nil }, Run: func(ctx context.Context, rt *TaskRuntime, record TaskRecord) error {
		var checkpoint struct {
			Child TaskID `json:"child"`
		}
		_ = json.Unmarshal(record.Checkpoint, &checkpoint)
		if checkpoint.Child == 0 {
			var childID TaskID
			err := rt.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				var err error
				childID, err = tx.CreateTask(record.Conversation, child, json.RawMessage(`{}`), TaskOptions{Owner: Owner{Task: rt.ID}})
				if err != nil {
					return TaskTransition{}, err
				}
				data, _ := json.Marshal(map[string]TaskID{"child": childID})
				return TaskTransition{Status: "waiting", Checkpoint: data, WaitOn: []TaskID{childID}, WaitPolicy: "allSettled"}, nil
			})
			return err
		}
		return rt.Complete(ctx, Outcome{Status: "completed"})
	}}
	if err := h.registry.Install(Extension{Name: "tree", Tasks: []TaskDefinition{child, parent}}); err != nil {
		t.Fatal(err)
	}
	var parentID TaskID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		parentID, err = tx.CreateTask(conversation.ID(), parent, json.RawMessage(`{}`), TaskOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := h.WaitTask(ctx, parentID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome == nil || result.Outcome.Status != "completed" {
		t.Fatalf("parent: %+v", result)
	}
}
