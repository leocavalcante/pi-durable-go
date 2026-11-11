package durable

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTaskStepFirstPhaseGate(t *testing.T) {
	for _, mode := range []string{"abort", "owner", "seal"} {
		t.Run(mode, func(t *testing.T) {
			h := testHarness(t)
			definition := TaskDefinition{Kind: "test.first-gate", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
				t.Error("Run passed a closed first-phase gate")
				return nil
			}}
			id := createTaskForTest(t, h, definition)
			ctx, cancel := context.WithCancel(context.Background())
			inv := &taskInvocation{ctx: ctx, cancel: cancel, done: make(chan struct{})}
			r := &TaskRuntime{h: h, ID: id, invocation: inv}
			inv.runtime = r
			// Model a durable reservation whose goroutine has not reached its
			// first step. The mark/seal must be checked again before user code.
			if err := h.Commit(context.Background(), func(tx *Tx) error {
				task := tx.state.mutTask(id)
				task.Status = "running"
				if mode == "abort" {
					markAbortTree(tx.state, id)
				}
				if mode == "owner" {
					parent, err := tx.CreateTask(task.Conversation, definition, nil, TaskOptions{})
					if err != nil {
						return err
					}
					task.Owner.Task = parent
					tx.state.mutTask(parent).AbortRequested = true
				}
				h.scheduler.mu.Lock()
				h.scheduler.invocations[id] = inv
				h.scheduler.mu.Unlock()
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before := h.session.state.Sequence
			if mode == "seal" {
				h.session.sealAdmission()
			}
			snapshot := h.registry.Snapshot()
			h.scheduler.runInvocation(id, inv, resolvedTask{Definition: definition, Identity: 1}, snapshot)
			if h.session.state.Sequence != before {
				t.Fatal("read-only end step wrote a snapshot")
			}
			if !r.ended.Load() {
				t.Fatal("step left runtime gate open")
			}
			select {
			case <-inv.done:
			default:
				t.Fatal("invocation not joined")
			}
		})
	}
}

func TestTaskStepEndsRuntimeBeforeOutcomePublication(t *testing.T) {
	h := testHarness(t)
	var runtime *TaskRuntime
	definition := TaskDefinition{Kind: "test.gate-publication", Version: 1, Run: func(_ context.Context, r *TaskRuntime, _ TaskRecord) error { runtime = r; return nil }}
	if err := h.registry.Install(Extension{Name: "gate", Tasks: []TaskDefinition{definition}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, definition)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.WaitTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Commit(ctx, func(*Tx) (TaskTransition, error) { t.Error("ended callback ran"); return TaskTransition{}, nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("runtime error=%v", err)
	}
}
