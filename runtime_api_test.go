package durable

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestTaskRuntimeModelIsTheHarnessModel(t *testing.T) {
	model := taskModelFunc(func(_ context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		return ModelResponse{Content: "answer for " + request.Model.ID}, nil
	})
	registry := NewRegistry()
	definition := TaskDefinition{Kind: "test.model-user", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		response, err := r.Model().Generate(ctx, ModelRequest{Model: ModelRef{Provider: "fake", ID: "m"}}, nil)
		if err != nil {
			return err
		}
		result, _ := json.Marshal(response.Content)
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted, Result: result})
	}}
	installTasks(t, registry, "model-user", definition)
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry})
	id := createTaskForTest(t, h, definition)
	h.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || string(record.Outcome.Result) != `"answer for m"` {
		t.Fatalf("task=%+v error=%v", record.Outcome, err)
	}
	// Without a model there is none.
	bare := taskTestHarness(t, HarnessOptions{})
	if (&TaskRuntime{h: bare}).Model() != nil {
		t.Fatal("a Harness without a model offered one")
	}
}
