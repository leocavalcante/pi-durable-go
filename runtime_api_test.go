package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

// greetHooks is a custom task's hook contract.
type greetHooks struct {
	Greet func(name string) (string, error)
}

func TestCustomTasksRunTheirExtensionsHooks(t *testing.T) {
	reports := make(chan Report, 4)
	registry := NewRegistry()
	var greetings []string
	definition := TaskDefinition{Kind: "test.greeter", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		if err := EachTaskHook(ctx, r, func(hooks greetHooks) error {
			greeting, err := hooks.Greet("pi")
			if err == nil {
				greetings = append(greetings, greeting)
			}
			return err
		}); err != nil {
			return err
		}
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	installTasks(t, registry, "greeter", definition)
	for _, extension := range []Extension{
		{Name: "hello", TaskHooks: map[string]any{"test.greeter": greetHooks{Greet: func(name string) (string, error) { return "hello " + name, nil }}}},
		{Name: "broken", TaskHooks: map[string]any{"test.greeter": greetHooks{Greet: func(string) (string, error) { return "", errors.New("no greeting") }}}},
		{Name: "other", TaskHooks: map[string]any{"test.other": greetHooks{Greet: func(string) (string, error) { return "never", nil }}}},
		{Name: "hi", TaskHooks: map[string]any{"test.greeter": greetHooks{Greet: func(name string) (string, error) { return "hi " + name, nil }}}},
	} {
		if err := registry.Install(extension); err != nil {
			t.Fatal(err)
		}
	}
	h := taskTestHarness(t, HarnessOptions{Registry: registry, OnReport: func(r Report) { reports <- r }})
	id := createTaskForTest(t, h, definition)
	h.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if record, err := h.WaitTask(ctx, id); err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeCompleted {
		t.Fatalf("task=%+v error=%v", record, err)
	}
	if strings.Join(greetings, ",") != "hello pi,hi pi" {
		t.Fatalf("greetings=%v", greetings)
	}
	if report := <-reports; report.Kind != ReportExtensionError || report.Extension != "broken" {
		t.Fatalf("report=%+v", report)
	}
}
