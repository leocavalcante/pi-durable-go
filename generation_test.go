package durable

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

type toolRoundModel struct{ calls atomic.Int32 }

func (m *toolRoundModel) Generate(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
	if m.calls.Add(1) == 1 {
		return ModelResponse{Content: "", ToolCalls: []ToolCall{{ID: "call-1", Name: "count", Arguments: json.RawMessage(`{"n":2}`)}}}, nil
	}
	return ModelResponse{Content: "finished"}, nil
}
func TestGenerationRunsToolAsDurableOwnedTask(t *testing.T) {
	model := &toolRoundModel{}
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{Model: model})
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int32
	err = h.registry.Install(Extension{Name: "counting", Tools: []ToolDefinition{{ToolSpec: ToolSpec{Name: "count", Parameters: json.RawMessage(`{"type":"object"}`)}, ReplaySafe: true, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
		executions.Add(1)
		return ToolResult{Content: "2"}, nil
	}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	conversation, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "test"}, Extensions: []string{"counting"}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	submission, err := conversation.Submit(ctx, SubmissionRequest{Type: "input", Content: "count"})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := submission.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != "done" {
		t.Fatalf("submission: %+v", settled)
	}
	if executions.Load() != 1 {
		t.Fatalf("executions=%d", executions.Load())
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, task := range state.Tasks {
		if task.Kind == "pi.tool" && task.Status == "terminal" && task.Outcome != nil && task.Outcome.Status == "completed" {
			found = true
		}
	}
	if !found {
		t.Fatal("tool result task not durably completed")
	}
}
