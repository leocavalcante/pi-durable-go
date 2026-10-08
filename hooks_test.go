package durable

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestExplicitEmptyToolAllowlistOffersNoTools(t *testing.T) {
	registry := NewRegistry()
	tool := ToolDefinition{ToolSpec: ToolSpec{Name: "test", Parameters: json.RawMessage(`{"type":"object"}`)}, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) { return ToolResult{}, nil }}
	if err := registry.Install(Extension{Name: "test", Tools: []ToolDefinition{tool}}); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot()
	if len(snapshot.Tools(Agent{})) != 1 || len(snapshot.Tools(Agent{Tools: []string{}})) != 0 || len(snapshot.Tools(Agent{Extensions: []string{}})) != 0 {
		t.Fatal("nil and empty selections conflated")
	}
	tool.Parameters[0] = '!'
	if string(registry.Snapshot().Tools(Agent{})[0].Parameters) != `{"type":"object"}` {
		t.Fatal("registry retained caller schema bytes")
	}
}
func TestToolHookRewriteIsRevalidatedBeforeExecute(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{Model: &toolRoundModel{}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	var executes atomic.Int32
	var hooks atomic.Int32
	ext := Extension{Name: "guard", Tools: []ToolDefinition{{ToolSpec: ToolSpec{Name: "count", Parameters: json.RawMessage(`{"type":"object","properties":{"n":{"type":"integer","minimum":1}},"required":["n"]}`)}, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
		executes.Add(1)
		return ToolResult{}, nil
	}}}, Hooks: Hooks{BeforeTool: func(ctx context.Context, call *ToolCall, api *ToolRuntime) error {
		hooks.Add(1)
		call.Arguments = json.RawMessage(`{"n":0}`)
		return nil
	}}}
	if err := h.registry.Install(ext); err != nil {
		t.Fatal(err)
	}
	c := testRoot(t, h)
	sub, err := c.Submit(ctx, SubmissionRequest{Content: "count"})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != "done" || hooks.Load() != 1 || executes.Load() != 0 {
		t.Fatalf("result %+v hooks %d executes %d", settled, hooks.Load(), executes.Load())
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range state.Entries {
		if e.Kind == "pi.tool-result" && e.Message.IsError {
			found = true
		}
	}
	if !found {
		t.Fatal("blocked result not persisted")
	}
}
func TestPromptSectionsAndBeforeRequestHook(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := testHarness(t)
	c := testRoot(t, h)
	if err := h.registry.Install(Extension{Name: "prompt", Sections: []Section{{Name: "project", Render: func(context.Context, ConversationRecord, *Harness) (string, error) {
		return "project instructions", nil
	}}}, Hooks: Hooks{BeforeRequest: func(ctx context.Context, req *ModelRequest, r *TaskRuntime) error {
		if len(req.Messages) < 2 || req.Messages[0].Role != "system" {
			t.Error("system prompt not first")
		}
		req.Messages = append(req.Messages, Message{Role: "user", Content: "hooked"})
		return nil
	}}}); err != nil {
		t.Fatal(err)
	}
	sub, err := c.Submit(ctx, SubmissionRequest{Content: "original"})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Entries[settled.Answer].Message.Content != "echo: hooked" {
		t.Fatal("request hook not applied")
	}
}
