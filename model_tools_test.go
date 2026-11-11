package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

func toolNames(request ModelRequest) []string {
	var names []string
	for _, tool := range request.Tools {
		names = append(names, tool.Name)
	}
	return names
}

func TestModelToolsSelectionAndDeferredLoading(t *testing.T) {
	spec := func(name string) ToolSpec {
		return ToolSpec{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}
	}
	run := func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
		return ToolResult{Content: "ok"}, nil
	}
	var mu sync.Mutex
	var offered [][]string
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		offered = append(offered, toolNames(request))
		n := len(offered)
		mu.Unlock()
		if n == 1 {
			return ModelResponse{ToolCalls: []ToolCall{{ID: "load", Name: "loader", Arguments: json.RawMessage(`{}`)}}}, nil
		}
		return ModelResponse{Content: "done"}, nil
	})
	extension := Extension{Name: "loadout", Tools: []ToolDefinition{
		{ToolSpec: spec("plain"), Execute: run},
		{ToolSpec: spec("removed"), Execute: run},
		{ToolSpec: spec("hidden"), Deferred: true, Execute: run},
		{ToolSpec: spec("loader"), Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
			return ToolResult{Content: "loaded", AddTools: []string{"hidden", "removed"}}, nil
		}},
	}}
	registry := NewRegistry()
	if err := registry.Install(extension); err != nil {
		t.Fatal(err)
	}
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"},
		ModelTools: &ModelTools{Remove: []string{"removed"}}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	mu.Lock()
	defer mu.Unlock()
	if len(offered) != 2 || jsonString(offered[0]) != `["plain","loader"]` || jsonString(offered[1]) != `["plain","removed","loader","hidden"]` {
		t.Fatalf("offered=%v", offered)
	}
	agent, err := root.Agent(context.Background())
	if err != nil || agent.ModelTools == nil || len(agent.ModelTools.Remove) != 0 || jsonString(agent.ModelTools.Add) != `["hidden","removed"]` {
		t.Fatalf("agent=%+v error=%v", agent.ModelTools, err)
	}
	if got := offeredTools(extension.Tools, &ModelTools{Only: []string{"hidden", "plain", "missing"}}); len(got) != 2 || got[0].Name != "hidden" {
		t.Fatalf("only=%+v", got)
	}
}

func jsonString(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}
