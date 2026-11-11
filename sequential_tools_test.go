package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

func TestToolRoundSequentialAndUnofferedCalls(t *testing.T) {
	for _, tc := range []struct {
		name       string
		settings   Settings
		mode       string
		sequential bool
	}{
		{name: "parallel"},
		{name: "settings-sequential", settings: Settings{ToolExecution: "sequential"}, sequential: true},
		{name: "tool-sequential", mode: "sequential", sequential: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			running, maxRunning := 0, 0
			var order []string
			release := make(chan struct{}, 4)
			model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
				if hasToolResult(request) {
					return ModelResponse{Content: "done"}, nil
				}
				return ModelResponse{ToolCalls: []ToolCall{
					{ID: "a", Name: "step", Arguments: json.RawMessage(`{"n":"a"}`)},
					{ID: "b", Name: "hidden", Arguments: json.RawMessage(`{}`)},
					{ID: "c", Name: "step", Arguments: json.RawMessage(`{"n":"c"}`)},
				}}, nil
			})
			var hiddenRan bool
			extension := Extension{Name: "steps", Tools: []ToolDefinition{
				{ToolSpec: ToolSpec{Name: "step", Parameters: json.RawMessage(`{"type":"object"}`)}, ExecutionMode: tc.mode,
					Execute: func(ctx context.Context, args json.RawMessage, _ *ToolRuntime) (ToolResult, error) {
						mu.Lock()
						running++
						if running > maxRunning {
							maxRunning = running
						}
						order = append(order, string(args))
						mu.Unlock()
						<-release
						mu.Lock()
						running--
						mu.Unlock()
						return ToolResult{Content: string(args)}, nil
					}},
				{ToolSpec: ToolSpec{Name: "hidden", Parameters: json.RawMessage(`{"type":"object"}`)},
					Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
						hiddenRan = true
						return ToolResult{}, nil
					}},
			}}
			registry := NewRegistry()
			if err := registry.Install(extension); err != nil {
				t.Fatal(err)
			}
			h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, Settings: tc.settings})
			// The agent offers only step; hidden is installed but unoffered.
			root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}, Tools: []string{"step"}}})
			if err != nil {
				t.Fatal(err)
			}
			release <- struct{}{}
			release <- struct{}{}
			record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
			if record.Status != "done" || hiddenRan {
				t.Fatalf("submission=%+v hidden ran=%v", record, hiddenRan)
			}
			mu.Lock()
			defer mu.Unlock()
			if tc.sequential && (maxRunning != 1 || len(order) != 2 || order[0] != `{"n":"a"}`) || !tc.sequential && len(order) != 2 {
				t.Fatalf("max running=%d order=%v", maxRunning, order)
			}
			state, _ := h.Snapshot(context.Background())
			for _, entry := range state.Entries {
				if entry.Kind == "pi.tool-result" && entry.Message.ToolCallID == "b" &&
					entry.Message.Content != "<harness>\n[error] Tool hidden is not available\n</harness>" {
					t.Fatalf("unoffered result=%q", entry.Message.Content)
				}
			}
		})
	}
}
