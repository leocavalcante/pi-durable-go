package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func TestToolResultHarnessErrorsPassAfterTool(t *testing.T) {
	for _, tc := range []struct {
		name, call, code, message string
		block                     bool
	}{
		{name: "unavailable", call: "missing", code: "tool_unavailable", message: "Tool missing is not available"},
		{name: "invalid", call: "strict", code: "invalid_arguments"},
		{name: "blocked", call: "strict", code: "blocked", message: "Tool call blocked: not today", block: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var seen []ToolResult
			args := json.RawMessage(`{}`)
			if tc.block {
				args = json.RawMessage(`{"n":1}`)
			}
			model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
				if hasToolResult(request) {
					return ModelResponse{Content: "done"}, nil
				}
				return ModelResponse{ToolCalls: []ToolCall{{ID: "c1", Name: tc.call, Arguments: args}}}, nil
			})
			extension := Extension{Name: "strict", Tools: []ToolDefinition{{ToolSpec: ToolSpec{Name: "strict",
				Parameters: json.RawMessage(`{"type":"object","required":["n"],"properties":{"n":{"type":"number"}}}`)},
				Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
					return ToolResult{Content: "ran"}, nil
				}}},
				Hooks: Hooks{
					BeforeTool: func(context.Context, *ToolCall, *ToolRuntime) error {
						if tc.block {
							return errors.New("not today")
						}
						return nil
					},
					AfterTool: func(_ context.Context, result *ToolResult, _ *ToolRuntime) error {
						mu.Lock()
						seen = append(seen, *result)
						mu.Unlock()
						result.Content = "seen by hook"
						return nil
					}}}
			h, root := runHarness(t, model, Settings{}, extension)
			waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != 1 || !seen[0].IsError || len(seen[0].Diagnostics) != 1 || seen[0].Diagnostics[0].Code != tc.code ||
				tc.message != "" && seen[0].Diagnostics[0].Message != tc.message {
				t.Fatalf("hook saw %+v", seen)
			}
			result := toolResultEntry(t, h, root.ID()).Message
			if result.Content != "seen by hook\n<harness>\n[error] "+seen[0].Diagnostics[0].Message+"\n</harness>" {
				t.Fatalf("result=%q", result.Content)
			}
		})
	}
}
