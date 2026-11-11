package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func nestedHarness(t *testing.T, outer func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error), tools ...ToolDefinition) (*Harness, *Conversation, *[]ModelRequest) {
	t.Helper()
	var mu sync.Mutex
	var requests []ModelRequest
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		requests = append(requests, request)
		mu.Unlock()
		if hasToolResult(request) {
			return ModelResponse{Content: "done"}, nil
		}
		return ModelResponse{ToolCalls: []ToolCall{{ID: "outer-call", Name: "outer", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	all := append([]ToolDefinition{{ToolSpec: ToolSpec{Name: "outer", Parameters: json.RawMessage(`{"type":"object"}`)}, Execute: outer}}, tools...)
	h, root := runHarness(t, model, Settings{}, Extension{Name: "nesting", Tools: all})
	return h, root, &requests
}

func TestNestedToolCallsRunAsOwnedTasks(t *testing.T) {
	var got NestedToolResult
	var callErr error
	h, root, requests := nestedHarness(t, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		got, callErr = r.ExecuteTool(ctx, "inner", json.RawMessage(`{"x":1}`), NestedCallOptions{})
		if callErr != nil {
			return ToolResult{}, callErr
		}
		var text string
		_ = json.Unmarshal(got.StructuredOutput, &text)
		return ToolResult{Content: "outer saw " + text}, nil
	}, ToolDefinition{ToolSpec: ToolSpec{Name: "inner", Parameters: json.RawMessage(`{"type":"object"}`)}, Callers: []string{"tools"},
		Execute: func(ctx context.Context, args json.RawMessage, r *ToolRuntime) (ToolResult, error) {
			if r.Call.ID != "outer-call/1" || string(args) != `{"x":1}` {
				t.Errorf("nested call=%+v args=%s", r.Call, args)
			}
			return ToolResult{Content: "inner result"}, nil
		}})
	record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	if record.Status != "done" || callErr != nil || got.IsError || got.TaskID == 0 {
		t.Fatalf("record=%+v nested=%+v err=%v", record, got, callErr)
	}
	if result := toolResultEntry(t, h, root.ID()); result.Message.Content != "outer saw inner result" {
		t.Fatalf("result=%q", result.Message.Content)
	}
	for _, tool := range (*requests)[0].Tools {
		if tool.Name == "inner" {
			t.Fatal("a tool callable only by tools was offered to the model")
		}
	}
	if live := readLiveState(t, h, root.ID()); live.NestedTools != nil {
		t.Fatalf("nested slots left: %+v", live.NestedTools)
	}
	state, _ := h.Snapshot(context.Background())
	if inner := state.Tasks[got.TaskID]; inner == nil || inner.Owner.Task == 0 || inner.Outcome.Status != OutcomeCompleted {
		t.Fatalf("inner task=%+v", inner)
	}
}

func TestNestedToolCallContracts(t *testing.T) {
	results := map[string]NestedToolResult{}
	var errs []error
	h, root, _ := nestedHarness(t, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		for _, call := range []struct{ key, name, args string }{
			{"model-only", "modelOnly", `{}`},
			{"typed", "typed", `{"ok":true}`},
			{"broken", "typed", `{"ok":false}`},
		} {
			result, err := r.ExecuteTool(ctx, call.name, json.RawMessage(call.args), NestedCallOptions{Key: call.key})
			if err != nil {
				return ToolResult{}, err
			}
			results[call.key] = result
		}
		_, err := r.ExecuteTool(ctx, "typed", json.RawMessage(`{"ok":false,"other":1}`), NestedCallOptions{Key: "typed"})
		errs = append(errs, err)
		_, err = r.ExecuteTool(ctx, "typed", json.RawMessage(`[]`), NestedCallOptions{})
		errs = append(errs, err)
		return ToolResult{Content: "ok"}, nil
	},
		ToolDefinition{ToolSpec: ToolSpec{Name: "modelOnly", Parameters: json.RawMessage(`{"type":"object"}`)}, Callers: []string{"model"},
			Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
				return ToolResult{Content: "ran"}, nil
			}},
		ToolDefinition{ToolSpec: ToolSpec{Name: "typed", Parameters: json.RawMessage(`{"type":"object"}`)},
			StructuredOutputSchema: json.RawMessage(`{"type":"object","required":["n"],"properties":{"n":{"type":"number"}}}`),
			Execute: func(_ context.Context, args json.RawMessage, _ *ToolRuntime) (ToolResult, error) {
				if strings.Contains(string(args), "true") {
					return ToolResult{Content: "typed", StructuredOutput: json.RawMessage(`{"n":7}`)}, nil
				}
				return ToolResult{Content: "typed", StructuredOutput: json.RawMessage(`{"n":"seven"}`)}, nil
			}})
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	if r := results["model-only"]; !r.IsError || len(r.Diagnostics) != 1 || r.Diagnostics[0].Code != "tool_unavailable" || r.StructuredOutput != nil {
		t.Fatalf("model-only=%+v", r)
	}
	if r := results["typed"]; r.IsError || string(r.StructuredOutput) != `{"n":7}` {
		t.Fatalf("typed=%+v", r)
	}
	if r := results["broken"]; !r.IsError || r.StructuredOutput != nil || r.Diagnostics[len(r.Diagnostics)-1].Code != "invalid_structured_output" {
		t.Fatalf("broken=%+v", r)
	}
	if len(errs) != 2 || !errors.Is(errs[0], ErrInvalid) || !errors.Is(errs[1], ErrInvalid) {
		t.Fatalf("errors=%v", errs)
	}
}

func TestNestedCallsLeftRunningAreAborted(t *testing.T) {
	started := make(chan struct{})
	aborted := make(chan struct{})
	h, root, _ := nestedHarness(t, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		go func() { _, _ = r.ExecuteTool(context.Background(), "slow", json.RawMessage(`{}`), NestedCallOptions{}) }()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Error("nested call never started")
		}
		return ToolResult{Content: "returned early"}, nil
	}, ToolDefinition{ToolSpec: ToolSpec{Name: "slow", Parameters: json.RawMessage(`{"type":"object"}`)},
		Execute: func(ctx context.Context, _ json.RawMessage, _ *ToolRuntime) (ToolResult, error) {
			close(started)
			<-ctx.Done()
			close(aborted)
			return ToolResult{}, ctx.Err()
		}})
	if record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"})); record.Status != "done" {
		t.Fatalf("record=%+v", record)
	}
	select {
	case <-aborted:
	case <-time.After(5 * time.Second):
		t.Fatal("a nested call left running was not aborted")
	}
}
