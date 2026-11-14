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
		// Arguments that are not an object fail the call's validation.
		results["array"], err = r.ExecuteTool(ctx, "typed", json.RawMessage(`[]`), NestedCallOptions{})
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
	if len(errs) != 2 || !errors.Is(errs[0], ErrInvalid) || errs[1] != nil {
		t.Fatalf("errors=%v", errs)
	}
	if r := results["array"]; !r.IsError || len(r.Diagnostics) != 1 || r.Diagnostics[0].Code != "invalid_arguments" {
		t.Fatalf("array=%+v", r)
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

func TestNestedCallKeysCannotLookLikeDefaultKeys(t *testing.T) {
	var errs []error
	h, root, _ := nestedHarness(t, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		for _, key := range []string{"3", "__proto__", "a/b"} {
			_, err := r.ExecuteTool(ctx, "typed", json.RawMessage(`{}`), NestedCallOptions{Key: key})
			errs = append(errs, err)
		}
		return ToolResult{Content: "ok"}, nil
	}, ToolDefinition{ToolSpec: ToolSpec{Name: "typed", Parameters: json.RawMessage(`{"type":"object"}`)},
		Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
			return ToolResult{Content: "ran"}, nil
		}})
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	if len(errs) != 3 {
		t.Fatalf("errors=%v", errs)
	}
	for _, err := range errs {
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), `must be non-empty, without "/", not "__proto__", and not a positive integer`) {
			t.Fatalf("error=%v", err)
		}
	}
}

// A nested caller's value depends on where the result came from (upstream
// finalResult and afterHarnessResult): a call the tool ran, a throw
// included, gives its output; a result the Harness wrote gives it only when
// hooks made it a success.
func TestNestedValuesFollowWhereTheResultCameFrom(t *testing.T) {
	results := map[string]NestedToolResult{}
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		if hasToolResult(request) {
			return ModelResponse{Content: "done"}, nil
		}
		return ModelResponse{ToolCalls: []ToolCall{{ID: "outer-call", Name: "outer", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	object := json.RawMessage(`{"type":"object"}`)
	tools := []ToolDefinition{
		{ToolSpec: ToolSpec{Name: "outer", Parameters: object}, Execute: func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
			for _, name := range []string{"thrower", "explained", "fixed", "typedError"} {
				result, err := r.ExecuteTool(ctx, name, json.RawMessage(`{}`), NestedCallOptions{Key: name})
				if err != nil {
					return ToolResult{}, err
				}
				results[name] = result
			}
			return ToolResult{Content: "ok"}, nil
		}},
		{ToolSpec: ToolSpec{Name: "thrower", Parameters: object}, Callers: []string{"tools"},
			Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
				return ToolResult{}, errors.New("inner boom")
			}},
		// Model-only tools: a nested call to them is a Harness-written error,
		// which the hooks below explain or turn into a success.
		{ToolSpec: ToolSpec{Name: "explained", Parameters: object}, Callers: []string{"model"}, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
			return ToolResult{}, nil
		}},
		{ToolSpec: ToolSpec{Name: "fixed", Parameters: object}, Callers: []string{"model"}, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
			return ToolResult{}, nil
		}},
		{ToolSpec: ToolSpec{Name: "typedError", Parameters: object}, Callers: []string{"tools"},
			StructuredOutputSchema: json.RawMessage(`{"type":"object"}`),
			Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
				return ToolResult{Content: "went wrong", IsError: true}, nil
			}},
	}
	hooks := Extension{Name: "hooks", Hooks: Hooks{AfterTool: func(_ context.Context, result *ToolResult, rt *ToolRuntime) error {
		switch rt.Call.Name {
		case "explained":
			result.Content, result.StructuredOutput = "explained", json.RawMessage(`"dropped"`)
		case "fixed":
			result.Content, result.IsError = "fixed", false
		}
		return nil
	}}}
	h, root := runHarness(t, model, Settings{}, Extension{Name: "nesting", Tools: tools}, hooks)
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))

	if r := results["thrower"]; !r.IsError || string(r.StructuredOutput) != `""` || len(r.Diagnostics) != 1 || r.Diagnostics[0].Message != "inner boom" {
		t.Fatalf("thrower=%+v structuredOutput=%s", r, r.StructuredOutput)
	}
	if r := results["explained"]; !r.IsError || r.StructuredOutput != nil {
		t.Fatalf("explained=%+v structuredOutput=%s", r, r.StructuredOutput)
	}
	if r := results["fixed"]; r.IsError || string(r.StructuredOutput) != `"fixed"` {
		t.Fatalf("fixed=%+v structuredOutput=%s", r, r.StructuredOutput)
	}
	r := results["typedError"]
	if last := r.Diagnostics[len(r.Diagnostics)-1]; !r.IsError || r.StructuredOutput != nil || last.Code != "tool_error" || last.Message != "went wrong" {
		t.Fatalf("typedError=%+v", r)
	}
}

// A tool result records usage under its tool only when it has some, as
// upstream records only a defined usage; nested calls likewise.
func TestToolUsageIsRecordedOnlyWhenReported(t *testing.T) {
	object := json.RawMessage(`{"type":"object"}`)
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		if hasToolResult(request) {
			return ModelResponse{Content: "done"}, nil
		}
		return ModelResponse{ToolCalls: []ToolCall{{ID: "a", Name: "plain", Arguments: json.RawMessage(`{}`)}, {ID: "b", Name: "spender", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	tools := []ToolDefinition{
		{ToolSpec: ToolSpec{Name: "plain", Parameters: object}, Execute: func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
			if _, err := r.ExecuteTool(ctx, "inner", json.RawMessage(`{}`), NestedCallOptions{}); err != nil {
				return ToolResult{}, err
			}
			return ToolResult{Content: "no usage"}, nil
		}},
		{ToolSpec: ToolSpec{Name: "spender", Parameters: object}, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
			return ToolResult{Content: "spent", Usage: Usage{InputTokens: 3, OutputTokens: 4}}, nil
		}},
		{ToolSpec: ToolSpec{Name: "inner", Parameters: object}, Callers: []string{"tools"}, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
			return ToolResult{Content: "inner, no usage"}, nil
		}},
	}
	h, root := runHarness(t, model, Settings{}, Extension{Name: "tools", Tools: tools})
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	ledger, err := ReadDocument[UsageState](context.Background(), h, usageDocument, uint64(root.ID()))
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.Tools) != 1 || ledger.Tools["spender"] != (Usage{InputTokens: 3, OutputTokens: 4}) {
		t.Fatalf("tools=%+v", ledger.Tools)
	}
}

// Arguments that are not an object are the nested call's to report, as
// upstream passes a script's arguments as given: its validation fails it.
// NestedCall finds the call an attempt admitted under a key.
func TestNestedCallsValidateArgumentsThatAreNotObjects(t *testing.T) {
	var got NestedToolResult
	var callErr error
	var record NestedCallRecord
	var found bool
	var lookupErr error
	ran := false
	h, root, _ := nestedHarness(t, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		got, callErr = r.ExecuteTool(ctx, "inner", json.RawMessage(`"not an object"`), NestedCallOptions{Key: "k"})
		record, found, lookupErr = r.NestedCall(ctx, "k")
		if _, missing, err := r.NestedCall(ctx, "other"); missing || err != nil {
			t.Errorf("a key never used: %v %v", missing, err)
		}
		return ToolResult{Content: "outer"}, nil
	}, ToolDefinition{ToolSpec: ToolSpec{Name: "inner", Parameters: json.RawMessage(`{"type":"object"}`)}, Callers: []string{"tools"},
		Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
			ran = true
			return ToolResult{Content: "inner"}, nil
		}})
	settled := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	if settled.Status != "done" || callErr != nil || ran || !got.IsError || len(got.Diagnostics) != 1 || got.Diagnostics[0].Code != "invalid_arguments" {
		t.Fatalf("settled %+v, nested %+v, err %v, ran %v", settled, got, callErr, ran)
	}
	if lookupErr != nil || !found || record.TaskID != got.TaskID || record.Call.Name != "inner" || string(record.Call.Arguments) != `"not an object"` || record.Result == nil {
		t.Fatalf("record %+v found %v err %v", record, found, lookupErr)
	}
}
