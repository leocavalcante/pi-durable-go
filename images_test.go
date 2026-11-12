package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestImagesReachTheModelFromInputsAndToolResults(t *testing.T) {
	png := ImageContent{MimeType: "image/png", Data: "iVBORw0KGgo="}
	var mu sync.Mutex
	var requests []ModelRequest
	model := taskModelFunc(func(_ context.Context, req ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, req)
		if len(requests) == 1 {
			return ModelResponse{ToolCalls: []ToolCall{{ID: "c1", Name: "look", Arguments: json.RawMessage(`{}`)}}}, nil
		}
		return ModelResponse{Content: "a cat"}, nil
	})
	registry := toolExtension("look", true, func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
		return ToolResult{Images: []ImageContent{png}}, nil
	})
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "what is this?", Images: []ImageContent{png}}))
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("%d requests", len(requests))
	}
	first := requests[0].Messages[len(requests[0].Messages)-1]
	if first.Role != "user" || len(first.Images) != 1 || first.Images[0] != png {
		t.Fatalf("user message=%+v", first)
	}
	last := requests[1].Messages[len(requests[1].Messages)-1]
	if last.Role != "tool" || len(last.Images) != 1 || last.Images[0] != png {
		t.Fatalf("tool message=%+v", last)
	}
	// pi-ai counts 4,800 characters per image: ceil((13 + 4800) / 3.5).
	if got := estimateMessageTokens(first); got != 1376 {
		t.Fatalf("estimate=%d", got)
	}
}

func TestImagesOnlyOnInputs(t *testing.T) {
	h := taskTestHarness(t, HarnessOptions{})
	root := testRoot(t, h)
	if _, err := root.Submit(context.Background(), SubmissionRequest{Type: "write", Entry: &Entry{Kind: "app.note"}, Images: []ImageContent{{MimeType: "image/png", Data: "x"}}}); err == nil {
		t.Fatal("a write carried images")
	}
	if _, err := root.Submit(context.Background(), SubmissionRequest{Content: "x", Images: []ImageContent{{MimeType: "image/png"}}}); err == nil {
		t.Fatal("an image without data was admitted")
	}
}

func TestIdentityReachesRequestsWithoutBeingStored(t *testing.T) {
	identity := &ClientIdentity{UserAgent: json.RawMessage(`"short"`)}
	var mu sync.Mutex
	var seen []*ClientIdentity
	model := taskModelFunc(func(_ context.Context, req ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, req.Identity)
		if len(seen) == 1 {
			return ModelResponse{ToolCalls: []ToolCall{{ID: "c1", Name: "ask", Arguments: json.RawMessage(`{}`)}}}, nil
		}
		return ModelResponse{Content: "done"}, nil
	})
	registry := toolExtension("ask", true, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		// A tool's own request carries the Harness identity too.
		response, err := r.Model().Generate(ctx, ModelRequest{Model: ModelRef{Provider: "fake", ID: "m"}}, nil)
		return ToolResult{Content: response.Content}, err
	})
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, Identity: identity})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("%d requests", len(seen))
	}
	// Each request carries the host's identity over Pi's attribution.
	for i, got := range seen {
		if got == nil || string(got.UserAgent) != `"short"` || string(got.Attribution) != string(DefaultHarnessIdentity.Attribution) {
			t.Fatalf("request %d identity=%+v", i, got)
		}
	}
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(state)
	if bytes.Contains(data, []byte(`"identity"`)) {
		t.Fatal("the identity was stored")
	}
}

func TestUnstructuredOutputForPrograms(t *testing.T) {
	png := ImageContent{MimeType: "image/png", Data: "AA=="}
	for _, tc := range []struct {
		result ToolResult
		want   string
	}{
		{ToolResult{Content: "text"}, `"text"`},
		{ToolResult{Images: []ImageContent{png}}, `{"data":"AA==","mimeType":"image/png","type":"image"}`},
		{ToolResult{Content: "see", Images: []ImageContent{png}}, `[{"text":"see","type":"text"},{"data":"AA==","mimeType":"image/png","type":"image"}]`},
	} {
		if got := string(unstructuredOutput(tc.result)); got != tc.want {
			t.Errorf("unstructuredOutput(%+v)=%s, want %s", tc.result, got, tc.want)
		}
	}
}

func TestTxConfigureChangesTheAgentInTheCallersCommit(t *testing.T) {
	h := testHarness(t)
	root := testRoot(t, h)
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		rec, err := tx.Conversation(root.ID())
		if err != nil {
			return err
		}
		agent := rec.Agent
		agent.Instructions = str("be brief")
		if err := tx.Configure(root.ID(), agent); err != nil {
			return err
		}
		_, err = tx.Append(root.ID(), Entry{Kind: "app.note"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	agent, err := root.Agent(context.Background())
	if err != nil || agent.Instructions == nil || *agent.Instructions != "be brief" {
		t.Fatalf("agent=%+v error=%v", agent, err)
	}
	if err := h.Commit(context.Background(), func(tx *Tx) error { return tx.Configure(root.ID(), Agent{MaxTurns: -1}) }); err == nil {
		t.Fatal("a negative MaxTurns was configured")
	}
}

func TestSettingsStreamReachesRequests(t *testing.T) {
	var mu sync.Mutex
	var seen []*StreamOptions
	model := taskModelFunc(func(_ context.Context, req ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, req.Stream)
		return ModelResponse{Content: "ok"}, nil
	})
	h := taskTestHarness(t, HarnessOptions{Model: model, Settings: Settings{Stream: StreamOptions{CacheRetention: "long"}}})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] == nil || seen[0].CacheRetention != "long" {
		t.Fatalf("stream=%+v", seen)
	}
}

func TestSettingsExtensionsAreTheDefaultSelection(t *testing.T) {
	var mu sync.Mutex
	var offered []string
	model := taskModelFunc(func(_ context.Context, req ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		for _, tool := range req.Tools {
			offered = append(offered, tool.Name)
		}
		return ModelResponse{Content: "ok"}, nil
	})
	registry := NewRegistry()
	for _, name := range []string{"a", "b"} {
		if err := registry.Install(Extension{Name: name, Tools: []ToolDefinition{{ToolSpec: ToolSpec{Name: "tool-" + name, Parameters: json.RawMessage(`{"type":"object"}`)},
			Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) { return ToolResult{}, nil }}}}); err != nil {
			t.Fatal(err)
		}
	}
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, Settings: Settings{Extensions: []string{"b"}}})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	mu.Lock()
	defer mu.Unlock()
	if len(offered) != 1 || offered[0] != "tool-b" {
		t.Fatalf("offered=%v", offered)
	}
}

func TestRuntimeAgentTaskEntryDiagnosticAndRetainedOutput(t *testing.T) {
	var resolved ResolvedAgent
	var retained string
	registry := toolExtension("look", true, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		var err error
		if resolved, err = r.Agent(ctx); err != nil {
			return ToolResult{}, err
		}
		if err := r.Output(ctx, "partial"); err != nil {
			return ToolResult{}, err
		}
		retained, _ = r.RetainedOutput(ctx)
		r.Diagnostic(ToolDiagnostic{Severity: "info", Code: "note", Message: "looked"})
		if _, err := r.Task(ctx, r.ID); err != nil {
			return ToolResult{}, err
		}
		return ToolResult{Content: "seen"}, nil
	})
	model := taskModelFunc(func(_ context.Context, req ModelRequest, _ func(string) error) (ModelResponse, error) {
		if req.Messages[len(req.Messages)-1].Role == "tool" {
			return ModelResponse{Content: "done"}, nil
		}
		return ModelResponse{ToolCalls: []ToolCall{{ID: "c1", Name: "look", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	if len(resolved.Tools) != 1 || resolved.Tools[0].Name != "look" || len(resolved.Callable) != 1 {
		t.Fatalf("resolved=%+v", resolved)
	}
	if retained != "partial" {
		t.Fatalf("retained=%q", retained)
	}
	if got := toolResultEntry(t, h, root.ID()).Message.Content; !strings.Contains(got, "[info] looked") {
		t.Fatalf("result=%q", got)
	}
}

func TestSubmissionAbortSaysWhatItFound(t *testing.T) {
	gate := newGatedTool()
	h, root := runHarness(t, toolThenEcho(), Settings{}, gate.extension())
	first := submit(t, root, SubmissionRequest{Content: "tool"})
	<-gate.started
	queued := submit(t, root, SubmissionRequest{Content: "later"})
	abort := func(id SubmissionID) string {
		sub, err := h.Submission(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		result, err := sub.Abort(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	if got := abort(queued); got != SubmissionAborted {
		t.Fatalf("queued abort=%q", got)
	}
	if got := abort(first); got != SubmissionAlreadyPlaced {
		t.Fatalf("placed abort=%q", got)
	}
	if got := abort(queued); got != SubmissionSettled {
		t.Fatalf("settled abort=%q", got)
	}
	gate.release <- struct{}{}
	waitSettled(t, h, first)
	view, err := root.ContextView(context.Background(), 0)
	if err != nil || len(view.Messages) == 0 || len(view.Entries) != len(view.Contributions) {
		t.Fatalf("view=%+v error=%v", view, err)
	}
}

func TestModelFuncIsReadAtEachUse(t *testing.T) {
	var current atomic.Value
	current.Store(Model(taskModelFunc(func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
		return ModelResponse{Content: "first"}, nil
	})))
	h := taskTestHarness(t, HarnessOptions{ModelFunc: func() Model { return current.Load().(Model) }})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "one"}))
	current.Store(Model(taskModelFunc(func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
		return ModelResponse{Content: "second"}, nil
	})))
	record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "two"}))
	entries, err := root.Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var answer string
	for _, e := range entries {
		if e.ID == record.Answer && e.Message != nil {
			answer = e.Message.Content
		}
	}
	if answer != "second" {
		t.Fatalf("answer=%q", answer)
	}
}

func TestMergeClientIdentityLetsLaterSourcesWin(t *testing.T) {
	off := &ClientIdentity{Attribution: json.RawMessage(`null`), Version: "2"}
	merged := MergeClientIdentity(&DefaultHarnessIdentity, nil, &ClientIdentity{UserAgent: json.RawMessage(`"short"`)}, off)
	if merged == nil || string(merged.UserAgent) != `"short"` || string(merged.Attribution) != `null` || merged.Version != "2" {
		t.Fatalf("merged=%+v", merged)
	}
	if MergeClientIdentity(nil, nil) != nil {
		t.Fatal("no sources merged to an identity")
	}
	// Merging never shares the sources' bytes.
	merged = MergeClientIdentity(&DefaultHarnessIdentity)
	merged.Attribution[0] = 'x'
	if DefaultHarnessIdentity.Attribution[0] != '{' {
		t.Fatal("the default identity was changed through a merge")
	}
}
