package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
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
	for i, got := range seen {
		if got != identity {
			t.Fatalf("request %d identity=%v", i, got)
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
		agent.Instructions = "be brief"
		if err := tx.Configure(root.ID(), agent); err != nil {
			return err
		}
		_, err = tx.Append(root.ID(), Entry{Kind: "app.note"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	agent, err := root.Agent(context.Background())
	if err != nil || agent.Instructions != "be brief" {
		t.Fatalf("agent=%+v error=%v", agent, err)
	}
	if err := h.Commit(context.Background(), func(tx *Tx) error { return tx.Configure(root.ID(), Agent{MaxTurns: -1}) }); err == nil {
		t.Fatal("a negative MaxTurns was configured")
	}
}
