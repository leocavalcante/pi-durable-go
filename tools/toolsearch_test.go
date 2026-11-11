package tools

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

func TestRankDocumentsFollowsUpstreamOrdering(t *testing.T) {
	documents := []SearchDocument{
		{Name: "window", Text: "Manage windows"},
		{Name: "screenshot_tool", Text: "Takes a screenshot of a window"},
		{Name: "mcp__peekaboo__app", Text: "Control an app"},
		{Name: "launcher", Text: "Opens the peekaboo app and other apps"},
		{Name: "mcp__github__issues", Text: "List issues"},
	}
	cases := []struct {
		query string
		first string
	}{
		{"screenshot window", "screenshot_tool"}, // covers both terms
		{"peekaboo app", "mcp__peekaboo__app"},   // whole local name bonus
		{"issues", "mcp__github__issues"},        // name over text, plural stem
		{"github__issues", "mcp__github__issues"},
	}
	for _, tc := range cases {
		if got := RankDocuments(tc.query, documents, 3); len(got) == 0 || got[0] != tc.first {
			t.Errorf("RankDocuments(%q)=%v, want %s first", tc.query, got, tc.first)
		}
	}
	if got := RankDocuments("the of", documents, 3); got != nil {
		t.Errorf("stop words matched %v", got)
	}
	if got := RankDocuments("截图", []SearchDocument{{Name: "shot", Text: "截图工具"}}, 3); len(got) != 1 {
		t.Errorf("non-ASCII query found %v", got)
	}
}

func TestToolSearchLoadsDeferredTools(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var mu sync.Mutex
	var offered [][]string
	model := modelFunc(func(ctx context.Context, request durable.ModelRequest) (durable.ModelResponse, error) {
		var names []string
		for _, tool := range request.Tools {
			names = append(names, tool.Name)
		}
		mu.Lock()
		offered = append(offered, names)
		n := len(offered)
		mu.Unlock()
		if n == 1 {
			return durable.ModelResponse{ToolCalls: []durable.ToolCall{{ID: "s", Name: ToolSearchName, Arguments: json.RawMessage(`{"query":"weather forecast"}`)}}}, nil
		}
		return durable.ModelResponse{Content: "done"}, nil
	})
	registry := durable.NewRegistry()
	deferred := func(name, description string) durable.ToolDefinition {
		return durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: name, Description: description, Parameters: json.RawMessage(`{"type":"object"}`)}, Deferred: true,
			Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
				return durable.ToolResult{}, nil
			}}
	}
	if err := registry.Install(ToolSearch()); err != nil {
		t.Fatal(err)
	}
	if err := registry.Install(durable.Extension{Name: "deferred", Tools: []durable.ToolDefinition{deferred("weather", "Weather forecast for a city"), deferred("stocks", "Stock quotes")}}); err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, durable.NewMemoryStorage(), durable.HarnessOptions{Model: model, Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Model: durable.ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	if record, err := sub.Wait(ctx); err != nil || record.Status != "done" {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(offered[0], ",") != ToolSearchName || strings.Join(offered[1], ",") != ToolSearchName+",weather" {
		t.Fatalf("offered=%v", offered)
	}
}

type modelFunc func(context.Context, durable.ModelRequest) (durable.ModelResponse, error)

func (f modelFunc) Generate(ctx context.Context, request durable.ModelRequest, _ func(string) error) (durable.ModelResponse, error) {
	return f(ctx, request)
}
