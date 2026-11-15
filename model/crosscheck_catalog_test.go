//go:build crosscheck

package model

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// TestCrosscheckOpenAICatalog sends one conversation to every catalog model
// through OpenAIModel and through pi-ai's adapter with the catalog's own
// model, and compares the request bodies, and the headers of the models
// with headers of their own.
func TestCrosscheckOpenAICatalog(t *testing.T) {
	data, err := os.ReadFile("catalog/openai-completions.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	lookup := durable.ToolSpec{Name: "lookup", Description: "Look something up", Parameters: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`)}
	rules := "<rules>\nbe kind\n</rules>"
	conversation := []durable.Message{
		{Role: "system", Sections: []durable.SystemSection{{Name: "rules", Text: &rules}}, ToolsAdded: []durable.ToolSpec{lookup}},
		{Role: "user", Content: "look at this"},
		{Role: "assistant", Thinking: "hmm", Content: "checking", ToolCalls: []durable.ToolCall{{ID: "call_1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)}},
			Usage: durable.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}},
		{Role: "tool", ToolCallID: "call_1", ToolName: "lookup", Content: "found"},
		{Role: "user", Content: "thanks"},
	}
	piConversation := []any{
		map[string]any{"role": "system", "content": "", "sections": map[string]string{"rules": rules}, "toolsAdded": []any{map[string]any{"name": "lookup", "description": "Look something up", "parameters": json.RawMessage(lookup.Parameters)}}, "timestamp": 1},
		map[string]any{"role": "user", "content": "look at this", "timestamp": 2},
		map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "thinking", "thinking": "hmm"}, map[string]any{"type": "text", "text": "checking"}, map[string]any{"type": "toolCall", "id": "call_1", "name": "lookup", "arguments": map[string]string{"q": "x"}}},
			"api": "openai-completions", "provider": "openai", "model": "gpt-4o-mini", "stopReason": "toolUse", "timestamp": 3,
			"usage": map[string]any{"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 2, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}},
		map[string]any{"role": "toolResult", "toolCallId": "call_1", "toolName": "lookup", "content": []any{map[string]any{"type": "text", "text": "found"}}, "isError": false, "timestamp": 4},
		map[string]any{"role": "user", "content": "thanks", "timestamp": 5},
	}
	type variant struct {
		provider, id, level string
		pi                  map[string]any
	}
	// A model without a base URL (Azure's, per resource) is configured with
	// one, as a user must.
	const azure = "https://example.openai.azure.com/openai/v1"
	for _, model := range raw.Models {
		if model["baseUrl"] == "" {
			model["baseUrl"] = azure
		}
	}
	var variants []variant
	for _, model := range raw.Models {
		provider, id := model["provider"].(string), model["id"].(string)
		variants = append(variants, variant{provider, id, "", model})
		if model["reasoning"] == true {
			variants = append(variants, variant{provider, id, "medium", model})
		}
	}
	var input []map[string]any
	for _, v := range variants {
		options := map[string]any{"sessionId": "sess-1"}
		if v.level != "" {
			options["reasoning"] = v.level
		}
		input = append(input, map[string]any{"model": v.pi, "messages": piConversation, "options": options})
	}
	var want []json.RawMessage
	crosscheck.Run(t, "openaiPayload", input, &want)
	if len(want) != len(variants) || len(variants) < 700 {
		t.Fatalf("%d answers for %d variants", len(want), len(variants))
	}
	failures := 0
	for i, v := range variants {
		adapter, ok := OpenAIModel(v.provider, v.id)
		if !ok {
			t.Fatalf("%s/%s is not in the catalog", v.provider, v.id)
		}
		if adapter.BaseURL == "" {
			adapter.BaseURL = azure
		}
		got := payloadOf(t, adapter, durable.ModelRequest{Model: durable.ModelRef{Provider: v.provider, ID: v.id}, SessionID: "sess-1", ThinkingLevel: v.level, Messages: conversation, Tools: []durable.ToolSpec{lookup}})
		if !crosscheck.Same(got, want[i]) {
			if failures++; failures <= 20 {
				t.Errorf("%s/%s (level %q): %s", v.provider, v.id, v.level, payloadDifference(got, want[i]))
			}
		}
	}
	if failures > 0 {
		t.Errorf("%d of %d payloads differ", failures, len(variants))
	}

	// Headers of the models with headers of their own.
	var headed []map[string]any
	for _, model := range raw.Models {
		if model["headers"] != nil {
			headed = append(headed, model)
		}
	}
	var headerInput []map[string]any
	for _, model := range headed {
		headerInput = append(headerInput, map[string]any{"model": model, "options": map[string]any{"sessionId": "s-1"}})
	}
	var wantHeaders []map[string]string
	crosscheck.Run(t, "openaiHeaders", headerInput, &wantHeaders)
	var got map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = map[string]string{}
		for name, values := range r.Header {
			name = strings.ToLower(name)
			if regexp.MustCompile(`^(user-agent|x-stainless-|host|connection|content-length|content-type|accept|accept-encoding|authorization|sec-fetch-mode)`).MatchString(name) {
				continue
			}
			got[name] = strings.Join(values, ", ")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()
	for i, model := range headed {
		provider, id := model["provider"].(string), model["id"].(string)
		adapter, _ := OpenAIModel(provider, id)
		adapter.BaseURL = server.URL + "/v1?for=" + url.QueryEscape(model["baseUrl"].(string))
		req := durable.ModelRequest{Model: durable.ModelRef{Provider: provider, ID: id}, SessionID: "s-1", Messages: []durable.Message{{Role: "user", Content: "hi"}}}
		if _, err := adapter.Generate(context.Background(), req, nil); err != nil {
			t.Fatalf("%s/%s: %v", provider, id, err)
		}
		if !reflect.DeepEqual(got, wantHeaders[i]) {
			t.Errorf("%s/%s headers:\n got %v\nwant %v", provider, id, got, wantHeaders[i])
		}
	}
}

// payloadDifference names the request fields that differ, with both values.
func payloadDifference(got, want json.RawMessage) string {
	var a, b map[string]any
	_ = json.Unmarshal(got, &a)
	_ = json.Unmarshal(want, &b)
	var fields []string
	for key, value := range a {
		if !reflect.DeepEqual(value, b[key]) {
			fields = append(fields, fmt.Sprintf("%s: %v, upstream %v", key, value, b[key]))
		}
	}
	for key, value := range b {
		if _, ok := a[key]; !ok {
			fields = append(fields, fmt.Sprintf("%s: missing, upstream %v", key, value))
		}
	}
	sort.Strings(fields)
	return strings.Join(fields, "; ")
}
