//go:build crosscheck

package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// Cross-checks with pi-ai's openai-completions adapter (make crosscheck).

// payloadOf is the body model.OpenAI would send for req.
func payloadOf(t *testing.T, m OpenAI, req durable.ModelRequest) json.RawMessage {
	t.Helper()
	var body json.RawMessage
	req.OnPayload = func(_ context.Context, _ string, payload json.RawMessage) (json.RawMessage, error) {
		body = append(json.RawMessage(nil), payload...)
		return nil, errors.New("captured")
	}
	_, _ = m.Generate(context.Background(), req, nil)
	if body == nil {
		t.Fatal("no request body")
	}
	return body
}

func TestCrosscheckOpenAIRequests(t *testing.T) {
	lookup := durable.ToolSpec{Name: "lookup", Description: "Look something up", Parameters: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`)}
	rules, notes := "<rules>\nbe kind\n</rules>", "plain notes"
	piModel := func(reasoning bool, levels map[string]any) map[string]any {
		m := map[string]any{"id": "gpt-4o-mini", "name": "gpt-4o-mini", "api": "openai-completions", "provider": "openai", "baseUrl": "https://api.openai.com/v1",
			"reasoning": reasoning, "input": []string{"text", "image"}, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
			"contextWindow": 128000, "maxTokens": 16384}
		if levels != nil {
			m["thinkingLevelMap"] = levels
		}
		return m
	}
	conversation := []durable.Message{
		{Role: "system", Sections: []durable.SystemSection{{Name: "rules", Text: &rules}, {Name: "notes", Text: &notes}}, ToolsAdded: []durable.ToolSpec{lookup}},
		{Role: "user", Content: "look atthis", Images: []durable.ImageContent{{Data: "iVBORw0KGgo=", MimeType: "image/png"}}, ImageOffsets: []int{7}},
		{Role: "assistant", Thinking: "hmm", Content: "checking", ToolCalls: []durable.ToolCall{{ID: "call_1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)}}},
		{Role: "tool", ToolCallID: "call_1", ToolName: "lookup", Content: "found", Images: []durable.ImageContent{{Data: "R0lGOD", MimeType: "image/gif"}}},
		{Role: "user", Content: "thanks"},
	}
	piConversation := []any{
		// Sections in their order: a Go map would sort them.
		map[string]any{"role": "system", "content": "", "sections": json.RawMessage(`{"rules":"<rules>\nbe kind\n</rules>","notes":"plain notes"}`), "toolsAdded": []any{map[string]any{"name": "lookup", "description": "Look something up", "parameters": json.RawMessage(lookup.Parameters)}}, "timestamp": 1},
		map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "look at"}, map[string]any{"type": "image", "data": "iVBORw0KGgo=", "mimeType": "image/png"}, map[string]any{"type": "text", "text": "this"}}, "timestamp": 2},
		map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "thinking", "thinking": "hmm"}, map[string]any{"type": "text", "text": "checking"}, map[string]any{"type": "toolCall", "id": "call_1", "name": "lookup", "arguments": map[string]string{"q": "x"}}},
			"api": "openai-completions", "provider": "openai", "model": "gpt-4o-mini", "stopReason": "toolUse", "timestamp": 3,
			"usage": map[string]any{"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 2, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}},
		map[string]any{"role": "toolResult", "toolCallId": "call_1", "toolName": "lookup", "content": []any{map[string]any{"type": "text", "text": "found"}, map[string]any{"type": "image", "data": "R0lGOD", "mimeType": "image/gif"}}, "isError": false, "timestamp": 4},
		map[string]any{"role": "user", "content": "thanks", "timestamp": 5},
	}
	str := func(s string) *string { return &s }
	type variant struct {
		adapter OpenAI
		pi      map[string]any
		level   string
	}
	variants := []variant{
		{OpenAI{BaseURL: "https://api.openai.com/v1", MaxTokens: 16384, Window: 128000}, piModel(false, nil), ""},
	}
	reasoning := []struct {
		levels   map[string]*string
		piLevels map[string]any
	}{
		{nil, nil},
		{map[string]*string{"off": str("none"), "minimal": nil}, map[string]any{"off": "none", "minimal": nil}},
		{map[string]*string{"xhigh": str("xhigh")}, map[string]any{"xhigh": "xhigh"}},
	}
	for _, r := range reasoning {
		for _, level := range []string{"", "off", "minimal", "high", "xhigh", "max"} {
			variants = append(variants, variant{OpenAI{BaseURL: "https://api.openai.com/v1", MaxTokens: 16384, Window: 128000, Reasoning: true, ThinkingLevelMap: r.levels}, piModel(true, r.piLevels), level})
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
	for i, v := range variants {
		got := payloadOf(t, v.adapter, durable.ModelRequest{Model: durable.ModelRef{Provider: "openai", ID: "gpt-4o-mini"}, SessionID: "sess-1", ThinkingLevel: v.level, Messages: conversation, Tools: []durable.ToolSpec{lookup}})
		if !crosscheck.Same(got, want[i]) {
			t.Errorf("variant %d (level %q):\n got %s\nwant %s", i, v.level, got, want[i])
		}
	}
}

func TestCrosscheckOpenAIResponses(t *testing.T) {
	streams := [][]string{
		{`data: {"choices":[{"delta":{"content":"Hel"}}]}`, `data: {"choices":[{"delta":{"content":"lo"},"finish_reason":"stop"}]}`, `data: {"choices":[],"usage":{"prompt_tokens":100,"completion_tokens":20,"total_tokens":120,"prompt_tokens_details":{"cached_tokens":40},"completion_tokens_details":{"reasoning_tokens":5}}}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_a","type":"function","function":{"name":"lookup","arguments":"{\"q\":"}}]}}]}`, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}},{"index":1,"id":"call_b","type":"function","function":{"name":"other","arguments":"{}"}}]}}]}`, `data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":3}}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"reasoning_content":"think "}}]}`, `data: {"choices":[{"delta":{"reasoning_content":"more"}}]}`, `data: {"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"content":"cut"},"finish_reason":"length"}]}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"content":"x"},"finish_reason":"content_filter"}]}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":30,"completion_tokens":2,"prompt_cache_hit_tokens":12}}`, `data: [DONE]`},
		// Malformed arguments stay the model's: the call carries argumentsError.
		{`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_m","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"x\""}}]}}]}`, `data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_r","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"line\nbreak\"}"}}]}}]}`, `data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_n","type":"function","function":{"name":"lookup","arguments":"[1,2]"}}]}}]}`, `data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`, `data: [DONE]`},
	}
	var want []struct {
		StopReason   string  `json:"stopReason"`
		ErrorMessage *string `json:"errorMessage"`
		Text         string  `json:"text"`
		Thinking     string  `json:"thinking"`
		ToolCalls    []struct {
			ID             string          `json:"id"`
			Name           string          `json:"name"`
			Arguments      json.RawMessage `json:"arguments"`
			ArgumentsError string          `json:"argumentsError"`
		} `json:"toolCalls"`
		Usage struct {
			Input, Output, CacheRead, CacheWrite int64
		} `json:"usage"`
	}
	crosscheck.Run(t, "openaiStream", map[string]any{"streams": streams}, &want)
	current := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, line := range streams[current] {
			fmt.Fprint(w, line+"\n\n")
		}
	}))
	defer server.Close()
	for current = range streams {
		response, err := (OpenAI{BaseURL: server.URL + "/v1"}).Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Messages: []durable.Message{{Role: "user", Content: "hi"}}}, nil)
		w := want[current]
		if w.StopReason == "error" {
			// Answers that end as errors are errors here; provider text stays
			// sanitized, harness-made messages are kept.
			if err == nil || w.ErrorMessage != nil && err.Error() != *w.ErrorMessage {
				t.Errorf("stream %d: error %v, upstream %v", current, err, *w.ErrorMessage)
			}
			continue
		}
		if err != nil {
			t.Errorf("stream %d: %v", current, err)
			continue
		}
		if response.StopReason != w.StopReason || response.Content != w.Text || response.Thinking != w.Thinking {
			t.Errorf("stream %d: %q %q %q, upstream %q %q %q", current, response.StopReason, response.Content, response.Thinking, w.StopReason, w.Text, w.Thinking)
		}
		if len(response.ToolCalls) != len(w.ToolCalls) {
			t.Errorf("stream %d: %d tool calls, upstream %d", current, len(response.ToolCalls), len(w.ToolCalls))
		}
		for i := range response.ToolCalls {
			if i < len(w.ToolCalls) && (response.ToolCalls[i].ID != w.ToolCalls[i].ID || response.ToolCalls[i].Name != w.ToolCalls[i].Name || !crosscheck.Same(response.ToolCalls[i].Arguments, w.ToolCalls[i].Arguments) || response.ToolCalls[i].ArgumentsError != w.ToolCalls[i].ArgumentsError) {
				t.Errorf("stream %d call %d: %+v, upstream %+v", current, i, response.ToolCalls[i], w.ToolCalls[i])
			}
		}
		u := response.Usage
		if u.InputTokens != w.Usage.Input || u.OutputTokens != w.Usage.Output || u.CacheReadTokens != w.Usage.CacheRead || u.CacheWriteTokens != w.Usage.CacheWrite {
			t.Errorf("stream %d: usage %+v, upstream %+v", current, u, w.Usage)
		}
	}
}
