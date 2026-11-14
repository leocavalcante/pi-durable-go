//go:build crosscheck

package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strings"
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
		// The answer ends with the stream: no [DONE] needed, a finish reason
		// is, unless the compat says the provider sends none.
		{`data: {"choices":[{"delta":{"content":"no done"},"finish_reason":"stop"}]}`},
		{`data: {"choices":[{"delta":{"content":"no reason"}}]}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"content":"inferred"}}]}`},
		{`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_i","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}]}`},
		// OpenRouter's reasoning details: text deltas merge, an encrypted one
		// stays apart, invalid ones are dropped, and no thinking text is needed.
		{`data: {"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.text","text":"Think","index":0,"format":"anthropic-claude-v1"}]}}]}`,
			`data: {"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.text","text":"ing","signature":"sig","index":0},{"type":"bogus"}]}}]}`,
			`data: {"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.encrypted","data":"QUJD","id":"r1"}],"content":"answer"},"finish_reason":"stop"}]}`, `data: [DONE]`},
		// A custom (grammar) tool's input fills its property, "input" for a
		// tool the request did not declare; a call without an index is
		// found by its ID.
		{`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","type":"custom","custom":{"name":"run","input":"pri"}}]}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"custom":{"input":"nt(\"1\")"}}]}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"id":"c2","type":"function","function":{"name":"lookup","arguments":"{\"q\":"}}]}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"id":"c2","function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}]}`},
		{`data: {"choices":[{"delta":{"reasoning":"visible ","reasoning_details":[{"type":"reasoning.summary","summary":"sum"}]}}]}`,
			`data: {"choices":[{"delta":{"reasoning":"thought","reasoning_details":[{"type":"reasoning.summary","summary":"mary","id":"s1"}]},"finish_reason":"stop"}]}`},
	}
	// Per stream, the compat of its model (nil: none).
	noFinishReason := map[string]any{"supportsFinishReason": false}
	compats := make([]map[string]any, len(streams))
	compats[len(streams)-4], compats[len(streams)-3] = noFinishReason, noFinishReason
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
		ReasoningDetails []map[string]any `json:"reasoningDetails"`
	}
	crosscheck.Run(t, "openaiStream", map[string]any{"streams": streams, "compats": compats}, &want)
	current := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, line := range streams[current] {
			fmt.Fprint(w, line+"\n\n")
		}
	}))
	defer server.Close()
	for current = range streams {
		adapter := OpenAI{BaseURL: server.URL + "/v1"}
		if compats[current] != nil {
			adapter.Compat = openAICompatOf(t, compats[current])
		}
		response, err := adapter.Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Messages: []durable.Message{{Role: "user", Content: "hi"}}}, nil)
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
		var replay openAIReplay
		_ = json.Unmarshal(response.ProviderData, &replay)
		if len(replay.ReasoningDetails) != len(w.ReasoningDetails) || len(w.ReasoningDetails) > 0 && !reflect.DeepEqual(replay.ReasoningDetails, w.ReasoningDetails) {
			t.Errorf("stream %d: reasoning details %v, upstream %v", current, replay.ReasoningDetails, w.ReasoningDetails)
		}
		u := response.Usage
		if u.InputTokens != w.Usage.Input || u.OutputTokens != w.Usage.Output || u.CacheReadTokens != w.Usage.CacheRead || u.CacheWriteTokens != w.Usage.CacheWrite {
			t.Errorf("stream %d: usage %+v, upstream %+v", current, u, w.Usage)
		}
	}
}

// Thinking formats pi-ai detects from the URL or takes from compat, and the
// replay of an answer's thinking to the model that gave it.
func TestCrosscheckOpenAIThinkingFormats(t *testing.T) {
	str := func(s string) *string { return &s }
	type variant struct {
		base, format string
		levels       map[string]*string
		level        string
		replay       string // the reasoning field of the earlier answer; "" for none
		sameModel    bool
	}
	var variants []variant
	bases := []string{"https://api.deepseek.com/v1", "https://api.z.ai/api/paas/v4", "https://api.together.xyz/v1", "https://api.ant-ling.com/v1", "https://openrouter.ai/api/v1", "https://example.test/v1"}
	maps := []map[string]*string{nil, {"off": nil}, {"off": str("none-please"), "low": str("light")}}
	for _, base := range bases {
		for _, levels := range maps {
			for _, level := range []string{"", "off", "low", "high"} {
				variants = append(variants, variant{base: base, levels: levels, level: level})
			}
		}
	}
	for _, format := range []string{"qwen", "qwen-chat-template", "string-thinking", "zai", "deepseek"} {
		for _, levels := range maps {
			for _, level := range []string{"off", "low"} {
				variants = append(variants, variant{base: "https://example.test/v1", format: format, levels: levels, level: level})
			}
		}
	}
	for _, field := range []string{"reasoning_content", "reasoning", "reasoning_text"} {
		for _, base := range []string{"https://example.test/v1", "https://api.deepseek.com/v1"} {
			variants = append(variants, variant{base: base, level: "low", replay: field, sameModel: true}, variant{base: base, level: "low", replay: field})
		}
	}
	var input []map[string]any
	for _, v := range variants {
		piModel := map[string]any{"id": "m1", "name": "m1", "api": "openai-completions", "provider": "custom", "baseUrl": v.base,
			"reasoning": true, "input": []string{"text"}, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
			"contextWindow": 128000, "maxTokens": 4096}
		if v.levels != nil {
			piLevels := map[string]any{}
			for k, value := range v.levels {
				if value == nil {
					piLevels[k] = nil
				} else {
					piLevels[k] = *value
				}
			}
			piModel["thinkingLevelMap"] = piLevels
		}
		if v.format != "" {
			piModel["compat"] = map[string]any{"thinkingFormat": v.format}
		}
		answerModel := "other"
		if v.sameModel {
			answerModel = "m1"
		}
		thinking := map[string]any{"type": "thinking", "thinking": "let me see"}
		if v.replay != "" {
			thinking["thinkingSignature"] = v.replay
		}
		messages := []any{
			map[string]any{"role": "user", "content": "hi", "timestamp": 1},
			map[string]any{"role": "assistant", "content": []any{thinking, map[string]any{"type": "text", "text": "ok"}},
				"api": "openai-completions", "provider": "custom", "model": answerModel, "stopReason": "stop", "timestamp": 2,
				"usage": map[string]any{"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 2, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}},
			map[string]any{"role": "user", "content": "more", "timestamp": 3},
		}
		// As upstream's generation: no reasoning option for off.
		options := map[string]any{}
		if v.level != "" && v.level != "off" {
			options["reasoning"] = v.level
		}
		input = append(input, map[string]any{"model": piModel, "messages": messages, "options": options})
	}
	var want []json.RawMessage
	crosscheck.Run(t, "openaiPayload", input, &want)
	for i, v := range variants {
		answer := durable.Message{Role: "assistant", Thinking: "let me see", Content: "ok"}
		if v.replay != "" {
			model := "other"
			if v.sameModel {
				model = "m1"
			}
			answer.ProviderData = json.RawMessage(fmt.Sprintf(`{"format":"openai-completions-v1","provider":"custom","model":%q,"reasoningField":%q}`, model, v.replay))
		}
		adapter := OpenAI{BaseURL: v.base, MaxTokens: 4096, Window: 128000, Reasoning: true, ThinkingLevelMap: v.levels, Compat: OpenAICompat{ThinkingFormat: v.format}, NoImages: true}
		got := payloadOf(t, adapter, durable.ModelRequest{Model: durable.ModelRef{Provider: "custom", ID: "m1"}, ThinkingLevel: v.level,
			Messages: []durable.Message{{Role: "user", Content: "hi"}, answer, {Role: "user", Content: "more"}}})
		if !crosscheck.Same(got, want[i]) {
			t.Errorf("variant %d %+v:\n got %s\nwant %s", i, v, got, want[i])
		}
	}
}

// openAICompatOf is the OpenAICompat of pi-ai's compat JSON.
func openAICompatOf(t *testing.T, compat map[string]any) OpenAICompat {
	t.Helper()
	var c OpenAICompat
	flag := func(key string) *bool {
		if v, ok := compat[key].(bool); ok {
			return &v
		}
		return nil
	}
	text := func(key string) string { s, _ := compat[key].(string); return s }
	raw := func(key string) map[string]json.RawMessage {
		object, ok := compat[key].(map[string]any)
		if !ok {
			return nil
		}
		out := map[string]json.RawMessage{}
		for k, v := range object {
			data, _ := json.Marshal(v)
			out[k] = data
		}
		return out
	}
	c.SupportsStore, c.SupportsDeveloperRole, c.SupportsReasoningEffort = flag("supportsStore"), flag("supportsDeveloperRole"), flag("supportsReasoningEffort")
	c.SupportsUsageInStreaming, c.RequiresToolResultName = flag("supportsUsageInStreaming"), flag("requiresToolResultName")
	c.SupportsFinishReason, c.SendSessionAffinityHeaders = flag("supportsFinishReason"), flag("sendSessionAffinityHeaders")
	c.SessionAffinityFormat = text("sessionAffinityFormat")
	c.RequiresAssistantAfterToolResult, c.RequiresNonNullAssistantContent = flag("requiresAssistantAfterToolResult"), flag("requiresNonNullAssistantContent")
	c.RequiresToolsWithToolHistory, c.RequiresThinkingAsText = flag("requiresToolsWithToolHistory"), flag("requiresThinkingAsText")
	c.RequiresReasoningContentOnAssistantMessages, c.ZaiToolStream = flag("requiresReasoningContentOnAssistantMessages"), flag("zaiToolStream")
	c.SupportsThinkingTokenBudget, c.SupportsLongCacheRetention = flag("supportsThinkingTokenBudget"), flag("supportsLongCacheRetention")
	c.MaxTokensField, c.ThinkingFormat, c.ThinkingTokenBudgetField = text("maxTokensField"), text("thinkingFormat"), text("thinkingTokenBudgetField")
	c.CacheControlFormat, c.PromptCacheKeyMode = text("cacheControlFormat"), text("promptCacheKeyMode")
	c.ChatTemplateKwargs, c.ChatTemplateArgs = raw("chatTemplateKwargs"), raw("chatTemplateArgs")
	if routing, ok := compat["openRouterRouting"]; ok {
		c.OpenRouterRouting, _ = json.Marshal(routing)
	}
	if routing, ok := compat["vercelGatewayRouting"].(map[string]any); ok {
		data, _ := json.Marshal(routing)
		c.VercelGatewayRouting = &GatewayRouting{}
		_ = json.Unmarshal(data, c.VercelGatewayRouting)
	}
	if priority, ok := compat["vllmPriority"].(float64); ok {
		c.VLLMPriority = &priority
	}
	return c
}

// pi-ai's compat settings, one variant each, and provider detection.
func TestCrosscheckOpenAICompat(t *testing.T) {
	lookup := durable.ToolSpec{Name: "lookup", Description: "Look something up", Parameters: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)}
	piTool := map[string]any{"name": "lookup", "description": "Look something up", "parameters": json.RawMessage(lookup.Parameters)}
	usage := map[string]any{"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 2, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}
	type variant struct {
		name, provider, id, base string
		compat                   map[string]any
		reasoning                bool
		level, retention         string
		budgets                  map[string]int
		noTools                  bool
		sampling                 map[string]any
		byLevel                  map[string]map[string]any
	}
	enabled := map[string]any{"$var": "thinking.enabled"}
	variants := []variant{
		{name: "store off", compat: map[string]any{"supportsStore": false}},
		{name: "developer role off", compat: map[string]any{"supportsDeveloperRole": false}, reasoning: true, level: "low"},
		{name: "no effort", compat: map[string]any{"supportsReasoningEffort": false}, reasoning: true, level: "low"},
		{name: "no usage in streaming", compat: map[string]any{"supportsUsageInStreaming": false}},
		{name: "max tokens", compat: map[string]any{"maxTokensField": "max_tokens"}},
		{name: "tool result name", compat: map[string]any{"requiresToolResultName": true}},
		{name: "assistant after tool result", compat: map[string]any{"requiresAssistantAfterToolResult": true}},
		{name: "non-null assistant content", compat: map[string]any{"requiresNonNullAssistantContent": true}},
		{name: "tools with tool history", compat: map[string]any{"requiresToolsWithToolHistory": true}, noTools: true},
		{name: "no tools without the flag", noTools: true},
		{name: "thinking as text", compat: map[string]any{"requiresThinkingAsText": true}, reasoning: true, level: "low"},
		{name: "reasoning content", compat: map[string]any{"requiresReasoningContentOnAssistantMessages": true}, reasoning: true},
		{name: "chat template", compat: map[string]any{"thinkingFormat": "chat-template", "chatTemplateKwargs": map[string]any{"enable_thinking": enabled, "effort": map[string]any{"$var": "thinking.effort"}, "budget": map[string]any{"$var": "thinking.budget"}, "fixed": "x", "only_on": map[string]any{"$var": "thinking.effort", "omitWhenOff": true}}}, reasoning: true, level: "medium"},
		{name: "chat template off", compat: map[string]any{"thinkingFormat": "chat-template", "chatTemplateKwargs": map[string]any{"enable_thinking": enabled, "only_on": map[string]any{"$var": "thinking.effort", "omitWhenOff": true}}}, reasoning: true},
		{name: "baseten", compat: map[string]any{"thinkingFormat": "baseten", "chatTemplateArgs": map[string]any{"enable_thinking": enabled}}, reasoning: true, level: "high"},
		{name: "baseten off", compat: map[string]any{"thinkingFormat": "baseten", "chatTemplateArgs": map[string]any{"enable_thinking": enabled}}, reasoning: true},
		{name: "openrouter routing", compat: map[string]any{"openRouterRouting": map[string]any{"order": []string{"a", "b"}, "allow_fallbacks": false}}},
		{name: "gateway routing", compat: map[string]any{"vercelGatewayRouting": map[string]any{"only": []string{"bedrock"}}}},
		{name: "zai tool stream", compat: map[string]any{"zaiToolStream": true}},
		{name: "budget field", compat: map[string]any{"thinkingTokenBudgetField": "thinking_budget"}, reasoning: true, level: "low", budgets: map[string]int{"low": 3000}},
		{name: "budget alias", compat: map[string]any{"supportsThinkingTokenBudget": true}, reasoning: true, level: "xhigh"},
		{name: "budget clamped", compat: map[string]any{"thinkingTokenBudgetField": "thinking_budget_tokens"}, reasoning: true, level: "high", budgets: map[string]int{"high": 100000}},
		{name: "long retention", retention: "long"},
		{name: "long retention unsupported", compat: map[string]any{"supportsLongCacheRetention": false}, retention: "long"},
		{name: "cache control", compat: map[string]any{"cacheControlFormat": "anthropic"}},
		{name: "cache control long", compat: map[string]any{"cacheControlFormat": "anthropic"}, retention: "long"},
		{name: "cache control none", compat: map[string]any{"cacheControlFormat": "anthropic"}, retention: "none"},
		{name: "cache key enabled", compat: map[string]any{"promptCacheKeyMode": "enabled"}},
		{name: "cache key disabled", base: "https://api.openai.com/v1", compat: map[string]any{"promptCacheKeyMode": "disabled"}},
		{name: "vllm priority", compat: map[string]any{"vllmPriority": -2.5}},
		{name: "sampling", sampling: map[string]any{"temperature": 0.3, "top_p": 0.9}, byLevel: map[string]map[string]any{"low": {"temperature": 0.7}}, reasoning: true, level: "low"},
		{name: "deepseek by provider", provider: "deepseek", reasoning: true, level: "low"},
		{name: "zai by provider", provider: "zai", reasoning: true, level: "low"},
		{name: "openrouter anthropic by provider", provider: "openrouter", id: "anthropic/claude-x", reasoning: true, level: "low"},
		{name: "openrouter openai by provider", provider: "openrouter", id: "openai/gpt-x", reasoning: true},
		{name: "xai by provider", provider: "xai", reasoning: true, level: "low"},
		{name: "together by provider", provider: "together", reasoning: true, level: "low", retention: "long"},
	}
	var input []map[string]any
	for _, v := range variants {
		provider, id, base := v.provider, v.id, v.base
		if provider == "" {
			provider = "custom"
		}
		if id == "" {
			id = "m1"
		}
		if base == "" {
			base = "https://example.test/v1"
		}
		piModel := map[string]any{"id": id, "name": id, "api": "openai-completions", "provider": provider, "baseUrl": base,
			"reasoning": v.reasoning, "input": []string{"text", "image"}, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
			"contextWindow": 128000, "maxTokens": 8192}
		if v.compat != nil {
			piModel["compat"] = v.compat
		}
		if v.sampling != nil {
			piModel["samplingParams"] = v.sampling
		}
		if v.byLevel != nil {
			piModel["samplingParamsByThinkingLevel"] = v.byLevel
		}
		system := map[string]any{"role": "system", "content": "Be brief.", "timestamp": 1}
		if !v.noTools {
			system["toolsAdded"] = []any{piTool}
		}
		messages := []any{
			system,
			map[string]any{"role": "user", "content": "hi", "timestamp": 2},
			map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "thinking", "thinking": "plan", "thinkingSignature": "reasoning_content"},
				map[string]any{"type": "toolCall", "id": "c1", "name": "lookup", "arguments": map[string]string{"q": "x"}}},
				"api": "openai-completions", "provider": provider, "model": id, "stopReason": "toolUse", "timestamp": 3, "usage": usage},
			map[string]any{"role": "toolResult", "toolCallId": "c1", "toolName": "lookup", "content": []any{map[string]any{"type": "text", "text": "found"}}, "isError": false, "timestamp": 4},
			map[string]any{"role": "user", "content": "next", "timestamp": 5},
		}
		options := map[string]any{"sessionId": "sess-1"}
		if v.level != "" {
			options["reasoning"] = v.level
		}
		if v.retention != "" {
			options["cacheRetention"] = v.retention
		}
		if v.budgets != nil {
			options["thinkingBudgets"] = v.budgets
		}
		input = append(input, map[string]any{"model": piModel, "messages": messages, "options": options})
	}
	var want []json.RawMessage
	crosscheck.Run(t, "openaiPayload", input, &want)
	for i, v := range variants {
		provider, id, base := v.provider, v.id, v.base
		if provider == "" {
			provider = "custom"
		}
		if id == "" {
			id = "m1"
		}
		if base == "" {
			base = "https://example.test/v1"
		}
		adapter := OpenAI{BaseURL: base, MaxTokens: 8192, Window: 128000, Reasoning: v.reasoning, Compat: openAICompatOf(t, v.compat),
			SamplingParams: v.sampling, SamplingParamsByThinkingLevel: v.byLevel}
		system := durable.Message{Role: "system", Content: "Be brief."}
		var tools []durable.ToolSpec
		if !v.noTools {
			system.ToolsAdded = []durable.ToolSpec{lookup}
			tools = []durable.ToolSpec{lookup}
		}
		answer := durable.Message{Role: "assistant", Thinking: "plan", ToolCalls: []durable.ToolCall{{ID: "c1", Name: "lookup", Arguments: json.RawMessage(`{"q":"x"}`)}},
			ProviderData: json.RawMessage(fmt.Sprintf(`{"format":"openai-completions-v1","provider":%q,"model":%q,"reasoningField":"reasoning_content"}`, provider, id))}
		req := durable.ModelRequest{Model: durable.ModelRef{Provider: provider, ID: id}, SessionID: "sess-1", ThinkingLevel: v.level, Tools: tools,
			Messages: []durable.Message{system, {Role: "user", Content: "hi"}, answer, {Role: "tool", ToolCallID: "c1", ToolName: "lookup", Content: "found"}, {Role: "user", Content: "next"}}}
		if v.retention != "" || v.budgets != nil {
			req.Stream = &durable.StreamOptions{CacheRetention: v.retention, ThinkingBudgets: v.budgets}
		}
		if got := payloadOf(t, adapter, req); !crosscheck.Same(got, want[i]) {
			t.Errorf("%s:\n got %s\nwant %s", v.name, got, want[i])
		}
	}
}

// Attribution and session-affinity headers, as a local server receives
// them from pi-ai's openai-completions and from model.OpenAI.
func TestCrosscheckOpenAIHeaders(t *testing.T) {
	attribution := map[string]any{"name": "pi", "url": "https://pi.dev", "categories": []string{"cli", "agent"}}
	type variant struct {
		provider, base string
		compat         map[string]any
		session        string
		attribution    map[string]any
		headers        map[string]string
	}
	variants := []variant{
		{provider: "openrouter", base: "https://openrouter.ai/api/v1", session: "s-1", attribution: attribution},
		{provider: "custom", base: "https://openrouter.ai/api/v1", session: "s-1"},
		{provider: "nvidia", base: "https://example.test/v1", session: "s-1", attribution: attribution},
		{provider: "opencode", base: "https://example.test/v1", session: "s-1", attribution: attribution},
		{provider: "custom", base: "https://example.test/v1", session: "s-1", attribution: attribution},
		{provider: "custom", base: "https://example.test/v1", session: "s-1", compat: map[string]any{"sendSessionAffinityHeaders": true}},
		{provider: "custom", base: "https://example.test/v1", session: "s-1", compat: map[string]any{"sendSessionAffinityHeaders": true, "sessionAffinityFormat": "openai-nosession"}},
		{provider: "openrouter", base: "https://openrouter.ai/api/v1", session: "s-1", compat: map[string]any{"sendSessionAffinityHeaders": false}},
		{provider: "openrouter", base: "https://openrouter.ai/api/v1", session: "s-1", headers: map[string]string{"x-session-id": "mine", "x-extra": "1"}},
	}
	var input []map[string]any
	for _, v := range variants {
		model := map[string]any{"id": "m1", "name": "m1", "api": "openai-completions", "provider": v.provider, "baseUrl": v.base,
			"reasoning": false, "input": []string{"text"}, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
			"contextWindow": 128000, "maxTokens": 1000}
		if v.compat != nil {
			model["compat"] = v.compat
		}
		options := map[string]any{}
		if v.session != "" {
			options["sessionId"] = v.session
		}
		if v.attribution != nil {
			options["identity"] = map[string]any{"attribution": v.attribution}
		}
		if v.headers != nil {
			options["headers"] = v.headers
		}
		input = append(input, map[string]any{"model": model, "options": options})
	}
	var want []map[string]string
	crosscheck.Run(t, "openaiHeaders", input, &want)
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
	for i, v := range variants {
		adapter := OpenAI{BaseURL: server.URL + "/v1?for=" + url.QueryEscape(v.base), Compat: openAICompatOf(t, v.compat)}
		req := durable.ModelRequest{Model: durable.ModelRef{Provider: v.provider, ID: "m1"}, SessionID: v.session, Messages: []durable.Message{{Role: "user", Content: "hi"}}}
		if v.attribution != nil {
			data, _ := json.Marshal(v.attribution)
			req.Identity = &durable.ClientIdentity{Attribution: data}
		}
		if v.headers != nil {
			req.Stream = &durable.StreamOptions{Headers: v.headers}
		}
		if _, err := adapter.Generate(context.Background(), req, nil); err != nil {
			t.Fatalf("variant %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, want[i]) {
			t.Errorf("variant %d %+v:\n got %v\nwant %v", i, v, got, want[i])
		}
	}
}

// pi-ai's transformMessages: tool call IDs of other models' answers, failed
// and aborted answers, and tool calls left without results.
func TestCrosscheckOpenAITransform(t *testing.T) {
	usage := map[string]any{"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 2, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}
	longPipe := "call_abc|fc_" + strings.Repeat("9", 60)
	type answer struct {
		calls      []string
		same       bool
		stopReason string
		text       string
	}
	type turn struct {
		answer  *answer
		results []string
		user    string
	}
	variants := []struct {
		provider string
		turns    []turn
	}{
		{"custom", []turn{{answer: &answer{calls: []string{longPipe}}, results: []string{longPipe}}, {user: "next"}}},
		{"custom", []turn{{answer: &answer{calls: []string{"call_1|item_2"}}, results: []string{"call_1|item_2"}}, {user: "next"}}},
		{"openai", []turn{{answer: &answer{calls: []string{strings.Repeat("x", 45)}}, results: []string{strings.Repeat("x", 45)}}, {user: "next"}}},
		{"openai", []turn{{answer: &answer{calls: []string{longPipe}, same: true}, results: []string{longPipe}}, {user: "next"}}},
		{"custom", []turn{{answer: &answer{text: "cut off", stopReason: "aborted"}}, {user: "again"}}},
		{"custom", []turn{{answer: &answer{text: "failed", stopReason: "error"}}, {user: "again"}}},
		{"custom", []turn{{answer: &answer{calls: []string{"a", "b"}}, results: []string{"a"}}, {user: "next"}}},
		{"custom", []turn{{answer: &answer{calls: []string{"a"}}}}},
	}
	var input []map[string]any
	var requests []durable.ModelRequest
	for _, v := range variants {
		piMessages := []any{map[string]any{"role": "user", "content": "hi", "timestamp": 1}}
		messages := []durable.Message{{Role: "user", Content: "hi"}}
		for _, turn := range v.turns {
			if a := turn.answer; a != nil {
				model := "other"
				if a.same {
					model = "m1"
				}
				stop := a.stopReason
				if stop == "" {
					stop = "toolUse"
				}
				var content []any
				m := durable.Message{Role: "assistant", Content: a.text, StopReason: stop}
				if a.text != "" {
					content = append(content, map[string]any{"type": "text", "text": a.text})
				}
				for _, id := range a.calls {
					content = append(content, map[string]any{"type": "toolCall", "id": id, "name": "lookup", "arguments": map[string]any{}})
					m.ToolCalls = append(m.ToolCalls, durable.ToolCall{ID: id, Name: "lookup", Arguments: json.RawMessage(`{}`)})
				}
				if a.same {
					m.ProviderData = json.RawMessage(fmt.Sprintf(`{"format":"openai-completions-v1","provider":%q,"model":"m1"}`, v.provider))
				}
				piMessages = append(piMessages, map[string]any{"role": "assistant", "content": content, "api": "openai-completions", "provider": v.provider, "model": model, "stopReason": stop, "timestamp": 2, "usage": usage})
				messages = append(messages, m)
			}
			for _, id := range turn.results {
				piMessages = append(piMessages, map[string]any{"role": "toolResult", "toolCallId": id, "toolName": "lookup", "content": []any{map[string]any{"type": "text", "text": "found"}}, "isError": false, "timestamp": 3})
				messages = append(messages, durable.Message{Role: "tool", ToolCallID: id, ToolName: "lookup", Content: "found"})
			}
			if turn.user != "" {
				piMessages = append(piMessages, map[string]any{"role": "user", "content": turn.user, "timestamp": 4})
				messages = append(messages, durable.Message{Role: "user", Content: turn.user})
			}
		}
		model := map[string]any{"id": "m1", "name": "m1", "api": "openai-completions", "provider": v.provider, "baseUrl": "https://example.test/v1",
			"reasoning": false, "input": []string{"text"}, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 128000, "maxTokens": 1000}
		input = append(input, map[string]any{"model": model, "messages": piMessages})
		requests = append(requests, durable.ModelRequest{Model: durable.ModelRef{Provider: v.provider, ID: "m1"}, Messages: messages})
	}
	var want []json.RawMessage
	crosscheck.Run(t, "openaiPayload", input, &want)
	for i, req := range requests {
		if got := payloadOf(t, OpenAI{BaseURL: "https://example.test/v1", MaxTokens: 1000, Window: 128000}, req); !crosscheck.Same(got, want[i]) {
			t.Errorf("variant %d:\n got %s\nwant %s", i, got, want[i])
		}
	}
}

// Mid-conversation system messages: updates in place, tool additions
// declared in place, removals folded into the request tools, and a system
// message held after the results of the tool calls it interrupts.
func TestCrosscheckOpenAIMidConversationSystemMessages(t *testing.T) {
	usage := map[string]any{"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 2, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}
	lookup := durable.ToolSpec{Name: "lookup", Description: "Look", Parameters: json.RawMessage(`{"type":"object"}`)}
	extra := durable.ToolSpec{Name: "extra", Description: "More", Parameters: json.RawMessage(`{"type":"object"}`)}
	piTool := func(s durable.ToolSpec) map[string]any {
		return map[string]any{"name": s.Name, "description": s.Description, "parameters": json.RawMessage(s.Parameters)}
	}
	r1, r2 := "rule one", "rule two"
	type step struct {
		go_ durable.Message
		pi  map[string]any
	}
	system := func(content string, sections []durable.SystemSection, piSections string, added []durable.ToolSpec, removed []string) step {
		m := durable.Message{Role: "system", Content: content, Sections: sections, ToolsAdded: added, ToolsRemoved: removed}
		pi := map[string]any{"role": "system", "content": content, "timestamp": 1}
		if piSections != "" {
			pi["sections"] = json.RawMessage(piSections)
		}
		var piAdded []any
		for _, a := range added {
			piAdded = append(piAdded, piTool(a))
		}
		if len(piAdded) > 0 {
			pi["toolsAdded"] = piAdded
		}
		var piRemoved []any
		for _, name := range removed {
			piRemoved = append(piRemoved, map[string]any{"name": name})
		}
		if len(piRemoved) > 0 {
			pi["toolsRemoved"] = piRemoved
		}
		return step{m, pi}
	}
	user := func(text string) step {
		return step{durable.Message{Role: "user", Content: text}, map[string]any{"role": "user", "content": text, "timestamp": 2}}
	}
	answer := func(text string, calls ...string) step {
		m := durable.Message{Role: "assistant", Content: text, StopReason: "stop"}
		content := []any{}
		if text != "" {
			content = append(content, map[string]any{"type": "text", "text": text})
		}
		for _, id := range calls {
			m.ToolCalls = append(m.ToolCalls, durable.ToolCall{ID: id, Name: "lookup", Arguments: json.RawMessage(`{}`)})
			content = append(content, map[string]any{"type": "toolCall", "id": id, "name": "lookup", "arguments": map[string]any{}})
			m.StopReason = "toolUse"
		}
		return step{m, map[string]any{"role": "assistant", "content": content, "api": "openai-completions", "provider": "custom", "model": "other", "stopReason": m.StopReason, "timestamp": 3, "usage": usage}}
	}
	result := func(id string) step {
		return step{durable.Message{Role: "tool", ToolCallID: id, ToolName: "lookup", Content: "found"},
			map[string]any{"role": "toolResult", "toolCallId": id, "toolName": "lookup", "content": []any{map[string]any{"type": "text", "text": "found"}}, "isError": false, "timestamp": 4}}
	}
	base := system("Base prompt.", []durable.SystemSection{{Name: "rules", Text: &r1}}, `{"rules":"rule one"}`, []durable.ToolSpec{lookup}, nil)
	update := system("", []durable.SystemSection{{Name: "rules", Text: &r2}, {Name: "gone", Text: nil}}, `{"rules":"rule two","gone":null}`, []durable.ToolSpec{extra}, nil)
	removal := system("Note.", nil, "", []durable.ToolSpec{extra}, []string{"lookup"})
	variants := []struct {
		compat map[string]any
		steps  []step
	}{
		{map[string]any{"supportsMidConvoSystemMessages": true}, []step{base, user("hi"), answer("ok"), update, user("more")}},
		{map[string]any{"supportsMidConvoSystemMessages": true, "supportsMidConvoToolAdditions": true}, []step{base, user("hi"), answer("ok"), update, user("more")}},
		{map[string]any{"supportsMidConvoSystemMessages": true, "supportsMidConvoToolAdditions": true}, []step{base, user("hi"), answer("ok"), removal, user("more")}},
		{map[string]any{"supportsMidConvoSystemMessages": true}, []step{base, user("hi"), answer("", "c1"), update, result("c1"), user("more")}},
		{map[string]any{"supportsMidConvoSystemMessages": true, "supportsMidConvoToolAdditions": true}, []step{base, user("hi"), answer("", "c1"), update, result("c1"), user("more")}},
		{map[string]any{"supportsMidConvoSystemMessages": true}, []step{user("hi"), base, answer("ok"), user("more")}},
		{nil, []step{base, user("hi"), answer("ok"), update, user("more")}},
	}
	var input []map[string]any
	for _, v := range variants {
		model := map[string]any{"id": "m1", "name": "m1", "api": "openai-completions", "provider": "custom", "baseUrl": "https://example.test/v1",
			"reasoning": false, "input": []string{"text"}, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 128000, "maxTokens": 1000}
		if v.compat != nil {
			model["compat"] = v.compat
		}
		var messages []any
		for _, s := range v.steps {
			messages = append(messages, s.pi)
		}
		input = append(input, map[string]any{"model": model, "messages": messages})
	}
	var want []json.RawMessage
	crosscheck.Run(t, "openaiPayload", input, &want)
	for i, v := range variants {
		var messages []durable.Message
		for _, s := range v.steps {
			messages = append(messages, s.go_)
		}
		var compat OpenAICompat
		if v.compat != nil {
			yes := true
			compat.SupportsMidConvoSystemMessages = &yes
			if v.compat["supportsMidConvoToolAdditions"] == true {
				compat.SupportsMidConvoToolAdditions = &yes
			}
		}
		req := durable.ModelRequest{Model: durable.ModelRef{Provider: "custom", ID: "m1"}, Messages: messages, Tools: durable.CurrentTools(messages)}
		if got := payloadOf(t, OpenAI{BaseURL: "https://example.test/v1", MaxTokens: 1000, Window: 128000, Compat: compat}, req); !crosscheck.Same(got, want[i]) {
			t.Errorf("variant %d:\n got %s\nwant %s", i, got, want[i])
		}
	}
}

// OpenRouter's reasoning details go back to the model that gave them, in
// place of the reasoning field.
func TestCrosscheckOpenAIReasoningDetailsReplay(t *testing.T) {
	usage := map[string]any{"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 2, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}
	details := []map[string]any{{"type": "reasoning.text", "text": "Thinking", "signature": "sig", "index": float64(0), "format": "anthropic-claude-v1"}, {"type": "reasoning.encrypted", "data": "QUJD", "id": "r1"}}
	detailsJSON, _ := json.Marshal(details)
	variants := []struct {
		same     bool
		thinking string
	}{{true, ""}, {true, "Thinking"}, {false, "Thinking"}}
	var input []map[string]any
	for _, v := range variants {
		model := "other"
		if v.same {
			model = "m1"
		}
		messages := []any{
			map[string]any{"role": "user", "content": "hi", "timestamp": 1},
			map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "thinking", "thinking": v.thinking, "thinkingSignature": string(detailsJSON)}, map[string]any{"type": "text", "text": "answer"}},
				"api": "openai-completions", "provider": "openrouter", "model": model, "stopReason": "stop", "timestamp": 2, "usage": usage},
			map[string]any{"role": "user", "content": "more", "timestamp": 3},
		}
		pi := map[string]any{"id": "m1", "name": "m1", "api": "openai-completions", "provider": "openrouter", "baseUrl": "https://openrouter.ai/api/v1",
			"reasoning": true, "input": []string{"text"}, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 128000, "maxTokens": 1000}
		input = append(input, map[string]any{"model": pi, "messages": messages})
	}
	var want []json.RawMessage
	crosscheck.Run(t, "openaiPayload", input, &want)
	for i, v := range variants {
		model := "other"
		if v.same {
			model = "m1"
		}
		replay, _ := json.Marshal(openAIReplay{Format: openAIReplayFormat, Provider: "openrouter", Model: model, ReasoningField: "reasoning", ReasoningDetails: details})
		req := durable.ModelRequest{Model: durable.ModelRef{Provider: "openrouter", ID: "m1"}, Messages: []durable.Message{{Role: "user", Content: "hi"},
			{Role: "assistant", Thinking: v.thinking, Content: "answer", ProviderData: replay}, {Role: "user", Content: "more"}}}
		if got := payloadOf(t, OpenAI{BaseURL: "https://openrouter.ai/api/v1", MaxTokens: 1000, Window: 128000, Reasoning: true}, req); !crosscheck.Same(got, want[i]) {
			t.Errorf("variant %d %+v:\n got %s\nwant %s", i, v, got, want[i])
		}
	}
}

// Grammar-constrained tools: custom tools in requests, their calls replayed
// as input text, their input streamed into arguments, and pi-ai's errors.
func TestCrosscheckOpenAIGrammarTools(t *testing.T) {
	usage := map[string]any{"input": 1, "output": 1, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 2, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}
	codeSchema := json.RawMessage(`{"type":"object","properties":{"code":{"type":"string"}},"required":["code"]}`)
	tool := func(sampling string, parameters json.RawMessage) durable.ToolSpec {
		return durable.ToolSpec{Name: "run", Description: "Run code", Parameters: parameters, ConstrainedSampling: json.RawMessage(sampling)}
	}
	variants := []struct {
		tool      durable.ToolSpec
		supported bool
		callArgs  string // a replayed call's arguments; "" for none
	}{
		{tool(`{"type":"grammar","variants":{"openai_lark":"start: /.+/"}}`, codeSchema), true, ""},
		{tool(`{"type":"grammar","variants":{"openai_regex":"^print\\(.*\\)$"}}`, codeSchema), true, `{"code":"print(1)"}`},
		{tool(`{"type":"grammar","variants":{"openai_lark":"start: /.+/","openai_regex":"x"}}`, codeSchema), false, `{"code":"print(1)"}`},
		{tool(`{"type":"grammar","variants":{}}`, codeSchema), true, ""},
		{tool(`{"type":"grammar","variants":{"openai_lark":"start: /.+/"}}`, json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"}},"required":["a","b"]}`)), true, ""},
		{tool(`{"type":"grammar","variants":{"openai_lark":"start: /.+/"}}`, json.RawMessage(`{"type":"object","properties":{"code":{"type":"number"}},"required":["code"]}`)), true, ""},
		{tool(`{"type":"grammar","variants":{"openai_lark":"start: /.+/"}}`, codeSchema), true, `{"code":7}`},
	}
	var input []map[string]any
	for _, v := range variants {
		piTool := map[string]any{"name": v.tool.Name, "description": v.tool.Description, "parameters": v.tool.Parameters, "constrainedSampling": v.tool.ConstrainedSampling}
		messages := []any{map[string]any{"role": "system", "content": "", "toolsAdded": []any{piTool}, "timestamp": 1}, map[string]any{"role": "user", "content": "go", "timestamp": 2}}
		if v.callArgs != "" {
			messages = append(messages,
				map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "toolCall", "id": "c1", "name": "run", "arguments": json.RawMessage(v.callArgs)}},
					"api": "openai-completions", "provider": "custom", "model": "m1", "stopReason": "toolUse", "timestamp": 3, "usage": usage},
				map[string]any{"role": "toolResult", "toolCallId": "c1", "toolName": "run", "content": []any{map[string]any{"type": "text", "text": "1"}}, "isError": false, "timestamp": 4},
				map[string]any{"role": "user", "content": "again", "timestamp": 5})
		}
		model := map[string]any{"id": "m1", "name": "m1", "api": "openai-completions", "provider": "custom", "baseUrl": "https://example.test/v1",
			"reasoning": false, "input": []string{"text"}, "cost": map[string]int{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 128000, "maxTokens": 1000,
			"compat": map[string]any{"supportsOpenAIGrammarTools": v.supported}}
		input = append(input, map[string]any{"model": model, "messages": messages})
	}
	var want []json.RawMessage
	crosscheck.Run(t, "openaiPayload", input, &want)
	for i, v := range variants {
		supported := v.supported
		messages := []durable.Message{{Role: "system", ToolsAdded: []durable.ToolSpec{v.tool}}, {Role: "user", Content: "go"}}
		if v.callArgs != "" {
			messages = append(messages,
				durable.Message{Role: "assistant", ToolCalls: []durable.ToolCall{{ID: "c1", Name: "run", Arguments: json.RawMessage(v.callArgs)}}, StopReason: "toolUse",
					ProviderData: json.RawMessage(`{"format":"openai-completions-v1","provider":"custom","model":"m1"}`)},
				durable.Message{Role: "tool", ToolCallID: "c1", ToolName: "run", Content: "1"}, durable.Message{Role: "user", Content: "again"})
		}
		adapter := OpenAI{BaseURL: "https://example.test/v1", MaxTokens: 1000, Window: 128000, Compat: OpenAICompat{SupportsOpenAIGrammarTools: &supported}}
		req := durable.ModelRequest{Model: durable.ModelRef{Provider: "custom", ID: "m1"}, Messages: messages, Tools: []durable.ToolSpec{v.tool}}
		var wantError struct {
			Error *string `json:"error"`
		}
		if json.Unmarshal(want[i], &wantError) == nil && wantError.Error != nil {
			_, err := adapter.Generate(context.Background(), req, nil)
			if err == nil || err.Error() != *wantError.Error {
				t.Errorf("variant %d: error %v, upstream %q", i, err, *wantError.Error)
			}
			continue
		}
		if got := payloadOf(t, adapter, req); !crosscheck.Same(got, want[i]) {
			t.Errorf("variant %d:\n got %s\nwant %s", i, got, want[i])
		}
	}
}
