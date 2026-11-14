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
		// The answer ends with the stream: no [DONE] needed, a finish reason
		// is, unless the compat says the provider sends none.
		{`data: {"choices":[{"delta":{"content":"no done"},"finish_reason":"stop"}]}`},
		{`data: {"choices":[{"delta":{"content":"no reason"}}]}`, `data: [DONE]`},
		{`data: {"choices":[{"delta":{"content":"inferred"}}]}`},
		{`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_i","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}]}`},
	}
	// Per stream, the compat of its model (nil: none).
	noFinishReason := map[string]any{"supportsFinishReason": false}
	compats := make([]map[string]any, len(streams))
	compats[len(streams)-2], compats[len(streams)-1] = noFinishReason, noFinishReason
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
	c.SupportsFinishReason = flag("supportsFinishReason")
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
