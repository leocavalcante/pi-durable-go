package model

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

func TestOpenAIStreamingTextToolsAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["stream"] != true {
			t.Errorf("stream flag = %v", body["stream"])
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("authorization missing")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		write := func(s string) { _, _ = w.Write([]byte(s)); w.(http.Flusher).Flush() }
		write("data: {\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\n\n")
		write(`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"q\":"}}]}}]}` + "\n\n")
		write(`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}` + "\n\n")
		write("data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n")
		write("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n")
		write("data: [DONE]\n\n")
	}))
	defer server.Close()
	model := OpenAI{BaseURL: server.URL + "/v1", APIKey: "test-key"}
	var deltas []string
	got, err := model.Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "test-model"}, Messages: []durable.Message{{Role: "user", Content: "hi"}}, Tools: []durable.ToolSpec{{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}}}, func(s string) error { deltas = append(deltas, s); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "hello " || strings.Join(deltas, "") != got.Content {
		t.Fatalf("content=%q deltas=%q", got.Content, deltas)
	}
	if got.Usage.InputTokens != 7 || got.Usage.OutputTokens != 3 {
		t.Fatalf("usage=%+v", got.Usage)
	}
	if len(got.ToolCalls) != 1 || got.ToolCalls[0].Name != "lookup" || string(got.ToolCalls[0].Arguments) != `{"q":"x"}` {
		t.Fatalf("tool calls=%+v", got.ToolCalls)
	}
	if got.StopReason != "toolUse" {
		t.Fatalf("stop reason=%q", got.StopReason)
	}
}

func TestOpenAISanitizesHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte("sensitive provider response"))
	}))
	defer server.Close()
	_, err := (OpenAI{BaseURL: server.URL}).Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}}, nil)
	if err == nil || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("error=%v", err)
	}
}

func TestOpenAIRejectsTruncatedSSE(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))
	}))
	defer server.Close()
	_, err := (OpenAI{BaseURL: server.URL}).Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}}, nil)
	if err == nil || err.Error() != "Stream ended without finish_reason" || !durable.IsRetryableModelError(err) {
		t.Fatalf("error=%v", err)
	}
}

// The answer ends with the stream, its finish reason saying how (pi-ai):
// [DONE] is not needed, and a provider without finish reasons
// (SupportsFinishReason false) ends with a stop or a tool use.
func TestOpenAIStreamsEndAsPiAis(t *testing.T) {
	no := false
	for _, c := range []struct {
		lines  []string
		compat OpenAICompat
		stop   string
		err    string
	}{
		{[]string{`data: {"choices":[{"delta":{"content":"hi"},"finish_reason":"stop"}]}`}, OpenAICompat{}, "stop", ""},
		{[]string{`data: {"choices":[{"delta":{"content":"hi"}}]}`, `data: [DONE]`}, OpenAICompat{}, "", "Stream ended without finish_reason"},
		{[]string{`data: {"choices":[{"delta":{"content":"hi"}}]}`, `data: [DONE]`}, OpenAICompat{SupportsFinishReason: &no}, "stop", ""},
		{[]string{`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","type":"function","function":{"name":"f","arguments":"{}"}}]}}]}`}, OpenAICompat{SupportsFinishReason: &no}, "toolUse", ""},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for _, line := range c.lines {
				_, _ = w.Write([]byte(line + "\n\n"))
			}
		}))
		got, err := (OpenAI{BaseURL: server.URL, Compat: c.compat}).Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}}, nil)
		server.Close()
		if c.err != "" {
			if err == nil || err.Error() != c.err {
				t.Errorf("%v: error=%v", c.lines, err)
			}
			continue
		}
		if err != nil || got.StopReason != c.stop {
			t.Errorf("%v: stop=%q err=%v", c.lines, got.StopReason, err)
		}
	}
}

func TestOpenAICancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := (OpenAI{BaseURL: server.URL}).Generate(ctx, durable.ModelRequest{Model: durable.ModelRef{ID: "m"}}, nil)
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request did not cancel")
	}
}

func TestOpenAIStreamsReasoningAndToolCallEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		write := func(s string) { _, _ = w.Write([]byte(s)); w.(http.Flusher).Flush() }
		// Both reasoning fields with the same text: the first non-empty one counts.
		write(`data: {"choices":[{"delta":{"reasoning_content":"plan","reasoning":"plan"}}]}` + "\n\n")
		write(`data: {"choices":[{"delta":{"content":"reading"}}]}` + "\n\n")
		write(`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"read","arguments":"{\"path\": \"a"}}]}}]}` + "\n\n")
		write(`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":".txt\"}"}}]}}]}` + "\n\n")
		write(`data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}` + "\n\n")
		write("data: [DONE]\n\n")
	}))
	defer server.Close()
	var events []durable.StreamEvent
	got, err := OpenAI{BaseURL: server.URL + "/v1"}.GenerateEvents(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Messages: []durable.Message{{Role: "user", Content: "hi"}}},
		func(event durable.StreamEvent) error { events = append(events, event); return nil })
	if err != nil {
		t.Fatal(err)
	}
	want := []durable.StreamEvent{
		{Type: "thinking_delta", Index: 0, Delta: "plan"},
		{Type: "text_delta", Index: 1, Delta: "reading"},
		{Type: "toolcall_start", Index: 2, ID: "call-1", Name: "read"},
		{Type: "toolcall_delta", Index: 2, Delta: `{"path": "a`},
		{Type: "toolcall_delta", Index: 2, Delta: `.txt"}`},
		{Type: "toolcall_end", Index: 2},
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%+v\nwant %+v", events, want)
	}
	if got.Thinking != "plan" || got.Content != "reading" || len(got.ToolCalls) != 1 {
		t.Fatalf("response=%+v", got)
	}
}

func TestOpenAISendsThePayloadItsHookReturns(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	var api string
	m := &OpenAI{BaseURL: server.URL + "/v1", APIKey: "test-key"}
	_, err := m.Generate(context.Background(), durable.ModelRequest{
		Model:    durable.ModelRef{Provider: "openai", ID: "gpt-test"},
		Messages: []durable.Message{{Role: "user", Content: "hi"}},
		OnPayload: func(_ context.Context, format string, payload json.RawMessage) (json.RawMessage, error) {
			api = format
			var body map[string]any
			if err := json.Unmarshal(payload, &body); err != nil {
				return nil, err
			}
			body["user"] = "hooked"
			return json.Marshal(body)
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if api != "openai-completions" || got["user"] != "hooked" || got["model"] != "gpt-test" {
		t.Fatalf("api=%q body=%v", api, got)
	}
}

// Expectations are pi-ai 1.5.2's openai-completions reasoning_effort for the
// same models and levels.
func TestOpenAIReasoningEffortIsPiAis(t *testing.T) {
	str := func(s string) *string { return &s }
	models := []OpenAI{
		{BaseURL: "https://api.openai.com/v1", Reasoning: true},
		{BaseURL: "https://api.openai.com/v1", Reasoning: true, ThinkingLevelMap: map[string]*string{"off": str("none"), "minimal": nil}},
		{BaseURL: "https://api.openai.com/v1", Reasoning: true, ThinkingLevelMap: map[string]*string{"xhigh": str("xhigh")}},
		{BaseURL: "https://api.openai.com/v1"},
	}
	want := []string{"", "", "minimal", "high", "high", "high", "none", "none", "low", "high", "high", "high", "", "", "minimal", "high", "xhigh", "xhigh", "", "", "", "", "", ""}
	i := 0
	for _, m := range models {
		for _, level := range []string{"", "off", "minimal", "high", "xhigh", "max"} {
			var effort string
			_, _ = m.Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "o"}, ThinkingLevel: level, Messages: []durable.Message{{Role: "user", Content: "hi"}},
				OnPayload: func(_ context.Context, _ string, payload json.RawMessage) (json.RawMessage, error) {
					var body struct {
						Effort string `json:"reasoning_effort"`
					}
					_ = json.Unmarshal(payload, &body)
					effort = body.Effort
					return nil, errors.New("captured")
				}}, nil)
			if effort != want[i] {
				t.Errorf("model %d level %q: %q, want %q", i/6, level, effort, want[i])
			}
			i++
		}
	}
}

// Usage and stop reasons are read as pi-ai's openai-completions reads them.
func TestOpenAIUsageAndStopReasonsArePiAis(t *testing.T) {
	stream := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(stream))
	}))
	defer server.Close()
	run := func(body string) (durable.ModelResponse, error) {
		stream = body
		return (&OpenAI{BaseURL: server.URL + "/v1"}).Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Messages: []durable.Message{{Role: "user", Content: "hi"}}}, nil)
	}
	response, err := run("data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":20,\"prompt_tokens_details\":{\"cached_tokens\":40,\"cache_write_tokens\":10}}}\n\ndata: [DONE]\n\n")
	if err != nil || response.Usage.InputTokens != 50 || response.Usage.CacheReadTokens != 40 || response.Usage.CacheWriteTokens != 10 || response.Usage.OutputTokens != 20 {
		t.Fatalf("usage=%+v error=%v", response.Usage, err)
	}
	response, err = run("data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":30,\"completion_tokens\":2,\"prompt_cache_hit_tokens\":12}}\n\ndata: [DONE]\n\n")
	if err != nil || response.Usage.InputTokens != 18 || response.Usage.CacheReadTokens != 12 {
		t.Fatalf("DeepSeek usage=%+v error=%v", response.Usage, err)
	}
	if _, err := run("data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n"); err == nil || err.Error() != "Provider finish_reason: content_filter" {
		t.Fatalf("content filter: error=%v", err)
	}
	if response, err := run("data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":\"end\"}]}\n\ndata: [DONE]\n\n"); err != nil || response.StopReason != "stop" {
		t.Fatalf("end: %+v error=%v", response, err)
	}
}

// A reasoning model's instructions are a developer message where pi-ai
// sends one.
func TestOpenAIInstructionRoleIsPiAis(t *testing.T) {
	rules := "rules"
	for _, c := range []struct {
		adapter OpenAI
		model   string
		role    string
	}{
		{OpenAI{BaseURL: "https://api.openai.com/v1"}, "m", "system"},
		{OpenAI{BaseURL: "https://api.openai.com/v1", Reasoning: true}, "m", "developer"},
		{OpenAI{Reasoning: true}, "m", "developer"},
		{OpenAI{BaseURL: "https://api.deepseek.com/v1", Reasoning: true}, "m", "system"},
		{OpenAI{BaseURL: "https://openrouter.ai/api/v1", Reasoning: true}, "meta/llama", "system"},
		{OpenAI{BaseURL: "https://openrouter.ai/api/v1", Reasoning: true}, "openai/o3", "developer"},
	} {
		var role string
		_, _ = c.adapter.Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: c.model}, Messages: []durable.Message{{Role: "system", Sections: []durable.SystemSection{{Name: "rules", Text: &rules}}}, {Role: "user", Content: "hi"}},
			OnPayload: func(_ context.Context, _ string, payload json.RawMessage) (json.RawMessage, error) {
				var body struct {
					Messages []struct {
						Role string `json:"role"`
					} `json:"messages"`
				}
				_ = json.Unmarshal(payload, &body)
				role = body.Messages[0].Role
				return nil, errors.New("captured")
			}}, nil)
		if role != c.role {
			t.Errorf("%s %s: %s, want %s", c.adapter.BaseURL, c.model, role, c.role)
		}
	}
}

// Malformed arguments are the model's: the answer stands and the call
// carries pi-ai's argumentsError, which tool validation reports back.
func TestOpenAIMalformedArgumentsAreTheModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"x\""}}]}}]}` + "\n\n"))
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}` + "\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	got, err := OpenAI{BaseURL: server.URL + "/v1"}.Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Messages: []durable.Message{{Role: "user", Content: "hi"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// pi-ai's argumentsError for the same text.
	want := "Expected ',' or '}' after property value in JSON at position 8 (line 1 column 9)\n\nReceived arguments:\n{\"q\":\"x\""
	if len(got.ToolCalls) != 1 || string(got.ToolCalls[0].Arguments) != "{}" || got.ToolCalls[0].ArgumentsError != want {
		t.Fatalf("tool calls=%+v", got.ToolCalls)
	}
}

// Thinking formats as pi-ai's openai-completions sends them (expectations
// from TestCrosscheckOpenAIThinkingFormats).
func TestOpenAIThinkingFormatsArePiAis(t *testing.T) {
	str := func(s string) *string { return &s }
	for _, c := range []struct {
		adapter OpenAI
		level   string
		want    map[string]any
	}{
		{OpenAI{BaseURL: "https://api.deepseek.com/v1", Reasoning: true}, "low", map[string]any{"thinking": map[string]any{"type": "enabled"}, "reasoning_effort": "low"}},
		{OpenAI{BaseURL: "https://api.deepseek.com/v1", Reasoning: true}, "", map[string]any{"thinking": map[string]any{"type": "disabled"}}},
		{OpenAI{BaseURL: "https://api.z.ai/api/paas/v4", Reasoning: true}, "high", map[string]any{"thinking": map[string]any{"type": "enabled", "clear_thinking": false}}},
		{OpenAI{BaseURL: "https://api.together.xyz/v1", Reasoning: true}, "off", map[string]any{"reasoning": map[string]any{"enabled": false}}},
		{OpenAI{BaseURL: "https://openrouter.ai/api/v1", Reasoning: true}, "", map[string]any{"reasoning": map[string]any{"effort": "none"}}},
		{OpenAI{BaseURL: "https://openrouter.ai/api/v1", Reasoning: true, ThinkingLevelMap: map[string]*string{"low": str("light")}}, "low", map[string]any{"reasoning": map[string]any{"effort": "light"}}},
		{OpenAI{BaseURL: "https://api.ant-ling.com/v1", Reasoning: true, ThinkingLevelMap: map[string]*string{"low": str("light")}}, "low", map[string]any{"reasoning": map[string]any{"effort": "light"}}},
		{OpenAI{BaseURL: "https://example.test/v1", Reasoning: true, Compat: OpenAICompat{ThinkingFormat: "qwen"}}, "low", map[string]any{"enable_thinking": true, "reasoning_effort": "low"}},
		{OpenAI{BaseURL: "https://example.test/v1", Reasoning: true, Compat: OpenAICompat{ThinkingFormat: "qwen-chat-template"}}, "", map[string]any{"chat_template_kwargs": map[string]any{"enable_thinking": false, "preserve_thinking": true}}},
		{OpenAI{BaseURL: "https://example.test/v1", Reasoning: true, Compat: OpenAICompat{ThinkingFormat: "string-thinking"}}, "high", map[string]any{"thinking": "high"}},
	} {
		body := sentPayload(t, c.adapter, durable.ModelRequest{Model: durable.ModelRef{Provider: "custom", ID: "m1"}, ThinkingLevel: c.level, Messages: []durable.Message{{Role: "user", Content: "hi"}}})
		for key, want := range c.want {
			if !reflect.DeepEqual(body[key], want) {
				t.Errorf("%s %+v level %q: %s=%v, want %v", c.adapter.BaseURL, c.adapter.Compat.ThinkingFormat, c.level, key, body[key], want)
			}
		}
		for _, key := range []string{"thinking", "reasoning", "reasoning_effort", "enable_thinking", "chat_template_kwargs"} {
			if _, wanted := c.want[key]; !wanted && body[key] != nil {
				t.Errorf("%s %+v level %q: unexpected %s=%v", c.adapter.BaseURL, c.adapter.Compat.ThinkingFormat, c.level, key, body[key])
			}
		}
	}
}

// An answer's thinking goes back in the field it came in, to the model that
// gave it only; DeepSeek gets an empty reasoning_content otherwise (pi-ai).
func TestOpenAIReplaysThinkingToTheSameModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"reasoning":"weighing"}}]}` + "\n\n"))
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"done"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	ref := durable.ModelRef{Provider: "custom", ID: "m1"}
	answer, err := OpenAI{BaseURL: server.URL + "/v1"}.Generate(context.Background(), durable.ModelRequest{Model: ref, Messages: []durable.Message{{Role: "user", Content: "hi"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	history := []durable.Message{{Role: "user", Content: "hi"}, {Role: "assistant", Thinking: answer.Thinking, Content: answer.Content, ProviderData: answer.ProviderData}, {Role: "user", Content: "more"}}
	assistant := func(m OpenAI, ref durable.ModelRef) map[string]any {
		body := sentPayload(t, m, durable.ModelRequest{Model: ref, Messages: history})
		return body["messages"].([]any)[1].(map[string]any)
	}
	if got := assistant(OpenAI{BaseURL: "https://example.test/v1"}, ref); got["reasoning"] != "weighing" {
		t.Fatalf("same model: %v", got)
	}
	if got := assistant(OpenAI{BaseURL: "https://example.test/v1"}, durable.ModelRef{Provider: "custom", ID: "m2"}); got["reasoning"] != nil {
		t.Fatalf("another model: %v", got)
	}
	if got := assistant(OpenAI{BaseURL: "https://api.deepseek.com/v1", Reasoning: true}, durable.ModelRef{Provider: "custom", ID: "m2"}); got["reasoning_content"] != "" {
		t.Fatalf("deepseek: %v", got)
	}
}

// sentPayload is the body m sends for req, decoded.
func sentPayload(t *testing.T, m OpenAI, req durable.ModelRequest) map[string]any {
	t.Helper()
	var body map[string]any
	req.OnPayload = func(_ context.Context, _ string, payload json.RawMessage) (json.RawMessage, error) {
		if err := json.Unmarshal(payload, &body); err != nil {
			t.Fatal(err)
		}
		return nil, errors.New("captured")
	}
	_, _ = m.Generate(context.Background(), req, nil)
	if body == nil {
		t.Fatal("no request body")
	}
	return body
}

// pi-ai's compat settings (expectations from TestCrosscheckOpenAICompat).
func TestOpenAICompatIsPiAis(t *testing.T) {
	yes := true
	lookup := durable.ToolSpec{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}
	history := []durable.Message{{Role: "system", Content: "Be brief."}, {Role: "user", Content: "hi"},
		{Role: "assistant", ToolCalls: []durable.ToolCall{{ID: "c1", Name: "lookup", Arguments: json.RawMessage(`{}`)}}},
		{Role: "tool", ToolCallID: "c1", ToolName: "lookup", Content: "found"}, {Role: "user", Content: "next"}}
	request := func(stream *durable.StreamOptions) durable.ModelRequest {
		return durable.ModelRequest{Model: durable.ModelRef{Provider: "custom", ID: "m1"}, SessionID: "s", ThinkingLevel: "medium", Messages: history, Tools: []durable.ToolSpec{lookup}, Stream: stream}
	}
	t.Run("bridge after tool results", func(t *testing.T) {
		body := sentPayload(t, OpenAI{BaseURL: "https://example.test/v1", Compat: OpenAICompat{RequiresAssistantAfterToolResult: &yes, RequiresToolResultName: &yes}}, request(nil))
		messages := body["messages"].([]any)
		tool, bridge := messages[3].(map[string]any), messages[4].(map[string]any)
		if messages[2].(map[string]any)["content"] != "" || tool["name"] != "lookup" || bridge["role"] != "assistant" || bridge["content"] != "I have processed the tool results." {
			t.Fatalf("messages=%v", messages)
		}
	})
	t.Run("chat template", func(t *testing.T) {
		m := OpenAI{BaseURL: "https://example.test/v1", MaxTokens: 8192, Reasoning: true, Compat: OpenAICompat{ThinkingFormat: "chat-template",
			ChatTemplateKwargs: map[string]json.RawMessage{"enable_thinking": json.RawMessage(`{"$var":"thinking.enabled"}`), "budget": json.RawMessage(`{"$var":"thinking.budget"}`), "fixed": json.RawMessage(`"x"`)}}}
		got := sentPayload(t, m, request(nil))["chat_template_kwargs"]
		if !reflect.DeepEqual(got, map[string]any{"enable_thinking": true, "budget": float64(7168), "fixed": "x"}) {
			t.Fatalf("chat_template_kwargs=%v", got)
		}
	})
	t.Run("anthropic cache control", func(t *testing.T) {
		body := sentPayload(t, OpenAI{BaseURL: "https://example.test/v1", Compat: OpenAICompat{CacheControlFormat: "anthropic"}}, request(&durable.StreamOptions{CacheRetention: "long"}))
		marker := map[string]any{"type": "ephemeral", "ttl": "1h"}
		messages := body["messages"].([]any)
		system := messages[0].(map[string]any)["content"].([]any)[0].(map[string]any)
		last := messages[len(messages)-1].(map[string]any)["content"].([]any)[0].(map[string]any)
		tool := body["tools"].([]any)[0].(map[string]any)
		if !reflect.DeepEqual(system["cache_control"], marker) || !reflect.DeepEqual(last["cache_control"], marker) || !reflect.DeepEqual(tool["cache_control"], marker) {
			t.Fatalf("body=%v", body)
		}
	})
	t.Run("provider detection and extras", func(t *testing.T) {
		priority := -2.5
		m := OpenAI{BaseURL: "https://example.test/v1", MaxTokens: 8192, Reasoning: true, SamplingParams: map[string]any{"temperature": 0.3},
			Compat: OpenAICompat{ThinkingTokenBudgetField: "thinking_budget", VLLMPriority: &priority, ZaiToolStream: &yes}}
		req := request(&durable.StreamOptions{ThinkingBudgets: map[string]int{"medium": 3000}})
		req.Model.Provider = "deepseek"
		body := sentPayload(t, m, req)
		if body["thinking_budget"] != float64(3000) || body["priority"] != priority || body["temperature"] != 0.3 || body["tool_stream"] != true ||
			!reflect.DeepEqual(body["thinking"], map[string]any{"type": "enabled"}) || body["max_tokens"] != float64(8192) {
			t.Fatalf("body=%v", body)
		}
	})
}
