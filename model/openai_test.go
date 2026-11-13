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
	if err == nil || !strings.Contains(err.Error(), "before [DONE]") {
		t.Fatalf("error=%v", err)
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
