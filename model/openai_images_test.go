package model

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

// imagePayload sends req through an OpenAI adapter and returns the messages
// the endpoint received.
func imagePayload(t *testing.T, adapter OpenAI, req durable.ModelRequest) []map[string]any {
	t.Helper()
	var messages []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []map[string]any `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		messages = body.Messages
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	adapter.BaseURL = server.URL
	if _, err := adapter.Generate(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	return messages
}

func TestOpenAISendsImagesLikePiAI(t *testing.T) {
	png := durable.ImageContent{MimeType: "image/png", Data: "iVBORw0KGgo="}
	req := durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Messages: []durable.Message{
		{Role: "user", Content: "what is this?", Images: []durable.ImageContent{png}},
		{Role: "assistant", ToolCalls: []durable.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}, {ID: "c2", Name: "read", Arguments: json.RawMessage(`{}`)}}},
		{Role: "tool", ToolCallID: "c1", Images: []durable.ImageContent{png}},
		{Role: "tool", ToolCallID: "c2"},
	}}
	got, _ := json.Marshal(imagePayload(t, OpenAI{}, req))
	want := `[{"content":[{"text":"what is this?","type":"text"},{"image_url":{"url":"data:image/png;base64,iVBORw0KGgo="},"type":"image_url"}],"role":"user"},` +
		`{"role":"assistant","tool_calls":[{"function":{"arguments":"{}","name":"read"},"id":"c1","type":"function"},{"function":{"arguments":"{}","name":"read"},"id":"c2","type":"function"}]},` +
		`{"content":"(see attached image)","role":"tool","tool_call_id":"c1"},{"content":"(no tool output)","role":"tool","tool_call_id":"c2"},` +
		`{"content":[{"text":"Attached image(s) from tool result:","type":"text"},{"image_url":{"url":"data:image/png;base64,iVBORw0KGgo="},"type":"image_url"}],"role":"user"}]`
	if string(got) != want {
		t.Fatalf("messages:\n%s\nwant\n%s", got, want)
	}

	got, _ = json.Marshal(imagePayload(t, OpenAI{NoImages: true}, req))
	want = `[{"content":[{"text":"what is this?","type":"text"},{"text":"(image omitted: model does not support images)","type":"text"}],"role":"user"},` +
		`{"role":"assistant","tool_calls":[{"function":{"arguments":"{}","name":"read"},"id":"c1","type":"function"},{"function":{"arguments":"{}","name":"read"},"id":"c2","type":"function"}]},` +
		`{"content":"(tool image omitted: model does not support images)","role":"tool","tool_call_id":"c1"},{"content":"(no tool output)","role":"tool","tool_call_id":"c2"}]`
	if string(got) != want {
		t.Fatalf("placeholder messages:\n%s\nwant\n%s", got, want)
	}
}

func TestOpenAISendsTheIdentityUserAgent(t *testing.T) {
	var agents []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agents = append(agents, r.Header.Get("User-Agent"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	for _, identity := range []*durable.ClientIdentity{
		{UserAgent: json.RawMessage(`"short"`)},
		{UserAgent: json.RawMessage(`{"custom":"my-app/2"}`)},
		{Version: "1.2.3"},
	} {
		req := durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Identity: identity}
		if _, err := (OpenAI{BaseURL: server.URL}).Generate(context.Background(), req, nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(agents) != 3 || agents[0] != "pi" || agents[1] != "my-app/2" || !strings.HasPrefix(agents[2], "pi/1.2.3 (") {
		t.Fatalf("user agents=%q", agents)
	}
}

func TestOpenAIFoldsPositionalSystemMessages(t *testing.T) {
	first, second := "<preamble>\nfirst\n</preamble>", "<preamble>\nsecond\n</preamble>"
	var body struct {
		Messages []map[string]any `json:"messages"`
		Tools    []map[string]any `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	req := durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Messages: []durable.Message{
		{Role: "system", Sections: []durable.SystemSection{{Name: "preamble", Text: &first}}, ToolsAdded: []durable.ToolSpec{{Name: "read", Parameters: json.RawMessage(`{"type":"object"}`)}}},
		{Role: "user", Content: "one"},
		{Role: "system", Sections: []durable.SystemSection{{Name: "preamble", Text: &second}}, ToolsAdded: []durable.ToolSpec{{Name: "bash", Parameters: json.RawMessage(`{"type":"object"}`)}}},
		{Role: "user", Content: "two"},
	}}
	if _, err := (OpenAI{BaseURL: server.URL}).Generate(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if len(body.Messages) != 3 || body.Messages[0]["role"] != "system" || body.Messages[0]["content"] != second || body.Messages[1]["content"] != "one" {
		t.Fatalf("messages=%v", body.Messages)
	}
	if len(body.Tools) != 2 {
		t.Fatalf("tools=%v", body.Tools)
	}
}

func TestOpenAISendsStreamHeaders(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Test")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	req := durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Stream: &durable.StreamOptions{Headers: map[string]string{"X-Test": "yes"}, TimeoutMs: 5000}}
	if _, err := (OpenAI{BaseURL: server.URL}).Generate(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if got != "yes" {
		t.Fatalf("header=%q", got)
	}
}
