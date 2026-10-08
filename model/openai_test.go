package model

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
