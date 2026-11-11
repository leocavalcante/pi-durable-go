package model

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

type modelErrorClass struct{ retryable, overflow bool }

func classOf(err error, provider string) modelErrorClass {
	return modelErrorClass{durable.IsRetryableModelError(err), durable.IsContextOverflowError(err, provider)}
}

func TestModelErrorClassificationOpenAIHTTP(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		provider string
		want     modelErrorClass
	}{
		{"503-json", 503, `{"error":{"message":"The server is overloaded secret"}}`, "openai", modelErrorClass{true, false}},
		{"429-rate-limit", 429, `{"error":{"message":"Rate limit reached secret","type":"requests","code":"rate_limit_exceeded"}}`, "openai", modelErrorClass{true, false}},
		{"429-quota", 429, `{"error":{"message":"You exceeded your current quota secret","type":"insufficient_quota","code":"insufficient_quota"}}`, "openai", modelErrorClass{false, false}},
		{"400-overflow", 400, `{"error":{"message":"This model's maximum context length is 8192 tokens. However, your messages resulted in 9000 tokens. secret","code":"context_length_exceeded"}}`, "openai", modelErrorClass{false, true}},
		{"413-no-body", 413, ``, "openai", modelErrorClass{false, true}},
		{"400-no-body-cerebras", 400, ``, "cerebras", modelErrorClass{false, true}},
		{"400-no-body-other", 400, ``, "openai", modelErrorClass{false, false}},
		{"401-auth", 401, `{"error":{"message":"Incorrect API key provided: secret"}}`, "openai", modelErrorClass{false, false}},
		{"502-text", 502, `upstream connect error secret`, "openai", modelErrorClass{true, false}},
		{"500-json-without-error", 500, `{"detail":"secret"}`, "openai", modelErrorClass{true, false}},
		{"404-string-error", 404, `{"error":"model not found secret"}`, "openai", modelErrorClass{false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			_, err := (OpenAI{BaseURL: server.URL}).Generate(context.Background(),
				durable.ModelRequest{Model: durable.ModelRef{Provider: tc.provider, ID: "m"}}, nil)
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("error=%v", err)
			}
			if got := classOf(err, tc.provider); got != tc.want {
				t.Fatalf("class=%+v, want %+v (text %q)", got, tc.want, openAIErrorText(tc.status, []byte(tc.body)))
			}
		})
	}
}

func TestModelErrorClassificationOpenAIStream(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stream string
		want   modelErrorClass
	}{
		{"premature-end", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n", modelErrorClass{true, false}},
		{"sse-transient", "data: {\"error\":{\"message\":\"Rate limit reached; secret\"}}\n\n", modelErrorClass{true, false}},
		{"sse-quota", "data: {\"error\":{\"message\":\"insufficient_quota secret\",\"code\":503}}\n\n", modelErrorClass{false, false}},
		{"sse-overflow", "data: {\"error\":{\"message\":\"prompt is too long: secret\"}}\n\n", modelErrorClass{false, true}},
		{"sse-unclassified", "data: {\"error\":{\"message\":\"invalid secret\"}}\n\n", modelErrorClass{false, false}},
		{"invalid-event", "data: {not json secret}\n\n", modelErrorClass{false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.stream))
			}))
			defer server.Close()
			_, err := (OpenAI{BaseURL: server.URL}).Generate(context.Background(),
				durable.ModelRequest{Model: durable.ModelRef{Provider: "openai", ID: "m"}}, nil)
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("error=%v", err)
			}
			if got := classOf(err, "openai"); got != tc.want {
				t.Fatalf("class=%+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestModelErrorClassificationOpenAITransportAndLocal(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := "http://" + listener.Addr().String()
	listener.Close()
	_, err = (OpenAI{BaseURL: refused}).Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}}, nil)
	if err == nil || classOf(err, "") != (modelErrorClass{true, false}) {
		t.Fatalf("refused connection error=%v class=%+v", err, classOf(err, ""))
	}
	for _, model := range []OpenAI{{BaseURL: "ftp://timeout.invalid"}} {
		_, err := model.Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{ID: "m"}}, nil)
		if err == nil || classOf(err, "") != (modelErrorClass{}) {
			t.Fatalf("configuration error=%v class=%+v", err, classOf(err, ""))
		}
	}
	_, err = (OpenAI{BaseURL: "http://127.0.0.1:1"}).Generate(context.Background(), durable.ModelRequest{
		Model: durable.ModelRef{ID: "m"}, Messages: []durable.Message{{Role: "timeout-role"}}}, nil)
	if err == nil || classOf(err, "") != (modelErrorClass{}) {
		t.Fatalf("request error=%v class=%+v", err, classOf(err, ""))
	}
}

func TestModelErrorClassificationPi(t *testing.T) {
	p := fakePi(t)
	for _, tc := range []struct {
		id   string
		want modelErrorClass
	}{
		{"transient-error", modelErrorClass{true, false}},
		{"quota-error", modelErrorClass{false, false}},
		{"overflow-error", modelErrorClass{false, true}},
		{"error", modelErrorClass{false, false}},
		{"unknown", modelErrorClass{false, false}},
	} {
		t.Run(tc.id, func(t *testing.T) {
			_, err := p.Generate(context.Background(), piRequest(tc.id), nil)
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatalf("error=%v", err)
			}
			if got := classOf(err, "fake"); got != tc.want {
				t.Fatalf("class=%+v, want %+v", got, tc.want)
			}
		})
	}
	// Without a resolvable pi-ai, nothing is classified as retryable.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "dist"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"package.json", filepath.Join("dist", "index.js")} {
		data, err := os.ReadFile(filepath.Join(p.PackageDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	p.PackageDir = dir
	_, err := p.Generate(context.Background(), piRequest("transient-error"), nil)
	if err == nil || classOf(err, "fake") != (modelErrorClass{}) {
		t.Fatalf("unresolvable pi-ai error=%v class=%+v", err, classOf(err, "fake"))
	}
	var classified durable.RetryableError
	if !errors.As(err, &classified) {
		t.Fatal("bridge error does not declare its classification")
	}
}
