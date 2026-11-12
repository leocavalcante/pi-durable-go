package model

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

// Optional tests use a real installed SDK but only temporary fake credentials
// and httptest endpoints. The wrapper denies every non-loopback fetch, including
// accidental catalog downloads. No user's auth.json is ever read.
func installedPi(t *testing.T, serverURL string) Pi {
	t.Helper()
	pkg := os.Getenv("PI_DURABLE_TEST_PI_PACKAGE_DIR")
	if pkg == "" {
		t.Skip("set PI_DURABLE_TEST_PI_PACKAGE_DIR to test an installed Pi 1.5.2 SDK")
	}
	if _, err := exec.LookPath("node"); err != nil {
		t.Fatal("installed SDK tests require Node.js 22.19+")
	}
	entry := (&url.URL{Scheme: "file", Path: filepath.Join(pkg, "dist", "index.js")}).String()
	dir := t.TempDir()
	writeJSON(t, filepath.Join(dir, "package.json"), map[string]string{"type": "module"})
	if err := os.MkdirAll(filepath.Join(dir, "dist"), 0700); err != nil {
		t.Fatal(err)
	}
	wrapper := fmt.Sprintf(`
const originalFetch = globalThis.fetch;
const local = %q;
globalThis.fetch = (input, options) => {
  const target = new URL(typeof input === "string" || input instanceof URL ? input : input.url);
  if (target.href === "https://auth.openai.com/oauth/token") return originalFetch(local + "/oauth/token", options);
  if (target.origin !== local) throw new Error("test blocked external fetch");
  return originalFetch(input, options);
};
const sdk = await import(%q);
export const ModelRuntime = sdk.ModelRuntime;
`, serverURL, entry)
	if err := os.WriteFile(filepath.Join(dir, "dist", "index.js"), []byte(wrapper), 0600); err != nil {
		t.Fatal(err)
	}
	return Pi{PackageDir: dir, AgentDir: t.TempDir()}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestPiInstalledSDKAPIKeys(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		keys = append(keys, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"local reply\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()
	p := installedPi(t, server.URL)
	t.Setenv("OPENAI_API_KEY", "fake-environment-key")
	models := map[string]any{"providers": map[string]any{"openai": map[string]any{
		"baseUrl": server.URL + "/v1", "api": "openai-completions", "apiKey": "fake-models-key",
		"models": []any{map[string]any{"id": "durable-test", "api": "openai-completions"}},
	}}}
	modelsPath := filepath.Join(p.AgentDir, "models.json")
	authPath := filepath.Join(p.AgentDir, "auth.json")
	writeJSON(t, modelsPath, models)
	for _, tc := range []struct {
		name string
		auth map[string]any
		key  string
	}{
		{"stored", map[string]any{"openai": map[string]any{"type": "api_key", "key": "fake-stored-key"}}, "Bearer fake-stored-key"},
		{"interpolation", map[string]any{"openai": map[string]any{"type": "api_key", "key": "$DURABLE_FAKE_KEY"}}, "Bearer fake-interpolated-key"},
		{"command", map[string]any{"openai": map[string]any{"type": "api_key", "key": "!printf fake-command-key"}}, "Bearer fake-command-key"},
		{"models", map[string]any{}, "Bearer fake-models-key"},
		{"environment", map[string]any{}, "Bearer fake-environment-key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DURABLE_FAKE_KEY", "fake-interpolated-key")
			if tc.name == "environment" {
				delete(models["providers"].(map[string]any)["openai"].(map[string]any), "apiKey")
				writeJSON(t, modelsPath, models)
			}
			writeJSON(t, authPath, tc.auth)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			response, err := p.Generate(ctx, durable.ModelRequest{Model: durable.ModelRef{Provider: "openai", ID: "durable-test"},
				Messages: []durable.Message{{Role: "user", Content: "local input"}}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if response.Content != "local reply" || strings.Contains(string(response.ProviderData), "fake-stored-key") {
				t.Fatalf("response=%+v", response)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(keys) == 0 || keys[len(keys)-1] != tc.key {
				t.Fatalf("keys=%q, want %q", keys, tc.key)
			}
		})
	}
	// This is Pi's real default-path mechanism, not a Go-side auth.json reader.
	t.Setenv("PI_CODING_AGENT_DIR", p.AgentDir)
	p.AgentDir = ""
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := p.Generate(ctx, durable.ModelRequest{Model: durable.ModelRef{Provider: "openai", ID: "durable-test"},
		Messages: []durable.Message{{Role: "user", Content: "local input"}}}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestPiInstalledSDKOAuthRefresh(t *testing.T) {
	// A fake JWT contains the claim Pi's Codex transport requires. It is not a
	// real token and all token/model endpoints below are loopback-only.
	claims := base64.RawURLEncoding.EncodeToString([]byte(`{"https://api.openai.com/auth":{"chatgpt_account_id":"fake-account"}}`))
	access := "fake." + claims + ".signature"
	var refreshes, generations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			refreshes.Add(1)
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("grant_type") != "refresh_token" || r.Form.Get("refresh_token") != "fake-old-refresh" {
				t.Errorf("refresh body=%v", r.Form)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"access_token": access, "refresh_token": "fake-rotated-refresh", "expires_in": 3600})
		case "/codex/responses":
			generations.Add(1)
			if r.Header.Get("Authorization") != "Bearer "+access || r.Header.Get("ChatGPT-Account-ID") != "fake-account" {
				t.Errorf("missing provider-specific OAuth headers")
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m1\",\"role\":\"assistant\",\"content\":[]}}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"oauth reply\"}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"m1\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"oauth reply\"}]}}\n\n")
			fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\n\n")
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	p := installedPi(t, server.URL)
	writeJSON(t, filepath.Join(p.AgentDir, "models.json"), map[string]any{"providers": map[string]any{"openai-codex": map[string]any{
		"baseUrl": server.URL, "models": []any{map[string]any{"id": "durable-test", "api": "openai-codex-responses"}},
	}}})
	authPath := filepath.Join(p.AgentDir, "auth.json")
	writeJSON(t, authPath, map[string]any{
		"openai-codex": map[string]any{"type": "oauth", "access": "fake-expired", "refresh": "fake-old-refresh", "expires": 0, "accountId": "fake-account"},
		"unrelated":    map[string]any{"type": "api_key", "key": "fake-unrelated-key"},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Two subprocesses share the real Pi AuthStorage implementation: only one
	// should refresh the expired token, and unrelated credentials must survive.
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := p.Generate(ctx, durable.ModelRequest{Model: durable.ModelRef{Provider: "openai-codex", ID: "durable-test"},
				Messages: []durable.Message{{Role: "user", Content: "local input"}}}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			if response.Content != "oauth reply" || strings.Contains(string(response.ProviderData), access) {
				t.Errorf("response=%+v", response)
			}
		}()
	}
	wg.Wait()
	if refreshes.Load() != 1 || generations.Load() != 2 {
		t.Fatalf("refreshes=%d, generations=%d", refreshes.Load(), generations.Load())
	}
	var stored map[string]map[string]any
	data, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	if stored["openai-codex"]["refresh"] != "fake-rotated-refresh" || stored["unrelated"]["key"] != "fake-unrelated-key" {
		t.Fatal("Pi did not safely persist rotated credentials")
	}
}

// The installed SDK's own pi-ai classifies provider errors inside Node; only
// the flags reach Go. The test wrapper links the installation's pi-ai so the
// bridge resolves it from the wrapper as from the real package.
func TestPiInstalledSDKErrorClassification(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		switch body.Model {
		case "transient":
			w.WriteHeader(503)
			fmt.Fprint(w, `{"error":{"message":"The server is overloaded secret-body","type":"server_error"}}`)
		case "quota":
			w.WriteHeader(429)
			fmt.Fprint(w, `{"error":{"message":"You exceeded your quota secret-body","type":"insufficient_quota","code":"insufficient_quota"}}`)
		default:
			w.WriteHeader(400)
			fmt.Fprint(w, `{"error":{"message":"This model's maximum context length is 8192 tokens secret-body","code":"context_length_exceeded"}}`)
		}
	}))
	defer server.Close()
	p := installedPi(t, server.URL)
	pkg := os.Getenv("PI_DURABLE_TEST_PI_PACKAGE_DIR")
	ai := filepath.Join(pkg, "..", "pi-ai")
	if _, err := os.Stat(filepath.Join(ai, "package.json")); err != nil {
		ai = filepath.Join(pkg, "node_modules", "@earendil-works", "pi-ai")
	}
	if _, err := os.Stat(filepath.Join(ai, "package.json")); err != nil {
		t.Fatalf("installed pi-ai not found next to %s", pkg)
	}
	link := filepath.Join(p.PackageDir, "node_modules", "@earendil-works")
	if err := os.MkdirAll(link, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(ai, filepath.Join(link, "pi-ai")); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(p.AgentDir, "models.json"), map[string]any{"providers": map[string]any{"openai": map[string]any{
		"baseUrl": server.URL + "/v1", "api": "openai-completions", "apiKey": "fake-models-key",
		"models": []any{map[string]any{"id": "transient"}, map[string]any{"id": "quota"}, map[string]any{"id": "overflow"}},
	}}})
	writeJSON(t, filepath.Join(p.AgentDir, "auth.json"), map[string]any{})
	for _, tc := range []struct {
		id   string
		want modelErrorClass
	}{
		{"transient", modelErrorClass{true, false}},
		{"quota", modelErrorClass{false, false}},
		{"overflow", modelErrorClass{false, true}},
	} {
		t.Run(tc.id, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_, err := p.Generate(ctx, durable.ModelRequest{Model: durable.ModelRef{Provider: "openai", ID: tc.id},
				Messages: []durable.Message{{Role: "user", Content: "local input"}}}, nil)
			if err == nil || strings.Contains(err.Error(), "secret-body") {
				t.Fatalf("error=%v", err)
			}
			if got := classOf(err, "openai"); got != tc.want {
				t.Fatalf("class=%+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPiInstalledSDKPositionalSystemMessages(t *testing.T) {
	var mu sync.Mutex
	var body struct {
		Messages []map[string]any `json:"messages"`
		Tools    []map[string]any `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"local reply\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()
	p := installedPi(t, server.URL)
	writeJSON(t, filepath.Join(p.AgentDir, "models.json"), map[string]any{"providers": map[string]any{"openai": map[string]any{
		"baseUrl": server.URL + "/v1", "api": "openai-completions", "apiKey": "fake-models-key",
		"models": []any{map[string]any{"id": "durable-test", "api": "openai-completions"}},
	}}})
	first, second := "<preamble>\nfirst\n</preamble>", "<preamble>\nsecond\n</preamble>"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	messages := []durable.Message{
		{Role: "system", Sections: []durable.SystemSection{{Name: "preamble", Text: &first}},
			ToolsAdded: []durable.ToolSpec{{Name: "read", Description: "Read a file", Parameters: json.RawMessage(`{"type":"object","properties":{}}`)}}},
		{Role: "user", Content: "one"},
		{Role: "assistant", Content: "ok"},
		{Role: "user", Content: "two"},
		{Role: "system", Sections: []durable.SystemSection{{Name: "preamble", Text: &second}},
			ToolsAdded: []durable.ToolSpec{{Name: "bash", Description: "Run a command", Parameters: json.RawMessage(`{"type":"object","properties":{}}`)}}},
	}
	response, err := p.Generate(ctx, durable.ModelRequest{Model: durable.ModelRef{Provider: "openai", ID: "durable-test"},
		Messages: messages, Tools: durable.CurrentTools(messages)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.Content != "local reply" {
		t.Fatalf("response=%+v", response)
	}
	mu.Lock()
	defer mu.Unlock()
	// pi-ai folds the system messages for this transport: the current prompt
	// leads, and both tools are declared once.
	if len(body.Messages) == 0 || body.Messages[0]["role"] != "system" || !strings.Contains(fmt.Sprint(body.Messages[0]["content"]), "second") ||
		strings.Contains(fmt.Sprint(body.Messages[0]["content"]), "first") {
		t.Fatalf("messages=%v", body.Messages)
	}
	if len(body.Tools) != 2 {
		t.Fatalf("tools=%v", body.Tools)
	}
}
