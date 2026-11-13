package model

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

func fakePi(t *testing.T) Pi {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node.js is optional; install it to exercise the Pi bridge")
	}
	dir, err := filepath.Abs("testdata/pi-sdk")
	if err != nil {
		t.Fatal(err)
	}
	return Pi{PackageDir: dir, AgentDir: t.TempDir()}
}

func piRequest(id string) durable.ModelRequest {
	return durable.ModelRequest{Model: durable.ModelRef{Provider: "fake", ID: id}, SessionID: "session-123",
		ThinkingLevel: "high", Messages: []durable.Message{{Role: "user", Content: "hello"}}}
}

func TestPiStreamingAndNativeReplay(t *testing.T) {
	p := fakePi(t)
	var deltas []string
	response, err := p.Generate(context.Background(), piRequest("text-tools"), func(delta string) error {
		deltas = append(deltas, delta)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Content != "hello world" || strings.Join(deltas, "") != response.Content || response.Thinking != "plan" {
		t.Fatalf("response=%+v, deltas=%q", response, deltas)
	}
	if response.Usage != (durable.Usage{InputTokens: 7, OutputTokens: 3, CacheReadTokens: 5, CacheWriteTokens: 2, Cost: .25}) {
		t.Fatalf("usage=%+v", response.Usage)
	}
	if len(response.ToolCalls) != 1 || response.ToolCalls[0].ID != "call-1" || string(response.ToolCalls[0].Arguments) != `{"q":"x"}` {
		t.Fatalf("tools=%+v", response.ToolCalls)
	}
	if !strings.Contains(string(response.ProviderData), "native-thinking-signature") || strings.Contains(string(response.ProviderData), "diagnostic") {
		t.Fatalf("native replay=%s", response.ProviderData)
	}
	req := piRequest("inspect")
	req.Messages = []durable.Message{
		{Role: "system", Content: "instructions"},
		{Role: "developer", Content: "more instructions"},
		{Role: "user", Content: "hello"},
		{Role: "assistant", Content: response.Content, Thinking: response.Thinking, ToolCalls: response.ToolCalls, ProviderData: response.ProviderData},
		{Role: "tool", Content: "tool failed", ToolCallID: "call-1", ToolName: "lookup", IsError: true},
	}
	req.Tools = []durable.ToolSpec{{Name: "lookup", Description: "Look up", Parameters: json.RawMessage(`{"type":"object"}`)}}
	inspect := inspectPi(t, p, req)
	if inspect.Create.AuthPath != filepath.Join(p.AgentDir, "auth.json") || inspect.Create.ModelsPath != filepath.Join(p.AgentDir, "models.json") {
		t.Fatalf("paths=%+v", inspect.Create)
	}
	if inspect.Options.SessionID != req.SessionID || inspect.Options.Reasoning != "high" || inspect.Options.MaxRetries != 0 || inspect.Options.Transport != "sse" {
		t.Fatalf("options=%+v", inspect.Options)
	}
	messages := inspect.Context.Messages
	if len(messages) != 5 || messages[0].Role != "system" || messages[1].Role != "system" || messages[4].Role != "toolResult" || !messages[4].IsError {
		t.Fatalf("messages=%+v", messages)
	}
	if !strings.Contains(string(messages[3].Content), "native-tool-signature") || messages[3].Model != "text-tools" {
		t.Fatalf("assistant native replay lost: %+v", messages[3])
	}
	if len(inspect.Context.Tools) != 1 || inspect.Context.Tools[0].Name != "lookup" {
		t.Fatalf("tools=%+v", inspect.Context.Tools)
	}
	// A hook/application changing visible text must invalidate old replay blocks.
	req.Messages[3].Content = "edited by hook"
	inspect = inspectPi(t, p, req)
	if strings.Contains(string(inspect.Context.Messages[3].Content), "native-") || !strings.Contains(string(inspect.Context.Messages[3].Content), "edited by hook") {
		t.Fatalf("stale native blocks replayed: %s", inspect.Context.Messages[3].Content)
	}
}

type piInspection struct {
	Create struct {
		AuthPath   string `json:"authPath"`
		ModelsPath string `json:"modelsPath"`
	} `json:"create"`
	Context struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
			Model   string          `json:"model"`
			IsError bool            `json:"isError"`
		} `json:"messages"`
		Tools []durable.ToolSpec `json:"tools"`
	} `json:"context"`
	Options struct {
		SessionID  string `json:"sessionId"`
		Reasoning  string `json:"reasoning"`
		Transport  string `json:"transport"`
		MaxRetries int    `json:"maxRetries"`
	} `json:"options"`
}

func inspectPi(t *testing.T, p Pi, req durable.ModelRequest) piInspection {
	t.Helper()
	response, err := p.Generate(context.Background(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	var value piInspection
	if err := json.Unmarshal([]byte(response.Content), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestPiSanitizedFailures(t *testing.T) {
	p := fakePi(t)
	for _, id := range []string{"fail", "error", "unknown", "bad-arguments"} {
		t.Run(id, func(t *testing.T) {
			_, err := p.Generate(context.Background(), piRequest(id), nil)
			if err == nil || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("error=%v", err)
			}
		})
	}
	for _, req := range []durable.ModelRequest{
		{},
		{Model: durable.ModelRef{Provider: "fake", ID: "inspect"}, Messages: []durable.Message{{Role: "bad-role"}}},
		{Model: durable.ModelRef{Provider: "fake", ID: "inspect"}, ThinkingLevel: "not-a-level"},
	} {
		if _, err := p.Generate(context.Background(), req, nil); err == nil {
			t.Fatal("invalid request succeeded")
		}
	}
	p.PackageDir = t.TempDir()
	if _, err := p.Generate(context.Background(), piRequest("text-tools"), nil); err == nil || !strings.Contains(err.Error(), "SDK unavailable") {
		t.Fatalf("missing SDK error=%v", err)
	}
	p.NodePath = filepath.Join(t.TempDir(), "missing-node")
	if _, err := p.Generate(context.Background(), piRequest("text-tools"), nil); err == nil || !strings.Contains(err.Error(), "cannot start Node") {
		t.Fatalf("missing Node error=%v", err)
	}
}

func TestPiLengthStopIsAnAnswer(t *testing.T) {
	p := fakePi(t)
	response, err := p.Generate(context.Background(), piRequest("length"), nil)
	if err != nil || response.StopReason != "length" || response.Content != "hello world" {
		t.Fatalf("response=%+v error=%v", response, err)
	}
	if response, err := p.Generate(context.Background(), piRequest("text-tools"), nil); err != nil || response.StopReason != "toolUse" {
		t.Fatalf("tool-use response=%+v error=%v", response, err)
	}
}

func TestPiCancellationSettlesCredentialWrites(t *testing.T) {
	p := fakePi(t)
	marker := filepath.Join(t.TempDir(), "settled")
	t.Setenv("PI_DURABLE_TEST_SETTLED", marker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := p.Generate(ctx, piRequest("hold"), func(string) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "settled" {
		t.Fatalf("credential writes not settled: %q, %v", data, err)
	}
	// A cancelled caller never starts a process.
	p.NodePath = "missing-node"
	if _, err := p.Generate(ctx, piRequest("hold"), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
}

func TestPiCallbackFailureAndLimits(t *testing.T) {
	p := fakePi(t)
	callbackErr := errors.New("stop publication")
	if _, err := p.Generate(context.Background(), piRequest("hold"), func(string) error { return callbackErr }); !errors.Is(err, callbackErr) {
		t.Fatalf("callback error=%v", err)
	}
	p.MaxResponseBytes = 32
	if _, err := p.Generate(context.Background(), piRequest("text-tools"), nil); !errors.Is(err, errResponseTooLarge) {
		t.Fatalf("size error=%v", err)
	}
}

func TestPiOutputProtocol(t *testing.T) {
	for _, output := range []string{
		"", "not-json\n", "{}\n", `{"type":"result"}` + "\n",
		`{"type":"delta","delta":"partial"}` + "\n",
		`{"type":"result","response":{}}` + "\n" + `{"type":"delta","delta":"late"}` + "\n",
		`{"type":"error","code":"secret-body"}` + "\n",
	} {
		if _, err := readPiOutput(strings.NewReader(output), nil, nil); err == nil || strings.Contains(err.Error(), "secret-body") {
			t.Fatalf("output=%q, error=%v", output, err)
		}
	}
}

func TestPiPackageDiscovery(t *testing.T) {
	root := t.TempDir()
	pkg := filepath.Join(root, "releases", "1.5.2", "node_modules", filepath.FromSlash(piPackageName))
	if err := os.MkdirAll(pkg, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "package.json"), []byte(`{"name":"`+piPackageName+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	versionFile := filepath.Join(root, "current-version")
	if err := os.WriteFile(versionFile, []byte("1.5.2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PI_PACKAGE_DIR", "")
	t.Setenv("PI_MANAGED_INSTALL_ROOT", root)
	if got := (Pi{}).packageDir(); got != pkg {
		t.Fatalf("managed discovery=%q", got)
	}
	t.Setenv("PI_PACKAGE_DIR", "/explicit/env")
	if got := (Pi{}).packageDir(); got != "/explicit/env" {
		t.Fatalf("environment discovery=%q", got)
	}
	if got := (Pi{PackageDir: "/explicit/option"}).packageDir(); got != "/explicit/option" {
		t.Fatalf("explicit discovery=%q", got)
	}
	for _, version := range []string{"", ".", "..", "../1.5.2", `..\1.5.2`} {
		if err := os.WriteFile(versionFile, []byte(version), 0600); err != nil {
			t.Fatal(err)
		}
		if got := managedPiPackage(root); got != "" {
			t.Fatalf("invalid version %q resolved to %q", version, got)
		}
	}
}

func TestPiLocalPackageResolution(t *testing.T) {
	p := fakePi(t)
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	// Pi's published package exposes an import-only entry point. CommonJS
	// require.resolve must not be used for this fallback.
	cwd := t.TempDir()
	pkg := filepath.Join(cwd, "node_modules", filepath.FromSlash(piPackageName))
	if err := os.MkdirAll(filepath.Join(pkg, "dist"), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(p.PackageDir, "dist", "index.js"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "dist", "index.js"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "package.json"), []byte(`{"type":"module","exports":{".":{"import":"./dist/index.js"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PI_PACKAGE_DIR", "")
	t.Setenv("PI_MANAGED_INSTALL_ROOT", "")
	t.Setenv("PATH", t.TempDir())
	t.Chdir(cwd)
	p.PackageDir, p.NodePath = "", node
	if _, err := p.Generate(context.Background(), piRequest("text-tools"), nil); err != nil {
		t.Fatal(err)
	}
}

func TestPiForcedCancellation(t *testing.T) {
	p := fakePi(t)
	// A trusted-but-broken SDK may ignore AbortSignal. Bound host shutdown anyway.
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "dist"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"type":"module"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dist", "index.js"), []byte(`await new Promise(() => { setInterval(() => {}, 1000); });`), 0600); err != nil {
		t.Fatal(err)
	}
	p.PackageDir = dir
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := p.Generate(ctx, piRequest("hold"), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	if time.Since(start) > 6*time.Second {
		t.Fatal("cancellation exceeded grace period")
	}
}

func TestPiContextWindowFromCatalog(t *testing.T) {
	p := fakePi(t)
	if got := p.ContextWindow(durable.ModelRef{Provider: "fake", ID: "small-window"}); got != 4096 {
		t.Fatalf("context window=%d", got)
	}
	if got := p.ContextWindow(durable.ModelRef{Provider: "fake", ID: "unknown"}); got != 0 {
		t.Fatalf("unknown model window=%d", got)
	}
}

// A model the catalog lacks is not available (upstream getModel), so a run
// for it ends no_model; one it has, or a catalog it cannot read, is.
func TestPiHasTheModelsOfItsCatalog(t *testing.T) {
	p := fakePi(t)
	var _ durable.ModelLookup = p
	if !p.HasModel(durable.ModelRef{Provider: "fake", ID: "small-window"}) || p.HasModel(durable.ModelRef{Provider: "fake", ID: "unknown"}) {
		t.Fatal("catalog lookup")
	}
	broken := p
	broken.NodePath = filepath.Join(t.TempDir(), "missing-node")
	if !broken.HasModel(durable.ModelRef{Provider: "fake", ID: "elsewhere"}) {
		t.Fatal("an unreadable catalog must not make a model unavailable")
	}
}

func TestPiStreamsThinkingAndToolCallEvents(t *testing.T) {
	var events []durable.StreamEvent
	response, err := fakePi(t).GenerateEvents(context.Background(), durable.ModelRequest{Model: durable.ModelRef{Provider: "fake", ID: "events"}}, func(event durable.StreamEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []durable.StreamEvent{
		{Type: "thinking_delta", Index: 0, Delta: "plan"},
		{Type: "text_delta", Index: 1, Delta: "reading"},
		{Type: "toolcall_start", Index: 2, ID: "call-9", Name: "read"},
		{Type: "toolcall_delta", Index: 2, Delta: `{"path": "a`},
		{Type: "toolcall_delta", Index: 2, Delta: `.txt"}`},
		{Type: "toolcall_end", Index: 2},
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%+v\nwant %+v", events, want)
	}
	if len(response.ToolCalls) != 1 || string(response.ToolCalls[0].Arguments) != `{"path":"a.txt"}` {
		t.Fatalf("response=%+v", response)
	}
	// Generate still streams only the text.
	var deltas []string
	if _, err := fakePi(t).Generate(context.Background(), durable.ModelRequest{Model: durable.ModelRef{Provider: "fake", ID: "events"}}, func(text string) error {
		deltas = append(deltas, text)
		return nil
	}); err != nil || !reflect.DeepEqual(deltas, []string{"reading"}) {
		t.Fatalf("deltas=%v error=%v", deltas, err)
	}
}

func TestPiDeferredAnswers(t *testing.T) {
	p := fakePi(t)
	ref := durable.ModelRef{Provider: "fake", ID: "deferred"}
	response, err := p.Generate(context.Background(), durable.ModelRequest{Model: ref, Stream: &durable.StreamOptions{Deferred: &durable.DeferredOptions{}}}, nil)
	if err != nil || response.StopReason != "deferred" || response.Deferred == nil || response.Deferred.ID != "job-later" ||
		response.Deferred.PollAfterMs != 10 || string(response.Deferred.Data) != `{"row":1}` {
		t.Fatalf("response=%+v deferred=%+v error=%v", response, response.Deferred, err)
	}
	// Not ready yet: deferred again, with only pi-ai's handle fields.
	later, err := p.FetchDeferred(context.Background(), ref, *response.Deferred)
	if err != nil || later.StopReason != "deferred" || later.Deferred == nil || later.Deferred.ID != "job-ready" {
		t.Fatalf("later=%+v error=%v", later, err)
	}
	answer, err := p.FetchDeferred(context.Background(), ref, *later.Deferred)
	if err != nil || answer.Content != "deferred answer" || answer.StopReason != "stop" {
		t.Fatalf("answer=%+v error=%v", answer, err)
	}
	// A failed fetch is a sanitized, classified provider error.
	_, err = p.FetchDeferred(context.Background(), ref, durable.DeferredHandle{ID: "job-fail"})
	if err == nil || strings.Contains(err.Error(), "secret") || !durable.IsRetryableModelError(err) {
		t.Fatalf("failed fetch error=%v", err)
	}
	cancelled := filepath.Join(t.TempDir(), "cancelled")
	t.Setenv("PI_DURABLE_TEST_CANCELLED", cancelled)
	if err := p.CancelDeferred(context.Background(), ref, durable.DeferredHandle{ID: "job-ready"}); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(cancelled); err != nil || string(data) != "job-ready" {
		t.Fatalf("cancelled=%q error=%v", data, err)
	}
}

func TestPiSendsThePayloadItsHookReturns(t *testing.T) {
	p := fakePi(t)
	var api string
	response, err := p.GenerateEvents(context.Background(), durable.ModelRequest{
		Model:    durable.ModelRef{Provider: "fake", ID: "payload"},
		Messages: []durable.Message{{Role: "user", Content: "hi"}},
		OnPayload: func(_ context.Context, format string, payload json.RawMessage) (json.RawMessage, error) {
			api = format
			var body map[string]any
			if err := json.Unmarshal(payload, &body); err != nil {
				return nil, err
			}
			body["hooked"] = true
			return json.Marshal(body)
		},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if api != "fake-api" || response.Content != `{"hooked":true,"messages":1,"model":"payload"}` {
		t.Fatalf("api=%q sent=%s", api, response.Content)
	}
	// A failing hook fails the provider request.
	_, err = p.GenerateEvents(context.Background(), durable.ModelRequest{
		Model:    durable.ModelRef{Provider: "fake", ID: "payload"},
		Messages: []durable.Message{{Role: "user", Content: "hi"}},
		OnPayload: func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
			return nil, errors.New("hook failed")
		},
	}, nil)
	if err == nil {
		t.Fatal("a failing payload hook was ignored")
	}
	// Without a hook, the bridge sends no payload events.
	response, err = p.GenerateEvents(context.Background(), durable.ModelRequest{
		Model:    durable.ModelRef{Provider: "fake", ID: "payload"},
		Messages: []durable.Message{{Role: "user", Content: "hi"}},
	}, nil)
	if err != nil || response.Content != `{"model":"payload","messages":1}` {
		t.Fatalf("without a hook: sent=%s error=%v", response.Content, err)
	}
}
