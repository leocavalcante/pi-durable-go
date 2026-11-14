package codemode

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/tools"
)

// Ports of upstream's code mode models.* tests
// (test/codemode-models.test.ts), with a catalog of a classifier and an
// image model.

const testPNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="

type testModels struct {
	mu          sync.Mutex
	classify    int
	images      int
	inFlight    int
	maxInFlight int
	// gate holds classifier calls while open is not closed; atFour opens it
	// shortly after four run, so a fifth would show up.
	open   chan struct{}
	atFour bool
}

func newTestModels() *testModels {
	open := make(chan struct{})
	close(open)
	return &testModels{open: open}
}

var catalog = []struct {
	modelType string
	model     json.RawMessage
}{
	{"classifier", json.RawMessage(`{"id":"judge","name":"Judge","api":"test-classifier","provider":"test","baseUrl":"https://example.invalid","headers":{"authorization":"secret"},"type":"classifier"}`)},
	{"image", json.RawMessage(`{"id":"painter","name":"Painter","api":"test-images","provider":"test","baseUrl":"https://example.invalid","type":"image"}`)},
}

func (m *testModels) ModelsOfType(_ context.Context, modelType, provider string) ([]json.RawMessage, error) {
	var found []json.RawMessage
	for _, entry := range catalog {
		if entry.modelType == modelType && (provider == "" || provider == "test") {
			found = append(found, entry.model)
		}
	}
	return found, nil
}

func (m *testModels) AvailableOfType(ctx context.Context, modelType, provider string) ([]json.RawMessage, error) {
	return m.ModelsOfType(ctx, modelType, provider)
}

func (m *testModels) ModelOfType(_ context.Context, modelType, provider, id string) (json.RawMessage, error) {
	for _, entry := range catalog {
		var model struct{ ID, Provider string }
		_ = json.Unmarshal(entry.model, &model)
		if entry.modelType == modelType && model.Provider == provider && model.ID == id {
			return entry.model, nil
		}
	}
	return nil, nil
}

func (m *testModels) Classify(ctx context.Context, _ json.RawMessage, classifierContext json.RawMessage) (json.RawMessage, error) {
	var input struct {
		State struct{ Fail bool } `json:"state"`
	}
	_ = json.Unmarshal(classifierContext, &input)
	m.mu.Lock()
	m.classify++
	if input.State.Fail {
		m.mu.Unlock()
		return json.RawMessage(`{"answers":{},"stopReason":"error","errorMessage":"quota exceeded"}`), nil
	}
	m.inFlight++
	m.maxInFlight = max(m.maxInFlight, m.inFlight)
	if m.atFour && m.inFlight == 4 {
		open := m.open
		go func() {
			time.Sleep(50 * time.Millisecond)
			close(open)
		}()
	}
	open := m.open
	m.mu.Unlock()
	select {
	case <-open:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	m.mu.Lock()
	m.inFlight--
	m.mu.Unlock()
	return json.RawMessage(`{"answers":{"good":{"value":true}},"usage":{"input":10,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":12,"cost":{"input":0.001,"output":0.0002,"cacheRead":0,"cacheWrite":0,"total":0.0012}},"stopReason":"stop"}`), nil
}

func (m *testModels) GenerateImages(context.Context, json.RawMessage, json.RawMessage) (json.RawMessage, error) {
	m.mu.Lock()
	m.images++
	m.mu.Unlock()
	return json.RawMessage(`{"output":[{"type":"image","data":"` + testPNG + `","mimeType":"image/png"}],"stopReason":"stop"}`), nil
}

type modelsRun struct {
	result codemodeResult
	usage  durable.UsageState
}

// runModels runs code with models.* and returns its result and usage.
func runModels(t *testing.T, models *testModels, code string, env durable.ExecutionEnv) modelsRun {
	t.Helper()
	sandbox := testSandbox(t)
	ctx := testContext(t)
	registry := durable.NewRegistry()
	if err := registry.Install(Extension(Options{Sandbox: sandbox, Models: models})); err != nil {
		t.Fatal(err)
	}
	if env == nil {
		env = testEnv{}
	}
	h, err := durable.Open(ctx, durable.NewMemoryStorage(), durable.HarnessOptions{Registry: registry, Model: &scriptModel{scripts: []string{code}},
		Environment: func(durable.ConversationRecord) (durable.ExecutionEnv, error) { return env, nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Model: durable.ModelRef{Provider: "fake", ID: "script"}}})
	if err != nil {
		t.Fatal(err)
	}
	submitAndWait(t, ctx, testChat{h: h, root: root}, "go")
	results := codemodeResults(t, ctx, h)
	if len(results) != 1 {
		t.Fatalf("%d results", len(results))
	}
	usage, err := h.Usage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return modelsRun{result: results[0], usage: usage}
}

func TestCodemodeModelsReadTheCatalogWithoutHeaders(t *testing.T) {
	run := runModels(t, newTestModels(), `const classifiers = await models.getModelsOfType("classifier", "test");
		const one = await models.getModelOfType("classifier", "test", "judge");
		const available = await models.getAvailableOfType("image");
		return [classifiers.map((m) => m.id), one.id, "headers" in one, available.map((m) => m.id)];`, nil)
	expectResult(t, run.result, `[["judge"],"judge",false,["painter"]]`, false)
}

func TestCodemodeModelsClassifyThroughAnInternalTool(t *testing.T) {
	models := newTestModels()
	run := runModels(t, models, `const model = await models.getModelOfType("classifier", "test", "judge");
		const result = await models.classify(model, {
			state: { text: "hi" },
			questions: { good: { type: "bool", instructions: "Is it good?", criteria: { true: "yes", false: "no" } } },
		});
		const found = (await searchTools("classify generate images models")).length;
		const described = (await describeTool("pi.codemode.models.classify")) !== undefined;
		return [result.stopReason, result.answers.good.value, Object.keys(tools).some((name) => name.includes("models")), ALL_TOOLS.some((tool) => tool.name.includes("models")), found, described];`, nil)
	expectResult(t, run.result, `["stop",true,false,false,0,false]`, false)
	if models.classify != 1 {
		t.Fatalf("classify ran %d times", models.classify)
	}
	if calls := run.result.details.Calls; len(calls) != 1 || calls[0].Name != "models.classify" || calls[0].Status != "ok" {
		t.Fatalf("calls %+v", calls)
	}
	// Counted once, under the internal tool, by the Harness.
	if cost := run.usage.Tools[ClassifyTool].Cost; math.Abs(cost-0.0012) > 1e-9 {
		t.Fatalf("usage %+v", run.usage)
	}
}

func TestCodemodeModelsResolveAFailedCallWithItsResult(t *testing.T) {
	run := runModels(t, newTestModels(), `const model = await models.getModelOfType("classifier", "test", "judge");
		const result = await models.classify(model, {
			state: { fail: true },
			questions: { good: { type: "bool", instructions: "?", criteria: { true: "y", false: "n" } } },
		});
		return [result.stopReason, result.errorMessage];`, nil)
	expectResult(t, run.result, `["error","quota exceeded"]`, false)
	// The row still shows the failure.
	if calls := run.result.details.Calls; len(calls) != 1 || calls[0].Name != "models.classify" || calls[0].Status != "error" {
		t.Fatalf("calls %+v", calls)
	}
}

func TestCodemodeModelsRejectABadContextBeforeAnyRequest(t *testing.T) {
	models := newTestModels()
	run := runModels(t, models, `const model = await models.getModelOfType("classifier", "test", "judge");
		try { await models.classify(model, { prompt: "x" }); } catch (error) { return error.message; }`, nil)
	if !strings.Contains(run.result.Content, "context.state must be an object") || !strings.Contains(run.result.Content, "Expected context:") || models.classify != 0 {
		t.Fatalf("result %q, classify ran %d times", run.result.Content, models.classify)
	}
}

func TestCodemodeModelsRunAtMostFourCallsAtOnce(t *testing.T) {
	models := newTestModels()
	models.open, models.atFour = make(chan struct{}), true
	run := runModels(t, models, `const model = await models.getModelOfType("classifier", "test", "judge");
		const context = { state: {}, questions: { good: { type: "bool", instructions: "?", criteria: { true: "y", false: "n" } } } };
		return (await Promise.all(Array.from({ length: 8 }, () => models.classify(model, context)))).length;`, nil)
	expectResult(t, run.result, "8", false)
	if models.classify != 8 || models.maxInFlight != 4 {
		t.Fatalf("classify %d, at once %d", models.classify, models.maxInFlight)
	}
}

// slowTempDirs opens the models' gate when the result spills to a file,
// after the script ended and before the tool returns.
type slowTempDirs struct {
	*tools.LocalEnv
	models *testModels
}

func (e slowTempDirs) CreateTempDir(ctx context.Context, prefix string) (string, error) {
	close(e.models.open)
	time.Sleep(100 * time.Millisecond)
	return e.LocalEnv.CreateTempDir(ctx, prefix)
}

func TestCodemodeModelsStartNoCallThatWaitedPastTheScriptsEnd(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	models := newTestModels()
	models.open = make(chan struct{})
	local, err := tools.NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	run := runModels(t, models, `const model = await models.getModelOfType("classifier", "test", "judge");
		const context = { state: {}, questions: { good: { type: "bool", instructions: "?", criteria: { true: "y", false: "n" } } } };
		for (let i = 0; i < 6; i++) models.classify(model, context).catch(() => {});
		// A journaled call after them: by its result, the first four have been launched.
		await models.getModelsOfType("image");
		return "x".repeat(60000);`, slowTempDirs{LocalEnv: local, models: models})
	if !strings.Contains(run.result.Content, "[Full output: ") || models.classify != 4 {
		t.Fatalf("result %.200q, classify ran %d times", run.result.Content, models.classify)
	}
	var details ToolDetails = run.result.details
	if details.FullOutputPath == "" {
		t.Fatal("no full output path")
	}
	if data, err := os.ReadFile(details.FullOutputPath); err != nil || len(data) != 60000 {
		t.Fatalf("full output %d bytes, %v", len(data), err)
	}
}

func TestCodemodeModelsNoteGeneratedImagesTheScriptDidNotShow(t *testing.T) {
	hidden := runModels(t, newTestModels(), `const model = await models.getModelOfType("image", "test", "painter");
		const result = await models.generateImages(model, { input: [{ type: "text", text: "a cat" }] });
		return result.output.length;`, nil)
	if !strings.Contains(hidden.result.Content, "Note: models.generateImages() returned 1 image that the script did not show") {
		t.Fatalf("result %q", hidden.result.Content)
	}
	shown := runModels(t, newTestModels(), `const model = await models.getModelOfType("image", "test", "painter");
		const result = await models.generateImages(model, { input: [{ type: "text", text: "a cat" }] });
		image(result.output[0]);`, nil)
	if strings.Contains(shown.result.Content, "Note:") || len(shown.result.Images) != 1 {
		t.Fatalf("result %q, %d images", shown.result.Content, len(shown.result.Images))
	}
}
