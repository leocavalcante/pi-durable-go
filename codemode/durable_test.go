package codemode

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

// The durable side with the fake sandbox: make check runs these without
// Node.

func list(values ...json.RawMessage) json.RawMessage {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = string(value)
	}
	return json.RawMessage("[" + strings.Join(parts, ",") + "]")
}

func fakeScripts() map[string]fakeScript {
	return map[string]fakeScript{
		"sequential": func(vm *fakeVM) (json.RawMessage, error) {
			a := vm.wait(vm.call("tool", "echo", `{"text":"a"}`))
			b := vm.wait(vm.call("tool", "echo", `{"text":"b"}`))
			return list(a.Value, b.Value), nil
		},
		"held": func(vm *fakeVM) (json.RawMessage, error) {
			a := vm.wait(vm.call("tool", "echo", `{"text":"a"}`))
			held := vm.wait(vm.call("tool", "hold", `{"id":1}`))
			b := vm.wait(vm.call("tool", "echo", `{"text":"b"}`))
			return list(a.Value, held.Value, b.Value), nil
		},
		"global": func(vm *fakeVM) (json.RawMessage, error) {
			n := vm.wait(vm.call("global", "next", `[]`))
			vm.wait(vm.call("tool", "hold", `{}`))
			m := vm.wait(vm.call("global", "next", `[]`))
			return list(n.Value, m.Value), nil
		},
		"store": func(vm *fakeVM) (json.RawMessage, error) {
			vm.storeValue("kept", json.RawMessage(`{"n":1}`))
			return json.RawMessage(`"stored"`), nil
		},
		"load": func(vm *fakeVM) (json.RawMessage, error) {
			return vm.load("kept"), nil
		},
		"throws": func(vm *fakeVM) (json.RawMessage, error) {
			vm.text("before")
			vm.wait(vm.call("tool", "echo", `{"text":"a"}`))
			return nil, errBoom
		},
	}
}

var errBoom = &scriptError{"boom"}

type scriptError struct{ message string }

func (e *scriptError) Error() string { return e.message }

func newFakeSetup(t *testing.T, globals ...Global) (*setup, *fakeSandbox) {
	fake := &fakeSandbox{fingerprint: "fake 1", scripts: fakeScripts()}
	return newSetupWith(t, fake, globals...), fake
}

func TestDurableCodemodeJournalsEachResultBeforeDelivering(t *testing.T) {
	s, _ := newFakeSetup(t)
	result := s.runOnce("sequential")
	expectResult(t, result, `["echo a","echo b"]`, false)
	if len(result.details.Calls) != 2 || result.details.Calls[1] != (NestedCall{ID: 2, Name: "echo", Args: `{"text":"b"}`, Status: "ok", DurationMs: result.details.Calls[1].DurationMs}) {
		t.Fatalf("calls %+v", result.details.Calls)
	}
}

func TestDurableCodemodeResumesAfterARestart(t *testing.T) {
	s, fake := newFakeSetup(t)
	result, h := s.restart("held", nil, nil)
	expectResult(t, result, `["echo a","held 1","echo b"]`, false)
	expectCounts(t, s.echo.snapshot(), map[string]int{"a": 1, "b": 1})
	if fake.starts != 2 || s.holds.runs != 2 {
		t.Fatalf("starts %d, hold runs %d", fake.starts, s.holds.runs)
	}
	if n := len(nestedCalls(t, s.ctx, h)); n != 3 {
		t.Fatalf("%d nested calls", n)
	}
}

func TestDurableCodemodeReplaysGlobalsFromTheJournal(t *testing.T) {
	counter := 0
	s, _ := newFakeSetup(t, Global{Name: "next", Spread: true, Execute: func(_ context.Context, _ json.RawMessage, _ *durable.ToolRuntime) (json.RawMessage, error) {
		counter++
		return json.Marshal(counter)
	}})
	result, _ := s.restart("global", nil, nil)
	expectResult(t, result, "[1,2]", false)
	if counter != 2 {
		t.Fatalf("next ran %d times", counter)
	}
}

func TestDurableCodemodeEndsDivergingReplays(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		s, _ := newFakeSetup(t)
		result, _ := s.restart("held", func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, JournalDocument, task, func(journal *journalState) {
				seven := 7
				journal.Entries[0].IssuedAfter = &seven
			})
		}, nil)
		expectResult(t, result, "The script took a different path when resumed after a restart: after result 1 it had made 2 calls, where it had made 7", true)
		expectCounts(t, s.echo.snapshot(), map[string]int{"a": 1})
	})
	t.Run("fingerprint", func(t *testing.T) {
		s, _ := newFakeSetup(t)
		result, _ := s.restart("held", func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) { run.Start.Fingerprint = "another" })
		}, nil)
		expectResult(t, result, "The script cannot resume: the sandbox changed since it started (another -> fake 1).", true)
	})
	t.Run("version", func(t *testing.T) {
		s, fake := newFakeSetup(t)
		result, _ := s.restart("held", func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) { run.Start.Version = 0 })
		}, nil)
		expectResult(t, result, "The script cannot resume: it started under code mode version 0, this is 2.", true)
		if fake.starts != 1 {
			t.Fatalf("starts %d", fake.starts)
		}
	})
}

func TestDurableCodemodeKeepsStoreValuesAcrossCalls(t *testing.T) {
	s, _ := newFakeSetup(t)
	s.holds.setBlock(false)
	s.model.scripts = []string{"store", "load"}
	chat := s.openSQLite()
	defer chat.h.Close(s.ctx)
	submitAndWait(t, s.ctx, chat, "go")
	results := codemodeResults(t, s.ctx, chat.h)
	if len(results) != 2 || results[0].Content != "stored" || results[1].Content != `{"n":1}` {
		t.Fatalf("results %+v", results)
	}
}

func TestDurableCodemodeReportsScriptErrors(t *testing.T) {
	s, _ := newFakeSetup(t)
	result := s.runOnce("throws")
	expectResult(t, result, "before\nScript error:\nError: boom\n\nTool calls made before the failure (they are not undone): echo (ok)", true)
}

// recordingModel answers "done" and keeps the requests it saw.
type recordingModel struct {
	mu       sync.Mutex
	requests []durable.ModelRequest
}

func (m *recordingModel) Generate(_ context.Context, request durable.ModelRequest, _ func(string) error) (durable.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, request)
	return durable.ModelResponse{Content: "done"}, nil
}

func TestCodemodeSectionListsTheCallableTools(t *testing.T) {
	ctx := testContext(t)
	model := &recordingModel{}
	deferred := echoTool(nil)
	deferred.Name, deferred.Deferred = "later", true
	modelOnly := echoTool(nil)
	modelOnly.Name, modelOnly.Callers = "modelonly", []string{"model"}
	chat := openChat(t, ctx, durable.NewMemoryStorage(), model,
		durable.Extension{Name: "test", Tools: []durable.ToolDefinition{echoTool(nil), deferred, modelOnly}},
		Extension(Options{Sandbox: &fakeSandbox{}}))
	defer chat.h.Close(context.Background())
	submitAndWait(t, ctx, chat, "go")
	if len(model.requests) == 0 {
		t.Fatal("no request")
	}
	var section *string
	for _, message := range model.requests[0].Messages {
		for _, candidate := range message.Sections {
			if candidate.Name == "codemode" {
				section = candidate.Text
			}
		}
	}
	// Deferred tools and tools only the model may call are left out.
	want := "<codemode>\nTools scripts can call (`await tools.<name>(args)`):\n\n### `echo`\necho\n\ncodemode tool declaration:\n```ts\ndeclare const tools: { echo(args: { id?: number; text?: string; }): Promise<string>; };\n```\n</codemode>"
	if section == nil || *section != want {
		got := "<nil>"
		if section != nil {
			got = *section
		}
		t.Fatalf("codemode section\n%s\nwant\n%s", got, want)
	}
	// The code mode tool is offered with its fixed description; scripts, not
	// the model, see the callable tools.
	var offered *durable.ToolSpec
	for i, tool := range model.requests[0].Tools {
		if tool.Name == ToolName {
			offered = &model.requests[0].Tools[i]
		}
	}
	if offered == nil || offered.Description != Description(false) || string(offered.Parameters) != string(toolSchema) {
		t.Fatalf("offered %+v", offered)
	}
}

func TestCodemodeToolDeclaration(t *testing.T) {
	tool := Tool(Options{})
	var sampling struct {
		Type     string            `json:"type"`
		Variants map[string]string `json:"variants"`
	}
	if err := json.Unmarshal(tool.ConstrainedSampling, &sampling); err != nil || sampling.Type != "grammar" || sampling.Variants["openai_lark"] != SourceGrammar {
		t.Fatalf("constrained sampling %s", tool.ConstrainedSampling)
	}
	if !tool.ReplaySafe || len(tool.Callers) != 1 || tool.Callers[0] != "model" || tool.OutputLimits.MaxBytes != 4<<20 || tool.OutputLimits.MaxLines != 100_000 {
		t.Fatalf("tool %+v", tool)
	}
	if !strings.Contains(Tool(Options{Models: noModels{}}).Description, ModelsDescription) || strings.Contains(tool.Description, "`models`") {
		t.Fatal("the models paragraph follows Options.Models")
	}
}

// noModels is a catalog without models.
type noModels struct{}

func (noModels) ModelsOfType(context.Context, string, string) ([]json.RawMessage, error) {
	return nil, nil
}
func (noModels) AvailableOfType(context.Context, string, string) ([]json.RawMessage, error) {
	return nil, nil
}
func (noModels) ModelOfType(context.Context, string, string, string) (json.RawMessage, error) {
	return nil, nil
}
func (noModels) Classify(context.Context, json.RawMessage, json.RawMessage) (ModelResult, error) {
	return ModelResult{}, nil
}
func (noModels) GenerateImages(context.Context, json.RawMessage, json.RawMessage) (ModelResult, error) {
	return ModelResult{}, nil
}
