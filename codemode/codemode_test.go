package codemode

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

// scriptModel calls codemode with each script in turn, one per request
// after a tool result, then answers "done".
type scriptModel struct {
	mu      sync.Mutex
	scripts []string
	next    int
}

func (m *scriptModel) Generate(ctx context.Context, request durable.ModelRequest, _ func(string) error) (durable.ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return durable.ModelResponse{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	last := request.Messages[len(request.Messages)-1]
	if last.Role == "user" && last.Content != "" || last.Role == "tool" {
		if m.next < len(m.scripts) {
			args, _ := json.Marshal(map[string]string{"code": m.scripts[m.next]})
			m.next++
			return durable.ModelResponse{ToolCalls: []durable.ToolCall{{ID: fmt.Sprintf("s%d", m.next), Name: ToolName, Arguments: args}}}, nil
		}
	}
	return durable.ModelResponse{Content: "done"}, nil
}

var parametersSchema = json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"},"id":{"type":"number"}}}`)

// echoTool answers "echo <text>" and counts its runs by text.
func echoTool(runs *sync.Map) durable.ToolDefinition {
	return durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: "echo", Description: "echo", Parameters: parametersSchema},
		Execute: func(ctx context.Context, args json.RawMessage, r *durable.ToolRuntime) (durable.ToolResult, error) {
			var input struct{ Text string }
			_ = json.Unmarshal(args, &input)
			if runs != nil {
				count, _ := runs.LoadOrStore(input.Text, new(int))
				*count.(*int)++
			}
			return durable.ToolResult{Content: "echo " + input.Text}, nil
		}}
}

type testChat struct {
	h    *durable.Harness
	root *durable.Conversation
}

func openChat(t *testing.T, ctx context.Context, storage durable.Storage, model durable.Model, extensions ...durable.Extension) testChat {
	t.Helper()
	return openChatWith(t, ctx, storage, model, nil, extensions...)
}

// openChatWith opens a chat whose conversations use env (nil: testEnv).
func openChatWith(t *testing.T, ctx context.Context, storage durable.Storage, model durable.Model, env durable.ExecutionEnv, extensions ...durable.Extension) testChat {
	t.Helper()
	if env == nil {
		env = testEnv{}
	}
	registry := durable.NewRegistry()
	for _, extension := range extensions {
		if err := registry.Install(extension); err != nil {
			t.Fatal(err)
		}
	}
	h, err := durable.Open(ctx, storage, durable.HarnessOptions{Registry: registry, Model: model,
		Environment: func(durable.ConversationRecord) (durable.ExecutionEnv, error) { return env, nil }})
	if err != nil {
		t.Fatal(err)
	}
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Model: durable.ModelRef{Provider: "fake", ID: "script"}}})
	if err != nil {
		t.Fatal(err)
	}
	return testChat{h: h, root: root}
}

// testEnv writes nothing: result files fail with its error.
type testEnv struct{}

func (testEnv) ReadFile(context.Context, string) ([]byte, error) { return nil, fmt.Errorf("no files") }
func (testEnv) WriteFile(context.Context, string, []byte) error  { return fmt.Errorf("no files") }
func (testEnv) Exec(context.Context, string, func(string) error) (durable.ExecResult, error) {
	return durable.ExecResult{}, fmt.Errorf("no processes")
}

var header = regexp.MustCompile(`^Script (completed|failed)\nWall time [\d.]+ seconds\nOutput:\n\n?`)

// codemodeResult is a code mode call's result, without its header.
type codemodeResult struct {
	durable.Message
	details ToolDetails
}

// codemodeResults are the code mode calls' results.
func codemodeResults(t *testing.T, ctx context.Context, h *durable.Harness) []codemodeResult {
	t.Helper()
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]*durable.Entry, 0, len(state.Entries))
	for _, entry := range state.Entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	var results []codemodeResult
	for _, entry := range entries {
		if entry.Kind == "pi.tool-result" && entry.Message.ToolName == ToolName {
			result := codemodeResult{Message: *entry.Message}
			result.Content = header.ReplaceAllString(result.Content, "")
			// The result's details are on its message, as upstream's.
			if len(entry.Message.Details) > 0 {
				if err := json.Unmarshal(entry.Message.Details, &result.details); err != nil {
					t.Fatalf("details %s: %v", entry.Message.Details, err)
				}
			}
			results = append(results, result)
		}
	}
	return results
}

func submitAndWait(t *testing.T, ctx context.Context, chat testChat, content string) {
	t.Helper()
	sub, err := chat.root.Submit(ctx, durable.SubmissionRequest{Content: content})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != "done" {
		t.Fatalf("submission %+v", settled)
	}
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCodemodeRunsScriptsThatCallTools(t *testing.T) {
	sandbox := installedSandbox(t)
	ctx := testContext(t)
	runs := &sync.Map{}
	model := &scriptModel{scripts: []string{
		`const [a, b] = await Promise.all([tools.echo({ text: "one" }), tools.echo({ text: "two" })]);
console.log("both", a, b);
store("kept", { a });
return [a, b];`,
		`return load("kept");`,
		`const found = await searchTools("echo");
return { found, described: (await describeTool("echo")).split("\n")[0], all: ALL_TOOLS.length };`,
		`try { await tools.echo("not an object"); } catch (error) { return "rejected: " + error.message; }`,
	}}
	chat := openChat(t, ctx, durable.NewMemoryStorage(), model, durable.Extension{Name: "test", Tools: []durable.ToolDefinition{echoTool(runs)}},
		Extension(Options{Sandbox: sandbox}))
	defer chat.h.Close(context.Background())
	submitAndWait(t, ctx, chat, "go")
	results := codemodeResults(t, ctx, chat.h)
	if len(results) != 4 {
		t.Fatalf("%d results", len(results))
	}
	want := []string{
		"[\"echo one\",\"echo two\"]\n<console_output>\nboth echo one echo two\n</console_output>",
		`{"a":"echo one"}`,
		`{"found":[{"name":"echo","description":"echo\n\ncodemode tool declaration:\n` + "```ts\\ndeclare const tools: { echo(args: { id?: number; text?: string; }): Promise<string>; };\\n```" + `"}],"described":"echo","all":1}`,
		"rejected: Validation failed for tool \"echo\":\n  - root: expected object, got string\n\nReceived arguments:\n\"not an object\"",
	}
	for i, result := range results {
		if result.IsError || result.Content != want[i] {
			t.Errorf("result %d (error %v):\n%s\nwant\n%s", i, result.IsError, result.Content, want[i])
		}
	}
	details := results[0].details
	if len(details.Calls) != 2 || details.Calls[0].Name != "echo" || details.Calls[0].Args != `{"text":"one"}` || details.Calls[0].Status != "ok" {
		t.Fatalf("details %+v", details)
	}
	for _, text := range []string{"one", "two"} {
		if count, _ := runs.Load(text); count == nil || *count.(*int) != 1 {
			t.Fatalf("echo %q ran %v times", text, count)
		}
	}
}
