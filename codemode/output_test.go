package codemode

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/js"
	"github.com/leocavalcante/pi-durable-go/tools"
)

// Ports of upstream's "durable code mode output and tool search" tests,
// with a local environment for the result's files.

// fullText is the whole result, header included, as upstream's tests join
// its items.
func fullText(t *testing.T, ctx context.Context, h *durable.Harness) (string, codemodeResult) {
	t.Helper()
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range state.Entries {
		if entry.Kind == "pi.tool-result" && entry.Message.ToolName == ToolName {
			result := codemodeResults(t, ctx, h)[0]
			return entry.Message.Content, result
		}
	}
	t.Fatal("no result")
	return "", codemodeResult{}
}

// runScript runs code once with a local environment.
func runScript(t *testing.T, s *setup, code string) (string, codemodeResult) {
	t.Helper()
	// The result's files go to temp directories of the test.
	t.Setenv("TMPDIR", t.TempDir())
	local, err := tools.NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if s.env == nil {
		s.env = local
	}
	s.runOnce(code)
	chat := s.openSQLite()
	defer chat.h.Close(context.Background())
	return fullText(t, s.ctx, chat.h)
}

func TestCodemodeOutputLayout(t *testing.T) {
	text, _ := runScript(t, newSetup(t), `text("one"); console.log("log", 1); text({ two: 2 }); return "three";`)
	if !regexp.MustCompile(`^Script completed\nWall time [\d.]+ seconds\nOutput:\n`).MatchString(text) ||
		!strings.Contains(text, "==> text 1/3 <==\none\n==> text 2/3 <==\n{\"two\":2}\n==> text 3/3 <==\nthree") ||
		!strings.Contains(text, "<console_output>\nlog 1\n</console_output>") {
		t.Fatalf("result %q", text)
	}
}

func TestCodemodeOutputCutToTheTokenBudget(t *testing.T) {
	text, result := runScript(t, newSetup(t), `// @options: {"max_output_tokens": 20}
		text("BEGIN" + "x".repeat(1000) + "END");`)
	path := result.details.FullOutputPath
	if !strings.Contains(text, "Warning: truncated output") || !strings.Contains(text, "BEGIN") || !strings.Contains(text, "END") ||
		path == "" || !strings.Contains(text, "[Full output: "+path) {
		t.Fatalf("result %q, path %q", text, path)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "BEGIN"+strings.Repeat("x", 1000)+"END" {
		t.Fatalf("full output %q, %v", data, err)
	}
}

func TestCodemodeOutputStaysInsideTheToolsLimits(t *testing.T) {
	// 6 MiB of text: over the Harness's 4 MiB limit, which would cut it
	// without a spill file.
	text, result := runScript(t, newSetup(t), `// @options: {"max_output_tokens": 2000000}
		return "x".repeat(6 * 1024 * 1024);`)
	if !strings.Contains(text, "Warning: truncated output") || js.Len(text) > 1024*1024+1000 {
		t.Fatalf("result of %d characters", js.Len(text))
	}
	if info, err := os.Stat(result.details.FullOutputPath); err != nil || info.Size() != 6*1024*1024 {
		t.Fatalf("full output %v, %v", info, err)
	}
	// 200,000 short lines: within the byte limit, over the 100,000 lines one.
	text, result = runScript(t, newSetup(t), `// @options: {"max_output_tokens": 2000000}
		return Array.from({ length: 200000 }, () => "a").join("\n");`)
	if !strings.Contains(text, "Warning: truncated output") || strings.Count(text, "\n")+1 >= 100_000 || result.details.FullOutputPath == "" {
		t.Fatalf("result of %d lines, path %q", strings.Count(text, "\n")+1, result.details.FullOutputPath)
	}
}

// crashAfterWrite writes a file, then waits until its call is aborted: the
// process dies after the full output was written and before the result was
// committed.
type crashAfterWrite struct {
	*tools.LocalEnv
	written chan string
}

func (e crashAfterWrite) WriteFile(ctx context.Context, path string, data []byte) error {
	err := e.LocalEnv.WriteFile(ctx, path, data)
	e.written <- path
	<-ctx.Done()
	return err
}

func TestCodemodeOutputFilesKeepTheirPathsAcrossACrash(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	s := newSetup(t)
	s.holds.setBlock(false)
	local, err := tools.NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan string, 1)
	s.env = crashAfterWrite{LocalEnv: local, written: written}
	s.model.scripts = []string{"// @options: {\"max_output_tokens\": 20}\nreturn \"x\".repeat(1000);"}
	chat := s.openSQLite()
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	first := <-written
	if err := chat.h.Close(s.ctx); err != nil {
		t.Fatal(err)
	}
	s.env = local
	chat = s.openSQLite()
	defer chat.h.Close(context.Background())
	chat.h.Resume()
	result := s.answer(chat.h, sub.ID())
	if result.details.FullOutputPath != first {
		t.Fatalf("path %q, first %q", result.details.FullOutputPath, first)
	}
	entries, err := os.ReadDir(filepath.Dir(first))
	if err != nil || len(entries) != 1 || entries[0].Name() != "output.txt" {
		t.Fatalf("directory %v, %v", entries, err)
	}
}

func TestCodemodeOutputSavesImages(t *testing.T) {
	text, result := runScript(t, newSetup(t), `image("data:image/png;base64,`+testPNG+`");`)
	saved := regexp.MustCompile(`\[Image saved to (\S+) \(image/png, \d+ bytes\)\]`).FindStringSubmatch(text)
	if saved == nil {
		t.Fatalf("result %q", text)
	}
	if data, err := os.ReadFile(saved[1]); err != nil || base64.StdEncoding.EncodeToString(data) != testPNG {
		t.Fatalf("saved %v", err)
	}
	if len(result.Images) != 1 {
		t.Fatalf("%d images", len(result.Images))
	}
}

func TestCodemodeReadResolvesWithAnImageBlockOrText(t *testing.T) {
	files := t.TempDir()
	png, _ := base64.StdEncoding.DecodeString(testPNG)
	if err := os.WriteFile(filepath.Join(files, "pixel.png"), png, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(files, "note.txt"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := newSetup(t)
	s.extra = []durable.Extension{tools.Coding()}
	pixel, _ := json.Marshal(filepath.Join(files, "pixel.png"))
	note, _ := json.Marshal(filepath.Join(files, "note.txt"))
	text, result := runScript(t, s, `const picture = await tools.read({ path: `+string(pixel)+` });
		text([picture.type, picture.mimeType, picture.data.length].join(" "));
		image(picture);
		text(await tools.read({ path: `+string(note)+` }));`)
	if result.IsError || !strings.Contains(text, "image image/png ") ||
		!regexp.MustCompile(`\[Image saved to \S+ \(image/png, \d+ bytes\)\]`).MatchString(text) || !strings.Contains(text, "hello") || len(result.Images) != 1 {
		t.Fatalf("result %q", text)
	}
}

func TestCodemodeCallsDeferredTools(t *testing.T) {
	s := newSetup(t)
	s.tools = append(s.tools, durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: "later", Description: "A deferred tool", Parameters: json.RawMessage(`{"type":"object"}`)},
		Deferred: true,
		Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
			return durable.ToolResult{Content: "later ran"}, nil
		}})
	text, _ := runScript(t, s, `return [await tools.later({}), ALL_TOOLS.some((tool) => tool.name === "later")];`)
	if !strings.Contains(text, `["later ran",true]`) {
		t.Fatalf("result %q", text)
	}
}

func TestCodemodeReportsAFailedScript(t *testing.T) {
	text, result := runScript(t, newSetup(t), `await tools.echo({ text: "a" }); throw new Error("boom");`)
	if !result.IsError || !strings.HasPrefix(text, "Script failed\n") || !strings.Contains(text, "Error: boom") ||
		!strings.Contains(text, "Tool calls made before the failure (they are not undone): echo (ok)") {
		t.Fatalf("result %q", text)
	}
}

func TestCodemodeFindsTools(t *testing.T) {
	s := newSetup(t)
	s.namespaces = []Namespace{{Name: "github", Description: "GitHub MCP server", Prefix: "mcp__github__"}}
	s.tools = append(s.tools, durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: "mcp__github__list_issues", Description: "mcp__github__list_issues", Parameters: parametersSchema},
		Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
			return durable.ToolResult{}, nil
		}})
	_, result := runScript(t, s, `const found = await searchTools("issues");
		const inNamespace = await searchTools("echo", { namespace: "github" });
		return {
			found: found.map((tool) => tool.name),
			inNamespace,
			described: (await describeTool("echo")).includes("echo(args:"),
			missing: (await describeTool("nope")) ?? null,
			namespace: await describeNamespace("github"),
			all: ALL_TOOLS.map((tool) => tool.name).includes("mcp__github__list_issues"),
		};`)
	expectResult(t, result, `{"found":["mcp__github__list_issues"],"inNamespace":[],"described":true,"missing":null,"namespace":{"name":"github","description":"GitHub MCP server","tools":["mcp__github__list_issues"]},"all":true}`, false)
}
