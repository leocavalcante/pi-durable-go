package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

type codingModel struct {
	name string
	args json.RawMessage
}

func (m codingModel) Generate(ctx context.Context, request durable.ModelRequest, delta func(string) error) (durable.ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return durable.ModelResponse{}, err
	}
	for _, msg := range request.Messages {
		if msg.Role == "tool" {
			return durable.ModelResponse{Content: msg.Content}, nil
		}
	}
	return durable.ModelResponse{ToolCalls: []durable.ToolCall{{ID: "coding-1", Name: m.name, Arguments: m.args}}}, nil
}
func runCoding(t *testing.T, dir, name, args string) (durable.Message, string) {
	t.Helper()
	// A hang bound, not a latency assertion: large reads under the race
	// detector on a loaded host take seconds.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := durable.NewRegistry()
	if err := registry.Install(Coding()); err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, durable.NewMemoryStorage(), durable.HarnessOptions{Registry: registry, Model: codingModel{name: name, args: json.RawMessage(args)}, Environment: func(c durable.ConversationRecord) (durable.ExecutionEnv, error) { return NewLocalEnv(c.Agent.Cwd) }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Cwd: dir}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, durable.SubmissionRequest{Content: "coding"})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != "done" {
		t.Fatalf("%+v", settled)
	}
	for _, entry := range state.Entries {
		if entry.Kind == "pi.tool-result" {
			return *entry.Message, state.Entries[settled.Answer].Message.Content
		}
	}
	t.Fatal("no tool result")
	return durable.Message{}, ""
}

// Like upstream, occurrences are counted without overlap: "aa" occurs once
// in "aaa".
func TestCodingEditCountsOccurrencesWithoutOverlap(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "text")
	if err := os.WriteFile(file, []byte("aaa"), 0600); err != nil {
		t.Fatal(err)
	}
	result, _ := runCoding(t, dir, "edit", `{"path":"text","edits":[{"oldText":"aa","newText":"b"}]}`)
	if result.IsError {
		t.Fatal(result.Content)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "ba" {
		t.Fatalf("edited %q", data)
	}
}
func TestCodingEditMatchesAllEditsAgainstOriginal(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "text")
	if err := os.WriteFile(file, []byte("one two"), 0600); err != nil {
		t.Fatal(err)
	}
	result, _ := runCoding(t, dir, "edit", `{"path":"text","edits":[{"oldText":"one","newText":"two"},{"oldText":"two","newText":"three"}]}`)
	if result.IsError {
		t.Fatal(result.Content)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "two three" {
		t.Fatalf("edits %s", data)
	}
}
func TestCodingReadWarnsOnOversizedLine(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "long"), []byte(strings.Repeat("x", 100*1024)), 0600); err != nil {
		t.Fatal(err)
	}
	result, _ := runCoding(t, dir, "read", `{"path":"long"}`)
	shown, notes, _ := strings.Cut(result.Content, "\n<harness>\n")
	if result.IsError || len(shown) != maxReadBytes || !strings.Contains(notes, "[warn] Line 1 is 100.0KB, exceeds the 50.0KB limit; showing its first 50.0KB. Use bash: sed -n '1p' long | tail -c +51201") {
		t.Fatalf("read result error=%v bytes=%d notes=%q", result.IsError, len(shown), notes)
	}
}

func TestCodingReadPagesLikeUpstream(t *testing.T) {
	dir := t.TempDir()
	var text strings.Builder
	for i := 1; i <= 2500; i++ {
		fmt.Fprintf(&text, "line %d\n", i)
	}
	if err := os.WriteFile(filepath.Join(dir, "many"), []byte(text.String()), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ args, first, last, note string }{
		{`{"path":"many"}`, "line 1", "line 2000", "[info] Showing lines 1-2000 of 2501. Use offset=2001 to continue."},
		{`{"path":"many","offset":2001}`, "line 2001", "line 2500", ""},
		{`{"path":"many","offset":10,"limit":5}`, "line 10", "line 14", "[info] 2487 more lines in file. Use offset=15 to continue."},
	} {
		result, _ := runCoding(t, dir, "read", tc.args)
		shown, notes, _ := strings.Cut(result.Content, "\n<harness>\n")
		lines := strings.Split(strings.TrimSuffix(shown, "\n"), "\n")
		if result.IsError || lines[0] != tc.first || lines[len(lines)-1] != tc.last || !strings.Contains(notes, tc.note) || tc.note == "" && notes != "" {
			t.Fatalf("%s: first=%q last=%q notes=%q", tc.args, lines[0], lines[len(lines)-1], notes)
		}
	}
	if result, _ := runCoding(t, dir, "read", `{"path":"many","offset":3000}`); !result.IsError || !strings.Contains(result.Content, "Offset 3000 is beyond end of file (2501 lines total)") {
		t.Fatalf("beyond: %+v", result)
	}
}
func TestLocalWritePreservesExistingSymlinkTarget(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	alias := filepath.Join(dir, "alias")
	if err := os.WriteFile(target, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, alias); err != nil {
		t.Skip(err)
	}
	env, err := NewLocalEnv(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.WriteFile(context.Background(), "alias", []byte("after")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(alias)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("symlink replaced")
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "after" {
		t.Fatal("target not updated")
	}
}

func TestCodingBashResultsLikeUpstream(t *testing.T) {
	dir := t.TempDir()
	result, _ := runCoding(t, dir, "bash", `{"command":"echo out; exit 3"}`)
	if !result.IsError || !strings.HasPrefix(result.Content, "out\n") || !strings.Contains(result.Content, "[error] Command exited with code 3") {
		t.Fatalf("exit: %+v", result)
	}
	result, _ = runCoding(t, dir, "bash", `{"command":"sleep 5","timeout":0.2}`)
	if !result.IsError || !strings.Contains(result.Content, "Command timed out after 0.2 seconds") {
		t.Fatalf("timeout: %+v", result)
	}
	result, _ = runCoding(t, dir, "bash", `{"command":"echo hi","timeout":-1}`)
	if !result.IsError || !strings.Contains(result.Content, "Invalid timeout: must be a finite number of seconds") {
		t.Fatalf("invalid timeout: %+v", result)
	}
	result, _ = runCoding(t, dir, "bash", `{"command":"seq 1 3000"}`)
	shown, notes, _ := strings.Cut(result.Content, "\n<harness>\n")
	lines := strings.Split(strings.TrimSuffix(shown, "\n"), "\n")
	if result.IsError || len(lines) != 2000 || lines[0] != "1001" || lines[1999] != "3000" ||
		!strings.Contains(notes, "[info] Full output: ") || !strings.Contains(notes, "[warn] Output truncated to its end: 1000 lines, 3893 bytes dropped") {
		t.Fatalf("spill: lines=%d first=%q notes=%q", len(lines), lines[0], notes)
	}
	path := strings.TrimSpace(strings.SplitN(strings.SplitN(notes, "Full output: ", 2)[1], "\n", 2)[0])
	defer os.Remove(path)
	full, err := os.ReadFile(path)
	if err != nil || strings.Count(string(full), "\n") != 3000 {
		t.Fatalf("spill file: %d lines, %v", strings.Count(string(full), "\n"), err)
	}
}

func TestCodingEditLikeUpstream(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "crlf")
	if err := os.WriteFile(file, []byte("\uFEFFfirst line  \r\nsays \u201chi\u201d\r\nlast\r\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// A fuzzy match: trailing spaces and smart quotes differ; unchanged lines keep their bytes.
	result, _ := runCoding(t, dir, "edit", `{"path":"crlf","edits":[{"oldText":"says \"hi\"","newText":"says bye"}]}`)
	if result.IsError {
		t.Fatal(result.Content)
	}
	data, _ := os.ReadFile(file)
	if string(data) != "\uFEFFfirst line  \r\nsays bye\r\nlast\r\n" {
		t.Fatalf("edited %q", data)
	}
	for _, tc := range []struct{ args, want string }{
		{`{"path":"crlf","edits":[{"oldText":"missing","newText":"x"}]}`, "Could not find the exact text in crlf. The old text must match exactly including all whitespace and newlines."},
		{`{"path":"crlf","edits":[{"oldText":"line","newText":"x"},{"oldText":"first line","newText":"y"}]}`, "edits[1] and edits[0] overlap in crlf. Merge them into one edit or target disjoint regions."},
		{`{"path":"crlf","edits":[{"oldText":"last","newText":"last"}]}`, "No changes made to crlf. The replacement produced identical content."},
	} {
		result, _ := runCoding(t, dir, "edit", tc.args)
		if !result.IsError || !strings.Contains(result.Content, tc.want) {
			t.Fatalf("%s: %+v", tc.args, result)
		}
	}
}

func TestPowerShellToolTriesProgramsInOrder(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX script as the fake PowerShell")
	}
	dir := t.TempDir()
	fake := filepath.Join(dir, "fake-pwsh")
	// Print the arguments PowerShell would get, one per line.
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nfor a in \"$@\"; do echo \"[$a]\"; done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := durable.NewRegistry()
	if err := registry.Install(PowerShell(filepath.Join(dir, "missing"), fake)); err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, durable.NewMemoryStorage(), durable.HarnessOptions{Registry: registry,
		Model:       codingModel{name: "powershell", args: json.RawMessage(`{"command":"Get-Date"}`)},
		Environment: func(c durable.ConversationRecord) (durable.ExecutionEnv, error) { return NewLocalEnv(c.Agent.Cwd) }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Cwd: dir}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, durable.SubmissionRequest{Content: "run"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Message != nil && e.Message.Role == "tool" {
			want := "[-NoProfile]\n[-NonInteractive]\n[-ExecutionPolicy]\n[Bypass]\n[-Command]\n[" + utf8Output + "\nGet-Date]\n"
			if e.Message.Content != want {
				t.Fatalf("result=%q", e.Message.Content)
			}
			return
		}
	}
	t.Fatal("no tool result")
}

func TestCodingToolsGiveProgramsStructuredOutput(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "note.txt"), []byte("hello\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var got []durable.NestedToolResult
	caller := durable.Extension{Name: "caller", Tools: []durable.ToolDefinition{{
		ToolSpec: durable.ToolSpec{Name: "program", Parameters: schema(`{"type":"object"}`)}, ReplaySafe: true,
		Execute: func(ctx context.Context, _ json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
			for _, call := range []struct{ name, args string }{{"read", `{"path":"note.txt"}`}, {"bash", `{"command":"echo out; exit 2"}`}} {
				result, err := rt.ExecuteTool(ctx, call.name, json.RawMessage(call.args), durable.NestedCallOptions{})
				if err != nil {
					return durable.ToolResult{}, err
				}
				got = append(got, result)
			}
			return durable.ToolResult{Content: "done"}, nil
		},
	}}}
	registry := durable.NewRegistry()
	for _, extension := range []durable.Extension{Coding(), caller} {
		if err := registry.Install(extension); err != nil {
			t.Fatal(err)
		}
	}
	h, err := durable.Open(ctx, durable.NewMemoryStorage(), durable.HarnessOptions{Registry: registry, Model: codingModel{name: "program", args: json.RawMessage(`{}`)},
		Environment: func(c durable.ConversationRecord) (durable.ExecutionEnv, error) { return NewLocalEnv(c.Agent.Cwd) }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Cwd: dir}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, durable.SubmissionRequest{Content: "run"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].StructuredOutput) != `"hello\n"` ||
		string(got[1].StructuredOutput) != `{"exitCode":2,"output":"out\n","truncated":false}` || !got[1].IsError {
		t.Fatalf("nested results=%+v", got)
	}
}

func TestEditAccessErrorsAreWordedAsUpstream(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "folder"), 0o755); err != nil {
		t.Fatal(err)
	}
	env, err := NewLocalEnv(dir)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		"missing.txt": "Could not edit file: missing.txt. Error code: not_found.",
		"folder":      "Could not edit file: folder. Path is not a file.",
	} {
		_, err := env.ReadFile(context.Background(), path)
		if got := editAccessError(context.Background(), path, err); got == nil || got.Error() != want {
			t.Fatalf("%s: %v, want %q", path, got, want)
		}
	}
}

// The parameter schemas are upstream's (1.5.2), which the model reads and
// arguments are coerced and checked against.
func TestCodingToolSchemasAreUpstreams(t *testing.T) {
	want := map[string]string{
		"read":  `{"type":"object","properties":{"path":{"type":"string","description":"Path to the file to read (relative or absolute)"},"offset":{"type":"number","description":"Line number to start reading from (1-indexed)"},"limit":{"type":"number","description":"Maximum number of lines to read"}},"required":["path"]}`,
		"write": `{"type":"object","properties":{"path":{"type":"string","description":"Path to the file to write (relative or absolute)"},"content":{"type":"string","description":"Content to write to the file"}},"required":["path","content"]}`,
		"edit":  `{"type":"object","properties":{"path":{"type":"string","description":"Path to the file to edit (relative or absolute)"},"edits":{"type":"array","items":{"type":"object","properties":{"oldText":{"type":"string","description":"Exact text for one targeted replacement. It must be unique in the original file and must not overlap with any other edits[].oldText in the same call."},"newText":{"type":"string","description":"Replacement text for this targeted edit."}},"required":["oldText","newText"]},"description":"One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead."}},"required":["path","edits"]}`,
		"bash":  `{"type":"object","properties":{"command":{"type":"string","description":"Bash command to execute"},"timeout":{"type":"number","description":"Timeout in seconds (optional, no default timeout)"}},"required":["command"]}`,
	}
	tools := append(Coding().Tools, PowerShell().Tools...)
	want["powershell"] = `{"type":"object","properties":{"command":{"type":"string","description":"PowerShell command to execute"},"timeout":{"type":"number","description":"Timeout in seconds (optional, no default timeout)"}},"required":["command"]}`
	for _, tool := range tools {
		if string(tool.Parameters) != want[tool.Name] {
			t.Errorf("%s parameters=%s\nwant %s", tool.Name, tool.Parameters, want[tool.Name])
		}
		if (tool.Name == "bash" || tool.Name == "powershell") && (tool.OutputLimits == nil || tool.OutputLimits.Retain != "tail") {
			t.Errorf("%s keeps its output's head", tool.Name)
		}
	}
}

func TestReadTakesNumbersAsUpstreamDoes(t *testing.T) {
	for _, c := range []struct {
		args    string
		start   int
		offset  string
		limit   int
		limited bool
	}{
		{`{"path":"a"}`, 0, "undefined", 0, false},
		{`{"path":"a","offset":0}`, 0, "0", 0, false},
		{`{"path":"a","offset":-3}`, 0, "-3", 0, false},
		{`{"path":"a","offset":2.9,"limit":1.5}`, 1, "2.9", 2, true},
		{`{"path":"a","offset":2.7,"limit":1.9}`, 1, "2.7", 2, true},
		{`{"path":"a","offset":5,"limit":0}`, 4, "5", 0, true},
		{`{"path":"a","limit":-2}`, 0, "undefined", 0, true},
		{`{"path":"a","unknown":true}`, 0, "undefined", 0, false},
	} {
		var a readArgs
		if err := decodeArgs(json.RawMessage(c.args), &a); err != nil {
			t.Fatalf("%s: %v", c.args, err)
		}
		start, offset, limit := readRange(a)
		if start != c.start || offset != c.offset || (limit != nil) != c.limited || limit != nil && *limit != c.limit {
			t.Fatalf("%s: start=%d offset=%s limit=%v", c.args, start, offset, limit)
		}
	}
}
