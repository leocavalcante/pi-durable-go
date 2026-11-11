package tools

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	durable "github.com/leocavalcante/pi-durable-go"
)

const maxReadBytes = 50 * 1024
const maxReadLines = 2000

// Coding returns read, write, edit, and bash tools. They are not replay-safe:
// interrupted operations may have had effects and are never silently retried.
// PowerShell returns the powershell tool: like bash, through PowerShell
// instead of the environment's shell, trying programs in order (default pwsh,
// then powershell); a program that cannot start is skipped. The environment
// must implement ArgvExecutor, as LocalEnv does.
func PowerShell(programs ...string) durable.Extension {
	if len(programs) == 0 {
		programs = []string{"pwsh", "powershell"}
	}
	return durable.Extension{Name: "powershell-tool", Tools: []durable.ToolDefinition{
		{ToolSpec: durable.ToolSpec{Name: "powershell", Description: "Execute a PowerShell command in the current working directory. Returns combined stdout and stderr. Output is truncated to last 2000 lines or 50KB (whichever is hit first). If truncated, full output is saved to a temp file. Optionally provide a timeout in seconds.", Parameters: schema(`{"type":"object","required":["command"],"properties":{"command":{"type":"string","minLength":1,"description":"PowerShell command to execute"},"timeout":{"type":"number","description":"Timeout in seconds (optional, no default timeout)"}},"additionalProperties":false}`)}, Execute: powershellTool(programs)},
	}}
}

func Coding() durable.Extension {
	return durable.Extension{Name: "coding-tools", Tools: []durable.ToolDefinition{
		{ToolSpec: durable.ToolSpec{Name: "read", Description: "Read the contents of a file. Supports text files and images (jpg, png, gif, webp). Images are sent as attachments. For text files, output is truncated to 2000 lines or 50KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete.", Parameters: schema(`{"type":"object","required":["path"],"properties":{"path":{"type":"string","description":"Path to the file to read (relative or absolute)"},"offset":{"type":"integer","minimum":1,"description":"Line number to start reading from (1-indexed)"},"limit":{"type":"integer","minimum":0,"description":"Maximum number of lines to read"}},"additionalProperties":false}`)}, Execute: readTool},
		{ToolSpec: durable.ToolSpec{Name: "write", Description: "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. Automatically creates parent directories.", Parameters: schema(`{"type":"object","required":["path","content"],"properties":{"path":{"type":"string"},"content":{"type":"string"}},"additionalProperties":false}`)}, Execute: writeTool},
		{ToolSpec: durable.ToolSpec{Name: "edit", Description: "Edit a single file using exact text replacement. Every edits[].oldText must match a unique, non-overlapping region of the original file. If two changes affect the same block or nearby lines, merge them into one edit instead of emitting overlapping edits. Do not include large unchanged regions just to connect distant changes.", Parameters: schema(`{"type":"object","required":["path","edits"],"properties":{"path":{"type":"string"},"edits":{"type":"array","minItems":1,"items":{"type":"object","required":["oldText","newText"],"properties":{"oldText":{"type":"string","minLength":1},"newText":{"type":"string"}},"additionalProperties":false}}},"additionalProperties":false}`)}, Execute: editTool},
		{ToolSpec: durable.ToolSpec{Name: "bash", Description: "Execute a bash command in the current working directory. Returns combined stdout and stderr. Output is truncated to last 2000 lines or 50KB (whichever is hit first). If truncated, full output is saved to a temp file. Optionally provide a timeout in seconds.", Parameters: schema(`{"type":"object","required":["command"],"properties":{"command":{"type":"string","minLength":1,"description":"Bash command to execute"},"timeout":{"type":"number","description":"Timeout in seconds (optional, no default timeout)"}},"additionalProperties":false}`)}, Execute: bashTool},
	}}
}

func schema(raw string) json.RawMessage { return json.RawMessage(raw) }

type readArgs struct {
	Path   string `json:"path"`
	Offset int    `json:"offset,omitempty"`
	Limit  *int   `json:"limit,omitempty"`
}
type writeArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}
type editItem struct {
	OldText string `json:"oldText"`
	NewText string `json:"newText"`
}
type editArgs struct {
	Path  string     `json:"path"`
	Edits []editItem `json:"edits"`
}
type bashArgs struct {
	Command string   `json:"command"`
	Timeout *float64 `json:"timeout,omitempty"`
}

// maxTimeoutSeconds is upstream's largest timeout (a 32-bit millisecond timer).
const maxTimeoutSeconds = 2147483647.0 / 1000

func decodeArgs(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		return errors.New("missing arguments")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fmt.Errorf("invalid arguments: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("invalid arguments: trailing JSON")
	}
	return nil
}
func env(rt *durable.ToolRuntime) (durable.ExecutionEnv, error) {
	if rt == nil {
		return nil, errors.New("missing tool runtime")
	}
	e, err := rt.Env()
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, errors.New("no execution environment is configured")
	}
	return e, nil
}
func lockMutation(e durable.ExecutionEnv, path string) func() {
	local, ok := e.(*LocalEnv)
	if !ok {
		return func() {}
	}
	lock := mutationLock(local, path)
	lock.Lock()
	return lock.Unlock
}
func result(content string) (durable.ToolResult, error) {
	return durable.ToolResult{Content: content}, nil
}

func readTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
	var a readArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if a.Path == "" {
		return durable.ToolResult{}, errors.New("path must not be empty")
	}
	if a.Offset < 0 || (a.Limit != nil && *a.Limit < 0) {
		return durable.ToolResult{}, errors.New("offset and limit must not be negative")
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	var reader io.Reader
	if opener, ok := e.(FileOpener); ok {
		file, err := opener.OpenFile(ctx, a.Path)
		if err != nil {
			return durable.ToolResult{}, err
		}
		defer file.Close()
		reader = file
	} else {
		data, err := e.ReadFile(ctx, a.Path)
		if err != nil {
			return durable.ToolResult{}, err
		}
		reader = bytes.NewReader(data)
	}
	// An image is told by its first bytes; anything else is text.
	size, known := fileSize(reader)
	buffered := bufio.NewReaderSize(reader, imageHeadBytes)
	head, _ := buffered.Peek(imageHeadBytes)
	if mimeType := detectImageType(head); mimeType != "" {
		var image io.Reader = buffered
		if !known {
			data, err := io.ReadAll(buffered)
			if err != nil {
				return durable.ToolResult{}, err
			}
			size, image = int64(len(data)), bytes.NewReader(data)
		}
		return readImage(image, size, a.Path, mimeType, rt.AcceptsImages(ctx))
	}
	return readText(buffered, a.Path, a.Offset, a.Limit)
}

func writeTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
	var a writeArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if a.Path == "" {
		return durable.ToolResult{}, errors.New("path must not be empty")
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	unlock := lockMutation(e, a.Path)
	defer unlock()
	if err := e.WriteFile(ctx, a.Path, []byte(a.Content)); err != nil {
		return durable.ToolResult{}, err
	}
	return result("Successfully wrote to " + a.Path)
}

func editTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
	var a editArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if a.Path == "" {
		return durable.ToolResult{}, errors.New("path must not be empty")
	}
	if len(a.Edits) == 0 {
		return durable.ToolResult{}, errors.New("Edit tool input is invalid. edits must contain at least one replacement.")
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	unlock := lockMutation(e, a.Path)
	defer unlock()
	original, err := e.ReadFile(ctx, a.Path)
	if err != nil {
		return durable.ToolResult{}, err
	}
	text, bom := string(original), ""
	if strings.HasPrefix(text, "\uFEFF") {
		text, bom = text[len("\uFEFF"):], "\uFEFF"
	}
	ending := detectLineEnding(text)
	edits := make([]textEdit, len(a.Edits))
	for i, edit := range a.Edits {
		edits[i] = textEdit{edit.OldText, edit.NewText}
	}
	base, updated, err := applyEditsToNormalizedContent(normalizeToLF(text), edits, a.Path)
	if err != nil {
		return durable.ToolResult{}, err
	}
	if err := e.WriteFile(ctx, a.Path, []byte(bom+restoreLineEndings(updated, ending))); err != nil {
		return durable.ToolResult{}, err
	}
	diff, firstChanged := generateDiffString(base, updated, 4)
	details := map[string]any{"diff": diff, "patch": generateUnifiedPatch(a.Path, base, updated, 4)}
	if firstChanged > 0 {
		details["firstChangedLine"] = firstChanged
	}
	data, err := json.Marshal(details)
	if err != nil {
		return durable.ToolResult{}, err
	}
	return durable.ToolResult{Content: fmt.Sprintf("Successfully replaced %d block(s) in %s.", len(a.Edits), a.Path), Details: data}, nil
}

func bashTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
	return shellTool(ctx, raw, rt, func(ctx context.Context, e durable.ExecutionEnv, command string, onOutput func(string) error) (durable.ExecResult, error) {
		return e.Exec(ctx, command, onOutput)
	})
}

// utf8Output makes PowerShell write UTF-8 whatever the console code page.
const utf8Output = "try { [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 } catch {}"

var powershellArgs = []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command"}

// powershellTool runs the command through pwsh, else Windows PowerShell,
// started directly with the script as an argument.
func powershellTool(programs []string) func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
	return func(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
		return shellTool(ctx, raw, rt, func(ctx context.Context, e durable.ExecutionEnv, command string, onOutput func(string) error) (durable.ExecResult, error) {
			runner, ok := e.(ArgvExecutor)
			if !ok {
				return durable.ExecResult{}, errors.New("the execution environment cannot run programs directly")
			}
			var lastErr error
			for _, program := range programs {
				argv := append(append([]string{program}, powershellArgs...), utf8Output+"\n"+command)
				result, err := runner.ExecArgv(ctx, argv, onOutput)
				if errors.Is(err, ErrSpawn) {
					lastErr = err // the next program may start
					continue
				}
				return result, err
			}
			return durable.ExecResult{}, lastErr
		})
	}
}

// shellTool validates a shell call and shapes its result like upstream.
func shellTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime, run func(context.Context, durable.ExecutionEnv, string, func(string) error) (durable.ExecResult, error)) (durable.ToolResult, error) {
	var a bashArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if strings.TrimSpace(a.Command) == "" {
		return durable.ToolResult{}, errors.New("command must not be empty")
	}
	if a.Timeout != nil {
		if math.IsNaN(*a.Timeout) || math.IsInf(*a.Timeout, 0) || *a.Timeout <= 0 {
			return durable.ToolResult{}, errors.New("Invalid timeout: must be a finite number of seconds")
		}
		if *a.Timeout > maxTimeoutSeconds {
			return durable.ToolResult{}, fmt.Errorf("Invalid timeout: maximum is %g seconds", maxTimeoutSeconds)
		}
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	runCtx := ctx
	if a.Timeout != nil {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(*a.Timeout*float64(time.Second)))
		defer cancel()
	}
	_, local := e.(*LocalEnv)
	spill := &outputSpill{enabled: local}
	defer spill.close()
	res, err := run(runCtx, e, a.Command, func(text string) error {
		spill.write(text)
		return rt.Output(ctx, text)
	})
	var diagnostics []durable.ToolDiagnostic
	if spill.path != "" {
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "info", Code: "full_output", Message: "Full output: " + spill.path})
	}
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return durable.ToolResult{}, ctx.Err()
		case runCtx.Err() != nil:
			return durable.ToolResult{}, fmt.Errorf("Command timed out after %g seconds", *a.Timeout)
		}
		return durable.ToolResult{}, err
	}
	content, droppedLines, droppedBytes := truncateTail(res.Output, spill.lines, spill.bytes)
	if droppedBytes > 0 {
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "warn", Code: "truncated",
			Message: fmt.Sprintf("Output truncated to its end: %d lines, %d bytes dropped", droppedLines, droppedBytes)})
	}
	if res.ExitCode != 0 {
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "error", Code: "exit_code", Message: fmt.Sprintf("Command exited with code %d", res.ExitCode)})
		return durable.ToolResult{Content: content, IsError: true, Diagnostics: diagnostics}, nil
	}
	return durable.ToolResult{Content: content, Diagnostics: diagnostics}, nil
}

// outputSpill counts a command's whole output and, once it passes the read
// limits, writes all of it to a temporary file (upstream's spill).
type outputSpill struct {
	enabled bool
	lines   int
	bytes   int
	pending strings.Builder
	file    *os.File
	path    string
}

func (s *outputSpill) write(text string) {
	s.bytes += len(text)
	s.lines += strings.Count(text, "\n")
	if !s.enabled {
		return
	}
	if s.file == nil {
		s.pending.WriteString(text)
		if s.bytes <= maxReadBytes && s.lines <= maxReadLines {
			return
		}
		f, err := os.CreateTemp("", "pi-bash-*.log")
		if err != nil {
			s.enabled = false
			return
		}
		s.file, s.path = f, f.Name()
		text = s.pending.String()
		s.pending.Reset()
	}
	_, _ = s.file.WriteString(text)
}

func (s *outputSpill) close() {
	if s.file != nil {
		_ = s.file.Close()
	}
}

// truncateTail keeps the last whole lines of a command's retained output
// within the read limits, and counts what the whole output lost: totalLines
// newlines and totalBytes bytes were produced in all.
func truncateTail(retained string, totalNewlines, totalBytes int) (string, int, int) {
	lines := strings.SplitAfter(retained, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	kept, size := 0, 0
	for i := len(lines) - 1; i >= 0 && kept < maxReadLines; i-- {
		if size+len(lines[i]) > maxReadBytes {
			break
		}
		size += len(lines[i])
		kept++
	}
	if kept == 0 && len(lines) > 0 { // one line over the limit: its end
		last := lines[len(lines)-1]
		cut := len(last) - maxReadBytes
		for cut < len(last) && !utf8.RuneStart(last[cut]) {
			cut++
		}
		content := last[cut:]
		return content, totalNewlines - strings.Count(content, "\n"), max(totalBytes, len(retained)) - len(content)
	}
	content := strings.Join(lines[len(lines)-kept:], "")
	if totalBytes < len(retained) { // an Exec that reports no output
		totalBytes, totalNewlines = len(retained), strings.Count(retained, "\n")
	}
	droppedBytes := totalBytes - len(content)
	droppedLines := totalNewlines - strings.Count(content, "\n")
	if droppedBytes == 0 {
		droppedLines = 0
	}
	return content, droppedLines, droppedBytes
}
