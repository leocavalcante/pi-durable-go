package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	durable "github.com/leocavalcante/pi-durable-go"
)

const maxReadBytes = 50 * 1024
const maxReadLines = 2000

// Coding returns read, write, edit, and bash tools. They are not replay-safe:
// interrupted operations may have had effects and are never silently retried.
func Coding() durable.Extension {
	return durable.Extension{Name: "coding-tools", Tools: []durable.ToolDefinition{
		{ToolSpec: durable.ToolSpec{Name: "read", Description: "Read a text file. Output is truncated to 2000 lines or 50KB (whichever comes first). Use offset/limit for large files.", Parameters: schema(`{"type":"object","required":["path"],"properties":{"path":{"type":"string"},"offset":{"type":"integer","minimum":1},"limit":{"type":"integer","minimum":0}},"additionalProperties":false}`)}, Execute: readTool},
		{ToolSpec: durable.ToolSpec{Name: "write", Description: "Write content to a file, creating parent directories and atomically replacing the destination.", Parameters: schema(`{"type":"object","required":["path","content"],"properties":{"path":{"type":"string"},"content":{"type":"string"}},"additionalProperties":false}`)}, Execute: writeTool},
		{ToolSpec: durable.ToolSpec{Name: "edit", Description: "Replace unique, exact text regions. Every replacement matches the original file and must not overlap another edit.", Parameters: schema(`{"type":"object","required":["path","edits"],"properties":{"path":{"type":"string"},"edits":{"type":"array","minItems":1,"items":{"type":"object","required":["oldText","newText"],"properties":{"oldText":{"type":"string","minLength":1},"newText":{"type":"string"}},"additionalProperties":false}}},"additionalProperties":false}`)}, Execute: editTool},
		{ToolSpec: durable.ToolSpec{Name: "bash", Description: "Execute a POSIX shell command in the working directory. CWD is not a sandbox. Output is limited to a 64 KiB tail.", Parameters: schema(`{"type":"object","required":["command"],"properties":{"command":{"type":"string","minLength":1}},"additionalProperties":false}`)}, Execute: bashTool},
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
	Command string `json:"command"`
}

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
	data, err := e.ReadFile(ctx, a.Path)
	if err != nil {
		return durable.ToolResult{}, err
	}
	text := strings.ToValidUTF8(string(data), "�")
	lines := strings.Split(text, "\n")
	if strings.HasSuffix(text, "\n") {
		lines = lines[:len(lines)-1]
	}
	start := a.Offset
	if start > 0 {
		start--
	}
	if start >= len(lines) && len(lines) > 0 {
		return durable.ToolResult{}, fmt.Errorf("offset %d is beyond end of file (%d lines total)", a.Offset, len(lines))
	}
	if start > len(lines) {
		return durable.ToolResult{}, fmt.Errorf("offset %d is beyond end of file (%d lines total)", a.Offset, len(lines))
	}
	end := len(lines)
	if a.Limit != nil && start+*a.Limit < end {
		end = start + *a.Limit
	}
	selected := strings.Join(lines[start:end], "\n")
	if end > start && strings.HasSuffix(text, "\n") && end == len(lines) {
		selected += "\n"
	}
	return result(truncateRead(selected))
}
func truncateRead(text string) string {
	const marker = "\n[Output truncated; use offset/limit for more.]"
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	truncated := len(lines) > maxReadLines
	if truncated {
		text = strings.Join(lines[:maxReadLines-1], "\n")
	}
	if len(text) > maxReadBytes {
		truncated = true
		text = strings.ToValidUTF8(text[:maxReadBytes-len(marker)], "")
	}
	if truncated {
		if len(text) > maxReadBytes-len(marker) {
			text = strings.ToValidUTF8(text[:maxReadBytes-len(marker)], "")
		}
		return text + marker
	}
	return text
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
	if a.Path == "" || len(a.Edits) == 0 {
		return durable.ToolResult{}, errors.New("path and at least one edit are required")
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
	text := string(original)
	type span struct {
		start, end  int
		replacement string
	}
	spans := make([]span, 0, len(a.Edits))
	for i, edit := range a.Edits {
		if edit.OldText == "" {
			return durable.ToolResult{}, fmt.Errorf("edits[%d].oldText must not be empty", i)
		}
		at := strings.Index(text, edit.OldText)
		if at < 0 {
			return durable.ToolResult{}, fmt.Errorf("could not find edits[%d] in %s", i, a.Path)
		}
		if strings.Index(text[at+1:], edit.OldText) >= 0 {
			return durable.ToolResult{}, fmt.Errorf("edits[%d].oldText is not unique", i)
		}
		spans = append(spans, span{at, at + len(edit.OldText), edit.NewText})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	for i := 1; i < len(spans); i++ {
		if spans[i].start < spans[i-1].end {
			return durable.ToolResult{}, errors.New("edit ranges overlap")
		}
	}
	var updated strings.Builder
	at := 0
	for _, s := range spans {
		updated.WriteString(text[at:s.start])
		updated.WriteString(s.replacement)
		at = s.end
	}
	updated.WriteString(text[at:])
	if updated.String() == text {
		return durable.ToolResult{}, errors.New("edits produce no changes")
	}
	if err := e.WriteFile(ctx, a.Path, []byte(updated.String())); err != nil {
		return durable.ToolResult{}, err
	}
	return result(fmt.Sprintf("Successfully replaced %d block(s) in %s.", len(spans), a.Path))
}

func bashTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
	var a bashArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if strings.TrimSpace(a.Command) == "" {
		return durable.ToolResult{}, errors.New("command must not be empty")
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	res, err := e.Exec(ctx, a.Command, func(text string) error { return rt.Output(ctx, text) })
	if err != nil {
		return durable.ToolResult{}, err
	}
	if res.ExitCode != 0 {
		return durable.ToolResult{}, fmt.Errorf("command exited with code %d: %s", res.ExitCode, res.Output)
	}
	return result(res.Output)
}
