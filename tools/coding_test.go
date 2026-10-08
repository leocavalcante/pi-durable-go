package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
func TestCodingEditRejectsOverlappingOccurrences(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "text")
	if err := os.WriteFile(file, []byte("aaa"), 0600); err != nil {
		t.Fatal(err)
	}
	result, _ := runCoding(t, dir, "edit", `{"path":"text","edits":[{"oldText":"aa","newText":"b"}]}`)
	if !result.IsError {
		t.Fatal("accepted overlapping occurrences")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "aaa" {
		t.Fatal("failed edit changed file")
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
	if result.IsError || len(result.Content) == 0 || !strings.Contains(result.Content, "truncated") || len(result.Content) > maxReadBytes {
		t.Fatalf("read result error=%v bytes=%d", result.IsError, len(result.Content))
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
