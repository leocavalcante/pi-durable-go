package model

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/storage"
)

func TestPiDurableToolsReopenAndCompaction(t *testing.T) {
	p := fakePi(t)
	// This exercises several Node starts plus SQLite reopen under the race
	// detector; the deadline bounds hangs, not model response latency.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "session.sqlite")
	open := func() *durable.Harness {
		t.Helper()
		store, err := storage.OpenSQLite(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		registry := durable.NewRegistry()
		if err := registry.Install(durable.Extension{Name: "lookup", Tools: []durable.ToolDefinition{{
			ToolSpec: durable.ToolSpec{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)},
			Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
				return durable.ToolResult{Content: "tool result"}, nil
			},
		}}}); err != nil {
			store.Close()
			t.Fatal(err)
		}
		keepNothing := 0.0
		h, err := durable.Open(ctx, store, durable.HarnessOptions{Model: p, Registry: registry,
			Settings: durable.Settings{Compaction: durable.CompactionSettings{KeepRecentTokens: &keepNothing}}})
		if err != nil {
			store.Close()
			t.Fatal(err)
		}
		// Register cleanup before the first submission: a deadline failure
		// must not leave a scheduler or its Node process alive across repeats.
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := h.Close(cleanup); err != nil {
				t.Error(err)
			}
		})
		return h
	}
	h := open()
	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{
		Model: durable.ModelRef{Provider: "fake", ID: "tool-round"}, Extensions: []string{"lookup"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	submit := func(c *durable.Conversation) {
		t.Helper()
		sub, err := c.Submit(ctx, durable.SubmissionRequest{Content: "test"})
		if err != nil {
			t.Fatal(err)
		}
		result, err := sub.Wait(ctx)
		if err != nil || result.Status != "done" {
			t.Fatalf("result=%+v, error=%v", result, err)
		}
	}
	submit(root)
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	h = open()
	root, err = h.Conversation(ctx, root.ID())
	if err != nil {
		t.Fatal(err)
	}
	messages, err := root.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, message := range messages {
		if strings.Contains(string(message.ProviderData), "native-tool-signature") {
			found = true
		}
	}
	if !found {
		t.Fatal("native replay data lost across SQLite reopen")
	}
	submit(root) // Fixture requires the native tool signature on replay.
	id, err := root.Compact(ctx, "retain goals")
	if err != nil {
		t.Fatal(err)
	}
	task, err := h.WaitTask(ctx, id)
	if err != nil || task.Outcome == nil || task.Outcome.Status != "completed" {
		t.Fatalf("compaction task=%+v, error=%v", task, err)
	}
}
