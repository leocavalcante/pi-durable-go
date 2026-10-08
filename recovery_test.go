package durable_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/storage"
)

type toolModel struct{ requests atomic.Int32 }

func (m *toolModel) Generate(ctx context.Context, request durable.ModelRequest, delta func(string) error) (durable.ModelResponse, error) {
	m.requests.Add(1)
	if err := ctx.Err(); err != nil {
		return durable.ModelResponse{}, err
	}
	for _, message := range request.Messages {
		if message.Role == "tool" {
			return durable.ModelResponse{Content: "finished: " + message.Content}, nil
		}
	}
	return durable.ModelResponse{ToolCalls: []durable.ToolCall{{ID: "call-1", Name: "effect", Arguments: json.RawMessage(`{}`)}}}, nil
}
func TestRestartToolReplayProtocol(t *testing.T) {
	for _, safe := range []bool{false, true} {
		name := "unsafe"
		if safe {
			name = "safe"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var attempts atomic.Int32
			started := make(chan struct{}, 1)
			registry := durable.NewRegistry()
			err := registry.Install(durable.Extension{Name: "effect", Tools: []durable.ToolDefinition{{ToolSpec: durable.ToolSpec{Name: "effect", Parameters: json.RawMessage(`{"type":"object"}`)}, ReplaySafe: safe, Execute: func(ctx context.Context, args json.RawMessage, api *durable.ToolRuntime) (durable.ToolResult, error) {
				n := attempts.Add(1)
				if err := api.Output(ctx, "committed progress"); err != nil {
					return durable.ToolResult{}, err
				}
				if n == 1 {
					started <- struct{}{}
					<-ctx.Done()
					return durable.ToolResult{}, ctx.Err()
				}
				return durable.ToolResult{Content: "effect complete"}, nil
			}}}})
			if err != nil {
				t.Fatal(err)
			}
			store, err := storage.OpenJSONL(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			model := &toolModel{}
			h, err := durable.Open(ctx, store, durable.HarnessOptions{Model: model, Registry: registry})
			if err != nil {
				t.Fatal(err)
			}
			root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Model: durable.ModelRef{Provider: "fake", ID: "tool"}}})
			if err != nil {
				t.Fatal(err)
			}
			sub, err := root.Submit(ctx, durable.SubmissionRequest{Content: "perform effect", RequestID: "job-1"})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("tool did not start")
			}
			if err := h.Close(ctx); err != nil {
				t.Fatal(err)
			}
			store, err = storage.OpenJSONL(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			h, err = durable.Open(ctx, store, durable.HarnessOptions{Model: model, Registry: registry})
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close(context.Background())
			sub, err = h.Submission(ctx, sub.ID())
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
			expected := int32(1)
			if safe {
				expected = 2
			}
			if attempts.Load() != expected {
				t.Fatalf("attempts %d, expected %d", attempts.Load(), expected)
			}
			state, err := h.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var toolResults int
			for _, e := range state.Entries {
				if e.Kind == "pi.tool-result" {
					toolResults++
					if !safe && !strings.Contains(e.Message.Content, "interrupted") {
						t.Fatalf("unsafe result %+v", e)
					}
				}
			}
			if toolResults != 1 {
				t.Fatalf("tool results %d", toolResults)
			}
		})
	}
}

type interruptedModel struct {
	calls   atomic.Int32
	started chan struct{}
}

func (m *interruptedModel) Generate(ctx context.Context, request durable.ModelRequest, delta func(string) error) (durable.ModelResponse, error) {
	if m.calls.Add(1) == 1 {
		if err := delta("partial"); err != nil {
			return durable.ModelResponse{}, err
		}
		m.started <- struct{}{}
		<-ctx.Done()
		return durable.ModelResponse{}, ctx.Err()
	}
	return durable.ModelResponse{Content: "recovered"}, nil
}
func TestRestartPreparedModelRequestAndCommittedPartial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	m := &interruptedModel{started: make(chan struct{}, 1)}
	store, err := storage.OpenJSONL(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, store, durable.HarnessOptions{Model: m})
	if err != nil {
		t.Fatal(err)
	}
	root, err := h.Root(ctx, durable.ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, durable.SubmissionRequest{Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.started:
	case <-ctx.Done():
		t.Fatal("model did not start")
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	store, err = storage.OpenJSONL(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	h, err = durable.Open(ctx, store, durable.HarnessOptions{Model: m})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	sub, err = h.Submission(ctx, sub.ID())
	if err != nil {
		t.Fatal(err)
	}
	settled, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != "done" {
		t.Fatalf("%+v", settled)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range state.Entries {
		if e.Message != nil && e.Message.Aborted && e.Message.Content == "partial" {
			found = true
		}
	}
	if !found {
		t.Fatal("committed partial attempt not preserved")
	}
	if m.calls.Load() != 2 {
		t.Fatalf("requests %d", m.calls.Load())
	}
}
