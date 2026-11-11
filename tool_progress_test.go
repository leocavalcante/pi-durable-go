package durable

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// toolCallingModel calls tool `name` once, then answers with the tool result.
type toolCallingModel struct {
	name  string
	calls atomic.Int32
}

func (m *toolCallingModel) Generate(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
	m.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, err
	}
	for _, message := range request.Messages {
		if message.Role == "tool" {
			return ModelResponse{Content: "done: " + message.Content}, nil
		}
	}
	return ModelResponse{ToolCalls: []ToolCall{{ID: "call-1", Name: m.name, Arguments: json.RawMessage(`{}`)}}}, nil
}

func toolExtension(name string, safe bool, execute func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error)) *Registry {
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "tools", Tools: []ToolDefinition{{
		ToolSpec: ToolSpec{Name: name, Parameters: json.RawMessage(`{"type":"object"}`)}, ReplaySafe: safe, Execute: execute,
	}}}); err != nil {
		panic(err)
	}
	return registry
}

func toolTask(t *testing.T, h *Harness) TaskRecord {
	t.Helper()
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range state.Tasks {
		if task.Kind == "pi.tool" {
			return *task
		}
	}
	t.Fatal("no tool task")
	return TaskRecord{}
}

func toolSlot(t *testing.T, h *Harness, cid ConversationID, id TaskID) ToolSlot {
	t.Helper()
	live, err := ReadDocument[LiveState](context.Background(), h, liveDocument, uint64(cid))
	if err != nil {
		t.Fatal(err)
	}
	if slot := live.toolSlot(id); slot != nil {
		return *slot
	}
	return ToolSlot{}
}

func toolResultEntry(t *testing.T, h *Harness, cid ConversationID) Entry {
	t.Helper()
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entries, err := entriesFromState(state, cid)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Kind == "pi.tool-result" {
			return entry
		}
	}
	t.Fatal("no tool result")
	return Entry{}
}

func TestToolProgressIsNotTaskProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started, release, published, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	registry := toolExtension("stream", true, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		close(started)
		<-release
		if err := r.Output(ctx, "chunk one, "); err != nil {
			return ToolResult{}, err
		}
		if err := r.Output(ctx, "chunk two"); err != nil {
			return ToolResult{}, err
		}
		if err := r.Details(ctx, json.RawMessage(`{"progress":0.5}`)); err != nil {
			return ToolResult{}, err
		}
		close(published)
		<-finish
		return ToolResult{}, nil
	})
	h := taskTestHarness(t, HarnessOptions{Model: &toolCallingModel{name: "stream"}, Registry: registry, Settings: Settings{RestartLimit: inf()}})
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	before := toolTask(t, h)
	if err := h.Commit(ctx, func(tx *Tx) error { tx.state.Tasks[before.ID].Restarts = 3; return nil }); err != nil {
		t.Fatal(err)
	}
	close(release)
	<-published
	after := toolTask(t, h)
	if after.Restarts != 3 || !jsonEqual(after.Checkpoint, before.Checkpoint) || strings.Contains(string(after.Checkpoint), "chunk") {
		t.Fatalf("progress changed task state: restarts=%d checkpoint=%s (before %s)", after.Restarts, after.Checkpoint, before.Checkpoint)
	}
	slot := toolSlot(t, h, root.ID(), after.ID)
	if slot.Output != "chunk one, chunk two" || string(slot.Details) != `{"progress":0.5}` || slot.Status != "running" {
		t.Fatalf("slot=%+v", slot)
	}
	close(finish)
	if settled, err := submission.Wait(ctx); err != nil || settled.Status != "done" {
		t.Fatalf("submission=%+v error=%v", settled, err)
	}
	// An empty result content falls back to the committed slot output.
	if entry := toolResultEntry(t, h, root.ID()); entry.Message.Content != "chunk one, chunk two" {
		t.Fatalf("result=%q", entry.Message.Content)
	}
	if tailBytes("a界b", 4) != "界b" || tailBytes("ab界", 2) != "" || tailBytes("abc", 5) != "abc" {
		t.Fatal("tailBytes must cut at a rune boundary")
	}
}

func TestToolProgressRerunsAndInterruptions(t *testing.T) {
	for _, safe := range []bool{true, false} {
		t.Run(map[bool]string{true: "safe-rerun-clears", false: "unsafe-keeps-output"}[safe], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var attempts atomic.Int32
			blocked := make(chan struct{}, 1)
			var rerunSlot atomic.Value
			var h *Harness
			var root *Conversation
			registry := toolExtension("effect", safe, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
				if attempts.Add(1) == 1 {
					if err := r.Output(ctx, "first attempt"); err != nil {
						return ToolResult{}, err
					}
					if err := r.Details(ctx, json.RawMessage(`{"first":true}`)); err != nil {
						return ToolResult{}, err
					}
					blocked <- struct{}{}
					<-ctx.Done()
					return ToolResult{}, ctx.Err()
				}
				live, err := ReadDocument[LiveState](ctx, r.Harness(), liveDocument, uint64(r.ConversationID))
				if err != nil {
					return ToolResult{}, err
				}
				data, _ := json.Marshal(live.toolSlot(r.ID))
				rerunSlot.Store(string(data))
				if err := r.Output(ctx, "second attempt"); err != nil {
					return ToolResult{}, err
				}
				return ToolResult{}, nil
			})
			hosts := newCrashHosts(t, nil, HarnessOptions{Model: &toolCallingModel{name: "effect"}, Registry: registry})
			h = hosts.open(nil)
			var err error
			root, err = h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
			if err != nil {
				t.Fatal(err)
			}
			submission, err := root.Submit(ctx, SubmissionRequest{Content: "go"})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-blocked:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			hosts.crash(h)
			h = hosts.open(nil)
			h.Resume()
			settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx)
			if err != nil || settled.Status != "done" {
				t.Fatalf("submission=%+v error=%v", settled, err)
			}
			result := toolResultEntry(t, h, root.ID()).Message.Content
			if safe {
				if attempts.Load() != 2 || result != "second attempt" {
					t.Fatalf("attempts=%d result=%q", attempts.Load(), result)
				}
				if slot, _ := rerunSlot.Load().(string); strings.Contains(slot, "first") {
					t.Fatalf("rerun saw the interrupted attempt's progress: %s", slot)
				}
				return
			}
			want := "Tool effect was interrupted and may have partially run\nCommitted output:\nfirst attempt"
			if attempts.Load() != 1 || result != want {
				t.Fatalf("attempts=%d result=%q, want %q", attempts.Load(), result, want)
			}
		})
	}
}

func TestToolProgressLegacyCheckpointOutput(t *testing.T) {
	for _, safe := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsafe", true: "safe"}[safe], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			registry := toolExtension("effect", safe, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
				return ToolResult{Content: "rerun result"}, nil
			})
			h := taskTestHarness(t, HarnessOptions{Model: &toolCallingModel{name: "effect"}, Registry: registry})
			root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
			if err != nil {
				t.Fatal(err)
			}
			// A version-1 tool record interrupted in execute, its output only in
			// its checkpoint, owned by a generation waiting on it.
			var tool TaskID
			if err := h.Commit(ctx, func(tx *Tx) error {
				assistant, err := tx.Append(root.ID(), Entry{Kind: "pi.assistant", Message: &Message{Role: "assistant",
					ToolCalls: []ToolCall{{ID: "call-1", Name: "effect", Arguments: json.RawMessage(`{}`)}}}})
				if err != nil {
					return err
				}
				live, err := readLive(tx, root.ID())
				if err != nil {
					return err
				}
				if err := startRun(tx, root.ID(), live, nil, assistant, false); err != nil {
					return err
				}
				if err := writeLive(tx, root.ID(), live); err != nil {
					return err
				}
				generation := live.Run.TaskID
				input, _ := json.Marshal(toolTaskInput{Assistant: assistant, CallID: "call-1", Call: ToolCall{ID: "call-1", Name: "effect", Arguments: json.RawMessage(`{}`)}})
				if tool, err = tx.CreateTask(root.ID(), toolTaskDefinition(), input, TaskOptions{Owner: Owner{Task: generation}}); err != nil {
					return err
				}
				record := tx.state.Tasks[tool]
				record.Version = 1
				record.Checkpoint = json.RawMessage(`{"phase":"execute","arguments":{},"replaySafe":` + map[bool]string{true: "true", false: "false"}[safe] + `,"output":"legacy output"}`)
				owner := tx.state.Tasks[generation]
				owner.Status = "waiting"
				owner.WaitOn, owner.WaitPolicy = []TaskID{tool}, "allSettled"
				owner.Checkpoint, _ = json.Marshal(generationCheckpoint{Phase: "tools", Children: []TaskID{tool}, Assistant: assistant})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			record, err := h.WaitTask(ctx, tool)
			if err != nil || record.Outcome == nil || record.Version != toolVersion {
				t.Fatalf("record=%+v error=%v", record, err)
			}
			result := toolResultEntry(t, h, root.ID()).Message.Content
			want := "Tool effect was interrupted and may have partially run\nCommitted output:\nlegacy output"
			if safe {
				want = "rerun result"
			}
			if result != want {
				t.Fatalf("result=%q, want %q", result, want)
			}
		})
	}
	migrated, err := migrateTool(1, json.RawMessage(`{"callId":"c"}`), json.RawMessage(`{"phase":"execute","output":"x"}`))
	if err != nil || string(migrated.Checkpoint) != `{"phase":"execute","output":"x"}` {
		t.Fatalf("migration=%+v error=%v", migrated, err)
	}
	if _, err := migrateTool(2, nil, nil); err == nil {
		t.Fatal("migrated from an unknown version")
	}
}
