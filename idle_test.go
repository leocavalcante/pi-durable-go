package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestWaitForIdleScopes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gate := newGatedTool()
	h, root := runHarness(t, toolThenEcho(), Settings{}, gate.extension())
	if err := h.WaitForIdle(ctx); err != nil {
		t.Fatal(err)
	}
	other, err := h.CreateConversation(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	submit(t, root, SubmissionRequest{Content: "tool"})
	<-gate.started
	// A background task does not keep its scope busy.
	background := TaskDefinition{Kind: "test.idle-background", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		return r.SleepUntil(ctx, r.Now().Add(time.Hour), nil)
	}}
	installTasks(t, h.registry, "idle", background)
	if err := h.Commit(ctx, func(tx *Tx) error {
		_, err := tx.CreateTask(other.ID(), background, nil, TaskOptions{Background: true})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := other.WaitForIdle(ctx); err != nil {
		t.Fatalf("a conversation with only background work is idle: %v", err)
	}
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	if err := root.WaitForIdle(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a busy conversation was idle: %v", err)
	}
	if err := h.WaitForIdle(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the harness was idle while a run waited: %v", err)
	}
	gate.release <- struct{}{}
	if err := root.WaitForIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.WaitForIdle(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestEnvironmentBuilderFailureIsAHostError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log := &reportLog{}
	registry := toolExtension("needs-env", false, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		if _, err := r.Env(); err != nil {
			return ToolResult{Content: "no env", IsError: true}, nil
		}
		return ToolResult{Content: "env"}, nil
	})
	h := taskTestHarness(t, HarnessOptions{Model: &toolCallingModel{name: "needs-env"}, Registry: registry, OnReport: log.add,
		Environment: func(ConversationRecord) (ExecutionEnv, error) { return nil, errors.New("no sandbox") }})
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	drained(t, h)
	reports := log.of(ReportHostError)
	if len(reports) != 1 || reports[0].Callback != "env" || reports[0].Task == nil || reports[0].Task.Kind != "pi.tool" {
		t.Fatalf("host_error reports=%+v", reports)
	}
}
