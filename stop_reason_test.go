package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
)

func TestStopReasonLengthIsAnAnswer(t *testing.T) {
	var executed atomic.Bool
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		return ModelResponse{Content: "cut off", StopReason: "length",
			ToolCalls: []ToolCall{{ID: "c1", Name: "never", Arguments: json.RawMessage(`{}`)}}}, nil
	})
	h, root := runHarness(t, model, Settings{}, Extension{Name: "never", Tools: []ToolDefinition{{ToolSpec: ToolSpec{Name: "never", Parameters: json.RawMessage(`{"type":"object"}`)},
		Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
			executed.Store(true)
			return ToolResult{}, nil
		}}}})
	if err := root.Configure(context.Background(), Agent{Model: ModelRef{Provider: "fake", ID: "m"}, ThinkingLevel: "high"}); err != nil {
		t.Fatal(err)
	}
	record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	if record.Status != "done" || executed.Load() {
		t.Fatalf("submission=%+v executed=%v", record, executed.Load())
	}
	state, _ := h.Snapshot(context.Background())
	answer := state.Entries[record.Answer].Message
	if answer.StopReason != "length" || answer.ThinkingLevel != "high" || answer.Content != "cut off" {
		t.Fatalf("answer=%+v", answer)
	}
	// The calls never ran: the context ends in their synthesized error results.
	messages, err := root.Context(context.Background())
	if err != nil || messages[len(messages)-1].Role != "tool" || !messages[len(messages)-1].IsError {
		t.Fatalf("context=%+v error=%v", messages, err)
	}
}

func TestStopReasonFailedAttemptIsAnErrorEntry(t *testing.T) {
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
		if err := delta("partial"); err != nil {
			return ModelResponse{}, err
		}
		return ModelResponse{}, errors.New("invalid request")
	})
	h, root := runHarness(t, model, Settings{})
	record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	if record.Reason != RunModelError {
		t.Fatalf("submission=%+v", record)
	}
	entries, _ := root.Entries(context.Background())
	last := entries[len(entries)-1].Message
	if last == nil || last.StopReason != "error" || last.ErrorMessage != "invalid request" || last.Content != "partial" {
		t.Fatalf("failed attempt entry=%+v", last)
	}
	messages, _ := root.Context(context.Background())
	for _, message := range messages {
		if message.Role == "assistant" {
			t.Fatalf("a failed attempt reached the context: %+v", message)
		}
	}
}
