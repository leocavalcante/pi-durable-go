package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

type delegationModel struct{}

func (delegationModel) Generate(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, err
	}
	if request.Model.ID == "child" {
		if len(request.Tools) != 0 {
			return ModelResponse{}, fmt.Errorf("child must have no tools")
		}
		return ModelResponse{Content: "child answer", Usage: Usage{InputTokens: 2, OutputTokens: 1}}, nil
	}
	for _, msg := range request.Messages {
		if msg.Role == "tool" {
			return ModelResponse{Content: msg.Content, Usage: Usage{InputTokens: 2, OutputTokens: 1}}, nil
		}
	}
	return ModelResponse{ToolCalls: []ToolCall{{ID: "delegate-1", Name: "delegate", Arguments: json.RawMessage(`{}`)}}, Usage: Usage{InputTokens: 2, OutputTokens: 1}}, nil
}
func TestOwnedForegroundSubagentAndUsage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{Model: delegationModel{}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	tool := ToolDefinition{ToolSpec: ToolSpec{Name: "delegate", Parameters: json.RawMessage(`{"type":"object"}`)}, ReplaySafe: true, Execute: func(ctx context.Context, args json.RawMessage, api *ToolRuntime) (ToolResult, error) {
		var childID ConversationID
		err := api.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			existing, err := tx.ConversationsByOwner(api.ID)
			if err != nil {
				return TaskTransition{}, err
			}
			if len(existing) > 0 {
				childID = existing[0].ID
			} else {
				childID, err = tx.CreateConversation(ConversationOptions{Owner: Owner{Task: api.ID}, Agent: Agent{Model: ModelRef{Provider: "fake", ID: "child"}, Tools: []string{}}})
				if err != nil {
					return TaskTransition{}, err
				}
			}
			task, err := tx.Task(api.ID)
			if err != nil {
				return TaskTransition{}, err
			}
			return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
		})
		if err != nil {
			return ToolResult{}, err
		}
		child, err := api.Harness().Conversation(ctx, childID)
		if err != nil {
			return ToolResult{}, err
		}
		sub, err := child.Submit(ctx, SubmissionRequest{Content: "work", RequestID: fmt.Sprintf("delegate:%d", api.ID)})
		if err != nil {
			return ToolResult{}, err
		}
		settled, err := sub.Wait(ctx)
		if err != nil {
			return ToolResult{}, err
		}
		if settled.Status != "done" {
			return ToolResult{}, fmt.Errorf("child: %s", settled.Reason)
		}
		state, err := api.Harness().Snapshot(ctx)
		if err != nil {
			return ToolResult{}, err
		}
		return ToolResult{Content: state.Entries[settled.Answer].Message.Content}, nil
	}}
	if err := h.registry.Install(Extension{Name: "delegation", Tools: []ToolDefinition{tool}}); err != nil {
		t.Fatal(err)
	}
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "parent"}}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmissionRequest{Content: "delegate"})
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
	if settled.Status != "done" || state.Entries[settled.Answer].Message.Content != "child answer" {
		t.Fatalf("%+v", settled)
	}
	if len(state.Conversations) != 2 {
		t.Fatalf("conversations %d", len(state.Conversations))
	}
	usage, err := h.Usage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage.Models["fake/child"].InputTokens != 2 || usage.Models["fake/parent"].InputTokens != 4 {
		t.Fatalf("usage %+v", usage)
	}
}
