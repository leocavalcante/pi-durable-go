package durable

import (
	"context"
	"encoding/json"
	"fmt"
)

type ToolRuntime struct {
	*TaskRuntime
	ConversationID ConversationID
	Call           ToolCall
}

var liveDocument = DocumentDefinition{Kind: "pi.live", Version: 1, Scope: "conversation", History: "latest", Fork: "initial", Initial: func() any { return map[string]any{} }}

func (r *ToolRuntime) Output(ctx context.Context, text string) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		var cp toolCheckpoint
		if err := json.Unmarshal(task.Checkpoint, &cp); err != nil {
			return TaskTransition{}, err
		}
		cp.Output += text
		if len(cp.Output) > 50*1024 {
			cp.Output = cp.Output[len(cp.Output)-50*1024:]
		}
		if err := r.updateLive(tx, func(slot map[string]any) { slot["output"] = cp.Output }); err != nil {
			return TaskTransition{}, err
		}
		data, err := json.Marshal(cp)
		return TaskTransition{Status: "running", Checkpoint: data}, err
	})
}
func (r *ToolRuntime) Details(ctx context.Context, value json.RawMessage) error {
	if !json.Valid(value) {
		return fmt.Errorf("%w: tool details JSON", ErrInvalid)
	}
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return err
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := r.updateLive(tx, func(slot map[string]any) { slot["details"] = decoded }); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
}
func (r *ToolRuntime) updateLive(tx *Tx, update func(map[string]any)) error {
	return UpdateDocument(tx, liveDocument, uint64(r.ConversationID), func(live *map[string]any) error {
		tools, ok := (*live)["tools"].(map[string]any)
		if !ok {
			tools = map[string]any{}
			(*live)["tools"] = tools
		}
		key := fmt.Sprintf("%d", r.ID)
		slot, ok := tools[key].(map[string]any)
		if !ok {
			slot = map[string]any{"callId": r.Call.ID, "name": r.Call.Name}
			tools[key] = slot
		}
		update(slot)
		return nil
	})
}

// Submit reuses this tool's owned child and deduplicated input after a restart.
// Use explicit Tx APIs when a call needs several independently named children.
func (r *ToolRuntime) Submit(ctx context.Context, content string) (ConversationID, SubmissionID, error) {
	var cid ConversationID
	var sid SubmissionID
	err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		existing, err := tx.ConversationsByOwner(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if len(existing) > 0 {
			cid = existing[0].ID
		} else {
			cid, err = tx.CreateConversation(ConversationOptions{Owner: Owner{Task: r.ID}})
			if err != nil {
				return TaskTransition{}, err
			}
		}
		requestID := fmt.Sprintf("tool-subagent:%d", r.ID)
		for _, s := range tx.state.Submissions {
			if s.Conversation == cid && s.Request.RequestID == requestID {
				sid = s.ID
				break
			}
		}
		if sid == 0 {
			sid = SubmissionID(tx.id())
			tx.state.Submissions[sid] = &SubmissionRecord{ID: sid, Conversation: cid, Request: SubmissionRequest{Type: "input", Content: content, RequestID: requestID}, Status: "queued", CreatedAt: tx.clock()}
			if err := advanceInbox(tx, cid); err != nil {
				return TaskTransition{}, err
			}
		}
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
	if err == nil {
		r.h.Resume()
	}
	return cid, sid, err
}
func (r *ToolRuntime) Env() (ExecutionEnv, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	if r.h.options.Environment == nil {
		return nil, fmt.Errorf("durable: no execution environment configured")
	}
	state, err := r.h.Snapshot(context.Background())
	if err != nil {
		return nil, err
	}
	c := state.Conversations[r.ConversationID]
	if c == nil {
		return nil, ErrNotFound
	}
	return r.h.options.Environment(*c)
}
