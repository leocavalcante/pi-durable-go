package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

type ToolRuntime struct {
	*TaskRuntime
	ConversationID ConversationID
	Call           ToolCall
}

// maxToolOutputBytes bounds a running tool's committed output, keeping the tail.
const maxToolOutputBytes = 50 * 1024

// Output appends committed progress to the call's pi.live slot, keeping the
// trailing 50 KiB. It is presentation, not task progress: the checkpoint is
// unchanged, so output does not reset the task's crash count.
func (r *ToolRuntime) Output(ctx context.Context, text string) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := r.updateLive(tx, func(slot *ToolSlot) {
			slot.Output = tailBytes(slot.Output+text, maxToolOutputBytes)
		}); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
}

// tailBytes keeps at most max trailing bytes of s, starting at a rune boundary.
func tailBytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := len(s) - max
	for cut < len(s) && !utf8.RuneStart(s[cut]) {
		cut++
	}
	return s[cut:]
}

// clearProgress removes what an interrupted attempt published, before a
// safe rerun reports from scratch, and shows the slot running again.
func (r *ToolRuntime) clearProgress(tx *Tx) error {
	return r.updateLive(tx, func(slot *ToolSlot) {
		slot.Output, slot.Details = "", nil
		slot.Status = "running"
	})
}

// Details replaces the call's published details in its pi.live slot.
func (r *ToolRuntime) Details(ctx context.Context, value json.RawMessage) error {
	if !json.Valid(value) {
		return fmt.Errorf("%w: tool details JSON", ErrInvalid)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return err
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := r.updateLive(tx, func(slot *ToolSlot) { slot.Details = append(json.RawMessage(nil), compact.Bytes()...) }); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
}

// updateLive edits the call's slot; a slot that is missing, as for a task
// created before slots existed, is added.
func (r *ToolRuntime) updateLive(tx *Tx, update func(*ToolSlot)) error {
	return updateLive(tx, r.ConversationID, func(live *LiveState) error {
		slot := live.toolSlot(r.ID)
		if slot == nil {
			live.Tools = append(live.Tools, ToolSlot{CallID: r.Call.ID, Name: r.Call.Name, Status: "running", TaskID: r.ID})
			slot = &live.Tools[len(live.Tools)-1]
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
			if sid, err = r.h.admitSubmission(tx, cid, SubmissionRequest{Type: "input", Content: content, RequestID: requestID}); err != nil {
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
	env, err := r.h.options.Environment(*c)
	if err != nil {
		r.ReportHost(err, "env")
	}
	return env, err
}
