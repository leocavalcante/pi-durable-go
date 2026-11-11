package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type toolTaskInput struct {
	Assistant EntryID  `json:"assistant"`
	CallID    string   `json:"callId"`
	Call      ToolCall `json:"call"`
}

// toolCheckpoint is pi.tool version 2: phases call and execute. Running
// output lives in the call's pi.live slot; Output is only read from version-1
// records, which stored it here.
type toolCheckpoint struct {
	Phase      string          `json:"phase"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	ReplaySafe bool            `json:"replaySafe,omitempty"`
	Output     string          `json:"output,omitempty"` // legacy version 1 only
}

// toolVersion is Go's own numbering of pi.tool's stored form. Version 2 moved
// output to the pi.live slot only; a version-1 Harness would drop committed
// output from interrupted and aborted results, so it leaves version 2 blocked.
const toolVersion = 2

func migrateTool(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
	if from != 1 {
		return MigratedTask{}, fmt.Errorf("pi.tool has no migration from version %d", from)
	}
	return MigratedTask{Input: input, Checkpoint: checkpoint}, nil
}

type toolTaskResult struct {
	EntryID EntryID    `json:"entryId"`
	Result  ToolResult `json:"result"`
}

// finishSlot marks a slot done and clears its running output.
func finishSlot(slot *ToolSlot) {
	slot.Status = "done"
	slot.Output, slot.Details = "", nil
}

func toolTaskDefinition() TaskDefinition {
	return TaskDefinition{Kind: "pi.tool", Version: toolVersion, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.Marshal(toolCheckpoint{Phase: "call"}) }, Run: runTool, Abort: func(ctx context.Context, r *TaskRuntime, t TaskRecord) error {
		return finishTool(ctx, r, t, ToolResult{Content: "aborted", IsError: true}, "aborted")
	}, Migrate: migrateTool}
}

func runTool(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	var input toolTaskInput
	var cp toolCheckpoint
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return err
	}
	if err := json.Unmarshal(task.Checkpoint, &cp); err != nil {
		return err
	}
	// After reading its call, and before the replay decision, a call cut off
	// restartLimit times in a row ends with an error result instead of
	// running again, even for a replay-unsafe tool.
	if (cp.Phase == "call" || cp.Phase == "execute") && cutOffTooOften(task.Restarts, r.h.restartLimit()) {
		message := fmt.Sprintf("Tool %s was interrupted %s; it is not run again", input.Call.Name, cutOffCount(task.Restarts))
		return finishToolEnding(ctx, r, task, ToolResult{Content: message, IsError: true}, OutcomeFailed, message, restartLimitDetail)
	}
	state, err := r.h.Snapshot(ctx)
	if err != nil {
		return err
	}
	c := state.Conversations[task.Conversation]
	if c == nil {
		return ErrNotFound
	}
	registry := r.Registry()
	var def *ToolDefinition
	for _, tool := range registry.Tools(c.Agent) {
		if tool.Name == input.Call.Name {
			copy := tool
			def = &copy
			break
		}
	}
	runtime := &ToolRuntime{TaskRuntime: r, ConversationID: task.Conversation, Call: input.Call}
	hooks := registry.Hooks(c.Agent, "pi.tool")
	if cp.Phase == "execute" {
		// Recovery after intent: rerun only when the stored and the current
		// policy both say safe.
		if !cp.ReplaySafe || def == nil || !def.ReplaySafe || def.Execute == nil {
			return finishTool(ctx, r, task, ToolResult{Content: fmt.Sprintf("Tool %s was interrupted and may have partially run", input.Call.Name), IsError: true}, "failed")
		}
		// The rerun reports from scratch: clear what the interrupted attempt
		// published. A version-1 record's checkpoint output goes too.
		if err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if err := runtime.clearProgress(tx); err != nil {
				return TaskTransition{}, err
			}
			checkpoint := task.Checkpoint
			if cp.Output != "" {
				cleared := toolCheckpoint{Phase: cp.Phase, Arguments: cp.Arguments, ReplaySafe: cp.ReplaySafe}
				var err error
				if checkpoint, err = json.Marshal(cleared); err != nil {
					return TaskTransition{}, err
				}
			}
			return TaskTransition{Status: "running", Checkpoint: checkpoint}, nil
		}); err != nil {
			return err
		}
		runtime.Call.Arguments = cp.Arguments
	} else if cp.Phase == "call" {
		if def == nil || def.Execute == nil {
			return finishTool(ctx, r, task, ToolResult{Content: "tool unavailable", IsError: true}, "completed")
		}
		if err := validateToolArguments(def.ToolSpec, runtime.Call.Arguments); err != nil {
			return finishTool(ctx, r, task, ToolResult{Content: err.Error(), IsError: true}, "completed")
		}
		for _, hook := range hooks {
			if hook.BeforeTool != nil {
				if err := hook.BeforeTool(ctx, &runtime.Call, runtime); err != nil {
					return finishTool(ctx, r, task, ToolResult{Content: "tool blocked: " + err.Error(), IsError: true}, "completed")
				}
			}
		}
		if runtime.Call.Name != input.Call.Name || runtime.Call.ID != input.Call.ID {
			return finishTool(ctx, r, task, ToolResult{Content: "tool hooks cannot change call identity", IsError: true}, "completed")
		}
		if err := validateToolArguments(def.ToolSpec, runtime.Call.Arguments); err != nil {
			return finishTool(ctx, r, task, ToolResult{Content: err.Error(), IsError: true}, "completed")
		}
		cp = toolCheckpoint{Phase: "execute", Arguments: runtime.Call.Arguments, ReplaySafe: def.ReplaySafe}
		if err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if err := runtime.updateLive(tx, func(slot *ToolSlot) { slot.Status = "running" }); err != nil {
				return TaskTransition{}, err
			}
			data, err := json.Marshal(cp)
			return TaskTransition{Status: "running", Checkpoint: data}, err
		}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("%w: tool phase", ErrInvalid)
	}
	started := time.Now()
	result, executeErr := def.Execute(ctx, cp.Arguments, runtime)
	durationMs := time.Since(started).Milliseconds()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	status := "completed"
	if executeErr != nil {
		status = "failed"
		result = ToolResult{Content: diagnostic(executeErr.Error()), IsError: true}
	}
	result.DurationMs = &durationMs
	for _, hook := range hooks {
		if hook.AfterTool != nil {
			if err := hook.AfterTool(ctx, &result, runtime); err != nil {
				if err = recordHookError(ctx, r, "afterTool", err); err != nil {
					return err
				}
			}
		}
	}
	result.DurationMs = &durationMs
	return finishTool(ctx, r, task, result, status)
}
func finishTool(ctx context.Context, r *TaskRuntime, task TaskRecord, result ToolResult, status string) error {
	return finishToolEnding(ctx, r, task, result, status, "", nil)
}

// finishToolEnding settles a call with its result entry. A failed outcome's
// Error is outcomeError when given, else the result content, and carries
// detail as its ErrorDetail.
func finishToolEnding(ctx context.Context, r *TaskRuntime, task TaskRecord, result ToolResult, status, outcomeError string, detail json.RawMessage) error {
	var input toolTaskInput
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return err
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		current, err := tx.Task(task.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		var cp toolCheckpoint
		if err := json.Unmarshal(current.Checkpoint, &cp); err != nil {
			return TaskTransition{}, err
		}
		// Committed output is the slot's; a version-1 record kept it in its
		// checkpoint instead.
		committed := cp.Output
		live, err := readLive(tx, task.Conversation)
		if err != nil {
			return TaskTransition{}, err
		}
		slot := live.toolSlot(task.ID)
		if slot != nil {
			if slot.Output != "" {
				committed = slot.Output
			}
			if len(slot.Details) > 0 && len(result.Details) == 0 {
				result.Details = append(json.RawMessage(nil), slot.Details...)
			}
		}
		if result.Content == "" {
			result.Content = committed
		}
		if (status == "aborted" || status == "failed") && committed != "" && result.Content != committed {
			result.Content += "\nCommitted output:\n" + committed
		}
		if len(result.Content) > 50*1024 {
			result.Content = "[truncated]\n" + result.Content[len(result.Content)-50*1024:]
		}
		details, err := json.Marshal(result)
		if err != nil {
			return TaskTransition{}, err
		}
		id, err := tx.Append(task.Conversation, Entry{Kind: "pi.tool-result", Message: &Message{Role: "tool", ToolCallID: input.Call.ID, ToolName: input.Call.Name, Content: result.Content, IsError: result.IsError, Usage: result.Usage, DurationMs: result.DurationMs}, Data: details})
		if err != nil {
			return TaskTransition{}, err
		}
		// The slot is done: its running output is cleared, and its entry is
		// the result; the round's end removes the slots.
		if slot != nil {
			finishSlot(slot)
			slot.Entry = id
			if err := writeLive(tx, task.Conversation, live); err != nil {
				return TaskTransition{}, err
			}
		}
		if err := recordToolUsage(tx, task.Conversation, input.Call.Name, result.Usage); err != nil {
			return TaskTransition{}, err
		}
		data, err := json.Marshal(toolTaskResult{EntryID: id, Result: result})
		if err != nil {
			return TaskTransition{}, err
		}
		outcome := &Outcome{Status: status, Result: data}
		if status == OutcomeFailed {
			outcome.Error = diagnostic(result.Content)
			if outcomeError != "" {
				outcome.Error = diagnostic(outcomeError)
			}
			outcome.ErrorDetail = detail
		}
		return TaskTransition{Status: "terminal", Outcome: outcome}, nil
	})
}
