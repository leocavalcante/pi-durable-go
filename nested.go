package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// NestedToolResult is a nested call's result, as ExecuteTool returns it.
// Programs read StructuredOutput: a tool's validated structured output, or,
// for a tool without a StructuredOutputSchema, its content as a JSON string.
// It is absent on an error result the Harness wrote itself; Diagnostics say
// what went wrong.
type NestedToolResult struct {
	TaskID           TaskID           `json:"taskId"`
	StructuredOutput json.RawMessage  `json:"structuredOutput,omitempty"`
	IsError          bool             `json:"isError"`
	Details          json.RawMessage  `json:"details,omitempty"`
	Diagnostics      []ToolDiagnostic `json:"diagnostics"`
	Usage            Usage            `json:"usage,omitempty"`
	DurationMs       *int64           `json:"durationMs,omitempty"`
}

// NestedCallOptions: Key identifies the call within its caller, so a rerun
// finds the calls it already made (default: the call's sequence number in
// this attempt). NoProgress commits no running output or details to the
// nested call's slot.
type NestedCallOptions struct {
	Key        string
	NoProgress bool
}

// nestedCallsDocument is the caller's task-scoped record of its nested calls
// by key: the call's task and, once it ended, its result.
var nestedCallsDocument = DocumentDefinition{Kind: "pi.tool.nested-calls", Version: 1, Scope: "task",
	Initial: func() any { return map[string]nestedCallState{} }}

// nestedCallDocument is upstream's NestedCallDoc: a task-scoped family of the
// calling task keyed by the nested call's key, one document per call, so no
// commit rewrites the others. It retires with the caller. Calls made before
// it existed stay in nestedCallsDocument, which is still read.
var nestedCallDocument = DocumentDefinition{Kind: "pi.tool.nested-call", Version: 1, Scope: "task", Family: true,
	InitialSeed: func(json.RawMessage) any { return nestedCallState{} }}

type nestedCallState struct {
	TaskID TaskID            `json:"taskId,omitempty"`
	Result *NestedToolResult `json:"result,omitempty"`
}

// NestedToolSlot presents one nested call in pi.live.nestedTools.
type NestedToolSlot struct {
	CallID       string          `json:"callId"`
	ParentCallID string          `json:"parentCallId"`
	ParentTaskID TaskID          `json:"parentTaskId"`
	TaskID       TaskID          `json:"taskId"`
	Name         string          `json:"name"`
	Status       string          `json:"status"`
	Arguments    json.RawMessage `json:"arguments"`
	Output       string          `json:"output,omitempty"`
	Details      json.RawMessage `json:"details,omitempty"`
	DroppedBytes int             `json:"droppedBytes,omitempty"`
	DroppedLines int             `json:"droppedLines,omitempty"`
	// Diagnostics the call reported while it ran.
	Diagnostics []ToolDiagnostic `json:"diagnostics,omitempty"`
	// Summary says how the call ended, once done.
	Summary *NestedToolSummary `json:"summary,omitempty"`
}

// NestedToolSummary: an error flag, the execution time, the spend, and up to
// 500 characters of error text.
type NestedToolSummary struct {
	IsError    bool   `json:"isError"`
	DurationMs *int64 `json:"durationMs,omitempty"`
	Usage      Usage  `json:"usage,omitempty"`
	Error      string `json:"error,omitempty"`
}

// ExecuteTool runs a nested call: another tool, callable by tools, as its
// own task owned by this call, and waits for its result. Nested calls run as
// soon as they are made; the caller decides what runs at once by what it
// awaits. Cancelling ctx stops only the wait. When this call settles, nested
// calls it left running are aborted first. It fails once Execute returned or
// the call ended. A rerun of a replay-safe caller finds calls it already made
// by key; reusing a key for another tool or other arguments fails.
func (r *ToolRuntime) ExecuteTool(ctx context.Context, name string, args json.RawMessage, options NestedCallOptions) (NestedToolResult, error) {
	if r.executed.Load() || r.check() != nil {
		return NestedToolResult{}, ErrClosed
	}
	key := options.Key
	if key == "" {
		key = strconv.FormatInt(r.sequence.Add(1), 10)
	} else if strings.Contains(key, "/") || key == "__proto__" || defaultNestedKey.MatchString(key) {
		// Explicit keys are path segments of the call ID that default keys
		// (positive integers) never collide with (upstream checkKey).
		keyJSON, _ := json.Marshal(key)
		return NestedToolResult{}, fmt.Errorf("%w: Nested call key %s must be non-empty, without \"/\", not \"__proto__\", and not a positive integer", ErrInvalid, keyJSON)
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(args, &object) != nil || object == nil {
		return NestedToolResult{}, fmt.Errorf("%w: nested call arguments must be a JSON object", ErrInvalid)
	}
	call := ToolCall{ID: r.Call.ID + "/" + key, Name: name, Arguments: append(json.RawMessage(nil), args...)}
	var id TaskID
	replayed := false
	err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		replayed = false
		current, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		existing, err := readNestedCall(tx.state, r.ID, key)
		if err != nil {
			return TaskTransition{}, err
		}
		if existing.TaskID != 0 {
			var input toolTaskInput
			if task, err := tx.Task(existing.TaskID); err == nil {
				_ = json.Unmarshal(task.Input, &input)
			}
			if input.Kind != "nested" || input.Call.Name != name || !jsonEqual(input.Call.Arguments, call.Arguments) {
				return TaskTransition{}, fmt.Errorf("%w: nested call %s was already made with another tool or other arguments", ErrInvalid, call.ID)
			}
			id = existing.TaskID
			if task, err := tx.Task(existing.TaskID); err == nil && task.Status == "terminal" {
				replayed = true // answered from its stored result
			}
			return TaskTransition{Status: "running", Checkpoint: current.Checkpoint}, nil
		}
		var cp toolCheckpoint
		_ = json.Unmarshal(current.Checkpoint, &cp)
		data, err := json.Marshal(toolTaskInput{Kind: "nested", Parent: r.ID, ParentCallID: r.Call.ID, Key: key, CallID: call.ID, Call: call, NoProgress: options.NoProgress})
		if err != nil {
			return TaskTransition{}, err
		}
		// A caller that does not rerun cannot wait for its calls after a restart.
		id, err = tx.CreateTask(r.ConversationID, toolTaskDefinition(), data, TaskOptions{Owner: Owner{Task: r.ID}, AbandonOnRestart: !cp.ReplaySafe})
		if err != nil {
			return TaskTransition{}, err
		}
		if err := writeNestedCall(tx, r.ID, key, nestedCallState{TaskID: id}); err != nil {
			return TaskTransition{}, err
		}
		if err := updateLive(tx, r.ConversationID, func(live *LiveState) error {
			live.NestedTools = append(live.NestedTools, NestedToolSlot{CallID: call.ID, ParentCallID: r.Call.ID, ParentTaskID: r.ID,
				TaskID: id, Name: name, Status: "pending", Arguments: call.Arguments})
			return nil
		}); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: current.Checkpoint}, nil
	})
	if err != nil {
		return NestedToolResult{}, err
	}
	if replayed {
		spanOf(ctx).event("pi.durable.replay", map[string]any{"tool.name": name})
	}
	wait := ctx
	if r.invocation != nil {
		bound, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(r.invocation.ctx, cancel)
		defer stop()
		wait = bound
	}
	settled, err := r.h.WaitTask(wait, id)
	if err != nil {
		return NestedToolResult{}, err
	}
	state, err := r.h.read(wait)
	if err != nil {
		return NestedToolResult{}, err
	}
	if stored, err := readNestedCall(state, r.ID, key); err == nil && stored.Result != nil {
		return *stored.Result, nil
	}
	// The scheduler faulted or orphaned it, storing no result (upstream
	// fallbackResult).
	result := NestedToolResult{TaskID: id, IsError: true}
	if settled.Outcome != nil {
		var message string
		switch settled.Outcome.Status {
		case OutcomeFaulted:
			message = fmt.Sprintf("Tool %s failed: %s", name, settled.Outcome.Error)
		case OutcomeOrphaned:
			message = fmt.Sprintf("Tool %s could not resume: %s", name, settled.Outcome.Reason)
		default:
			message = fmt.Sprintf("Tool %s ended without a result", name)
		}
		result.Diagnostics = []ToolDiagnostic{{Severity: "error", Code: settled.Outcome.Status, Message: message}}
	}
	return result, nil
}

// defaultNestedKey matches the keys default nested calls take.
var defaultNestedKey = regexp.MustCompile(`^[1-9][0-9]*$`)

// readNestedCall reads one nested call of a caller: its family member, or
// its entry in the single document earlier versions kept.
func readNestedCall(state *State, caller TaskID, key string) (nestedCallState, error) {
	var call nestedCallState
	member, err := state.document(memberKey(nestedCallDocument, uint64(caller), key))
	if err != nil {
		return call, err
	}
	if member != nil {
		err := json.Unmarshal(member.Value, &call)
		return call, err
	}
	legacy, err := state.document(documentKey(nestedCallsDocument, uint64(caller)))
	if err != nil {
		return call, err
	}
	if doc := legacy; doc != nil {
		var calls map[string]nestedCallState
		if err := json.Unmarshal(doc.Value, &calls); err != nil {
			return call, err
		}
		return calls[key], nil
	}
	return call, nil
}

// writeNestedCall stores one nested call as its family member.
func writeNestedCall(tx *Tx, caller TaskID, key string, call nestedCallState) error {
	data, err := json.Marshal(call)
	if err != nil {
		return err
	}
	return tx.PutFamilyDocument(nestedCallDocument, uint64(caller), key, data)
}

// settleNested stores a nested call's result with its caller and marks its
// slot done. The caller's documents retire with it, so a result for a
// caller that already ended is dropped.
func settleNested(tx *Tx, input toolTaskInput, task TaskRecord, def *ToolDefinition, result ToolResult) (json.RawMessage, error) {
	nested := NestedToolResult{TaskID: task.ID, IsError: result.IsError, Details: result.Details, Diagnostics: result.Diagnostics, Usage: result.Usage, DurationMs: result.DurationMs}
	if nested.Diagnostics == nil {
		nested.Diagnostics = []ToolDiagnostic{}
	}
	harnessWritten := result.IsError && len(result.Diagnostics) > 0 && result.Content == "" && len(result.StructuredOutput) == 0
	switch {
	case def != nil && len(def.StructuredOutputSchema) > 0:
		nested.StructuredOutput = result.StructuredOutput
	case !harnessWritten:
		nested.StructuredOutput = unstructuredOutput(result)
	}
	if parent, err := tx.Task(input.Parent); err == nil && parent.Status != "terminal" {
		state, err := readNestedCall(tx.state, input.Parent, input.Key)
		if err != nil {
			return nil, err
		}
		state.TaskID, state.Result = task.ID, &nested
		if err := writeNestedCall(tx, input.Parent, input.Key, state); err != nil {
			return nil, err
		}
	}
	if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
		for i := range live.NestedTools {
			if slot := &live.NestedTools[i]; slot.TaskID == task.ID {
				slot.Status, slot.Output, slot.Details = "done", "", nil
				summary := &NestedToolSummary{IsError: result.IsError, DurationMs: result.DurationMs, Usage: result.Usage}
				if result.IsError {
					text := result.Content
					for _, d := range result.Diagnostics {
						if d.Severity == "error" {
							text = d.Message
						}
					}
					if runes := []rune(text); len(runes) > 500 {
						text = string(runes[:500])
					}
					summary.Error = text
				}
				slot.Summary = summary
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := recordToolUsage(tx, task.Conversation, input.Call.Name, result.Usage); err != nil {
		return nil, err
	}
	return json.Marshal(nested)
}

// abortNestedCalls marks the caller's live nested calls aborted and removes
// the slots below it, before the caller settles.
func abortNestedCalls(tx *Tx, caller TaskRecord) error {
	for _, id := range ownedLive(tx.state, caller.ID) {
		if task := tx.state.Tasks[id]; task != nil && task.Kind == "pi.tool" && task.Owner.Task == caller.ID {
			markAbortTree(tx.state, id)
		}
	}
	return updateLive(tx, caller.Conversation, func(live *LiveState) error {
		removed := map[TaskID]bool{caller.ID: true}
		for _, slot := range live.NestedTools {
			if removed[slot.ParentTaskID] {
				removed[slot.TaskID] = true
			}
		}
		kept := live.NestedTools[:0]
		for _, slot := range live.NestedTools {
			if !removed[slot.ParentTaskID] {
				kept = append(kept, slot)
			}
		}
		live.NestedTools = kept
		if len(live.NestedTools) == 0 {
			live.NestedTools = nil
		}
		return nil
	})
}

// unstructuredOutput is what programs get from a tool without a schema
// (upstream): its bounded output as a string when it is text, one image as
// itself, and otherwise the content list.
func unstructuredOutput(result ToolResult) json.RawMessage {
	image := func(img ImageContent) map[string]string {
		return map[string]string{"type": "image", "data": img.Data, "mimeType": img.MimeType}
	}
	var value any = result.Content
	switch {
	case len(result.Images) == 1 && result.Content == "":
		value = image(result.Images[0])
	case len(result.Images) > 0:
		var blocks []any
		if result.Content != "" {
			blocks = append(blocks, map[string]string{"type": "text", "text": result.Content})
		}
		for _, img := range result.Images {
			blocks = append(blocks, image(img))
		}
		value = blocks
	}
	data, _ := json.Marshal(value)
	return data
}
