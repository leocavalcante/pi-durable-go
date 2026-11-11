package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type generationInput struct {
	Submissions []SubmissionID `json:"submissions,omitempty"`
}

// generationCheckpoint is pi.generation version 2. Phases: prepare, request,
// retry (a backoff until Until, in Unix milliseconds), and tools. Attempt is
// the 1-based model attempt of the current turn; zero reads as 1.
type generationCheckpoint struct {
	Phase     string       `json:"phase"`
	Attempt   int          `json:"attempt,omitempty"`
	Until     int64        `json:"until,omitempty"`
	Request   ModelRequest `json:"request,omitempty"`
	Partial   string       `json:"partial,omitempty"`
	Turns     int          `json:"turns,omitempty"`
	Children  []TaskID     `json:"children,omitempty"`
	Assistant EntryID      `json:"assistant,omitempty"`
}

func (cp generationCheckpoint) attempt() int {
	if cp.Attempt < 1 {
		return 1
	}
	return cp.Attempt
}

// generationVersion is Go's own numbering of pi.generation's stored form.
// Version 2 replaced version 1's retry count with a 1-based attempt and added
// the retry phase; a version-1 Harness leaves version-2 records blocked.
const generationVersion = 2

// migrateGeneration accepts version-1 records: attempt = retry + 1, every
// other field kept as stored.
func migrateGeneration(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
	if from != 1 {
		return MigratedTask{}, fmt.Errorf("pi.generation has no migration from version %d", from)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(checkpoint, &fields); err != nil || fields == nil {
		return MigratedTask{}, fmt.Errorf("pi.generation version 1 checkpoint is not an object")
	}
	retry := 0
	if raw, ok := fields["retry"]; ok {
		if err := json.Unmarshal(raw, &retry); err != nil {
			return MigratedTask{}, fmt.Errorf("pi.generation version 1 retry count: %w", err)
		}
		delete(fields, "retry")
	}
	attempt, err := json.Marshal(retry + 1)
	if err != nil {
		return MigratedTask{}, err
	}
	fields["attempt"] = attempt
	migrated, err := json.Marshal(fields)
	return MigratedTask{Input: input, Checkpoint: migrated}, err
}

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

func (tx *Tx) createGeneration(cid ConversationID, submissions []SubmissionID) (TaskID, error) {
	input, err := json.Marshal(generationInput{Submissions: submissions})
	if err != nil {
		return 0, err
	}
	return tx.CreateTask(cid, generationTaskDefinition(), input, TaskOptions{})
}
func generationTaskDefinition() TaskDefinition {
	return TaskDefinition{Kind: "pi.generation", Version: generationVersion, Initial: func(json.RawMessage) (json.RawMessage, error) {
		return json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: 1})
	}, Run: runGeneration, Abort: settleGenerationAbort, Migrate: migrateGeneration}
}
func toolTaskDefinition() TaskDefinition {
	return TaskDefinition{Kind: "pi.tool", Version: toolVersion, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.Marshal(toolCheckpoint{Phase: "call"}) }, Run: runTool, Abort: func(ctx context.Context, r *TaskRuntime, t TaskRecord) error {
		return finishTool(ctx, r, t, ToolResult{Content: "aborted", IsError: true}, "aborted")
	}, Migrate: migrateTool}
}

func runGeneration(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	var input generationInput
	var cp generationCheckpoint
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return err
	}
	if err := json.Unmarshal(task.Checkpoint, &cp); err != nil {
		return err
	}
	switch cp.Phase {
	case "prepare":
		// A retry prepares the same turn again, so agent, section, and settings
		// changes made during its backoff apply; only a first attempt is a turn.
		attempt := cp.attempt()
		max := r.h.options.Settings.MaxTurns
		if max <= 0 {
			max = 100
		}
		if attempt == 1 && cp.Turns >= max {
			return failGeneration(ctx, r, task, input, fmt.Errorf("maximum generation turns (%d) exceeded", max))
		}
		state, err := r.h.Snapshot(ctx)
		if err != nil {
			return err
		}
		c := state.Conversations[task.Conversation]
		if c == nil {
			return ErrNotFound
		}
		if r.h.options.Model == nil {
			return failGeneration(ctx, r, task, input, errors.New("no model configured"))
		}
		request, err := buildRequest(ctx, r, state, *c)
		if err != nil {
			return failGeneration(ctx, r, task, input, err)
		}
		cp.Phase = "request"
		cp.Request = request
		cp.Partial = ""
		cp.Attempt = attempt
		cp.Until = 0
		if attempt == 1 {
			cp.Turns++
		}
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			metadata, err := json.Marshal(struct {
				Messages []Message  `json:"system"`
				Tools    []ToolSpec `json:"tools"`
			}{systemMessages(request.Messages), request.Tools})
			if err != nil {
				return TaskTransition{}, err
			}
			// Journal prompt/tool choices, while the complete prepared request is the
			// recovery authority. Native positional system-update caching is not ported.
			entries, err := entriesFromState(tx.state, task.Conversation)
			if err != nil {
				return TaskTransition{}, err
			}
			var previous json.RawMessage
			for _, e := range entries {
				if e.Kind == "pi.system" {
					previous = e.Data
				}
			}
			if string(previous) != string(metadata) {
				if _, err := tx.Append(task.Conversation, Entry{Kind: "pi.system", Data: metadata}); err != nil {
					return TaskTransition{}, err
				}
			}
			data, err := json.Marshal(cp)
			return TaskTransition{Status: "pending", Checkpoint: data}, err
		})
	case "request":
		if r.h.options.Model == nil {
			return failGeneration(ctx, r, task, input, errors.New("no model configured"))
		}
		if cp.Partial != "" {
			err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				if _, err := tx.Append(task.Conversation, Entry{Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: cp.Partial, Aborted: true}}); err != nil {
					return TaskTransition{}, err
				}
				cp.Partial = ""
				data, err := json.Marshal(cp)
				return TaskTransition{Status: "running", Checkpoint: data}, err
			})
			if err != nil {
				return err
			}
		}
		started := time.Now()
		response, err := r.h.options.Model.Generate(ctx, cp.Request, func(delta string) error {
			cp.Partial += delta
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
				data, err := json.Marshal(cp)
				return TaskTransition{Status: "running", Checkpoint: data}, err
			})
		})
		responseDurationMs := time.Since(started).Milliseconds()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// The policy governs the next attempt, so it is read now. An
			// overflow never retries: only a compaction could make it fit.
			policy := r.h.options.Settings.Retry.resolve()
			attempt := cp.attempt()
			if !IsContextOverflowError(err, cp.Request.Model.Provider) && IsRetryableModelError(err) && policy.allows(attempt) {
				until := retryDeadline(r.h.now(), retryDelayMs(policy, attempt))
				return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
					if cp.Partial != "" {
						if _, e := tx.Append(task.Conversation, Entry{Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: cp.Partial, Aborted: true, Usage: response.Usage}}); e != nil {
							return TaskTransition{}, e
						}
					}
					if e := recordModelUsage(tx, task.Conversation, cp.Request.Model, response.Usage); e != nil {
						return TaskTransition{}, e
					}
					return retryTransition(cp, attempt, until)
				})
			}
			return failGeneration(ctx, r, task, input, err)
		}
		state, e := r.h.Snapshot(ctx)
		if e != nil {
			return e
		}
		conv := state.Conversations[task.Conversation]
		if conv == nil {
			return ErrNotFound
		}
		hooks := r.Registry().Hooks(conv.Agent, "pi.generation")
		for _, hook := range hooks {
			if hook.AfterResponse != nil {
				if e := hook.AfterResponse(ctx, &response, r); e != nil {
					if e = recordHookError(ctx, r, "afterResponse", e); e != nil {
						return e
					}
				}
			}
		}
		var continuation string
		if len(response.ToolCalls) == 0 {
			for _, hook := range hooks {
				if hook.OnYield != nil {
					value, e := hook.OnYield(ctx, response, r)
					if e != nil {
						if e = recordHookError(ctx, r, "onYield", e); e != nil {
							return e
						}
						continue
					}
					if value != "" {
						continuation = value
						break
					}
				}
			}
		}
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			message := Message{Role: "assistant", Content: response.Content, Thinking: response.Thinking, ToolCalls: response.ToolCalls, Usage: response.Usage, DurationMs: &responseDurationMs, ProviderData: response.ProviderData}
			assistant, err := tx.Append(task.Conversation, Entry{Kind: "pi.assistant", Message: &message})
			if err != nil {
				return TaskTransition{}, err
			}
			if err := recordModelUsage(tx, task.Conversation, cp.Request.Model, response.Usage); err != nil {
				return TaskTransition{}, err
			}
			if len(response.ToolCalls) == 0 {
				if continuation != "" {
					if _, err := tx.Append(task.Conversation, Entry{Kind: "pi.user", Message: &Message{Role: "user", Content: continuation}}); err != nil {
						return TaskTransition{}, err
					}
					next, err := json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: 1, Turns: cp.Turns})
					return TaskTransition{Status: "pending", Checkpoint: next}, err
				}
				return settleRun(tx, task, input, assistant, "", true)
			}
			seen := map[string]bool{}
			var children []TaskID
			for _, call := range response.ToolCalls {
				if call.ID == "" || seen[call.ID] {
					return TaskTransition{}, fmt.Errorf("%w: missing/duplicate tool call ID", ErrInvalid)
				}
				seen[call.ID] = true
				data, err := json.Marshal(toolTaskInput{Assistant: assistant, CallID: call.ID, Call: call})
				if err != nil {
					return TaskTransition{}, err
				}
				child, err := tx.CreateTask(task.Conversation, toolTaskDefinition(), data, TaskOptions{Owner: Owner{Task: task.ID}})
				if err != nil {
					return TaskTransition{}, err
				}
				children = append(children, child)
			}
			next, err := json.Marshal(generationCheckpoint{Phase: "tools", Turns: cp.Turns, Children: children, Assistant: assistant})
			return TaskTransition{Status: "waiting", Checkpoint: next, WaitOn: children, WaitPolicy: "allSettled"}, err
		})
	case "tools":
		state, err := r.h.Snapshot(ctx)
		if err != nil {
			return err
		}
		var results []ToolResult
		terminate := len(cp.Children) > 0
		handoff := ""
		for _, id := range cp.Children {
			child := state.Tasks[id]
			if child == nil || child.Status != "terminal" {
				return fmt.Errorf("%w: unfinished tool round", ErrInvalid)
			}
			var result toolTaskResult
			if child.Outcome != nil && len(child.Outcome.Result) > 0 {
				if err := json.Unmarshal(child.Outcome.Result, &result); err != nil {
					return err
				}
			} else {
				result.Result = ToolResult{Content: "tool failed without result", IsError: true}
			}
			results = append(results, result.Result)
			terminate = terminate && result.Result.Terminate
			if result.Result.Handoff != "" {
				handoff = result.Result.Handoff
			}
		}
		conv := state.Conversations[task.Conversation]
		if conv == nil {
			return ErrNotFound
		}
		for _, hook := range r.Registry().Hooks(conv.Agent, "pi.generation") {
			if hook.AfterTools != nil {
				if err := hook.AfterTools(ctx, results, r); err != nil {
					if err = recordHookError(ctx, r, "afterTools", err); err != nil {
						return err
					}
				}
			}
		}
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if handoff != "" {
				if _, err := tx.Append(task.Conversation, Entry{Kind: "pi.reset", Message: &Message{Role: "user", Content: handoff}}); err != nil {
					return TaskTransition{}, err
				}
			}
			if terminate || handoff != "" {
				return settleRun(tx, task, input, cp.Assistant, "", true)
			}
			reset, err := placeBusyBoundary(tx, task.Conversation, &input)
			if err != nil {
				return TaskTransition{}, err
			}
			if reset {
				return settleRun(tx, task, input, 0, "reset", false)
			}
			rec, err := tx.Task(task.ID)
			if err != nil {
				return TaskTransition{}, err
			}
			rec.Input, err = json.Marshal(input)
			if err != nil {
				return TaskTransition{}, err
			}
			next, err := json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: 1, Turns: cp.Turns})
			return TaskTransition{Status: "pending", Checkpoint: next}, err
		})
	case "retry":
		// After the backoff. A phase that runs before until, after an early
		// wake or a clock that moved backward, backs off again unchanged.
		if cp.Until > r.h.now().UnixMilli() {
			wake := time.UnixMilli(cp.Until).UTC()
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
				return TaskTransition{Status: "pending", Checkpoint: task.Checkpoint, WakeAt: &wake}, nil
			})
		}
		return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			next, err := json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: cp.attempt() + 1, Turns: cp.Turns})
			return TaskTransition{Status: "running", Checkpoint: next}, err
		})
	default:
		return fmt.Errorf("%w: generation phase %q", ErrInvalid, cp.Phase)
	}
}

// retryTransition backs a failed attempt off until the retry deadline; the
// next attempt prepares its request again (Pi Durable's retry phase).
func retryTransition(cp generationCheckpoint, attempt int, until time.Time) (TaskTransition, error) {
	next, err := json.Marshal(generationCheckpoint{Phase: "retry", Attempt: attempt, Until: until.UnixMilli(), Turns: cp.Turns})
	wake := time.UnixMilli(until.UnixMilli()).UTC()
	return TaskTransition{Status: "pending", Checkpoint: next, WakeAt: &wake}, err
}

func systemMessages(messages []Message) []Message {
	var out []Message
	for _, m := range messages {
		if m.Role == "system" {
			out = append(out, m)
		}
	}
	return out
}
func buildRequest(ctx context.Context, r *TaskRuntime, state *State, c ConversationRecord) (ModelRequest, error) {
	messages, err := contextFromState(state, c.ID)
	if err != nil {
		return ModelRequest{}, err
	}
	registry := r.Registry()
	var prompt strings.Builder
	for _, section := range registry.Sections(c.Agent) {
		if section.Render == nil {
			continue
		}
		text, err := section.Render(ctx, c, r.h)
		if err != nil {
			return ModelRequest{}, err
		}
		if text != "" {
			fmt.Fprintf(&prompt, "<%s>\n%s\n</%s>\n", section.Name, text, section.Name)
		}
	}
	if c.Agent.Instructions != "" {
		fmt.Fprintf(&prompt, "<instructions>\n%s\n</instructions>", c.Agent.Instructions)
	}
	if prompt.Len() > 0 {
		messages = append([]Message{{Role: "system", Content: prompt.String()}}, messages...)
	}
	request := ModelRequest{Model: c.Agent.Model, SessionID: c.ProviderSessionID, ThinkingLevel: c.Agent.ThinkingLevel, Messages: messages}
	for _, tool := range registry.Tools(c.Agent) {
		request.Tools = append(request.Tools, tool.ToolSpec)
	}
	for _, hook := range registry.Hooks(c.Agent, "pi.generation") {
		if hook.BeforeRequest != nil {
			if err := hook.BeforeRequest(ctx, &request, r); err != nil {
				return ModelRequest{}, err
			}
		}
	}
	return request, nil
}
func recordHookError(ctx context.Context, r *TaskRuntime, phase string, cause error) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		data, err := json.Marshal(map[string]string{"phase": phase, "error": diagnostic(cause.Error())})
		if err != nil {
			return TaskTransition{}, err
		}
		_, err = tx.Append(task.Conversation, Entry{Kind: "app.hook-error", Data: data})
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, err
	})
}
func settleRun(tx *Tx, task TaskRecord, input generationInput, answer EntryID, reason string, done bool) (TaskTransition, error) {
	for _, id := range input.Submissions {
		if sub := tx.state.Submissions[id]; sub != nil {
			sub.Reason = reason
			sub.Answer = answer
			if done {
				sub.Status = "done"
			} else {
				sub.Status = "unanswered"
			}
		}
	}
	conv, err := tx.Conversation(task.Conversation)
	if err != nil {
		return TaskTransition{}, err
	}
	if conv.Busy == task.ID {
		conv.Busy = 0
	}
	if done || reason == "reset" {
		if err := advanceInbox(tx, task.Conversation); err != nil {
			return TaskTransition{}, err
		}
	}
	status := "completed"
	if !done {
		status = "failed"
	}
	result, _ := json.Marshal(map[string]EntryID{"entryId": answer})
	outcome := &Outcome{Status: status, Result: result}
	if status == OutcomeFailed {
		outcome.Error = diagnostic(reason)
	}
	return TaskTransition{Status: "terminal", Outcome: outcome}, nil
}
func failGeneration(ctx context.Context, r *TaskRuntime, task TaskRecord, input generationInput, cause error) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		current, err := tx.Task(task.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		var cp generationCheckpoint
		if err := json.Unmarshal(current.Checkpoint, &cp); err != nil {
			return TaskTransition{}, err
		}
		if cp.Partial != "" {
			if _, err := tx.Append(task.Conversation, Entry{Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: cp.Partial, Aborted: true}}); err != nil {
				return TaskTransition{}, err
			}
		}
		return settleRun(tx, task, input, 0, diagnostic(cause.Error()), false)
	})
}
func settleGenerationAbort(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	var input generationInput
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return err
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		current, err := tx.Task(task.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		var cp generationCheckpoint
		if err := json.Unmarshal(current.Checkpoint, &cp); err != nil {
			return TaskTransition{}, err
		}
		if cp.Partial != "" {
			if _, err := tx.Append(task.Conversation, Entry{Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: cp.Partial, Aborted: true}}); err != nil {
				return TaskTransition{}, err
			}
		}
		transition, err := settleRun(tx, task, input, 0, "aborted", false)
		if err == nil {
			transition.Outcome.Status = OutcomeAborted
			transition.Outcome.Error = ""
		}
		return transition, err
	})
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
		if err := r.Commit(ctx, func(*Tx) (TaskTransition, error) {
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
		if doc := tx.state.Documents[documentKey(liveDocument, uint64(task.Conversation))]; doc != nil {
			var live map[string]any
			if err := json.Unmarshal(doc.Value, &live); err != nil {
				return TaskTransition{}, err
			}
			if tools, ok := live["tools"].(map[string]any); ok {
				key := fmt.Sprintf("%d", task.ID)
				if slot, ok := tools[key].(map[string]any); ok {
					if output, ok := slot["output"].(string); ok {
						committed = output
					}
					if details, ok := slot["details"]; ok && len(result.Details) == 0 {
						result.Details, err = json.Marshal(details)
						if err != nil {
							return TaskTransition{}, err
						}
					}
				}
				delete(tools, key)
			}
			data, err := json.Marshal(live)
			if err != nil {
				return TaskTransition{}, err
			}
			if err := tx.PutDocument(liveDocument, uint64(task.Conversation), data); err != nil {
				return TaskTransition{}, err
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
		}
		return TaskTransition{Status: "terminal", Outcome: outcome}, nil
	})
}
func queuedSubmissions(tx *Tx, cid ConversationID) []*SubmissionRecord {
	var queued []*SubmissionRecord
	for _, s := range tx.state.Submissions {
		if s.Conversation == cid && s.Status == "queued" {
			queued = append(queued, s)
		}
	}
	sort.Slice(queued, func(i, j int) bool { return queued[i].ID < queued[j].ID })
	return queued
}
func placeBusyBoundary(tx *Tx, cid ConversationID, input *generationInput) (bool, error) {
	reset := false
	for _, s := range queuedSubmissions(tx, cid) {
		if s.Request.Type == "write" {
			if err := placeWrite(tx, cid, s); err != nil {
				return false, err
			}
			if s.Status == "done" && s.Request.Entry.Kind == "pi.reset" {
				reset = true
			}
			continue
		}
		if s.Request.WhenBusy == "steer" && !reset {
			if _, err := tx.Append(cid, inputEntry(s.Request)); err != nil {
				return false, err
			}
			s.Status = "running"
			input.Submissions = append(input.Submissions, s.ID)
		}
	}
	return reset, nil
}
func advanceInbox(tx *Tx, cid ConversationID) error {
	c, err := tx.Conversation(cid)
	if err != nil {
		return err
	}
	if c.Busy != 0 {
		return nil
	}
	for _, s := range queuedSubmissions(tx, cid) {
		if s.Request.Type == "write" {
			if err := placeWrite(tx, cid, s); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.Append(cid, inputEntry(s.Request)); err != nil {
			return err
		}
		s.Status = "running"
		id, err := tx.createGeneration(cid, []SubmissionID{s.ID})
		if err != nil {
			return err
		}
		c.Busy = id
		break
	}
	return nil
}
