package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// generationInput is empty for version-3 generations, whose run inputs live
// in pi.live.run. Version-2 generations ran whole runs and listed their
// inputs here; the State version-3 upgrade moves them to pi.live.run.
type generationInput struct {
	Submissions []SubmissionID `json:"submissions,omitempty"`
}

// generationCheckpoint is pi.generation version 3, one turn of a run.
// Phases: prepare, request, retry (a sleep until Until, in Unix
// milliseconds), and tools. Attempt is the 1-based model attempt of the turn;
// zero reads as 1.
type generationCheckpoint struct {
	Phase     string       `json:"phase"`
	Attempt   int          `json:"attempt,omitempty"`
	Until     int64        `json:"until,omitempty"`
	Request   ModelRequest `json:"request,omitempty"`
	Partial   string       `json:"partial,omitempty"`
	Turns     int          `json:"turns,omitempty"` // version 2 only
	Children  []TaskID     `json:"children,omitempty"`
	Assistant EntryID      `json:"assistant,omitempty"`
	// Run is the answered run whose AfterRun hooks the finish phase asks.
	Run *RunSummary `json:"run,omitempty"`
}

func (cp generationCheckpoint) attempt() int {
	if cp.Attempt < 1 {
		return 1
	}
	return cp.Attempt
}

// generationVersion is Go's own numbering of pi.generation's stored form.
// Version 2 replaced version 1's retry count with a 1-based attempt and added
// the retry phase. Version 3 is one generation per turn, with the run in
// pi.live; a version-2 Harness would loop a run in one generation, so it
// leaves version-3 records blocked.
const generationVersion = 3

// migrateGeneration accepts older records. From version 1: attempt = retry +
// 1, every other field kept. Version 2 checkpoints are unchanged.
func migrateGeneration(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
	switch from {
	case 1:
	case 2:
		return MigratedTask{Input: input, Checkpoint: checkpoint}, nil
	default:
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

// createGeneration creates a conversation-owned generation for a turn.
func (tx *Tx) createGeneration(cid ConversationID) (TaskID, error) {
	return tx.CreateTask(cid, generationTaskDefinition(), json.RawMessage(`{}`), TaskOptions{})
}

func generationTaskDefinition() TaskDefinition {
	return TaskDefinition{Kind: "pi.generation", Version: generationVersion, Initial: func(json.RawMessage) (json.RawMessage, error) {
		return json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: 1})
	}, Run: runGeneration, Abort: settleGenerationAbort, Migrate: migrateGeneration}
}

// runGeneration runs one turn of a run (Pi Durable §8.3): prepare the
// request, request (or retry) the model, and either answer, ending the run,
// or start a tool round whose end hands the run to the next generation.
func runGeneration(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	var cp generationCheckpoint
	if err := json.Unmarshal(task.Checkpoint, &cp); err != nil {
		return err
	}
	// Each phase first checks whether crashes cut the generation off too
	// often, which ends its run instead of running the phase again (§5.4).
	switch cp.Phase {
	case "prepare", "request", "retry", "tools":
		if limit := r.h.restartLimit(); cutOffTooOften(task.Restarts, limit) {
			return endGenerationCutOff(ctx, r, task, cp, limit)
		}
	case "finish", "drain":
		if cutOffTooOften(task.Restarts, r.h.restartLimit()) {
			return endAnsweredCutOff(ctx, r, task, cp)
		}
	}
	switch cp.Phase {
	case "prepare":
		return prepareGeneration(ctx, r, task, cp)
	case "request":
		return requestGeneration(ctx, r, task, cp)
	case "retry":
		// After the backoff. A phase that runs before until, after an early
		// wake or a clock that moved backward, sleeps again unchanged.
		if cp.Until > r.h.now().UnixMilli() {
			wake := time.UnixMilli(cp.Until).UTC()
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
				return TaskTransition{Status: "sleeping", Checkpoint: task.Checkpoint, WakeAt: &wake}, nil
			})
		}
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			attempt := cp.attempt() + 1
			if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
				live.Generation = &LiveGeneration{Attempt: attempt}
				return nil
			}); err != nil {
				return TaskTransition{}, err
			}
			next, err := json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: attempt})
			return TaskTransition{Status: "running", Checkpoint: next}, err
		})
	case "tools":
		return finishToolRound(ctx, r, task, cp)
	case "finish":
		return finishRun(ctx, r, task, cp)
	case "drain":
		// The after-run tasks are terminal, whatever their outcomes.
		return endAnsweredRun(ctx, r, task, cp.Assistant)
	default:
		return fmt.Errorf("%w: generation phase %q", ErrInvalid, cp.Phase)
	}
}

// prepareGeneration renders the system prompt and tool loadout and moves to
// request. A retry prepares again, so agent, section, and settings changes
// made during its backoff apply.
func prepareGeneration(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint) error {
	if r.h.options.Model == nil {
		return failNoModel(ctx, r, task)
	}
	state, err := r.h.Snapshot(ctx)
	if err != nil {
		return err
	}
	c := state.Conversations[task.Conversation]
	if c == nil {
		return ErrNotFound
	}
	// The model owes an answer only to a context ending in a user message or
	// tool results; otherwise the run ends before any request.
	messages, err := contextFromState(state, task.Conversation)
	if err != nil {
		return err
	}
	var last *Message
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "system" {
			last = &messages[i]
			break
		}
	}
	if last == nil || last.Role != "user" && last.Role != "tool" {
		return failRun(ctx, r, task, runEnd{reason: RunNothingToAnswer}, "The conversation has nothing to answer", false)
	}
	request := buildRequest(ctx, r, state, *c)
	attempt := cp.attempt()
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		metadata, err := json.Marshal(struct {
			Messages []Message  `json:"system"`
			Tools    []ToolSpec `json:"tools"`
		}{systemMessages(request.Messages), request.Tools})
		if err != nil {
			return TaskTransition{}, err
		}
		// Journal prompt/tool choices, while the complete prepared request is
		// the recovery authority. Native positional system-update caching is
		// not ported.
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
		if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
			live.Generation = &LiveGeneration{Attempt: attempt}
			return nil
		}); err != nil {
			return TaskTransition{}, err
		}
		data, err := json.Marshal(generationCheckpoint{Phase: "request", Attempt: attempt, Request: request})
		return TaskTransition{Status: "running", Checkpoint: data}, err
	})
}

func requestGeneration(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint) error {
	// The host stopped while this request was in flight: a failed attempt
	// under the retry policy, not a resend (§8.3). A clean close is not a
	// crash and leaves no count, so its request is resent below.
	if task.Restarts > 0 {
		return chargeInterruptedRequest(ctx, r, task, cp)
	}
	if r.h.options.Model == nil {
		return failNoModel(ctx, r, task)
	}
	if cp.Partial != "" {
		err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if _, err := tx.Append(task.Conversation, Entry{Kind: "pi.assistant", Message: abortedMessage(cp.Partial, cp.Request.ThinkingLevel)}); err != nil {
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
		// The policy governs the next attempt, so it is read now. An overflow
		// never retries: only a compaction could make it fit.
		policy := r.h.options.Settings.Retry.resolve()
		attempt := cp.attempt()
		text := diagnostic(err.Error())
		// The failed attempt is kept as an error entry, which context
		// derivation excludes, with what it streamed.
		failed := Message{Role: "assistant", Content: cp.Partial, Aborted: true, StopReason: "error", ErrorMessage: text,
			ThinkingLevel: cp.Request.ThinkingLevel, Usage: response.Usage}
		if !IsContextOverflowError(err, cp.Request.Model.Provider) && IsRetryableModelError(err) && policy.allows(attempt) {
			until := retryDeadline(r.h.now(), retryDelayMs(policy, attempt))
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				if _, e := appendAssistant(tx, task.Conversation, cp.Request.Model, failed); e != nil {
					return TaskTransition{}, e
				}
				return retryTransition(tx, task.Conversation, attempt, until, text)
			})
		}
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if _, e := appendAssistant(tx, task.Conversation, cp.Request.Model, failed); e != nil {
				return TaskTransition{}, e
			}
			return failRunTx(tx, r.h, task, runEnd{reason: RunModelError, detail: text}, text, false)
		})
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
	stopReason := response.StopReason
	if stopReason == "" {
		stopReason = "stop"
		if len(response.ToolCalls) > 0 {
			stopReason = "toolUse"
		}
	}
	// Record the requested level, as pi-agent-core's agent loop does.
	message := Message{Role: "assistant", Content: response.Content, Thinking: response.Thinking, ToolCalls: response.ToolCalls, Usage: response.Usage,
		DurationMs: &responseDurationMs, ProviderData: response.ProviderData, StopReason: stopReason, ThinkingLevel: cp.Request.ThinkingLevel}
	// A tool round needs tool calls the model meant to make; an answer cut off
	// at its output limit is an answer, and calls it was cut off in never run.
	if stopReason == "toolUse" && len(response.ToolCalls) > 0 {
		return startToolRound(ctx, r, task, cp, message)
	}
	var continuation string
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
	return answer(ctx, r, task, cp, message, continuation)
}

// appendAssistant appends a provider result and its usage in one commit.
func appendAssistant(tx *Tx, cid ConversationID, model ModelRef, message Message) (EntryID, error) {
	if err := recordModelUsage(tx, cid, model, message.Usage); err != nil {
		return 0, err
	}
	return tx.Append(cid, Entry{Kind: "pi.assistant", Message: &message})
}

// answer is a final answer; the final boundary places queued items (§6).
// The first OnYield continuation appends a user message and hands the run to
// a successor generation, but only when no user item or reset is queued.
// Otherwise the run's inputs settle done, and selected user items start the
// next run.
func answer(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint, message Message, continuation string) error {
	afterRun, err := asksAfterRun(ctx, r, task)
	if err != nil {
		return err
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		cid := task.Conversation
		continues := continuation != ""
		for _, item := range queuedSubmissions(tx, cid) {
			if item.Request.Type == "input" || isReset(item.Request) {
				continues = false
			}
		}
		live, err := readRunLive(tx, task)
		if err != nil {
			return TaskTransition{}, err
		}
		entry, err := appendAssistant(tx, cid, cp.Request.Model, message)
		if err != nil {
			return TaskTransition{}, err
		}
		result, err := json.Marshal(map[string]EntryID{"entryId": entry})
		if err != nil {
			return TaskTransition{}, err
		}
		completed := TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted, Result: result}}
		if !continues && afterRun {
			// Commit A of a run with after-run hooks: the final boundary
			// waits for the run's end, so queued items stay queued.
			if held, ok := holdRun(tx, live, task.ID, RunSummary{Reason: "answer", Answer: entry, Last: entry}); ok {
				return held, writeLive(tx, cid, live)
			}
		}
		boundary, err := r.h.applyBoundary(tx, cid, true)
		if err != nil {
			return TaskTransition{}, err
		}
		if continues {
			if _, err := tx.Append(cid, Entry{Kind: "pi.user", Message: &Message{Role: "user", Content: continuation}}); err != nil {
				return TaskTransition{}, err
			}
			// The continuation starts the run's turn count over: bounding
			// OnYield loops is the extension's job.
			if live.Run != nil && live.Run.TaskID == task.ID {
				live.Run.Turns = 0
			}
			next, err := tx.createGeneration(cid)
			if err != nil {
				return TaskTransition{}, err
			}
			if err := handOver(tx, cid, live, task.ID, next); err != nil {
				return TaskTransition{}, err
			}
			live.Generation = nil
			return completed, writeLive(tx, cid, live)
		}
		if err := endRun(tx, cid, live, task.ID, runEnd{done: true, answer: entry}); err != nil {
			return TaskTransition{}, err
		}
		if len(boundary.users) > 0 {
			if err := startRun(tx, cid, live, boundary.users, boundary.first, false); err != nil {
				return TaskTransition{}, err
			}
		}
		return completed, writeLive(tx, cid, live)
	})
}

// startToolRound appends the tool-calling answer and starts its round in one
// commit: a tool task per call, owned by the generation, in call order, and a
// slot per call. The generation then waits in its tools phase.
func startToolRound(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint, message Message) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		cid := task.Conversation
		live, err := readRunLive(tx, task)
		if err != nil {
			return TaskTransition{}, err
		}
		assistant, err := appendAssistant(tx, cid, cp.Request.Model, message)
		if err != nil {
			return TaskTransition{}, err
		}
		seen := map[string]bool{}
		var children []TaskID
		var slots []ToolSlot
		for _, call := range message.ToolCalls {
			if call.ID == "" || seen[call.ID] {
				return TaskTransition{}, fmt.Errorf("%w: missing/duplicate tool call ID", ErrInvalid)
			}
			seen[call.ID] = true
			data, err := json.Marshal(toolTaskInput{Assistant: assistant, CallID: call.ID, Call: call})
			if err != nil {
				return TaskTransition{}, err
			}
			child, err := tx.CreateTask(cid, toolTaskDefinition(), data, TaskOptions{Owner: Owner{Task: task.ID}})
			if err != nil {
				return TaskTransition{}, err
			}
			children = append(children, child)
			slots = append(slots, ToolSlot{CallID: call.ID, Name: call.Name, Status: "pending", TaskID: child})
		}
		live.Generation = nil
		live.Tools = slots
		if live.Run != nil && live.Run.TaskID == task.ID {
			// A tool round is one turn of the run (Agent.MaxTurns).
			live.Run.Turns++
			// A run its boundary started without placing anything, such as a
			// continue after an answer, begins here.
			if live.Run.First == 0 {
				live.Run.First = assistant
			}
		}
		if err := writeLive(tx, cid, live); err != nil {
			return TaskTransition{}, err
		}
		next, err := json.Marshal(generationCheckpoint{Phase: "tools", Children: children, Assistant: assistant})
		return TaskTransition{Status: "waiting", Checkpoint: next, WaitOn: children, WaitPolicy: "allSettled"}, err
	})
}

// finishToolRound runs once the round's tools are terminal: apply their
// controls, append a handoff's reset, and then end the run at the final
// boundary (terminate), end it like a failure at the conversation's turn
// limit, end it at a queued reset, or hand it to the next generation at the
// post-tools boundary, in the handoff's new context when there is one (§8.5).
func finishToolRound(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint) error {
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
	// Read from the phase's agent; a Configure during the round applies at
	// its end.
	maxTurns := conv.Agent.MaxTurns
	afterRun := false
	if terminate {
		if afterRun, err = asksAfterRun(ctx, r, task); err != nil {
			return err
		}
	}
	var newestResult EntryID
	for _, id := range cp.Children {
		var result toolTaskResult
		if child := state.Tasks[id]; child != nil && child.Outcome != nil && json.Unmarshal(child.Outcome.Result, &result) == nil && result.EntryID > newestResult {
			newestResult = result.EntryID
		}
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		cid := task.Conversation
		live, err := readRunLive(tx, task)
		if err != nil {
			return TaskTransition{}, err
		}
		result, err := json.Marshal(map[string]EntryID{"entryId": cp.Assistant})
		if err != nil {
			return TaskTransition{}, err
		}
		completed := TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted, Result: result}}
		// The run's last entry so far: the newest result, or the assistant.
		last := cp.Assistant
		if newestResult > last {
			last = newestResult
		}
		if handoff != "" {
			// The handoff's reset precedes every queued item, so queued writes
			// and selected steers land in its context.
			reset, err := tx.Append(cid, Entry{Kind: "pi.reset", Message: &Message{Role: "user", Content: handoff}})
			if err != nil {
				return TaskTransition{}, err
			}
			last = reset
		}
		if afterRun {
			if held, ok := holdRun(tx, live, task.ID, RunSummary{Reason: "terminate", Answer: cp.Assistant, Last: last}); ok {
				return held, writeLive(tx, cid, live)
			}
		}
		turns := 0
		if live.Run != nil && live.Run.TaskID == task.ID {
			turns = live.Run.Turns
		}
		switch {
		case terminate:
			boundary, err := r.h.applyBoundary(tx, cid, true)
			if err != nil {
				return TaskTransition{}, err
			}
			if err := endRun(tx, cid, live, task.ID, runEnd{done: true, answer: cp.Assistant}); err != nil {
				return TaskTransition{}, err
			}
			if len(boundary.users) > 0 {
				if err := startRun(tx, cid, live, boundary.users, boundary.first, false); err != nil {
					return TaskTransition{}, err
				}
			}
		case maxTurns > 0 && turns >= maxTurns:
			// The turn limit ends the run like a failure: AfterFailedRun
			// decides the queued items, and a continue resumes it on the
			// round's results.
			detail := fmt.Sprintf("%d turns", turns)
			if turns == 1 {
				detail = "1 turn"
			}
			if err := r.h.endFailedRun(tx, cid, live, task.ID, runEnd{reason: RunTurnLimit, detail: detail}); err != nil {
				return TaskTransition{}, err
			}
		default:
			boundary, err := r.h.applyBoundary(tx, cid, false)
			if err != nil {
				return TaskTransition{}, err
			}
			if boundary.reset {
				// The queued reset cut the run's context before an answer.
				if err := endRun(tx, cid, live, task.ID, runEnd{reason: RunReset}); err != nil {
					return TaskTransition{}, err
				}
				if len(boundary.users) > 0 {
					if err := startRun(tx, cid, live, boundary.users, boundary.first, true); err != nil {
						return TaskTransition{}, err
					}
				}
				break
			}
			live.Tools = nil
			if live.Run != nil && live.Run.TaskID == task.ID {
				live.Run.Inputs = append(live.Run.Inputs, boundary.users...)
			}
			next, err := tx.createGeneration(cid)
			if err != nil {
				return TaskTransition{}, err
			}
			if err := handOver(tx, cid, live, task.ID, next); err != nil {
				return TaskTransition{}, err
			}
		}
		return completed, writeLive(tx, cid, live)
	})
}

// retryTransition sleeps a failed attempt until the retry deadline, noting
// the backoff in pi.live; the next attempt prepares its request again.
func retryTransition(tx *Tx, cid ConversationID, attempt int, until time.Time, text string) (TaskTransition, error) {
	if err := updateLive(tx, cid, func(live *LiveState) error {
		live.Generation = &LiveGeneration{Attempt: attempt, Retry: &LiveRetry{At: until.UnixMilli(), Error: text}}
		return nil
	}); err != nil {
		return TaskTransition{}, err
	}
	next, err := json.Marshal(generationCheckpoint{Phase: "retry", Attempt: attempt, Until: until.UnixMilli()})
	wake := time.UnixMilli(until.UnixMilli()).UTC()
	return TaskTransition{Status: "sleeping", Checkpoint: next, WakeAt: &wake}, err
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

// buildRequest renders sections and runs BeforeRequest hooks. A section or
// hook that fails is reported as extension_error and skipped, as upstream's
// hooks.each does; the request goes on without it.
func buildRequest(ctx context.Context, r *TaskRuntime, state *State, c ConversationRecord) ModelRequest {
	messages, _ := contextFromState(state, c.ID)
	registry := r.Registry()
	var prompt strings.Builder
	for _, section := range registry.sections(c.Agent) {
		if section.Render == nil {
			continue
		}
		text, err := section.Render(ctx, c, r.h)
		if err != nil {
			r.reportExtension(err, section.extension)
			continue
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
	for _, hook := range registry.hooks(c.Agent) {
		if hook.BeforeRequest != nil {
			replaced := request
			replaced.Messages = append([]Message(nil), request.Messages...)
			replaced.Tools = append([]ToolSpec(nil), request.Tools...)
			if err := containPanic(func() error { return hook.BeforeRequest(ctx, &replaced, r) }); err != nil {
				r.reportExtension(err, hook.extension)
				continue
			}
			request = replaced
		}
	}
	return request
}

// reportExtension reports extension code the task called that failed.
func (r *TaskRuntime) reportExtension(err error, extension string) {
	if err == nil || r.check() != nil {
		return
	}
	report := r.taskReport(ReportExtensionError, err, "")
	report.Extension = extension
	r.h.report(report)
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

// failRun ends a run without an answer: endFailedRun applies AfterFailedRun
// when failed is true (model_error, no_model, interrupted, restart_limit,
// nothing_to_answer). A committed partial is kept as an aborted entry when
// salvage is set. The task fails with message and ErrorDetail {"reason":
// end.reason}.
func failRun(ctx context.Context, r *TaskRuntime, task TaskRecord, end runEnd, message string, salvage bool) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		return failRunTx(tx, r.h, task, end, message, salvage)
	})
}

func failRunTx(tx *Tx, h *Harness, task TaskRecord, end runEnd, message string, salvage bool) (TaskTransition, error) {
	cid := task.Conversation
	live, err := readRunLive(tx, task)
	if err != nil {
		return TaskTransition{}, err
	}
	if salvage {
		if err := convertPartial(tx, task); err != nil {
			return TaskTransition{}, err
		}
	}
	if err := h.endFailedRun(tx, cid, live, task.ID, end); err != nil {
		return TaskTransition{}, err
	}
	if err := writeLive(tx, cid, live); err != nil {
		return TaskTransition{}, err
	}
	errorDetail, err := json.Marshal(map[string]string{"reason": end.reason})
	if err != nil {
		return TaskTransition{}, err
	}
	return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeFailed, Error: diagnostic(message), ErrorDetail: errorDetail}}, nil
}

// convertPartial appends the committed partial of the task's current
// checkpoint as an aborted assistant entry.
func convertPartial(tx *Tx, task TaskRecord) error {
	current, err := tx.Task(task.ID)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(current.Checkpoint, &fields)
	var partial string
	_ = json.Unmarshal(fields["partial"], &partial)
	if partial == "" {
		return nil
	}
	var thinking string
	var request struct {
		ThinkingLevel string `json:"thinkingLevel"`
	}
	_ = json.Unmarshal(fields["request"], &request)
	thinking = request.ThinkingLevel
	_, err = tx.Append(task.Conversation, Entry{Kind: "pi.assistant", Message: abortedMessage(partial, thinking)})
	return err
}

// abortedMessage is a committed partial of an attempt that did not finish.
func abortedMessage(partial, thinkingLevel string) *Message {
	return &Message{Role: "assistant", Content: partial, Aborted: true, StopReason: "aborted", ThinkingLevel: thinkingLevel}
}

// failNoModel ends the run: no model is configured. Its inputs settle
// unanswered with reason no_model and no detail.
func failNoModel(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	const message = "No model is configured"
	return failRun(ctx, r, task, runEnd{reason: RunNoModel, detail: message, settlement: &runSettlement{reason: RunNoModel}}, message, true)
}

// endGenerationCutOff ends a run whose generation was cut off restartLimit
// times in a row. At the limit it still keeps a committed partial as an
// aborted entry; past it, that conversion may be what keeps failing.
func endGenerationCutOff(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint, limit float64) error {
	text := fmt.Sprintf("The generation was interrupted %s in its %s phase", cutOffCount(task.Restarts), cp.Phase)
	return failRun(ctx, r, task, runEnd{reason: RunRestartLimit, detail: text}, text, salvageable(task.Restarts, limit))
}

// chargeInterruptedRequest charges a request a crash cut off as a failed
// attempt. Its committed partial becomes an aborted entry; with a retry left
// the generation sleeps and then prepares the next attempt, otherwise the run
// ends interrupted. Nothing is classified: a crash is not an error message.
func chargeInterruptedRequest(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint) error {
	attempt := cp.attempt()
	policy := r.h.options.Settings.Retry.resolve()
	retry := policy.allows(attempt)
	until := retryDeadline(r.h.now(), retryDelayMs(policy, attempt))
	text := fmt.Sprintf("The host stopped while the model was answering, on attempt %d", attempt)
	if !retry {
		return failRun(ctx, r, task, runEnd{reason: RunInterrupted, detail: text}, text, true)
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		if err := convertPartial(tx, task); err != nil {
			return TaskTransition{}, err
		}
		return retryTransition(tx, task.Conversation, attempt, until, text)
	})
}

// settleGenerationAbort is the abort handler: its partial becomes an aborted
// entry, and the run ends unanswered with reason aborted.
func settleGenerationAbort(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		cid := task.Conversation
		live, err := readRunLive(tx, task)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := convertPartial(tx, task); err != nil {
			return TaskTransition{}, err
		}
		if err := endRun(tx, cid, live, task.ID, runEnd{reason: RunAborted}); err != nil {
			return TaskTransition{}, err
		}
		if err := writeLive(tx, cid, live); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeAborted}}, nil
	})
}

func queuedSubmissions(tx *Tx, cid ConversationID) []*SubmissionRecord {
	var queued []*SubmissionRecord
	for _, s := range tx.state.Submissions {
		if s.Conversation == cid && s.Status == "queued" {
			queued = append(queued, s)
		}
	}
	sortSubmissions(queued)
	return queued
}

// asksAfterRun reports whether the phase's agent selects an AfterRun hook, so
// a run that answers waits for the tasks it asks for (§8.3).
func asksAfterRun(ctx context.Context, r *TaskRuntime, task TaskRecord) (bool, error) {
	state, err := r.h.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return false, ErrNotFound
	}
	for _, hook := range r.Registry().hooks(conv.Agent) {
		if hook.AfterRun != nil {
			return true, nil
		}
	}
	return false, nil
}

// holdRun is commit A of a run that answered with AfterRun hooks selected: it
// settles the run's inputs done with the answer and keeps pi.live.run for the
// after-run tasks, which the finish phase asks the hooks for.
func holdRun(tx *Tx, live *LiveState, task TaskID, summary RunSummary) (TaskTransition, bool) {
	run := live.Run
	if run == nil || run.TaskID != task {
		return TaskTransition{}, false
	}
	for _, id := range run.Inputs {
		settleSubmission(tx, id, "done", summary.Answer, "", "")
	}
	run.Inputs = []SubmissionID{}
	run.AfterRun = &LiveAfterRun{}
	live.Generation, live.Tools = nil, nil
	// A run started before First was recorded has its answer as first.
	summary.First = run.First
	if summary.First == 0 {
		summary.First = summary.Answer
	}
	next, err := json.Marshal(generationCheckpoint{Phase: "finish", Run: &summary})
	if err != nil {
		return TaskTransition{}, false
	}
	return TaskTransition{Status: "running", Checkpoint: next}, true
}

// finishRun asks the AfterRun hooks for tasks and creates them as the
// generation's children in the commit that moves to drain; without any, the
// run ends now.
func finishRun(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint) error {
	if cp.Run == nil {
		return fmt.Errorf("%w: finish phase without a run", ErrInvalid)
	}
	state, err := r.h.Snapshot(ctx)
	if err != nil {
		return err
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return ErrNotFound
	}
	snapshot := r.Registry()
	type prepared struct {
		definition TaskDefinition
		input      json.RawMessage
	}
	var tasks []prepared
	for _, hook := range snapshot.hooks(conv.Agent) {
		if hook.AfterRun == nil {
			continue
		}
		var asked []RunEndTask
		if err := containPanic(func() error {
			var err error
			asked, err = hook.AfterRun(ctx, *cp.Run, r)
			return err
		}); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.reportExtension(err, hook.extension)
			continue
		}
		// Check each request off the line, so a broken one is skipped instead
		// of failing the creating commit.
		for _, request := range asked {
			definition, ok := snapshot.Task(request.Kind)
			switch {
			case strings.HasPrefix(request.Kind, "pi."):
				r.reportExtension(fmt.Errorf("an AfterRun hook cannot start the built-in task %s", request.Kind), hook.extension)
			case !ok:
				r.reportExtension(fmt.Errorf("task %s is not installed", request.Kind), hook.extension)
			case len(request.Input) > 0 && !json.Valid(request.Input):
				r.reportExtension(fmt.Errorf("task %s input is not JSON", request.Kind), hook.extension)
			default:
				tasks = append(tasks, prepared{definition: definition, input: request.Input})
			}
		}
	}
	if len(tasks) == 0 {
		return endAnsweredRun(ctx, r, task, cp.Run.Answer)
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		cid := task.Conversation
		var ids []TaskID
		for _, prepared := range tasks {
			id, err := tx.CreateTask(cid, prepared.definition, prepared.input, TaskOptions{Owner: Owner{Task: task.ID}})
			if err != nil {
				return TaskTransition{}, err
			}
			ids = append(ids, id)
		}
		live, err := readRunLive(tx, task)
		if err != nil {
			return TaskTransition{}, err
		}
		if live.Run != nil && live.Run.TaskID == task.ID && live.Run.AfterRun != nil {
			live.Run.AfterRun.Tasks = ids
		}
		if err := writeLive(tx, cid, live); err != nil {
			return TaskTransition{}, err
		}
		next, err := json.Marshal(generationCheckpoint{Phase: "drain", Assistant: cp.Run.Answer, Children: ids})
		return TaskTransition{Status: "waiting", Checkpoint: next, WaitOn: ids, WaitPolicy: "allSettled"}, err
	})
}

// endAnsweredRun is commit B of a run that answered: its after-run tasks are
// terminal, or there were none. Place what the final boundary selects, end
// the run, and start the next one for selected user items.
func endAnsweredRun(ctx context.Context, r *TaskRuntime, task TaskRecord, answer EntryID) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		return endAnsweredRunTx(tx, r.h, task, answer)
	})
}

func endAnsweredRunTx(tx *Tx, h *Harness, task TaskRecord, answer EntryID) (TaskTransition, error) {
	cid := task.Conversation
	live, err := readRunLive(tx, task)
	if err != nil {
		return TaskTransition{}, err
	}
	boundary, err := h.applyBoundary(tx, cid, true)
	if err != nil {
		return TaskTransition{}, err
	}
	if err := endRun(tx, cid, live, task.ID, runEnd{done: true, answer: answer}); err != nil {
		return TaskTransition{}, err
	}
	if len(boundary.users) > 0 {
		if err := startRun(tx, cid, live, boundary.users, boundary.first, false); err != nil {
			return TaskTransition{}, err
		}
	}
	if err := writeLive(tx, cid, live); err != nil {
		return TaskTransition{}, err
	}
	result, err := json.Marshal(map[string]EntryID{"entryId": answer})
	if err != nil {
		return TaskTransition{}, err
	}
	return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted, Result: result}}, nil
}

// endAnsweredCutOff ends a run that answered once its finish or drain phase
// was cut off restartLimit times: it ends completed, without a failure and
// without the tasks it would still wait for, and is reported as
// task_interrupted. Queued items stay queued, as after a failed run.
func endAnsweredCutOff(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint) error {
	answer := cp.Assistant
	if cp.Run != nil {
		answer = cp.Run.Answer
	}
	text := fmt.Sprintf("The generation was interrupted %s in its %s phase", cutOffCount(task.Restarts), cp.Phase)
	err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		live, err := readRunLive(tx, task)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := endRun(tx, task.Conversation, live, task.ID, runEnd{done: true, answer: answer}); err != nil {
			return TaskTransition{}, err
		}
		if err := writeLive(tx, task.Conversation, live); err != nil {
			return TaskTransition{}, err
		}
		result, err := json.Marshal(map[string]EntryID{"entryId": answer})
		if err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted, Result: result}}, nil
	})
	if err == nil {
		report := r.taskReport(ReportTaskInterrupted, fmt.Errorf("%s", text), reasonRestartLimit)
		report.Restarts = task.Restarts
		r.h.report(report)
	}
	return err
}
