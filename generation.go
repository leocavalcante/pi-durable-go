package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	Phase    string       `json:"phase"`
	Attempt  int          `json:"attempt,omitempty"`
	Until    int64        `json:"until,omitempty"`
	Request  ModelRequest `json:"request,omitempty"`
	Partial  string       `json:"partial,omitempty"`
	Turns    int          `json:"turns,omitempty"` // version 2 only
	Children []TaskID     `json:"children,omitempty"`
	// Pending lists the calls of a sequential round not started yet.
	Pending   []string `json:"pending,omitempty"`
	Assistant EntryID  `json:"assistant,omitempty"`
	// Run is the answered run whose AfterRun hooks the finish phase asks.
	Run *RunSummary `json:"run,omitempty"`
	// Compacted is the blocking compaction this generation waited for; it
	// starts no other one. Overflow is the error text of the overflow that
	// started it, Threshold set when the blocking threshold did.
	Compacted TaskID `json:"compacted,omitempty"`
	Overflow  string `json:"overflow,omitempty"`
	Threshold bool   `json:"threshold,omitempty"`
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
			next, err := json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: attempt, Compacted: cp.Compacted})
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
	if r.h.model() == nil {
		return failNoModel(ctx, r, task)
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return err
	}
	c := state.Conversations[task.Conversation]
	if c == nil {
		return ErrNotFound
	}
	c = cloneRecord(c) // its agent reaches extension code
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
	attempt := cp.attempt()
	if cp.Compacted != 0 && (cp.Overflow != "" || cp.Threshold) {
		outcomes, err := r.Outcomes(ctx, []TaskID{cp.Compacted})
		if err != nil {
			return err
		}
		outcome := outcomes[0]
		var result struct {
			EntryID EntryID `json:"entryId"`
		}
		_ = json.Unmarshal(outcome.Result, &result)
		// After an overflow, only a summary makes the request fit. A threshold
		// compaction that failed left a context it judged too large.
		if cp.Overflow != "" && (outcome.Status != OutcomeCompleted || result.EntryID == 0) {
			return failRun(ctx, r, task, runEnd{reason: RunModelError, detail: cp.Overflow}, cp.Overflow, false)
		}
		if cp.Threshold && outcome.Status != OutcomeCompleted {
			text := compactionFailureText(outcome)
			return failRun(ctx, r, task, runEnd{reason: RunModelError, detail: text}, text, false)
		}
	}
	request, planned := buildRequest(ctx, r, state, *c)
	threshold := ""
	if cp.Compacted == 0 {
		threshold = thresholdCompaction(r, state, *c, planned)
	}
	if threshold == "blocking" {
		// Compact first and prepare again; the transcript is unchanged until
		// the compaction appends.
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			child, err := createCompaction(tx, task.Conversation, compactionInput{Reason: "threshold"}, task.ID)
			if err != nil {
				return TaskTransition{}, err
			}
			next, err := json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: attempt, Compacted: child, Threshold: true})
			return TaskTransition{Status: "waiting", Checkpoint: next, WaitOn: []TaskID{child}, WaitPolicy: "allSettled"}, err
		})
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		if threshold == "background" {
			live, err := readLive(tx, task.Conversation)
			if err != nil {
				return TaskTransition{}, err
			}
			// Checked in this commit, so a compaction admitted meanwhile counts.
			if len(live.Compactions) == 0 {
				if _, err := createCompaction(tx, task.Conversation, compactionInput{Reason: "threshold"}, 0); err != nil {
					return TaskTransition{}, err
				}
			}
		}
		// The prompt and tool changes the request was planned with, at their
		// transcript positions (spec §7.4).
		for _, entry := range planned {
			if _, err := tx.Append(task.Conversation, entry); err != nil {
				return TaskTransition{}, err
			}
		}
		if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
			live.Generation = &LiveGeneration{Attempt: attempt}
			return nil
		}); err != nil {
			return TaskTransition{}, err
		}
		data, err := json.Marshal(generationCheckpoint{Phase: "request", Attempt: attempt, Request: request, Compacted: cp.Compacted})
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
	if r.h.model() == nil {
		return failNoModel(ctx, r, task)
	}
	// A partial left by an interrupted attempt becomes an aborted entry, and
	// the attempt shown is presentation, in pi.live.
	attempt := cp.attempt()
	if err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		if err := convertPartial(tx, task); err != nil {
			return TaskTransition{}, err
		}
		if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
			live.Generation = &LiveGeneration{Attempt: attempt}
			return nil
		}); err != nil {
			return TaskTransition{}, err
		}
		checkpoint := task.Checkpoint
		if cp.Partial != "" { // a legacy checkpoint partial, converted above
			cleared := cp
			cleared.Partial = ""
			var err error
			if checkpoint, err = json.Marshal(cleared); err != nil {
				return TaskTransition{}, err
			}
		}
		return TaskTransition{Status: "running", Checkpoint: checkpoint}, nil
	}); err != nil {
		return err
	}
	cp.Partial = ""
	stream := newPartialStream(r, task, attempt)
	started := time.Now()
	response, err := r.h.generate(ctx, cp.Request, stream.delta)
	stream.stop()
	partial := stream.text()
	responseDurationMs := time.Since(started).Milliseconds()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The policy governs the next attempt, so it is read now. An overflow
		// never retries: only a compaction could make it fit.
		policy := r.h.settings().Retry.resolve()
		attempt := cp.attempt()
		text := diagnostic(err.Error())
		// AfterResponse hooks see every response, a failed one too (upstream
		// classify); retries and overflows follow the model's error.
		seen := ModelResponse{Content: partial, Usage: response.Usage, StopReason: "error", ErrorMessage: text}
		if e := afterResponse(ctx, r, task, &seen); e != nil {
			return e
		}
		if seen.ErrorMessage != "" {
			text = diagnostic(seen.ErrorMessage)
		}
		// The failed attempt is kept as an error entry, which context
		// derivation excludes, with what it streamed.
		failed := Message{Role: "assistant", Content: seen.Content, Thinking: seen.Thinking, Aborted: true, StopReason: "error", ErrorMessage: text,
			ThinkingLevel: cp.Request.ThinkingLevel, Usage: seen.Usage}
		overflow := IsContextOverflowError(err, cp.Request.Model.Provider)
		if overflow && cp.Compacted == 0 {
			if compacted, err := overflowCompaction(ctx, r, task, cp, failed, text); compacted || err != nil {
				return err
			}
		}
		if !overflow && IsRetryableModelError(err) && policy.allows(attempt) {
			until := retryDeadline(r.h.now(), retryDelayMs(policy, attempt))
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				if _, e := appendAssistant(tx, task.Conversation, cp.Request.Model, failed); e != nil {
					return TaskTransition{}, e
				}
				return retryTransition(tx, task.Conversation, attempt, until, text, cp.Compacted)
			})
		}
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if _, e := appendAssistant(tx, task.Conversation, cp.Request.Model, failed); e != nil {
				return TaskTransition{}, e
			}
			return failRunTx(tx, r.h, task, runEnd{reason: RunModelError, detail: text}, text, false)
		})
	}
	if e := afterResponse(ctx, r, task, &response); e != nil {
		return e
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
	state, e := r.h.read(ctx)
	if e != nil {
		return e
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return ErrNotFound
	}
	var continuation string
	for _, hook := range r.Registry().hooks(conv.Agent) {
		if hook.OnYield != nil {
			var value string
			e := containPanic(func() error {
				var err error
				value, err = hook.OnYield(ctx, response, r)
				return err
			})
			if e != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.reportExtension(e, hook.extension)
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
	// Offered tools are those the request carried; a call to another one never
	// resolves its name.
	offered := map[string]bool{}
	for _, tool := range cp.Request.Tools {
		offered[tool.Name] = true
	}
	// Read as the round starts; a tool is resolved as its tool task resolves it.
	sequential := r.h.settings().ToolExecution == "sequential"
	if state, err := r.h.read(ctx); err == nil {
		if conv := state.Conversations[task.Conversation]; conv != nil {
			for _, tool := range r.Registry().Tools(cloneRecord(conv).Agent) {
				for _, call := range message.ToolCalls {
					if call.Name == tool.Name && offered[call.Name] && tool.ExecutionMode == "sequential" {
						sequential = true
					}
				}
			}
		}
	}
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
		var pending []string
		var slots []ToolSlot
		for _, call := range message.ToolCalls {
			if call.ID == "" || seen[call.ID] {
				return TaskTransition{}, fmt.Errorf("%w: missing/duplicate tool call ID", ErrInvalid)
			}
			seen[call.ID] = true
			unoffered := !offered[call.Name]
			if sequential && len(children) > 0 {
				pending = append(pending, call.ID)
				slots = append(slots, ToolSlot{CallID: call.ID, Name: call.Name, Status: "pending", Unoffered: unoffered})
				continue
			}
			child, err := createToolTask(tx, cid, task.ID, assistant, call, unoffered)
			if err != nil {
				return TaskTransition{}, err
			}
			children = append(children, child)
			slots = append(slots, ToolSlot{CallID: call.ID, Name: call.Name, Status: "pending", TaskID: child, Unoffered: unoffered})
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
		next, err := json.Marshal(generationCheckpoint{Phase: "tools", Children: children, Pending: pending, Assistant: assistant})
		return TaskTransition{Status: "waiting", Checkpoint: next, WaitOn: children, WaitPolicy: "allSettled"}, err
	})
}

// finishToolRound runs once the round's tools are terminal: apply their
// controls, append a handoff's reset, and then end the run at the final
// boundary (terminate), end it like a failure at the conversation's turn
// limit, end it at a queued reset, or hand it to the next generation at the
// post-tools boundary, in the handoff's new context when there is one (§8.5).
func finishToolRound(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint) error {
	if len(cp.Pending) > 0 {
		return startNextCall(ctx, r, task, cp)
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return err
	}
	var results []ToolResult
	var added []string
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
		if child.Outcome != nil && child.Outcome.Status == OutcomeCompleted {
			added = append(added, result.Result.AddTools...)
		}
		terminate = terminate && result.Result.Terminate
		if result.Result.Handoff != "" {
			handoff = result.Result.Handoff
		}
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return ErrNotFound
	}
	for _, hook := range r.Registry().hooks(conv.Agent) {
		if hook.AfterTools != nil {
			if err := containPanic(func() error { return hook.AfterTools(ctx, results, r) }); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.reportExtension(err, hook.extension)
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
		if len(added) > 0 {
			if err := addTools(tx, cid, added); err != nil {
				return TaskTransition{}, err
			}
		}
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
func retryTransition(tx *Tx, cid ConversationID, attempt int, until time.Time, text string, compacted TaskID) (TaskTransition, error) {
	if err := updateLive(tx, cid, func(live *LiveState) error {
		live.Generation = &LiveGeneration{Attempt: attempt, Retry: &LiveRetry{At: until.UnixMilli(), Error: text}}
		return nil
	}); err != nil {
		return TaskTransition{}, err
	}
	next, err := json.Marshal(generationCheckpoint{Phase: "retry", Attempt: attempt, Until: until.UnixMilli(), Compacted: compacted})
	wake := time.UnixMilli(until.UnixMilli()).UTC()
	return TaskTransition{Status: "sleeping", Checkpoint: next, WakeAt: &wake}, err
}

// afterResponse runs the agent's AfterResponse hooks on a response, in
// order; a failing hook is reported as extension_error and skipped.
func afterResponse(ctx context.Context, r *TaskRuntime, task TaskRecord, response *ModelResponse) error {
	state, err := r.h.read(ctx)
	if err != nil {
		return err
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return ErrNotFound
	}
	for _, hook := range r.Registry().hooks(conv.Agent) {
		if hook.AfterResponse == nil {
			continue
		}
		if err := containPanic(func() error { return hook.AfterResponse(ctx, response, r) }); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.reportExtension(err, hook.extension)
		}
	}
	return nil
}

// buildRequest plans the prompt and tool changes of the next request (spec
// §7.4): it renders the agent's sections, plans the pi.system entries that
// make the replayed prompt and tools equal them, derives the request's
// messages as if those entries were appended, and runs BeforeRequest hooks.
// A section or hook that fails is reported as extension_error and skipped,
// as upstream's hooks.each does; a failed section keeps the text the model
// was shown.
func buildRequest(ctx context.Context, r *TaskRuntime, state *State, c ConversationRecord) (ModelRequest, []Entry) {
	request := ModelRequest{Model: c.Agent.Model, SessionID: c.ProviderSessionID, ThinkingLevel: c.Agent.ThinkingLevel}
	view, err := deriveContextView(state, c.ID, 0)
	if err != nil {
		return request, nil
	}
	shown := replaySections(view.messages)
	registry := r.Registry()
	var desired orderedText
	sections := registry.sections(c.Agent)
	var input PromptInput
	if len(sections) > 0 {
		input = PromptInput{ConversationID: c.ID, Conversation: *cloneRecord(&c), Agent: resolveAgent(registry, c.Agent), Shown: map[string]string{}, Read: r.h}
		for name, text := range shown.text {
			input.Shown[name] = text
		}
		if r.h.options.Environment != nil {
			env, err := r.h.options.Environment(*cloneRecord(&c))
			if err != nil {
				r.ReportHost(err, "env")
			} else {
				input.Env = env
			}
		}
	}
	for _, section := range sections {
		if section.Render == nil {
			continue
		}
		text, err := section.Render(ctx, input)
		if errors.Is(err, ErrOmitSection) {
			continue
		}
		if err != nil {
			r.reportExtension(err, section.extension)
			if kept, ok := shown.text[section.Name]; ok {
				desired.set(section.Name, kept)
			}
			continue
		}
		if section.Untagged {
			desired.set(section.Name, text)
		} else {
			desired.set(section.Name, fmt.Sprintf("<%s>\n%s\n</%s>", section.Name, text, section.Name))
		}
	}
	// The agent's instructions, when set (even empty), come after every
	// extension section.
	if c.Agent.Instructions != nil {
		desired.set("instructions", "<instructions>\n"+*c.Agent.Instructions+"\n</instructions>")
	}
	var tools []ToolSpec
	for _, tool := range offeredTools(registry.Tools(c.Agent), c.Agent.ModelTools) {
		tools = append(tools, tool.ToolSpec)
	}
	planned := planSystemEntries(view, desired, tools)
	request.Messages = view.messages
	if len(planned) > 0 {
		if messages, err := messagesWith(state, c.ID, planned); err == nil {
			request.Messages = messages
		}
	}
	// The system messages declare the tools; the list mirrors them for
	// adapters that send a tool list.
	request.Tools = CurrentTools(request.Messages)
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
	return request, planned
}

// messagesWith derives the conversation's context as if the planned entries
// were appended after its newest entry.
func messagesWith(state *State, cid ConversationID, planned []Entry) ([]Message, error) {
	history, err := entriesFromState(state, cid)
	if err != nil {
		return nil, err
	}
	for i, e := range planned {
		e.ID, e.Conversation = EntryID(state.NextID+uint64(i)), cid
		history = append(history, e)
	}
	view, err := deriveContext(history)
	if err != nil {
		return nil, err
	}
	return view.messages, nil
}

// reportExtension reports extension code the task called that failed.
func (r *TaskRuntime) reportExtension(err error, extension string) {
	if err == nil || r.check() != nil {
		return
	}
	report := r.taskReport(ReportExtensionError, err, "")
	report.Extension = extension
	r.emit(report)
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

// convertPartial appends a committed partial left by an interrupted,
// aborted, faulted, or orphaned attempt as an aborted assistant entry and
// removes it from pi.live.generation. Records from before partials moved to
// pi.live kept theirs in the checkpoint.
func convertPartial(tx *Tx, task TaskRecord) error {
	current, err := tx.Task(task.ID)
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(current.Checkpoint, &fields)
	var partial string
	_ = json.Unmarshal(fields["partial"], &partial)
	if live, err := readLive(tx, task.Conversation); err == nil && live.Generation != nil && live.Generation.Message != nil &&
		(live.Run == nil || live.Run.TaskID == task.ID) {
		if partial == "" {
			partial = live.Generation.Message.Content
		}
		live.Generation.Message = nil
		if err := writeLive(tx, task.Conversation, live); err != nil {
			return err
		}
	}
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
	policy := r.h.settings().Retry.resolve()
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
		return retryTransition(tx, task.Conversation, attempt, until, text, cp.Compacted)
	})
}

// settleGenerationAbort is the abort handler: its partial becomes an aborted
// entry, calls of a sequential round never started get aborted results
// (unoffered ones tool_unavailable), and the run ends unanswered with reason
// aborted.
func settleGenerationAbort(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	var cp generationCheckpoint
	_ = json.Unmarshal(task.Checkpoint, &cp)
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		cid := task.Conversation
		live, err := readRunLive(tx, task)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := convertPartial(tx, task); err != nil {
			return TaskTransition{}, err
		}
		if cp.Phase == "tools" && len(cp.Pending) > 0 {
			if entry, err := tx.Entry(cp.Assistant); err == nil && entry.Message != nil {
				for _, id := range cp.Pending {
					for _, call := range entry.Message.ToolCalls {
						if call.ID != id {
							continue
						}
						result := harnessError("aborted", fmt.Sprintf("Tool %s was aborted", call.Name))
						if unofferedCall(live, call.ID) {
							result = harnessError("tool_unavailable", fmt.Sprintf("Tool %s is not available", call.Name))
						}
						if _, err := appendToolResult(tx, cid, call, result); err != nil {
							return TaskTransition{}, err
						}
					}
				}
			}
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

// queuedSubmissions returns the conversation's queued submissions, mutable
// in this transaction, in ID order.
func queuedSubmissions(tx *Tx, cid ConversationID) []*SubmissionRecord {
	var queued []*SubmissionRecord
	for id, s := range tx.state.Submissions {
		if s.Conversation == cid && s.Status == "queued" {
			queued = append(queued, tx.state.mutSubmission(id))
		}
	}
	sortSubmissions(queued)
	return queued
}

// asksAfterRun reports whether the phase's agent selects an AfterRun hook, so
// a run that answers waits for the tasks it asks for (§8.3).
func asksAfterRun(ctx context.Context, r *TaskRuntime, task TaskRecord) (bool, error) {
	state, err := r.h.read(ctx)
	if err != nil {
		return false, err
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return false, ErrNotFound
	}
	conv = cloneRecord(conv)
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
	state, err := r.h.read(ctx)
	if err != nil {
		return err
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return ErrNotFound
	}
	conv = cloneRecord(conv)
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
		r.emit(report)
	}
	return err
}

// defaultPartialInterval is upstream's progress.partialIntervalMs default.
const defaultPartialInterval = 100 * time.Millisecond

// partialStream commits a streamed partial to pi.live.generation.message at
// most every Settings.Progress.PartialIntervalMs, with one commit in flight,
// on the invocation's pacing timer (upstream streamResponse). Partials are
// presentation, not task progress: the checkpoint does not change.
type partialStream struct {
	r        *TaskRuntime
	task     TaskRecord
	attempt  int
	interval time.Duration
	mu       sync.Mutex
	partial  strings.Builder
	dirty    bool
	armed    bool
	stopped  bool
	disarm   func()
	inFlight sync.WaitGroup
}

func newPartialStream(r *TaskRuntime, task TaskRecord, attempt int) *partialStream {
	interval := defaultPartialInterval
	if ms := r.h.settings().Progress.PartialIntervalMs; ms != nil && *ms >= 0 {
		interval = time.Duration(*ms * float64(time.Millisecond))
	}
	return &partialStream{r: r, task: task, attempt: attempt, interval: interval}
}

func (p *partialStream) delta(text string) error {
	if err := p.r.check(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partial.WriteString(text)
	p.dirty = true
	if !p.armed && !p.stopped {
		p.armed = true
		p.disarm = p.r.SetTimer(p.flush, p.interval)
	}
	return nil
}

func (p *partialStream) flush() {
	p.mu.Lock()
	if p.stopped || !p.dirty {
		p.armed = false
		p.mu.Unlock()
		return
	}
	text := p.partial.String()
	p.dirty = false
	p.inFlight.Add(1)
	p.mu.Unlock()
	err := p.r.CommitWith(context.Background(), progressCommit(), func(tx *Tx) (TaskTransition, error) {
		current, err := tx.Task(p.task.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := updateLive(tx, p.task.Conversation, func(live *LiveState) error {
			if live.Generation == nil {
				live.Generation = &LiveGeneration{Attempt: p.attempt}
			}
			live.Generation.Message = &Message{Role: "assistant", Content: text}
			return nil
		}); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: current.Checkpoint}, nil
	})
	p.inFlight.Done()
	if err != nil && p.r.check() == nil {
		p.r.Report(err) // the committed state stays consistent
	}
	p.mu.Lock()
	p.armed = false
	if p.dirty && !p.stopped {
		p.armed = true
		p.disarm = p.r.SetTimer(p.flush, p.interval)
	}
	p.mu.Unlock()
}

// stop ends throttling and waits for a commit in flight, so no stale partial
// lands after the attempt's outcome.
func (p *partialStream) stop() {
	p.mu.Lock()
	p.stopped = true
	disarm := p.disarm
	p.mu.Unlock()
	if disarm != nil {
		disarm()
	}
	p.inFlight.Wait()
}

func (p *partialStream) text() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.partial.String()
}

// createToolTask creates the tool task of one call, owned by the generation.
func createToolTask(tx *Tx, cid ConversationID, generation TaskID, assistant EntryID, call ToolCall, unoffered bool) (TaskID, error) {
	data, err := json.Marshal(toolTaskInput{Assistant: assistant, CallID: call.ID, Call: call, Unoffered: unoffered})
	if err != nil {
		return 0, err
	}
	return tx.CreateTask(cid, toolTaskDefinition(), data, TaskOptions{Owner: Owner{Task: generation}})
}

// startNextCall starts the next call of a sequential round and waits for it.
func startNextCall(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		cid := task.Conversation
		entry, err := tx.Entry(cp.Assistant)
		if err != nil {
			return TaskTransition{}, err
		}
		var call *ToolCall
		for i := range entry.Message.ToolCalls {
			if entry.Message.ToolCalls[i].ID == cp.Pending[0] {
				call = &entry.Message.ToolCalls[i]
			}
		}
		if call == nil {
			return TaskTransition{}, fmt.Errorf("%w: pending call %q not in its answer", ErrInvalid, cp.Pending[0])
		}
		live, err := readRunLive(tx, task)
		if err != nil {
			return TaskTransition{}, err
		}
		// The round's slots say which calls their request did not offer.
		child, err := createToolTask(tx, cid, task.ID, cp.Assistant, *call, unofferedCall(live, call.ID))
		if err != nil {
			return TaskTransition{}, err
		}
		for i := range live.Tools {
			if live.Tools[i].CallID == call.ID && live.Tools[i].TaskID == 0 {
				live.Tools[i].TaskID = child
			}
		}
		if err := writeLive(tx, cid, live); err != nil {
			return TaskTransition{}, err
		}
		next := cp
		next.Children = append(append([]TaskID(nil), cp.Children...), child)
		next.Pending = cp.Pending[1:]
		data, err := json.Marshal(next)
		return TaskTransition{Status: "waiting", Checkpoint: data, WaitOn: []TaskID{child}, WaitPolicy: "allSettled"}, err
	})
}

func unofferedCall(live *LiveState, callID string) bool {
	for _, slot := range live.Tools {
		if slot.CallID == callID {
			return slot.Unoffered
		}
	}
	return false
}

// thresholdCompaction decides which compaction preparation starts before its
// request (§8.3): blocking above contextWindow − ReserveTokens, background
// above the background threshold below that, and only when range selection
// finds a cut. The context window comes from a ContextWindowModel; unknown
// disables thresholds.
func thresholdCompaction(r *TaskRuntime, state *State, c ConversationRecord, planned []Entry) string {
	model, ok := r.h.model().(ContextWindowModel)
	if !ok {
		return ""
	}
	window := float64(model.ContextWindow(c.Agent.Model))
	policy := resolveCompaction(r.h.settings().Compaction, c.Agent.Compaction, c.Agent.Model)
	if !policy.enabled || window <= 0 {
		return ""
	}
	view, err := deriveContextView(state, c.ID, 0)
	if err != nil {
		return ""
	}
	var extra []Message
	for _, entry := range planned {
		if entry.Message != nil {
			extra = append(extra, *entry.Message)
		}
	}
	tokens := float64(estimateContext(view, extra))
	blocking := window - policy.reserveTokens
	background := blocking - policy.backgroundTokens
	over := ""
	switch {
	case tokens > blocking:
		over = "blocking"
	case policy.backgroundTokens > 0 && tokens > background:
		over = "background"
	}
	if over == "" || selectCut(view, policy.keepRecentTokens) < 0 {
		return ""
	}
	return over
}

// overflowCompaction starts a blocking compaction after a context overflow,
// when the policy allows and a cut exists: the failed attempt is kept, and
// the generation prepares again once the summary is in place.
func overflowCompaction(ctx context.Context, r *TaskRuntime, task TaskRecord, cp generationCheckpoint, failed Message, text string) (bool, error) {
	state, err := r.h.read(ctx)
	if err != nil {
		return false, err
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return false, ErrNotFound
	}
	conv = cloneRecord(conv)
	policy := resolveCompaction(r.h.settings().Compaction, conv.Agent.Compaction, cp.Request.Model)
	if !policy.enabled {
		return false, nil
	}
	view, err := deriveContextView(state, task.Conversation, 0)
	if err != nil || selectCut(view, policy.keepRecentTokens) < 0 {
		return false, nil
	}
	return true, r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		if _, err := appendAssistant(tx, task.Conversation, cp.Request.Model, failed); err != nil {
			return TaskTransition{}, err
		}
		if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
			live.Generation = nil
			return nil
		}); err != nil {
			return TaskTransition{}, err
		}
		child, err := createCompaction(tx, task.Conversation, compactionInput{Reason: "overflow"}, task.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		next, err := json.Marshal(generationCheckpoint{Phase: "prepare", Attempt: cp.attempt(), Compacted: child, Overflow: text})
		return TaskTransition{Status: "waiting", Checkpoint: next, WaitOn: []TaskID{child}, WaitPolicy: "allSettled"}, err
	})
}

// compactionFailureText is the error of a blocking compaction that ended
// without completing.
func compactionFailureText(outcome Outcome) string {
	switch outcome.Status {
	case OutcomeFailed, OutcomeFaulted:
		return "Compaction failed: " + outcome.Error
	case OutcomeOrphaned:
		return "Compaction failed: " + outcome.Reason
	default:
		return "Compaction was aborted"
	}
}

// offeredTools are the tools a request offers (§8.3): model-callable, then
// selected by the agent's ModelTools.
func offeredTools(tools []ToolDefinition, filter *ModelTools) []ToolDefinition {
	var callable []ToolDefinition
	byName := map[string]ToolDefinition{}
	for _, tool := range tools {
		if callableBy(tool, "model") {
			callable = append(callable, tool)
			byName[tool.Name] = tool
		}
	}
	if filter != nil && filter.Only != nil {
		var out []ToolDefinition
		seen := map[string]bool{}
		for _, name := range filter.Only {
			if tool, ok := byName[name]; ok && !seen[name] {
				out = append(out, tool)
				seen[name] = true
			}
		}
		return out
	}
	removed := map[string]bool{}
	var add []string
	if filter != nil {
		for _, name := range filter.Remove {
			removed[name] = true
		}
		add = filter.Add
	}
	var out []ToolDefinition
	for _, tool := range callable {
		if !tool.Deferred && !removed[tool.Name] {
			out = append(out, tool)
		}
	}
	// Loaded deferred tools follow in load order.
	seen := map[string]bool{}
	for _, name := range add {
		if tool, ok := byName[name]; ok && tool.Deferred && !removed[name] && !seen[name] {
			out = append(out, tool)
			seen[name] = true
		}
	}
	return out
}

// addTools applies a round's AddTools to the conversation's agent: each name
// joins an explicit Tools list, leaves ModelTools.Remove, and is appended to
// ModelTools.Add (or Only), which loads deferred tools.
func addTools(tx *Tx, cid ConversationID, added []string) error {
	conv, err := tx.Conversation(cid)
	if err != nil {
		return err
	}
	before, err := json.Marshal(conv.Agent)
	if err != nil {
		return err
	}
	agent := conv.Agent
	contains := func(list []string, name string) bool {
		for _, item := range list {
			if item == name {
				return true
			}
		}
		return false
	}
	if agent.Tools != nil {
		tools := append([]string(nil), agent.Tools...)
		for _, name := range added {
			if !contains(tools, name) {
				tools = append(tools, name)
			}
		}
		agent.Tools = tools
	}
	selection := ModelTools{}
	if agent.ModelTools != nil {
		selection = *agent.ModelTools
	}
	if selection.Only != nil {
		only := append([]string(nil), selection.Only...)
		for _, name := range added {
			if !contains(only, name) {
				only = append(only, name)
			}
		}
		selection.Only = only
	} else {
		var remove []string
		for _, name := range selection.Remove {
			if !contains(added, name) {
				remove = append(remove, name)
			}
		}
		selection.Remove = remove
		add := append([]string(nil), selection.Add...)
		for _, name := range added {
			if !contains(add, name) {
				add = append(add, name)
			}
		}
		selection.Add = add
	}
	agent.ModelTools = &selection
	after, err := json.Marshal(agent)
	if err != nil || jsonEqual(before, after) {
		return err
	}
	conv.Agent = agent
	_, err = tx.Append(cid, Entry{Kind: "pi.agent", Data: after})
	return err
}
