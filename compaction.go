package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// compactionInput is pi.compaction's input: why it runs (manual, threshold,
// or overflow) and optional instructions. Version-2 records instead pinned a
// summary Request and its Cut; they finish under their own flow.
type compactionInput struct {
	Reason       string       `json:"reason,omitempty"`
	Instructions string       `json:"instructions,omitempty"`
	Request      ModelRequest `json:"request,omitempty"` // version 2 only
	Cut          EntryID      `json:"cut,omitempty"`     // version 2 only
}

// compactionCheckpoint is pi.compaction version 3. Phases: select, summarize
// (the pinned summary request: model, thinking level, the newest entry, and
// the first entry kept), and retry (a sleep until Until, in Unix
// milliseconds). Version-2 records use request and publish. Attempt is
// 1-based; zero reads as 1.
type compactionCheckpoint struct {
	Phase         string   `json:"phase"`
	Attempt       int      `json:"attempt,omitempty"`
	Until         int64    `json:"until,omitempty"`
	Summary       string   `json:"summary,omitempty"` // version-2 publish
	Model         ModelRef `json:"model,omitempty"`
	ThinkingLevel string   `json:"thinkingLevel,omitempty"`
	Tail          EntryID  `json:"tail,omitempty"`
	FirstKept     EntryID  `json:"firstKept,omitempty"`
}

func (cp compactionCheckpoint) attempt() int {
	if cp.Attempt < 1 {
		return 1
	}
	return cp.Attempt
}

// compactionVersion is Go's own numbering of pi.compaction's stored form.
// Version 2 added the retry phase; version 3 is upstream's select and
// summarize, choosing the range when it runs. Earlier records migrate
// unchanged and finish under their pinned request.
const compactionVersion = 3

func migrateCompaction(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
	if from != 1 && from != 2 {
		return MigratedTask{}, fmt.Errorf("pi.compaction has no migration from version %d", from)
	}
	return MigratedTask{Input: input, Checkpoint: checkpoint}, nil
}

// CompactionExtension is retained for legacy registration; built-in resolution
// always wins and does not require an extension install.
// Deprecated: compaction is a Harness built-in, including after reopen.
func CompactionExtension() Extension {
	return Extension{Name: "pi.compaction", Tasks: []TaskDefinition{compactionTaskDefinition()}}
}
func compactionTaskDefinition() TaskDefinition {
	return TaskDefinition{
		Kind: "pi.compaction", Version: compactionVersion,
		Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"phase":"select"}`), nil },
		Run:     runCompaction,
		Migrate: migrateCompaction,
		Abort: func(ctx context.Context, r *TaskRuntime, t TaskRecord) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				if err := updateLive(tx, t.Conversation, func(live *LiveState) error {
					live.removeCompaction(t.ID)
					return nil
				}); err != nil {
					return TaskTransition{}, err
				}
				return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeAborted}}, nil
			})
		},
	}
}

// createCompaction creates a compaction task with its status. owner is the
// generation that waits for it (a blocking compaction); without one it is
// conversation-owned, and background unless it is manual.
func createCompaction(tx *Tx, cid ConversationID, input compactionInput, owner TaskID) (TaskID, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return 0, err
	}
	id, err := tx.CreateTask(cid, compactionTaskDefinition(), data, TaskOptions{Owner: Owner{Task: owner}, Background: owner == 0 && input.Reason != "manual"})
	if err != nil {
		return 0, err
	}
	return id, updateLive(tx, cid, func(live *LiveState) error {
		live.Compactions = append(live.Compactions, CompactionStatus{TaskID: id, Trigger: input.Reason, Blocking: owner != 0, Attempt: 1})
		return nil
	})
}

// Compact admits a manual compaction task and returns its ID. It summarizes
// while the conversation keeps working and places its summary through a write
// submission: at once when idle, otherwise at the next boundary (§8.7). When
// the policy finds nothing to compact, it completes without a summary.
func (c *Conversation) Compact(ctx context.Context, instructions string) (TaskID, error) {
	var id TaskID
	err := c.h.Commit(ctx, func(tx *Tx) error {
		var err error
		id, err = createCompaction(tx, c.id, compactionInput{Reason: "manual", Instructions: instructions}, 0)
		return err
	})
	if err == nil {
		c.h.Resume()
	}
	return id, err
}

func runCompaction(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	var input compactionInput
	var checkpoint compactionCheckpoint
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return err
	}
	if err := json.Unmarshal(task.Checkpoint, &checkpoint); err != nil {
		return err
	}
	// A compaction cut off restartLimit times in a row fails instead of
	// running its phase again (§5.4).
	switch checkpoint.Phase {
	case "request", "retry", "publish":
		if cutOffTooOften(task.Restarts, r.h.restartLimit()) {
			return failCompaction(ctx, r, task, "Compaction was interrupted "+cutOffCount(task.Restarts), reasonRestartLimit)
		}
	}
	switch checkpoint.Phase {
	case "select", "summarize":
		if cutOffTooOften(task.Restarts, r.h.restartLimit()) {
			return failCompaction(ctx, r, task, "Compaction was interrupted "+cutOffCount(task.Restarts), reasonRestartLimit)
		}
	}
	switch checkpoint.Phase {
	case "select":
		return selectCompaction(ctx, r, task, input)
	case "summarize":
		return summarizeCompaction(ctx, r, task, input, checkpoint)
	case "retry":
		if input.Request.Model.ID == "" && input.Request.Messages == nil {
			// Version 3: back to summarize, with the same pinned range.
			if checkpoint.Until > r.h.now().UnixMilli() {
				wake := time.UnixMilli(checkpoint.Until).UTC()
				return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
					return TaskTransition{Status: "sleeping", Checkpoint: task.Checkpoint, WakeAt: &wake}, nil
				})
			}
			next := checkpoint
			next.Phase, next.Attempt, next.Until = "summarize", checkpoint.attempt()+1, 0
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
					if status := live.compaction(task.ID); status != nil {
						status.Attempt, status.Retry = next.Attempt, nil
					}
					return nil
				}); err != nil {
					return TaskTransition{}, err
				}
				data, err := json.Marshal(next)
				return TaskTransition{Status: "running", Checkpoint: data}, err
			})
		}
	}
	switch checkpoint.Phase {
	case "request":
		if r.h.options.Model == nil {
			return failCompaction(ctx, r, task, "No model is configured", RunNoModel)
		}
		response, err := r.h.options.Model.Generate(ctx, input.Request, func(string) error { return nil })
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Same policy and classification as a generation, without the
			// overflow check (Pi Durable §8.7); read when the attempt failed.
			policy := r.h.settings().Retry.resolve()
			attempt := checkpoint.attempt()
			text := diagnostic(err.Error())
			if IsRetryableModelError(err) && policy.allows(attempt) {
				until := retryDeadline(r.h.now(), retryDelayMs(policy, attempt)).UnixMilli()
				next, err := json.Marshal(compactionCheckpoint{Phase: "retry", Attempt: attempt, Until: until})
				if err != nil {
					return err
				}
				wake := time.UnixMilli(until).UTC()
				return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
					if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
						if status := live.compaction(task.ID); status != nil {
							status.Retry = &LiveRetry{At: until, Error: text}
						}
						return nil
					}); err != nil {
						return TaskTransition{}, err
					}
					return TaskTransition{Status: "sleeping", Checkpoint: next, WakeAt: &wake}, nil
				})
			}
			return failCompaction(ctx, r, task, text, RunModelError)
		}
		next, err := json.Marshal(compactionCheckpoint{Phase: "publish", Summary: response.Content})
		if err != nil {
			return err
		}
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			def := DocumentDefinition{Kind: "pi.usage", Version: 1, Scope: "conversation", History: "latest", Fork: "initial", Initial: func() any { return UsageState{Models: map[string]Usage{}, Tools: map[string]Usage{}} }}
			if err := UpdateDocument(tx, def, uint64(task.Conversation), func(u *UsageState) error {
				if u.Models == nil {
					u.Models = map[string]Usage{}
				}
				key := input.Request.Model.Provider + "/" + input.Request.Model.ID
				old := u.Models[key]
				old.InputTokens += response.Usage.InputTokens
				old.OutputTokens += response.Usage.OutputTokens
				old.CacheReadTokens += response.Usage.CacheReadTokens
				old.CacheWriteTokens += response.Usage.CacheWriteTokens
				old.Cost += response.Usage.Cost
				u.Models[key] = old
				return nil
			}); err != nil {
				return TaskTransition{}, err
			}
			return TaskTransition{Status: "pending", Checkpoint: next}, nil
		})
	case "retry":
		// After the backoff: the next attempt resends the same pinned request.
		// A phase run before until backs off again unchanged.
		if checkpoint.Until > r.h.now().UnixMilli() {
			wake := time.UnixMilli(checkpoint.Until).UTC()
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
				return TaskTransition{Status: "sleeping", Checkpoint: task.Checkpoint, WakeAt: &wake}, nil
			})
		}
		attempt := checkpoint.attempt() + 1
		next, err := json.Marshal(compactionCheckpoint{Phase: "request", Attempt: attempt})
		if err != nil {
			return err
		}
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
				if status := live.compaction(task.ID); status != nil {
					status.Attempt, status.Retry = attempt, nil
				}
				return nil
			}); err != nil {
				return TaskTransition{}, err
			}
			return TaskTransition{Status: "running", Checkpoint: next}, nil
		})
	case "publish":
		return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			entries, err := entriesFromState(tx.state, task.Conversation)
			if err != nil {
				return TaskTransition{}, err
			}
			cutIndex := -1
			for i, e := range entries {
				if e.ID == input.Cut {
					cutIndex = i
					break
				}
			}
			stale := cutIndex < 0
			for i, e := range entries {
				if i <= cutIndex {
					continue
				}
				if e.Kind == "pi.reset" {
					stale = true
				}
				if e.Kind == "pi.compaction" {
					var other struct {
						Cut EntryID `json:"cut"`
					}
					if err := json.Unmarshal(e.Data, &other); err != nil {
						return TaskTransition{}, err
					}
					for j, candidate := range entries {
						if candidate.ID == other.Cut && j >= cutIndex {
							stale = true
						}
					}
				}
			}
			live, err := readLive(tx, task.Conversation)
			if err != nil {
				return TaskTransition{}, err
			}
			live.removeCompaction(task.ID)
			if stale {
				if err := writeLive(tx, task.Conversation, live); err != nil {
					return TaskTransition{}, err
				}
				return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: "completed", Result: json.RawMessage(`{"stale":true}`)}}, nil
			}
			// A compaction that produces a summary removes the last failure.
			live.CompactionFailure = nil
			if err := writeLive(tx, task.Conversation, live); err != nil {
				return TaskTransition{}, err
			}
			data, err := json.Marshal(struct {
				Summary string  `json:"summary"`
				Cut     EntryID `json:"cut"`
			}{checkpoint.Summary, input.Cut})
			if err != nil {
				return TaskTransition{}, err
			}
			// A conversation-owned compaction places its summary through a
			// write submission: at once when idle, else at the next boundary.
			id, err := r.h.admitSubmission(tx, task.Conversation, SubmissionRequest{Type: "write", Entry: &Entry{Kind: "pi.compaction", Data: data}, RequestID: fmt.Sprintf("compaction:%d", task.ID)})
			if err != nil {
				return TaskTransition{}, err
			}
			result, err := json.Marshal(struct {
				SubmissionID SubmissionID `json:"submissionId"`
			}{id})
			if err != nil {
				return TaskTransition{}, err
			}
			return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: "completed", Result: result}}, nil
		})
	default:
		return fmt.Errorf("%w: compaction phase", ErrInvalid)
	}
}

// failCompaction fails a compaction with message and ErrorDetail {"reason":
// reason}, recording the failure in pi.live.compactionFailure.
func failCompaction(ctx context.Context, r *TaskRuntime, task TaskRecord, message, reason string) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
			live.failCompaction(task.ID, message)
			return nil
		}); err != nil {
			return TaskTransition{}, err
		}
		detail, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeFailed, Error: diagnostic(message), ErrorDetail: detail}}, nil
	})
}

// selectCompaction selects an old prefix of the model context to summarize,
// keeping KeepRecentTokens; with nothing to compact, it completes.
func selectCompaction(ctx context.Context, r *TaskRuntime, task TaskRecord, input compactionInput) error {
	if r.h.options.Model == nil {
		return failCompaction(ctx, r, task, "No model is configured", RunNoModel)
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
	policy := resolveCompaction(r.h.settings().Compaction, conv.Agent.Compaction, conv.Agent.Model)
	view, err := deriveContextView(state, task.Conversation, 0)
	if err != nil {
		return err
	}
	cut := selectCut(view, policy.keepRecentTokens)
	if cut < 0 {
		return completeCompaction(ctx, r, task)
	}
	request := CompactionRequest{Reason: input.Reason, Entries: append([]Entry(nil), view.entries[:cut]...), FirstKept: view.entries[cut].ID, Instructions: input.Instructions}
	for _, messages := range view.contributions[:cut] {
		request.Messages = append(request.Messages, messages...)
	}
	request.Messages = orderToolResults(request.Messages)
	for _, hook := range r.Registry().hooks(conv.Agent) {
		if hook.BeforeCompact == nil {
			continue
		}
		var decision CompactionDecision
		if err := containPanic(func() error {
			var err error
			decision, err = hook.BeforeCompact(ctx, request, r)
			return err
		}); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.reportExtension(err, hook.extension)
			continue
		}
		if decision.Decline {
			return completeCompaction(ctx, r, task)
		}
		if strings.TrimSpace(decision.Summary) != "" {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				return placeSummary(tx, r.h, task, input, request.FirstKept, strings.TrimSpace(decision.Summary))
			})
		}
	}
	var tail EntryID
	for _, e := range view.entries {
		if e.ID > tail {
			tail = e.ID
		}
	}
	next, err := json.Marshal(compactionCheckpoint{Phase: "summarize", Attempt: 1, Model: conv.Agent.Model, ThinkingLevel: conv.Agent.ThinkingLevel,
		Tail: tail, FirstKept: view.entries[cut].ID})
	if err != nil {
		return err
	}
	return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
		return TaskTransition{Status: "running", Checkpoint: next}, nil
	})
}

// completeCompaction removes the status and completes without a summary.
func completeCompaction(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
			live.removeCompaction(task.ID)
			return nil
		}); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted, Result: json.RawMessage(`{}`)}}, nil
	})
}

// summarizeCompaction requests the summary of the pinned range: the context
// as of Tail, before FirstKept, serialized as a transcript.
func summarizeCompaction(ctx context.Context, r *TaskRuntime, task TaskRecord, input compactionInput, cp compactionCheckpoint) error {
	if r.h.options.Model == nil {
		return failCompaction(ctx, r, task, "No model is configured", RunNoModel)
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
	view, err := deriveContextView(state, task.Conversation, cp.Tail)
	if err != nil {
		return err
	}
	cut := -1
	for i, e := range view.entries {
		if e.ID == cp.FirstKept {
			cut = i
			break
		}
	}
	if cut < 0 {
		return completeCompaction(ctx, r, task) // the range is gone, as after a reset
	}
	var summarized []Message
	for _, messages := range view.contributions[:cut] {
		summarized = append(summarized, messages...)
	}
	summarized = orderToolResults(summarized)
	request := ModelRequest{Model: cp.Model, SessionID: conv.ProviderSessionID, ThinkingLevel: cp.ThinkingLevel, Messages: []Message{
		{Role: "system", Content: summarizationSystemPrompt},
		{Role: "user", Content: summaryPrompt(summarized, input.Instructions)},
	}}
	response, err := r.h.options.Model.Generate(ctx, request, func(string) error { return nil })
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		text := diagnostic(err.Error())
		policy := r.h.settings().Retry.resolve()
		attempt := cp.attempt()
		if IsRetryableModelError(err) && policy.allows(attempt) {
			until := retryDeadline(r.h.now(), retryDelayMs(policy, attempt)).UnixMilli()
			next := cp
			next.Phase, next.Until = "retry", until
			wake := time.UnixMilli(until).UTC()
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				if err := updateLive(tx, task.Conversation, func(live *LiveState) error {
					if status := live.compaction(task.ID); status != nil {
						status.Retry = &LiveRetry{At: until, Error: text}
					}
					return nil
				}); err != nil {
					return TaskTransition{}, err
				}
				data, err := json.Marshal(next)
				return TaskTransition{Status: "sleeping", Checkpoint: data, WakeAt: &wake}, err
			})
		}
		return failCompaction(ctx, r, task, "Summarization failed: "+text, RunModelError)
	}
	summary := strings.TrimSpace(response.Content)
	switch {
	case response.StopReason == "length":
		return failCompaction(ctx, r, task, "Summarization hit the token limit; the summary is incomplete", RunModelError)
	case len(response.ToolCalls) > 0:
		return failCompaction(ctx, r, task, "Summarization attempted to call a tool", RunModelError)
	case summary == "":
		return failCompaction(ctx, r, task, "Summarization produced no text", RunModelError)
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		if err := recordModelUsage(tx, task.Conversation, cp.Model, response.Usage); err != nil {
			return TaskTransition{}, err
		}
		return placeSummary(tx, r.h, task, input, cp.FirstKept, summary)
	})
}

// placeSummary places the summary entry and completes (§8.7). A blocking
// compaction, owned by its generation, appends it: the generation holds the
// run and waits. A conversation-owned one admits it as a write submission,
// placed at once when idle, otherwise at the next boundary, or settled stale.
func placeSummary(tx *Tx, h *Harness, task TaskRecord, input compactionInput, firstKept EntryID, summary string) (TaskTransition, error) {
	cid := task.Conversation
	if err := updateLive(tx, cid, func(live *LiveState) error {
		live.removeCompaction(task.ID)
		live.CompactionFailure = nil
		return nil
	}); err != nil {
		return TaskTransition{}, err
	}
	// Go's summary entries keep the entries after their cut: the entry just
	// before the first kept one.
	entries, err := entriesFromState(tx.state, cid)
	if err != nil {
		return TaskTransition{}, err
	}
	var cut EntryID
	for i, e := range entries {
		if e.ID == firstKept && i > 0 {
			cut = entries[i-1].ID
		}
	}
	if cut == 0 {
		return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted, Result: json.RawMessage(`{}`)}}, nil
	}
	data, err := json.Marshal(struct {
		Summary string  `json:"summary"`
		Cut     EntryID `json:"cut"`
		Text    string  `json:"text"`
		Reason  string  `json:"reason,omitempty"`
	}{summary, cut, summaryPrefix + summary + summarySuffix, input.Reason})
	if err != nil {
		return TaskTransition{}, err
	}
	entry := &Entry{Kind: "pi.compaction", Data: data}
	var result json.RawMessage
	if task.Owner.Task != 0 {
		id, err := tx.Append(cid, *entry)
		if err != nil {
			return TaskTransition{}, err
		}
		result, err = json.Marshal(map[string]EntryID{"entryId": id})
		if err != nil {
			return TaskTransition{}, err
		}
	} else {
		id, err := h.admitSubmission(tx, cid, SubmissionRequest{Type: "write", Entry: entry, RequestID: fmt.Sprintf("compaction:%d", task.ID)})
		if err != nil {
			return TaskTransition{}, err
		}
		result, err = json.Marshal(map[string]SubmissionID{"submissionId": id})
		if err != nil {
			return TaskTransition{}, err
		}
	}
	return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted, Result: result}}, nil
}
