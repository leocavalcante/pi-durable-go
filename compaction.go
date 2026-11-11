package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type compactionInput struct {
	Request ModelRequest `json:"request"`
	Cut     EntryID      `json:"cut"`
}

// compactionCheckpoint is pi.compaction version 2. Phases: request (the
// pinned summary request), retry (a backoff until Until, in Unix
// milliseconds), and publish. Attempt is 1-based; zero reads as 1.
type compactionCheckpoint struct {
	Phase   string `json:"phase"`
	Attempt int    `json:"attempt,omitempty"`
	Until   int64  `json:"until,omitempty"`
	Summary string `json:"summary,omitempty"`
}

func (cp compactionCheckpoint) attempt() int {
	if cp.Attempt < 1 {
		return 1
	}
	return cp.Attempt
}

// compactionVersion is Go's own numbering of pi.compaction's stored form.
// Version 2 added the retry phase and attempt; version-1 records read as
// attempt 1 unchanged, and a version-1 Harness leaves version 2 blocked.
const compactionVersion = 2

func migrateCompaction(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
	if from != 1 {
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
		Initial: func(json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`{"phase":"request","attempt":1}`), nil
		},
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
func (c *Conversation) Compact(ctx context.Context, instructions string) (TaskID, error) {
	state, err := c.h.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	rec := state.Conversations[c.id]
	if rec == nil {
		return 0, ErrNotFound
	}
	entries, err := entriesFromState(state, c.id)
	if err != nil {
		return 0, err
	}
	cut := rec.Head
	if rec.Busy != 0 {
		cut = 0
		for _, e := range entries {
			if e.Message != nil && e.Message.Role == "assistant" && len(e.Message.ToolCalls) == 0 && contributesContext(e.Message) {
				cut = e.ID
			}
		}
	}
	if cut == 0 {
		return 0, fmt.Errorf("%w: no completed turn to compact", ErrInvalid)
	}
	// Derive exactly the model context at the retained turn boundary.
	copyRec := *rec
	copyRec.Head = cut
	state.Conversations[c.id] = &copyRec
	messages, err := contextFromState(state, c.id)
	if err != nil {
		return 0, err
	}
	// Native continuation data is not summary input. In particular, encrypted
	// thinking/signatures should not be exposed to the summarizing model.
	for i := range messages {
		messages[i].ProviderData = nil
	}
	transcript, err := json.Marshal(messages)
	if err != nil {
		return 0, err
	}
	request := ModelRequest{Model: rec.Agent.Model, SessionID: rec.ProviderSessionID, ThinkingLevel: rec.Agent.ThinkingLevel, Messages: []Message{
		{Role: "system", Content: "Summarize this conversation for another agent to continue. Preserve goals, decisions, unresolved work, exact identifiers, and relevant tool results. Return only the summary. " + instructions},
		{Role: "user", Content: string(transcript)},
	}}
	input, err := json.Marshal(compactionInput{Request: request, Cut: cut})
	if err != nil {
		return 0, err
	}
	var id TaskID
	def := compactionTaskDefinition()
	err = c.h.Commit(ctx, func(tx *Tx) error {
		var err error
		// A manual compaction is conversation-owned and not background, so a
		// conversation abort aborts it (upstream createCompaction).
		if id, err = tx.CreateTask(c.id, def, input, TaskOptions{}); err != nil {
			return err
		}
		return updateLive(tx, c.id, func(live *LiveState) error {
			live.Compactions = append(live.Compactions, CompactionStatus{TaskID: id, Trigger: "manual", Attempt: 1})
			return nil
		})
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
			policy := r.h.options.Settings.Retry.resolve()
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
