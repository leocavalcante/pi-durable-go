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
			return r.Complete(ctx, Outcome{Status: OutcomeAborted})
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
			if e.Message != nil && e.Message.Role == "assistant" && len(e.Message.ToolCalls) == 0 && !e.Message.Aborted {
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
		id, err = tx.CreateTask(c.id, def, input, TaskOptions{Background: true})
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
	switch checkpoint.Phase {
	case "request":
		if r.h.options.Model == nil {
			return r.Complete(ctx, Outcome{Status: OutcomeFailed, Error: "durable: no model configured"})
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
			if IsRetryableModelError(err) && policy.allows(attempt) {
				until := retryDeadline(r.h.now(), retryDelayMs(policy, attempt)).UnixMilli()
				next, err := json.Marshal(compactionCheckpoint{Phase: "retry", Attempt: attempt, Until: until})
				if err != nil {
					return err
				}
				wake := time.UnixMilli(until).UTC()
				return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
					return TaskTransition{Status: "pending", Checkpoint: next, WakeAt: &wake}, nil
				})
			}
			return r.Complete(ctx, Outcome{Status: OutcomeFailed, Error: diagnostic(err.Error())})
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
				return TaskTransition{Status: "pending", Checkpoint: task.Checkpoint, WakeAt: &wake}, nil
			})
		}
		next, err := json.Marshal(compactionCheckpoint{Phase: "request", Attempt: checkpoint.attempt() + 1})
		if err != nil {
			return err
		}
		return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
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
			if stale {
				return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: "completed", Result: json.RawMessage(`{"stale":true}`)}}, nil
			}
			data, err := json.Marshal(struct {
				Summary string  `json:"summary"`
				Cut     EntryID `json:"cut"`
			}{checkpoint.Summary, input.Cut})
			if err != nil {
				return TaskTransition{}, err
			}
			id := SubmissionID(tx.id())
			tx.state.Submissions[id] = &SubmissionRecord{ID: id, Conversation: task.Conversation, Request: SubmissionRequest{Type: "write", Entry: &Entry{Kind: "pi.compaction", Data: data}, RequestID: fmt.Sprintf("compaction:%d", task.ID)}, Status: "queued", CreatedAt: tx.clock()}
			rec, err := tx.Conversation(task.Conversation)
			if err != nil {
				return TaskTransition{}, err
			}
			if rec.Busy == 0 {
				if err := advanceInbox(tx, task.Conversation); err != nil {
					return TaskTransition{}, err
				}
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
