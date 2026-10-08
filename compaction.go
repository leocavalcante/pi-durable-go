package durable

import (
	"context"
	"encoding/json"
	"fmt"
)

type compactionInput struct {
	Request ModelRequest `json:"request"`
	Cut     EntryID      `json:"cut"`
}
type compactionCheckpoint struct {
	Phase   string `json:"phase"`
	Summary string `json:"summary,omitempty"`
}

// CompactionExtension restores the durable compaction task definition. Install
// it on each reopen before resuming pending compactions. Compact installs it too.
func CompactionExtension() Extension {
	return Extension{Name: "pi.compaction", Tasks: []TaskDefinition{{
		Kind: "pi.compaction", Version: 1,
		Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"phase":"request"}`), nil },
		Run:     runCompaction,
		Abort: func(ctx context.Context, r *TaskRuntime, t TaskRecord) error {
			return r.Complete(ctx, Outcome{Status: "aborted"})
		},
	}}}
}
func (c *Conversation) Compact(ctx context.Context, instructions string) (TaskID, error) {
	if err := c.h.registry.Install(CompactionExtension()); err != nil {
		return 0, err
	}
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
	def := CompactionExtension().Tasks[0]
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
			return fmt.Errorf("durable: no model configured")
		}
		response, err := r.h.options.Model.Generate(ctx, input.Request, func(string) error { return nil })
		if err != nil {
			return err
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
