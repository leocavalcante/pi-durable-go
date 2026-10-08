package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

type TaskRuntime struct {
	h          *Harness
	ID         TaskID
	invocation *taskInvocation
	ended      atomic.Bool
}

func (r *TaskRuntime) Harness() *Harness { return r.h }
func (r *TaskRuntime) check() error {
	if r == nil || r.h == nil || r.ended.Load() {
		return ErrClosed
	}
	if r.invocation != nil {
		return r.invocation.ctx.Err()
	}
	return nil
}
func (r *TaskRuntime) Commit(ctx context.Context, change func(*Tx) (TaskTransition, error)) error {
	if err := r.check(); err != nil {
		return err
	}
	if change == nil {
		return ErrInvalid
	}
	return r.h.Commit(ctx, func(tx *Tx) error {
		if err := r.check(); err != nil {
			return err
		}
		current, err := tx.Task(r.ID)
		if err != nil {
			return err
		}
		if current.Status != "running" {
			return ErrConflict
		}
		transition, err := change(tx)
		if err != nil {
			return err
		}
		if err := r.check(); err != nil {
			return err
		}
		return applyTransition(tx, current, transition, r.h.now())
	})
}
func (r *TaskRuntime) Complete(ctx context.Context, outcome Outcome) error {
	return r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "terminal", Outcome: &outcome}, nil })
}

// Memo with nil candidate reads only. With a candidate, the first committed
// value wins. Memos do not atomically couple commits to outside services.
func (r *TaskRuntime) Memo(ctx context.Context, key string, candidate json.RawMessage) (json.RawMessage, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, fmt.Errorf("%w: memo key", ErrInvalid)
	}
	if candidate == nil {
		state, err := r.h.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		task := state.Tasks[r.ID]
		if task == nil {
			return nil, ErrNotFound
		}
		return append(json.RawMessage(nil), task.Memos[key]...), nil
	}
	if !json.Valid(candidate) {
		return nil, fmt.Errorf("%w: memo JSON", ErrInvalid)
	}
	var value json.RawMessage
	err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if task.Memos == nil {
			task.Memos = map[string]json.RawMessage{}
		}
		if existing, ok := task.Memos[key]; ok {
			value = append(json.RawMessage(nil), existing...)
		} else {
			value = append(json.RawMessage(nil), candidate...)
			task.Memos[key] = value
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
	return value, err
}
func applyTransition(tx *Tx, task *TaskRecord, tr TaskTransition, now time.Time) error {
	switch tr.Status {
	case "pending", "running", "waiting", "terminal":
	default:
		return fmt.Errorf("%w: task transition %q", ErrInvalid, tr.Status)
	}
	if tr.Checkpoint != nil && !json.Valid(tr.Checkpoint) {
		return fmt.Errorf("%w: task checkpoint JSON", ErrInvalid)
	}
	if tr.Status == "waiting" {
		if tr.WaitPolicy != "" && tr.WaitPolicy != "allSettled" && tr.WaitPolicy != "failFast" {
			return fmt.Errorf("%w: wait policy", ErrInvalid)
		}
		for _, id := range tr.WaitOn {
			target := tx.state.Tasks[id]
			if id == task.ID || target == nil {
				return fmt.Errorf("%w: wait target", ErrInvalid)
			}
			if tr.WaitPolicy == "failFast" && target.Owner.Task != task.ID {
				return fmt.Errorf("%w: failFast requires owned children", ErrInvalid)
			}
		}
	}
	if tr.Status == "terminal" {
		if tr.Outcome == nil {
			return fmt.Errorf("%w: terminal task requires outcome", ErrInvalid)
		}
		switch tr.Outcome.Status {
		case "completed", "failed", "aborted":
		default:
			return fmt.Errorf("%w: outcome status", ErrInvalid)
		}
		if len(tr.Outcome.Result) > 0 && !json.Valid(tr.Outcome.Result) {
			return fmt.Errorf("%w: outcome JSON", ErrInvalid)
		}
		task.Outcome = tr.Outcome
		if tr.Outcome.Status != "completed" {
			for _, child := range ownedLive(tx.state, task.ID) {
				markAbortTree(tx.state, child)
			}
		}
		if len(ownedLive(tx.state, task.ID)) > 0 {
			task.Status = "completing"
			task.WaitOn = nil
			task.WakeAt = nil
			return nil
		}
		terminalizeTask(tx, task, now)
		return nil
	}
	task.Status = tr.Status
	task.Outcome = nil
	if tr.Checkpoint != nil {
		task.Checkpoint = append(json.RawMessage(nil), tr.Checkpoint...)
	}
	task.WaitOn = append([]TaskID(nil), tr.WaitOn...)
	task.WaitPolicy = tr.WaitPolicy
	task.WakeAt = tr.WakeAt
	return nil
}
