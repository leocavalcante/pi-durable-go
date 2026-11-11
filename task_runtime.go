package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// TaskRuntime belongs to one invocation, including its continuing phases. Join
// phase-owned goroutines before returning if their writes must count before that
// phase's step. An ending step revokes the runtime on the Session line.
type TaskRuntime struct {
	h            *Harness
	ID           TaskID
	invocation   *taskInvocation
	aborting     bool // immutable invocation mode
	ended        atomic.Bool
	snapshot     atomic.Pointer[RegistrySnapshot]
	extension    string // definition installer captured at reservation
	kind         string
	conversation ConversationID
}

func (r *TaskRuntime) Harness() *Harness { return r.h }

// Registry is the immutable snapshot for this phase. A new phase refreshes it;
// replacing an extension cannot change a phase that is already running.
func (r *TaskRuntime) Registry() *RegistrySnapshot {
	if snapshot := r.snapshot.Load(); snapshot != nil {
		return snapshot
	}
	return r.h.registry.Snapshot()
}

// Report enqueues a best-effort diagnostic without waiting for host code. It
// is safe in a commit callback, but makes no claim that the writes committed.
func (r *TaskRuntime) Report(err error) {
	if err == nil || r.check() != nil {
		return
	}
	r.h.report(r.taskReport(ReportTaskError, err, ""))
}

func (r *TaskRuntime) taskReport(kind ReportKind, err error, reason string) Report {
	return Report{Kind: kind, Err: err, Reason: reason, Extension: r.extension,
		Task: &ReportTask{ID: r.ID, Kind: r.kind, Conversation: r.conversation}}
}

// Outcomes reads terminal receipts in input order. Records and JSON are detached.
// Call outside a transaction; a live target is an invalid outcome read.
func (r *TaskRuntime) Outcomes(ctx context.Context, ids []TaskID) ([]Outcome, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	state, err := r.h.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.check(); err != nil {
		return nil, err
	}
	out := make([]Outcome, 0, len(ids))
	for _, id := range ids {
		task := state.Tasks[id]
		if task == nil {
			return nil, ErrNotFound
		}
		if task.Status != "terminal" || task.Outcome == nil {
			return nil, fmt.Errorf("%w: task %d is not terminal", ErrInvalid, id)
		}
		out = append(out, *task.Outcome)
	}
	return out, nil
}

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
	var interrupted *Report
	err := r.h.Commit(ctx, func(tx *Tx) error {
		interrupted = nil
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
		if !r.aborting && (current.AbortRequested || belowCancellationIntent(tx.state, current)) {
			return context.Canceled
		}
		// Capture before change: a callback can mutate the current record's
		// slices and deadline through Tx.Task. Memos and documents are not
		// task state and must not reset the crash count.
		previous := *current
		previous.Checkpoint = append(json.RawMessage(nil), current.Checkpoint...)
		previous.WaitOn = append([]TaskID(nil), current.WaitOn...)
		if current.WakeAt != nil {
			wakeAt := *current.WakeAt
			previous.WakeAt = &wakeAt
		}
		transition, err := change(tx)
		if err != nil {
			return err
		}
		if err := r.check(); err != nil {
			return err
		}
		if r.aborting && transition.Status != "running" && transition.Status != "terminal" {
			if transition.Status == "sleeping" || transition.WakeAt != nil {
				return fmt.Errorf("%w: Abort handler of task %d cannot sleep", ErrInvalid, r.ID)
			}
			if transition.Status == "waiting" {
				return fmt.Errorf("%w: Abort handler of task %d cannot wait", ErrInvalid, r.ID)
			}
			return fmt.Errorf("%w: abort handlers cannot yield or wait", ErrInvalid)
		}
		if err := applyTransition(tx, current, transition, r.h.now()); err != nil {
			return err
		}
		if !sameTaskState(&previous, current) {
			current.Restarts = 0
		}
		// A task that ends for crashes says so, so a host keeps a crash-loop
		// signal: task_interrupted with the count from before this commit. A
		// held (completing) outcome is reported when it is committed.
		if transition.Status == "terminal" {
			if reason := interruptionOf(transition.Outcome); reason != "" {
				report := r.taskReport(ReportTaskInterrupted, errors.New(transition.Outcome.Error), reason)
				report.Restarts = previous.Restarts
				interrupted = &report
			}
		}
		return nil
	})
	if err == nil && interrupted != nil {
		r.h.report(*interrupted)
	}
	return err
}

func sameTaskState(a, b *TaskRecord) bool {
	return a.Status == b.Status && jsonEqual(a.Checkpoint, b.Checkpoint) &&
		slices.Equal(a.WaitOn, b.WaitOn) && a.WaitPolicy == b.WaitPolicy &&
		(a.WakeAt == nil && b.WakeAt == nil || a.WakeAt != nil && b.WakeAt != nil && a.WakeAt.Equal(*b.WakeAt))
}

// SleepUntil commits a sleep: the task holds no invocation until the Harness
// clock reaches until, then resumes at checkpoint (nil keeps the current one).
// It returns once the sleep is stored; the phase should return right after.
// An abort mark ends a sleep at once. Committing a "sleeping" transition with
// WakeAt is the same sleep, for one that must commit with other writes.
func (r *TaskRuntime) SleepUntil(ctx context.Context, until time.Time, checkpoint json.RawMessage) error {
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		if checkpoint == nil {
			current, err := tx.Task(r.ID)
			if err != nil {
				return TaskTransition{}, err
			}
			checkpoint = current.Checkpoint
		}
		return TaskTransition{Status: "sleeping", Checkpoint: checkpoint, WakeAt: &until}, nil
	})
}

// Now reads the Harness clock, which sleeping tasks store deadlines on.
func (r *TaskRuntime) Now() time.Time { return r.h.now() }

// SetTimer calls callback once after about delay on the Harness's timer
// (HarnessOptions.SetTimer), for pacing work inside a phase such as throttled
// progress commits. It is not durable: the invocation's end disarms it, and
// after the end nothing is armed. It returns a function that disarms it. Wait
// for time between phases with SleepUntil instead.
func (r *TaskRuntime) SetTimer(callback func(), delay time.Duration) (stop func()) {
	inv := r.invocation
	if inv == nil || r.check() != nil || callback == nil {
		return func() {}
	}
	key := new(int)
	var once sync.Once
	var disarm func()
	fire := func() {
		inv.timersMu.Lock()
		_, armed := inv.timers[key]
		delete(inv.timers, key)
		inv.timersMu.Unlock()
		if armed {
			callback()
		}
	}
	inv.timersMu.Lock()
	if inv.ended {
		inv.timersMu.Unlock()
		return func() {}
	}
	if inv.timers == nil {
		inv.timers = map[*int]func(){}
	}
	inv.timers[key] = func() {}
	inv.timersMu.Unlock()
	cancel := r.h.setTimer(fire, delay)
	disarm = func() {
		once.Do(func() {
			inv.timersMu.Lock()
			delete(inv.timers, key)
			inv.timersMu.Unlock()
			cancel()
		})
	}
	inv.timersMu.Lock()
	if _, armed := inv.timers[key]; armed {
		inv.timers[key] = disarm
		inv.timersMu.Unlock()
	} else {
		inv.timersMu.Unlock()
		if inv.ended {
			cancel()
		}
	}
	return disarm
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
		if err := r.check(); err != nil {
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

// OwnedTasks returns detached, live, directly task-owned records in ID order,
// including children created by an earlier invocation. Tasks in owned
// conversations are deliberately excluded. Call outside a Commit callback.
func (r *TaskRuntime) OwnedTasks(ctx context.Context) ([]TaskRecord, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	state, err := r.h.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.check(); err != nil {
		return nil, err
	}
	owned := make([]TaskRecord, 0)
	for _, task := range state.Tasks {
		if task.Owner.Task == r.ID && task.Status != "terminal" {
			owned = append(owned, *task)
		}
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i].ID < owned[j].ID })
	return owned, nil
}

// AbortOwned marks a directly owned child and waits for its terminal receipt.
// A restart mark is retained so an abandonment's abort handler sees its reason.
// Cancelling this wait does not undo the durable mark. The invocation ending
// cancels the wait as well. Call outside a Commit callback.
func (r *TaskRuntime) AbortOwned(ctx context.Context, id TaskID) error {
	if err := r.check(); err != nil {
		return err
	}
	bound, cancel := context.WithCancel(ctx)
	defer cancel()
	if r.invocation != nil {
		stop := context.AfterFunc(r.invocation.ctx, cancel)
		defer stop()
	}
	var reports []Report
	var childInvocation *taskInvocation
	var decisionErr error
	_, err := r.h.scheduler.commitInternal(bound, func(tx *Tx) error {
		reject := func(err error) error { decisionErr = err; return errNoChange }
		if err := r.check(); err != nil {
			return reject(err)
		}
		current, err := tx.Task(r.ID)
		if err != nil {
			return reject(err)
		}
		if current.Status != "running" {
			return reject(ErrConflict)
		}
		if !r.aborting && (current.AbortRequested || belowCancellationIntent(tx.state, current)) {
			return reject(context.Canceled)
		}
		child, err := tx.Task(id)
		if err != nil {
			return reject(err)
		}
		if child.Owner.Task != r.ID {
			return reject(fmt.Errorf("%w: task %d is not owned by task %d", ErrInvalid, id, r.ID))
		}
		if child.Status == "terminal" {
			return errNoChange
		}
		_, childInvocation, err = r.h.scheduler.markAbort(tx, child, true, &reports)
		return err
	})
	r.h.scheduler.enqueueReports(reports)
	if err != nil {
		return err
	}
	if decisionErr != nil {
		return decisionErr
	}
	if childInvocation != nil && !childInvocation.aborting {
		childInvocation.cancel()
	}
	r.h.scheduler.kick()
	_, err = r.h.scheduler.wait(bound, id)
	return err
}

func validateTaskOutcome(out Outcome) error {
	if len(out.Result) > 0 && !json.Valid(out.Result) || len(out.ErrorDetail) > 0 && !json.Valid(out.ErrorDetail) {
		return fmt.Errorf("%w: outcome JSON", ErrInvalid)
	}
	switch out.Status {
	case OutcomeCompleted:
		if out.Error != "" || len(out.ErrorDetail) > 0 || out.Reason != "" {
			return fmt.Errorf("%w: completed outcome diagnostics", ErrInvalid)
		}
	case OutcomeFailed:
		if out.Error == "" || out.Reason != "" {
			return fmt.Errorf("%w: failed outcome requires an error and no reason", ErrInvalid)
		}
	case OutcomeAborted:
		if out.Error != "" || len(out.ErrorDetail) > 0 {
			return fmt.Errorf("%w: aborted outcome uses reason, not error", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: outcome status %q is not task-writable", ErrInvalid, out.Status)
	}
	return nil
}

func applyTransition(tx *Tx, task *TaskRecord, tr TaskTransition, now time.Time) error {
	switch tr.Status {
	case "pending", "running", "waiting", "sleeping", "terminal":
	default:
		return fmt.Errorf("%w: task transition %q", ErrInvalid, tr.Status)
	}
	if tr.Checkpoint != nil && !json.Valid(tr.Checkpoint) {
		return fmt.Errorf("%w: task checkpoint JSON", ErrInvalid)
	}
	// Go previously slept as pending or waiting with a deadline and nothing
	// to wait on; that form is a sleep. Upstream waits have no deadline.
	if tr.WakeAt != nil && (tr.Status == "pending" || tr.Status == "waiting" && len(tr.WaitOn) == 0) {
		tr.Status = "sleeping"
	}
	switch tr.Status {
	case "sleeping":
		if tr.WakeAt == nil || tr.WakeAt.IsZero() {
			return fmt.Errorf("%w: Task %d cannot sleep without a time to wake", ErrInvalid, task.ID)
		}
		tr.WaitOn, tr.WaitPolicy = nil, ""
	case "waiting":
		if tr.WakeAt != nil {
			return fmt.Errorf("%w: Task %d cannot wait with a deadline; sleep or wait", ErrInvalid, task.ID)
		}
	case "running":
		tr.WakeAt = nil
	}
	if tr.Status == "waiting" {
		if tr.WaitPolicy != "" && tr.WaitPolicy != "allSettled" && tr.WaitPolicy != "failFast" {
			return fmt.Errorf("%w: wait policy", ErrInvalid)
		}
		if err := checkWait(tx.state, task.ID, tr.WaitOn, tr.WaitPolicy); err != nil {
			return err
		}
	}
	if tr.Status == "terminal" {
		if tr.Outcome == nil {
			return fmt.Errorf("%w: terminal task requires outcome", ErrInvalid)
		}
		if err := validateTaskOutcome(*tr.Outcome); err != nil {
			return err
		}
		task.Outcome = tr.Outcome
		task.Memos = nil
		task.Restarts = 0
		if tr.Outcome.Status != "completed" {
			markOwned(tx.state, task.ID, "")
		}
		if len(ownedLive(tx.state, task.ID)) > 0 {
			task.Status = "completing"
			task.WaitOn = nil
			task.WaitPolicy = ""
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
