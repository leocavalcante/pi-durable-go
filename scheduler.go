package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type taskInvocation struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	aborting bool // guarded by Scheduler.mu
}
type Scheduler struct {
	h                *Harness
	mu               sync.Mutex
	enabled, closing bool
	invocations      map[TaskID]*taskInvocation
	wake             chan struct{}
	done             chan struct{}
}
type TaskInspection struct {
	Record  TaskRecord
	Ready   bool
	Blocked string
}
type HarnessInspection struct {
	Scheduling string
	Tasks      []TaskInspection
}

func newScheduler(h *Harness) *Scheduler {
	return &Scheduler{h: h, invocations: map[TaskID]*taskInvocation{}, wake: make(chan struct{}, 1), done: make(chan struct{})}
}
func (s *Scheduler) state() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return "closing"
	}
	if s.enabled {
		return "running"
	}
	return "paused"
}
func (s *Scheduler) resume() {
	s.mu.Lock()
	if s.enabled || s.closing {
		s.mu.Unlock()
		return
	}
	s.enabled = true
	s.mu.Unlock()
	go s.loop()
	s.kick()
}
func (s *Scheduler) close() {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.closing = true
	for _, inv := range s.invocations {
		inv.cancel()
	}
	enabled := s.enabled
	s.mu.Unlock()
	if !enabled {
		close(s.done)
	}
	s.kick()
}
func (s *Scheduler) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *Scheduler) loop() {
	defer close(s.done)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.dispatch()
		s.mu.Lock()
		done := s.closing && len(s.invocations) == 0
		s.mu.Unlock()
		if done {
			return
		}
		select {
		case <-s.wake:
		case <-ticker.C:
		}
	}
}
func ownedLive(state *State, id TaskID) []TaskID {
	var live []TaskID
	for taskID, t := range state.Tasks {
		if t.Status == "terminal" {
			continue
		}
		if t.Owner.Task == id {
			live = append(live, taskID)
			continue
		}
		c := state.Conversations[t.Conversation]
		if c != nil && c.Owner.Task == id && t.Owner.Task == 0 && !t.Background {
			live = append(live, taskID)
		}
	}
	return live
}
func markAbortTree(state *State, id TaskID) {
	seen := map[TaskID]bool{}
	var visit func(TaskID)
	visit = func(taskID TaskID) {
		if seen[taskID] {
			return
		}
		seen[taskID] = true
		t := state.Tasks[taskID]
		if t == nil || t.Status == "terminal" {
			return
		}
		t.AbortRequested = true
		for _, child := range ownedLive(state, taskID) {
			visit(child)
		}
	}
	visit(id)
}
func terminalizeTask(tx *Tx, t *TaskRecord, now time.Time) {
	t.Status = "terminal"
	t.EndedAt = &now
	t.Checkpoint = nil
	t.WaitOn = nil
	t.WaitPolicy = ""
	t.WakeAt = nil
	t.Memos = nil
	for key, doc := range tx.state.Documents {
		if doc.Scope == "task" && doc.ScopeID == uint64(t.ID) {
			delete(tx.state.Documents, key)
		}
	}
}
func (s *Scheduler) dispatch() {
	state, err := s.h.Snapshot(context.Background())
	if err != nil {
		return
	}
	var ids []TaskID
	for id := range state.Tasks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		task := state.Tasks[id]
		if task.Status == "terminal" {
			continue
		}
		s.mu.Lock()
		inv := s.invocations[id]
		aborting := inv != nil && inv.aborting
		closing := s.closing
		s.mu.Unlock()
		if closing {
			return
		}
		if task.AbortRequested {
			// Ensure descendants are marked before invoking any ancestor abort handler.
			descendants := ownedLive(state, id)
			needsMark := false
			for _, child := range descendants {
				if !state.Tasks[child].AbortRequested {
					needsMark = true
				}
			}
			if needsMark {
				_ = s.h.Commit(context.Background(), func(tx *Tx) error { markAbortTree(tx.state, id); return nil })
				s.cancelMarked()
				continue
			}
			if inv != nil {
				if !aborting {
					inv.cancel()
				}
				continue
			}
			if len(descendants) > 0 {
				continue
			}
			if task.Status != "pending" && task.Status != "completing" {
				_ = s.h.Commit(context.Background(), func(tx *Tx) error {
					t, err := tx.Task(id)
					if err != nil {
						return err
					}
					if t.Status != "terminal" {
						t.Status = "pending"
						t.WaitOn = nil
						t.WakeAt = nil
					}
					return nil
				})
				continue
			}
		} else if inv != nil {
			continue
		}
		if task.Status == "completing" {
			if len(ownedLive(state, id)) == 0 {
				_ = s.h.Commit(context.Background(), func(tx *Tx) error {
					t, err := tx.Task(id)
					if err != nil {
						return err
					}
					if t.Status == "completing" && len(ownedLive(tx.state, id)) == 0 {
						terminalizeTask(tx, t, s.h.now())
					}
					return nil
				})
			}
			continue
		}
		if task.Status == "waiting" {
			all, failed := true, false
			for _, childID := range task.WaitOn {
				child := state.Tasks[childID]
				if child == nil || child.Status != "terminal" {
					all = false
					continue
				}
				if child.Outcome == nil || child.Outcome.Status != "completed" {
					failed = true
				}
			}
			if failed && task.WaitPolicy == "failFast" {
				needsMark := false
				for _, childID := range task.WaitOn {
					child := state.Tasks[childID]
					if child != nil && child.Owner.Task == id && child.Status != "terminal" && !child.AbortRequested {
						needsMark = true
					}
				}
				if needsMark {
					_ = s.h.Commit(context.Background(), func(tx *Tx) error {
						for _, childID := range task.WaitOn {
							child := tx.state.Tasks[childID]
							if child != nil && child.Owner.Task == id {
								markAbortTree(tx.state, childID)
							}
						}
						return nil
					})
					s.cancelMarked()
				}
			}
			if !all || task.WakeAt != nil && s.h.now().Before(*task.WakeAt) {
				continue
			}
			_ = s.h.Commit(context.Background(), func(tx *Tx) error {
				t, err := tx.Task(id)
				if err != nil {
					return err
				}
				if t.Status == "waiting" {
					t.Status = "pending"
					t.WaitOn = nil
					t.WakeAt = nil
				}
				return nil
			})
			continue
		}
		if task.Status != "pending" {
			continue
		}
		if !task.AbortRequested && task.WakeAt != nil && s.h.now().Before(*task.WakeAt) {
			continue
		}
		def, ok := s.h.registry.resolve(task.Kind)
		if !ok || task.Version > def.Version {
			continue
		}
		if task.Version < def.Version {
			if def.Migrate == nil {
				continue
			}
			checkpoint, err := def.Migrate(task.Version, append(json.RawMessage(nil), task.Checkpoint...))
			if err != nil {
				continue
			}
			if !json.Valid(checkpoint) {
				continue
			}
			_ = s.h.Commit(context.Background(), func(tx *Tx) error {
				t, err := tx.Task(id)
				if err != nil {
					return err
				}
				if t.Version == task.Version && t.Status == "pending" {
					t.Version = def.Version
					t.Checkpoint = checkpoint
				}
				return nil
			})
			continue
		}
		s.start(*task, def)
	}
}
func (s *Scheduler) cancelMarked() {
	state, err := s.h.Snapshot(context.Background())
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, inv := range s.invocations {
		if t := state.Tasks[id]; t != nil && t.AbortRequested && !inv.aborting {
			inv.cancel()
		}
	}
}
func (s *Scheduler) start(task TaskRecord, def TaskDefinition) {
	ctx, cancel := context.WithCancel(context.Background())
	inv := &taskInvocation{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	if s.closing || s.invocations[task.ID] != nil {
		s.mu.Unlock()
		cancel()
		return
	}
	s.invocations[task.ID] = inv
	s.mu.Unlock()
	go func() {
		defer func() {
			if ctx.Err() != nil {
				s.mu.Lock()
				closing := s.closing
				s.mu.Unlock()
				if !closing {
					_ = s.h.Commit(context.Background(), func(tx *Tx) error {
						t, e := tx.Task(task.ID)
						if e != nil {
							return e
						}
						if t.Status == "running" {
							t.Status = "pending"
						}
						return nil
					})
				}
			}
			cancel()
			s.mu.Lock()
			delete(s.invocations, task.ID)
			close(inv.done)
			s.mu.Unlock()
			s.kick()
		}()
		err := s.h.Commit(context.Background(), func(tx *Tx) error {
			t, err := tx.Task(task.ID)
			if err != nil {
				return err
			}
			if t.Status != "pending" {
				return ErrConflict
			}
			if t.AbortRequested && len(ownedLive(tx.state, t.ID)) > 0 {
				return ErrConflict
			}
			t.Status = "running"
			t.WakeAt = nil
			if t.StartedAt == nil {
				now := s.h.now()
				t.StartedAt = &now
			}
			return nil
		})
		if err != nil {
			return
		}
		current, err := s.h.Task(context.Background(), task.ID)
		if err != nil {
			return
		}
		r := &TaskRuntime{h: s.h, ID: task.ID, invocation: inv}
		defer r.ended.Store(true)
		s.mu.Lock()
		inv.aborting = current.AbortRequested
		s.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
		if current.AbortRequested {
			if def.Abort != nil {
				err = def.Abort(ctx, r, current)
			} else {
				err = r.Complete(ctx, Outcome{Status: "aborted"})
			}
		} else if def.Run == nil {
			err = fmt.Errorf("task has no Run handler")
		} else {
			err = def.Run(ctx, r, current)
		}
		if ctx.Err() != nil {
			// Close leaves work pending, not durably cancelled. The next host can reopen.
			// An explicit abort needs a fresh invocation context for compensation.
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if !closing {
				_ = s.h.Commit(context.Background(), func(tx *Tx) error {
					t, e := tx.Task(task.ID)
					if e != nil {
						return e
					}
					if t.Status == "running" {
						t.Status = "pending"
					}
					return nil
				})
			}
			return
		}
		if err != nil {
			_ = r.Complete(context.Background(), Outcome{Status: "failed", Error: err.Error()})
			return
		}
		latest, e := s.h.Task(context.Background(), task.ID)
		if e == nil && latest.Status == "running" {
			_ = r.Complete(context.Background(), Outcome{Status: "failed", Error: "task handler returned without a durable transition"})
		}
	}()
}
func (s *Scheduler) abort(ctx context.Context, id TaskID) error {
	err := s.h.Commit(ctx, func(tx *Tx) error {
		if _, err := tx.Task(id); err != nil {
			return err
		}
		markAbortTree(tx.state, id)
		return nil
	})
	if err != nil {
		return err
	}
	s.cancelMarked()
	s.resume()
	s.kick()
	return nil
}
func (s *Scheduler) wait(ctx context.Context, id TaskID) (TaskRecord, error) {
	states, stop, err := s.h.session.Watch(ctx)
	if err != nil {
		return TaskRecord{}, err
	}
	defer stop()
	s.resume()
	for {
		select {
		case <-ctx.Done():
			return TaskRecord{}, ctx.Err()
		case state, ok := <-states:
			if !ok {
				if ctx.Err() != nil {
					return TaskRecord{}, ctx.Err()
				}
				return TaskRecord{}, ErrClosed
			}
			t := state.Tasks[id]
			if t == nil {
				return TaskRecord{}, ErrNotFound
			}
			if t.Status == "terminal" {
				return *t, nil
			}
		}
	}
}
func (h *Harness) reconcile(ctx context.Context) error {
	return h.Commit(ctx, func(tx *Tx) error {
		for _, t := range tx.state.Tasks {
			if t.Status == "running" {
				t.Status = "pending"
			}
		}
		return nil
	})
}
func (r *Registry) resolve(kind string) (TaskDefinition, bool) {
	if kind == "pi.generation" {
		return generationTaskDefinition(), true
	}
	if kind == "pi.tool" {
		return toolTaskDefinition(), true
	}
	return r.Snapshot().Task(kind)
}
func contextCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
func encodeTaskCheckpoint(value any) json.RawMessage           { data, _ := json.Marshal(value); return data }
func requireTaskRecord(tx *Tx, id TaskID) (*TaskRecord, error) { return tx.Task(id) }
