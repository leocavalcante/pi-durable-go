package durable

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type taskInvocation struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	aborting bool // immutable invocation mode, fixed before publication
	runtime  *TaskRuntime
}

type Scheduler struct {
	h                *Harness
	mu               sync.Mutex // Session line -> mu; never call Session/registry under mu
	enabled, closing bool
	invocations      map[TaskID]*taskInvocation
	migrations       map[TaskID]migrationFailure
	wake             chan struct{}
	done             chan struct{}
	// Captured at open; consumed by the first scheduling pass, not inspection.
	abandoned map[TaskID]struct{}
}

var errNoChange = errors.New("durable: no scheduler write")

// commitInternal distinguishes read-only decisions from writes. Bookkeeping
// cannot safely be retried after a real error, even a certified rejection.
func (s *Scheduler) commitInternal(ctx context.Context, fn func(*Tx) error) (bool, error) {
	noChange := false
	err := s.h.commit(ctx, func(tx *Tx) error {
		if fn == nil {
			return nil
		}
		err := fn(tx)
		noChange = errors.Is(err, errNoChange)
		return err
	}, true)
	if noChange {
		return false, nil
	}
	if err == nil {
		return true, nil
	}
	// Session.commit classifies errors and seals fatal failures while still
	// holding the line. Sealed admission and caller cancellation stay nonfatal.
	return false, err
}

func newScheduler(h *Harness) *Scheduler {
	return &Scheduler{h: h, invocations: map[TaskID]*taskInvocation{}, migrations: map[TaskID]migrationFailure{}, wake: make(chan struct{}, 1), done: make(chan struct{})}
}
func (s *Scheduler) state() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateLocked()
}
func (s *Scheduler) stateLocked() string {
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
	// No reservations share the abandonment pass. The complete ordinary tree
	// carries its durable marks before any handler is replayed.
	if len(s.abandoned) > 0 {
		if committed, _ := s.commitInternal(context.Background(), func(tx *Tx) error {
			for id := range s.abandoned {
				markAbortTreeReason(tx.state, id, "restart")
			}
			return nil
		}); committed {
			s.abandoned = nil
			s.kick()
		}
		return
	}
	if cascadeCancellation(state) {
		if committed, _ := s.commitInternal(context.Background(), func(tx *Tx) error {
			if !cascadeCancellation(tx.state) {
				return errNoChange
			}
			return nil
		}); committed {
			s.cancelMarked()
			s.kick()
		}
		return
	}
	ids := sortedTaskIDs(state)
	for _, id := range ids {
		task := state.Tasks[id]
		s.mu.Lock()
		inv, closing := s.invocations[id], s.closing
		if task.Status == "terminal" {
			delete(s.migrations, id)
		}
		s.mu.Unlock()
		if closing || s.h.Err() != nil {
			return
		}
		if task.Status == "terminal" {
			continue
		}
		if inv != nil {
			if task.AbortRequested && !inv.aborting {
				inv.cancel()
			}
			continue
		}
		if task.Status == "completing" {
			_, err := s.commitInternal(context.Background(), func(tx *Tx) error {
				t := tx.state.Tasks[id]
				if t == nil || t.Status != "completing" || len(ownedLive(tx.state, id)) != 0 || s.active(id) != nil {
					return errNoChange
				}
				return s.settleTerminal(tx, t)
			})
			if err != nil {
				return
			}
			continue
		}
		if task.Status == "waiting" && !task.AbortRequested && task.WaitPolicy == "failFast" && waitFailed(state, task) {
			_, err := s.commitInternal(context.Background(), func(tx *Tx) error {
				t := tx.state.Tasks[id]
				if t == nil || t.Status != "waiting" || t.WaitPolicy != "failFast" || !waitFailed(tx.state, t) {
					return errNoChange
				}
				changed := false
				for _, childID := range t.WaitOn {
					child := tx.state.Tasks[childID]
					if child != nil && child.Owner.Task == id && child.Status != "terminal" && (!child.AbortRequested || child.AbortReason != "") {
						markAbortTree(tx.state, childID)
						changed = true
					}
				}
				if !changed {
					return errNoChange
				}
				return nil
			})
			if err != nil {
				return
			}
			s.cancelMarked()
		}
		if err := s.reserve(id); err != nil {
			return
		}
	}
}

func sortedTaskIDs(state *State) []TaskID {
	ids := make([]TaskID, 0, len(state.Tasks))
	for id := range state.Tasks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
func liveWaits(state *State, task *TaskRecord) []TaskID {
	if task.AbortRequested {
		return ownedLive(state, task.ID)
	}
	var live []TaskID
	for _, id := range task.WaitOn {
		if child := state.Tasks[id]; child == nil || child.Status != "terminal" {
			live = append(live, id)
		}
	}
	return live
}
func waitFailed(state *State, task *TaskRecord) bool {
	for _, id := range task.WaitOn {
		child := state.Tasks[id]
		if child != nil && child.Status == "terminal" && (child.Outcome == nil || child.Outcome.Status != OutcomeCompleted) {
			return true
		}
	}
	return false
}
func (s *Scheduler) active(id TaskID) *taskInvocation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.invocations[id]
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

func (s *Scheduler) reserve(id TaskID) error {
	var inv *taskInvocation
	var resolved resolvedTask
	var snapshot *RegistrySnapshot
	var reports []Report
	committed, err := s.commitInternal(context.Background(), func(tx *Tx) error {
		t := tx.state.Tasks[id]
		if t == nil || t.Status == "terminal" || t.Status == "completing" {
			return errNoChange
		}
		s.mu.Lock()
		unavailable := s.closing || s.invocations[id] != nil
		s.mu.Unlock()
		if unavailable || s.h.Err() != nil {
			return errNoChange
		}
		if t.AbortRequested {
			if len(ownedLive(tx.state, id)) != 0 {
				return errNoChange
			}
		} else if (t.Status != "pending" && t.Status != "waiting") || len(liveWaits(tx.state, t)) != 0 || t.WakeAt != nil && s.h.now().Before(*t.WakeAt) || belowCancellationIntent(tx.state, t) {
			return errNoChange
		}
		snapshot = s.h.registry.Snapshot()
		var migrated *MigratedTask
		var blocked string
		resolved, migrated, blocked = s.resolveForStart(t, snapshot, &reports)
		if blocked != "" {
			if t.AbortRequested && t.AbortReason != "restart" {
				t.Outcome = &Outcome{Status: OutcomeOrphaned, Reason: blocked}
				return s.settleTerminal(tx, t)
			}
			return errNoChange
		}
		if migrated != nil {
			t.Version = resolved.Definition.Version
			t.Input, t.Checkpoint = migrated.Input, migrated.Checkpoint
		}
		t.Status = "running"
		t.WaitOn, t.WaitPolicy, t.WakeAt = nil, "", nil
		if t.StartedAt == nil {
			now := s.h.now()
			t.StartedAt = &now
		}
		ctx, cancel := context.WithCancel(context.Background())
		inv = &taskInvocation{ctx: ctx, cancel: cancel, done: make(chan struct{}), aborting: t.AbortRequested}
		r := &TaskRuntime{h: s.h, ID: id, invocation: inv, aborting: inv.aborting, extension: resolved.Extension, kind: t.Kind, conversation: t.Conversation}
		r.snapshot.Store(snapshot)
		inv.runtime = r
		s.mu.Lock()
		s.invocations[id] = inv
		s.mu.Unlock()
		return nil
	})
	s.enqueueReports(reports)
	if inv != nil {
		if committed {
			go s.runInvocation(id, inv, resolved, snapshot)
		} else {
			s.finishInvocation(id, inv)
		}
	}
	return err
}
func (s *Scheduler) finishInvocation(id TaskID, inv *taskInvocation) {
	inv.runtime.ended.Store(true)
	inv.cancel()
	s.mu.Lock()
	delete(s.invocations, id)
	close(inv.done)
	s.mu.Unlock()
	s.kick()
}
func (s *Scheduler) enqueueReports(reports []Report) {
	for _, report := range reports {
		s.h.report(report)
	}
}

func (h *Harness) reconcile(ctx context.Context) error {
	abandoned := make(map[TaskID]struct{})
	_, err := h.scheduler.commitInternal(ctx, func(tx *Tx) error {
		for _, t := range tx.state.Tasks {
			if t.AbandonOnRestart && t.Status != "terminal" {
				abandoned[t.ID] = struct{}{}
			}
			if t.Status == "running" {
				t.Status = "pending"
			}
		}
		return nil
	})
	if err == nil {
		h.scheduler.abandoned = abandoned
	}
	return err
}
