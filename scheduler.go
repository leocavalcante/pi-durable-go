package durable

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type taskInvocation struct {
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	aborting bool // immutable invocation mode, fixed before publication
	runtime  *TaskRuntime
	// Pacing timers of TaskRuntime.SetTimer; the invocation's end disarms them.
	timersMu sync.Mutex
	timers   map[*int]func()
	ended    bool
	// span is the running phase's telemetry span, for report events.
	span atomic.Pointer[span]
}

// disarmTimers stops every pacing timer; later SetTimer calls arm nothing.
func (inv *taskInvocation) disarmTimers() {
	inv.timersMu.Lock()
	timers := inv.timers
	inv.timers, inv.ended = nil, true
	inv.timersMu.Unlock()
	for _, stop := range timers {
		stop()
	}
}

type Scheduler struct {
	h                *Harness
	mu               sync.Mutex // Session line -> mu; never call Session/registry under mu
	enabled, closing bool
	invocations      map[TaskID]*taskInvocation
	stopped          map[TaskID]struct{} // invocations ended after the Session sealed
	migrations       map[TaskID]migrationFailure
	wake             chan struct{}
	done             chan struct{}
	// Captured at open; consumed by the first scheduling pass, not inspection.
	abandoned map[TaskID]struct{}
	// The waker recomputes Wake after changes: it arms the timer for the
	// earliest sleep, or offers the value to HarnessOptions.OnWake.
	wakeDirty  chan struct{}
	wakerDone  chan struct{}
	timer      *wakeTimer // guarded by mu
	lastWake   *Wake      // last value OnWake accepted; waker goroutine only
	wakeClosed chan struct{}
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
	return &Scheduler{
		h:           h,
		invocations: map[TaskID]*taskInvocation{},
		stopped:     map[TaskID]struct{}{},
		migrations:  map[TaskID]migrationFailure{},
		wake:        make(chan struct{}, 1),
		done:        make(chan struct{}),
		wakeDirty:   make(chan struct{}, 1),
		wakerDone:   make(chan struct{}),
		wakeClosed:  make(chan struct{}),
	}
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

// resume enables scheduling and always starts a pass, so a host woken by its
// own alarm (HarnessOptions.OnWake) resumes due sleeps by calling Resume.
func (s *Scheduler) resume() {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	start := !s.enabled
	s.enabled = true
	s.mu.Unlock()
	if start {
		go s.loop()
	}
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
	if s.timer != nil {
		s.timer.stop()
		s.timer = nil
	}
	s.mu.Unlock()
	close(s.wakeClosed)
	if !enabled {
		close(s.done)
	}
	s.kick()
}

// Called only after done, when every invocation has joined. A clean close is
// not a crash: stopped tasks still running are released without counting them.
// The Session fails and reports any write error; Close returns only the backend
// close error, matching upstream join() swallowing its write-back error.
func (s *Scheduler) writeBackStopped() {
	s.mu.Lock()
	empty := len(s.stopped) == 0
	s.mu.Unlock()
	if empty {
		return
	}
	_ = s.h.session.writeAtClose(func(tx *Tx) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		changed := false
		for id := range s.stopped {
			if task := tx.state.mutTask(id); task != nil && task.Status == "running" {
				task.Status = "pending"
				changed = true
			}
		}
		if !changed {
			return errNoChange
		}
		return nil
	})
}

// kick starts a scheduling pass and a wake recomputation. It never blocks:
// callers include the commit line and timer callbacks.
func (s *Scheduler) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
	select {
	case s.wakeDirty <- struct{}{}:
	default:
	}
}

// loop runs passes on kicks only: every commit, invocation end, registry
// change, Resume, and the wake timer kick it. A pass makes every sleep whose
// time has come runnable, so a pass started by any change may be the first
// after a deadline.
func (s *Scheduler) loop() {
	defer close(s.done)
	for {
		s.dispatch()
		s.mu.Lock()
		done := s.closing && len(s.invocations) == 0
		s.mu.Unlock()
		if done {
			return
		}
		<-s.wake
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
	t.Restarts = 0
	for key, doc := range tx.state.Documents {
		if doc.Scope == "task" && doc.ScopeID == uint64(t.ID) {
			delete(tx.state.Documents, key)
		}
	}
}
func (s *Scheduler) dispatch() {
	state, err := s.h.read(context.Background())
	if err != nil {
		return
	}
	// No reservations share the abandonment pass. The complete ordinary tree
	// carries its durable marks before any handler is replayed.
	s.mu.Lock()
	abandoned := s.abandoned
	s.mu.Unlock()
	if len(abandoned) > 0 {
		if committed, _ := s.commitInternal(context.Background(), func(tx *Tx) error {
			for id := range abandoned {
				markAbortTreeReason(tx.state, id, "restart")
			}
			return nil
		}); committed {
			s.mu.Lock()
			s.abandoned = nil
			s.mu.Unlock()
			s.kick()
		}
		return
	}
	if cascadePending(state) {
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
				t := tx.state.mutTask(id)
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
	state, err := s.h.read(context.Background())
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
	var cutOff *Report
	committed, err := s.commitInternal(context.Background(), func(tx *Tx) error {
		cutOff = nil
		t := tx.state.mutTask(id)
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
		} else if (t.Status != "pending" && t.Status != "waiting" && t.Status != "sleeping") || len(liveWaits(tx.state, t)) != 0 || t.WakeAt != nil && s.h.now().Before(*t.WakeAt) || belowCancellationIntent(tx.state, t) {
			return errNoChange
		}
		// Work cut off restartLimit times in a row would run forever: an abort
		// handler, or a phase of a kind that does not end itself at the limit,
		// ends the task faulted instead. As upstream, this precedes resolution,
		// so it also ends blocked and would-be orphaned tasks.
		if t.Restarts > 0 && (t.AbortRequested || !limitsRestarts(t.Kind)) && cutOffTooOften(t.Restarts, s.h.restartLimit()) {
			message := fmt.Sprintf("Task %d (%s) was cut off %d times without progress", id, t.Kind, t.Restarts)
			if t.AbortRequested {
				message = fmt.Sprintf("Abort handler of task %d (%s) was cut off %d times", id, t.Kind, t.Restarts)
			}
			cutOff = &Report{Kind: ReportTaskFaulted, Err: errors.New(message), Reason: reasonRestartLimit,
				Extension: s.h.resolve(t.Kind, s.h.registry.Snapshot()).Extension,
				Task:      &ReportTask{ID: id, Kind: t.Kind, Conversation: t.Conversation}}
			return s.terminate(tx, t, Outcome{Status: OutcomeFaulted, Error: message, ErrorDetail: restartLimitDetail})
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
	// A fault is reported only once its write is durable.
	if committed && cutOff != nil {
		s.h.report(*cutOff)
	}
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
	inv.disarmTimers()
	s.mu.Lock()
	if s.h.Err() != nil {
		// Capture here, not just in close(): an invocation may finish between
		// admission sealing and the scheduler's cancellation/join pass.
		s.stopped[id] = struct{}{}
	}
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
		for id, t := range tx.state.Tasks {
			if t.AbandonOnRestart && t.Status != "terminal" {
				abandoned[t.ID] = struct{}{}
			}
			if t.Status == "running" {
				t = tx.state.mutTask(id)
				t.Status = "pending"
				t.Restarts++
			}
		}
		return nil
	})
	if err == nil {
		h.scheduler.mu.Lock()
		h.scheduler.abandoned = abandoned
		h.scheduler.mu.Unlock()
	}
	return err
}
