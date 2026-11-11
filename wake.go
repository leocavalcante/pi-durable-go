package durable

import (
	"context"
	"runtime/debug"
	"sync"
	"time"
)

// Wake says when a Session next needs a running process (Harness.Wake).
// Active: some task can run now or is running, or scheduler bookkeeping such
// as open's recovery has work, so a process that dies must be woken again
// soon. At: the earliest sleep not yet due, if any. Neither: nothing needs a
// process until new work arrives. Tasks blocked on a missing or unmigratable
// definition, completing tasks, and waits on live tasks do not count; the
// work they wait for does.
type Wake struct {
	Active bool
	At     *time.Time
}

func (w Wake) equal(other Wake) bool {
	if w.Active != other.Active || (w.At == nil) != (other.At == nil) {
		return false
	}
	return w.At == nil || w.At.Equal(*other.At)
}

// Wake answers when this Session next needs a process, also while
// scheduling is paused. A host that wakes the process itself, such as with a
// platform alarm, arms from it and calls Resume when woken; see
// HarnessOptions.OnWake.
func (h *Harness) Wake() Wake {
	if h == nil || h.Err() != nil || h.options.ReadOnly {
		return Wake{}
	}
	state, err := h.read(context.Background())
	if err != nil {
		return Wake{}
	}
	return h.scheduler.wakeFor(state)
}

// wakeFor derives the wake from a committed State, which it only reads.
func (s *Scheduler) wakeFor(state *State) Wake {
	now := s.h.now()
	s.mu.Lock()
	active := len(s.invocations) > 0 || len(s.abandoned) > 0
	failures := make(map[TaskID]migrationFailure, len(s.migrations))
	for id, failure := range s.migrations {
		failures[id] = failure
	}
	s.mu.Unlock()
	var at *time.Time
	var snapshot *RegistrySnapshot
	for _, id := range sortedTaskIDs(state) {
		task := state.Tasks[id]
		switch task.Status {
		case "terminal":
			continue
		case "completing":
			// Finalization is scheduler work once owned work drained.
			if len(ownedLive(state, id)) == 0 {
				active = true
			}
			continue
		}
		if task.AbortRequested {
			if len(ownedLive(state, id)) > 0 {
				continue
			}
		} else {
			if len(liveWaits(state, task)) > 0 {
				continue
			}
			if task.WakeAt != nil && now.Before(*task.WakeAt) {
				if at == nil || task.WakeAt.Before(*at) {
					wake := *task.WakeAt
					at = &wake
				}
				continue
			}
			if belowCancellationIntent(state, task) {
				active = true // its cascade mark is pending
				continue
			}
		}
		if active {
			continue // already active; keep scanning only for the earliest sleep
		}
		// A task no definition can take waits for a registry change, except an
		// abort-marked one, which is orphaned.
		if !task.AbortRequested || task.AbortReason != "" {
			if snapshot == nil {
				snapshot = s.h.registry.Snapshot()
			}
			resolved := s.h.resolve(task.Kind, snapshot)
			decision := fit(task, resolved, failures[id])
			if decision.reason != "" || decision.migrates && resolved.Definition.Migrate == nil {
				continue
			}
		}
		active = true
	}
	if !active && cascadePending(state) {
		active = true
	}
	return Wake{Active: active, At: at}
}

// wakeTimer is the armed timer for the earliest sleep.
type wakeTimer struct {
	at      time.Time
	mu      sync.Mutex
	stopped bool
	cancel  func()
}

func (t *wakeTimer) stop() {
	t.mu.Lock()
	t.stopped = true
	cancel := t.cancel
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (t *wakeTimer) armed(cancel func()) {
	t.mu.Lock()
	stopped := t.stopped
	if !stopped {
		t.cancel = cancel
	}
	t.mu.Unlock()
	if stopped && cancel != nil {
		cancel()
	}
}

// setTimer arms a host timer (HarnessOptions.SetTimer, default time.AfterFunc).
func (h *Harness) setTimer(callback func(), delay time.Duration) func() {
	if h.options.SetTimer != nil {
		if stop := h.options.SetTimer(callback, delay); stop != nil {
			return stop
		}
		return func() {}
	}
	timer := time.AfterFunc(delay, callback)
	return func() { timer.Stop() }
}

// wakeLoop recomputes the wake after changes: it keeps the timer for the
// earliest sleep while scheduling runs, or offers changed values to OnWake.
// It never runs on the Session line and never blocks it.
func (s *Scheduler) wakeLoop() {
	defer close(s.wakerDone)
	for {
		select {
		case <-s.wakeClosed:
			return
		case <-s.wakeDirty:
		}
		state, err := s.h.read(context.Background())
		if err != nil {
			continue
		}
		wake := s.wakeFor(state)
		if s.h.options.OnWake == nil {
			s.mu.Lock()
			running := s.enabled && !s.closing
			s.mu.Unlock()
			if running {
				s.armTimer(wake.At)
			} else {
				s.armTimer(nil)
			}
			continue
		}
		if s.lastWake != nil && s.lastWake.equal(wake) {
			continue
		}
		if s.offerWake(wake) {
			s.lastWake = &wake
		}
	}
}

// offerWake calls OnWake; a panic is reported, and the value is offered
// again at the next change.
func (s *Scheduler) offerWake(wake Wake) (delivered bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			delivered = false
			s.h.report(Report{Kind: ReportHostError, Err: &panicError{value: recovered, stack: debug.Stack()}, Callback: "onWake"})
		}
	}()
	s.h.options.OnWake(wake)
	return true
}

// armTimer keeps one timer for at; it rechecks the clock when it fires, so a
// wake before the deadline only arms it again.
func (s *Scheduler) armTimer(at *time.Time) {
	s.mu.Lock()
	if s.closing || s.timer == nil && at == nil || s.timer != nil && at != nil && s.timer.at.Equal(*at) {
		s.mu.Unlock()
		return
	}
	old := s.timer
	var timer *wakeTimer
	if at != nil {
		timer = &wakeTimer{at: *at}
	}
	s.timer = timer
	s.mu.Unlock()
	if old != nil {
		old.stop()
	}
	if timer == nil {
		return
	}
	timer.armed(s.h.setTimer(func() {
		s.mu.Lock()
		if s.timer == timer {
			s.timer = nil
		}
		s.mu.Unlock()
		s.kick()
	}, at.Sub(s.h.now())))
}
