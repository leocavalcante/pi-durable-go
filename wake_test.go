package durable

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTimers records armed timers; tests fire them explicitly.
type fakeTimers struct {
	mu     sync.Mutex
	timers []*fakeTimer
}

type fakeTimer struct {
	callback func()
	delay    time.Duration
	stopped  bool
}

func (f *fakeTimers) SetTimer(callback func(), delay time.Duration) func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	timer := &fakeTimer{callback: callback, delay: delay}
	f.timers = append(f.timers, timer)
	return func() {
		f.mu.Lock()
		timer.stopped = true
		f.mu.Unlock()
	}
}

// armed returns the live timers.
func (f *fakeTimers) armed() []*fakeTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*fakeTimer
	for _, timer := range f.timers {
		if !timer.stopped {
			out = append(out, timer)
		}
	}
	return out
}

func (f *fakeTimers) waitArmed(t *testing.T, match func(*fakeTimer) bool) *fakeTimer {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, timer := range f.armed() {
			if match(timer) {
				return timer
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no matching timer was armed")
		}
		time.Sleep(time.Millisecond)
	}
}

func (f *fakeTimers) fire(timer *fakeTimer) {
	f.mu.Lock()
	if timer.stopped {
		f.mu.Unlock()
		return
	}
	timer.stopped = true
	f.mu.Unlock()
	timer.callback()
}

func sleeperDefinition(d time.Duration, phases *atomic.Int32) TaskDefinition {
	return TaskDefinition{Kind: "test.sleeper", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		if phases.Add(1) == 1 {
			return r.SleepUntil(ctx, r.Now().Add(d), []byte(`{"slept":true}`))
		}
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
}

func TestWakeTimerResumesASleepWithoutPolling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var phases atomic.Int32
	definition := sleeperDefinition(30*time.Millisecond, &phases)
	h := taskTestHarness(t, HarnessOptions{})
	installTasks(t, h.registry, "sleepers", definition)
	id := createTaskForTest(t, h, definition)
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeCompleted || phases.Load() != 2 {
		t.Fatalf("record=%+v phases=%d error=%v", record, phases.Load(), err)
	}
}

func TestWakeFakeTimersArmForTheEarliestSleep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := newTestClock()
	timers := &fakeTimers{}
	var phases atomic.Int32
	definition := sleeperDefinition(time.Hour, &phases)
	h := taskTestHarness(t, HarnessOptions{Now: clock.Now, SetTimer: timers.SetTimer})
	installTasks(t, h.registry, "sleepers", definition)
	id := createTaskForTest(t, h, definition)
	h.Resume()
	timer := timers.waitArmed(t, func(timer *fakeTimer) bool { return timer.delay == time.Hour })
	if task, err := h.Task(ctx, id); err != nil || task.Status != "sleeping" {
		t.Fatalf("task=%+v error=%v", task, err)
	}
	// A timer that fires before the deadline only arms it again.
	clock.advance(time.Minute)
	timers.fire(timer)
	again := timers.waitArmed(t, func(timer *fakeTimer) bool { return timer.delay == 59*time.Minute })
	if phases.Load() != 1 {
		t.Fatal("an early timer resumed the sleep")
	}
	clock.advance(59 * time.Minute)
	timers.fire(again)
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeCompleted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	if len(timers.armed()) != 0 {
		t.Fatalf("timers left armed with nothing sleeping: %d", len(timers.armed()))
	}
}

func TestWakeOnWakeHostArmsItsOwnWakeup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := newTestClock()
	timers := &fakeTimers{}
	wakes := make(chan Wake, 32)
	var phases atomic.Int32
	definition := sleeperDefinition(time.Hour, &phases)
	registry := NewRegistry()
	installTasks(t, registry, "sleepers", definition)
	store := NewMemoryStorage()
	h, err := Open(ctx, store, HarnessOptions{Registry: registry, Now: clock.Now, SetTimer: timers.SetTimer, OnWake: func(w Wake) { wakes <- w }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	next := func(match func(Wake) bool) Wake {
		t.Helper()
		for {
			select {
			case w := <-wakes:
				if match(w) {
					return w
				}
			case <-ctx.Done():
				t.Fatal("no matching wake was offered")
			}
		}
	}
	next(func(w Wake) bool { return !w.Active && w.At == nil }) // idle after open
	id := createTaskForTest(t, h, definition)
	next(func(w Wake) bool { return w.Active })
	if !h.Wake().Active {
		t.Fatal("Harness.Wake does not see a runnable task")
	}
	h.Resume()
	until := clock.Now().Add(time.Hour)
	w := next(func(w Wake) bool { return !w.Active && w.At != nil })
	if !w.At.Equal(until) {
		t.Fatalf("wake=%+v, want at %v", w, until)
	}
	for _, timer := range timers.armed() {
		if timer.delay == time.Hour {
			t.Fatal("an OnWake host got a sleep timer")
		}
	}
	// The host's alarm fires: it resumes, and the due sleep runs.
	clock.advance(time.Hour)
	h.Resume()
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeCompleted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	next(func(w Wake) bool { return !w.Active && w.At == nil })
}

func TestWakeIgnoresBlockedAndCompletingWork(t *testing.T) {
	clock := newTestClock()
	h := taskTestHarness(t, HarnessOptions{Now: clock.Now})
	root := testRoot(t, h)
	if w := h.Wake(); w.Active || w.At != nil {
		t.Fatalf("idle wake=%+v", w)
	}
	missing := TaskDefinition{Kind: "test.wake-missing", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	seedTask(t, h, root.ID(), missing, TaskOptions{}, nil)
	if w := h.Wake(); w.Active {
		t.Fatalf("a blocked task made the wake active: %+v", w)
	}
	later := clock.Now().Add(time.Minute)
	seedTask(t, h, root.ID(), missing, TaskOptions{}, func(task *TaskRecord) { task.Status, task.WakeAt = "sleeping", &later })
	if w := h.Wake(); w.Active || w.At == nil || !w.At.Equal(later) {
		t.Fatalf("sleeping wake=%+v", w)
	}
	seedTask(t, h, root.ID(), missing, TaskOptions{}, func(task *TaskRecord) { task.AbortRequested = true })
	if w := h.Wake(); !w.Active {
		t.Fatal("an abort-marked blocked task is orphaned by the next pass, so it is work")
	}
}

func TestTaskRuntimeSetTimerIsInvocationBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	timers := &fakeTimers{}
	fired := make(chan struct{}, 4)
	var late atomic.Bool
	release := make(chan struct{})
	definition := TaskDefinition{Kind: "test.pacing", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		r.SetTimer(func() { fired <- struct{}{} }, time.Second)
		stopped := r.SetTimer(func() { t.Error("a disarmed timer fired") }, 2*time.Second)
		stopped()
		r.SetTimer(func() { late.Store(true) }, 3*time.Second) // still armed when the invocation ends
		<-release
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	h := taskTestHarness(t, HarnessOptions{SetTimer: timers.SetTimer})
	installTasks(t, h.registry, "pacing", definition)
	id := createTaskForTest(t, h, definition)
	h.Resume()
	first := timers.waitArmed(t, func(timer *fakeTimer) bool { return timer.delay == time.Second })
	third := timers.waitArmed(t, func(timer *fakeTimer) bool { return timer.delay == 3*time.Second })
	timers.fire(first)
	<-fired
	close(release)
	if _, err := h.WaitTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	// The terminal commit publishes before the invocation ends; wait for the end.
	for h.scheduler.active(id) != nil {
		time.Sleep(time.Millisecond)
	}
	timers.fire(third) // fires after the end: disarmed, nothing runs
	if late.Load() {
		t.Fatal("a timer ran after its invocation ended")
	}
}
