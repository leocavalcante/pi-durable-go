package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSleepResumesAtDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := newTestClock()
	var phases atomic.Int32
	slept := make(chan struct{}, 1)
	definition := TaskDefinition{Kind: "test.sleep", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		if phases.Add(1) == 1 {
			if !r.Now().Equal(clock.Now()) {
				t.Error("TaskRuntime.Now is not the Harness clock")
			}
			err := r.SleepUntil(ctx, r.Now().Add(time.Hour), json.RawMessage(`{"phase":"after"}`))
			slept <- struct{}{}
			return err
		}
		if string(task.Checkpoint) != `{"phase":"after"}` {
			t.Errorf("resumed at %s", task.Checkpoint)
		}
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	h := taskTestHarness(t, HarnessOptions{Now: clock.Now})
	installTasks(t, h.registry, "sleepy", definition)
	id := createTaskForTest(t, h, definition)
	h.Resume()
	<-slept
	sleeping := waitForTask(t, h, id, func(task TaskRecord) bool { return task.Status == "sleeping" })
	for h.scheduler.active(id) != nil { // the sleep commits before its invocation ends
		time.Sleep(time.Millisecond)
	}
	if sleeping.WakeAt == nil || !sleeping.WakeAt.Equal(clock.Now().Add(time.Hour)) {
		t.Fatalf("sleeping=%+v", sleeping)
	}
	inspection, err := h.Inspect(ctx)
	if err != nil || len(inspection.Tasks) != 1 || inspection.Tasks[0].State.Kind != "sleeping" || !inspection.Tasks[0].State.Until.Equal(*sleeping.WakeAt) {
		t.Fatalf("inspection=%+v error=%v", inspection, err)
	}
	h.Resume()
	time.Sleep(50 * time.Millisecond)
	if phases.Load() != 1 {
		t.Fatal("a sleep resumed before its deadline")
	}
	clock.advance(time.Hour)
	h.Resume()
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeCompleted || phases.Load() != 2 {
		t.Fatalf("record=%+v phases=%d error=%v", record, phases.Load(), err)
	}
}

func TestSleepEndsAtAnAbortMark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := newTestClock()
	sleepErr := make(chan error, 1)
	definition := TaskDefinition{Kind: "test.sleep-abort", Version: 1,
		Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
			return r.SleepUntil(ctx, r.Now().Add(24*time.Hour), nil)
		},
		Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
			// An abort handler cannot sleep.
			sleepErr <- r.SleepUntil(ctx, r.Now().Add(time.Hour), nil)
			return r.Complete(ctx, Outcome{Status: OutcomeAborted})
		}}
	h := taskTestHarness(t, HarnessOptions{Now: clock.Now})
	installTasks(t, h.registry, "sleepy", definition)
	id := createTaskForTest(t, h, definition)
	h.Resume()
	waitForTask(t, h, id, func(task TaskRecord) bool { return task.Status == "sleeping" })
	if _, err := h.AbortTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeAborted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	if err := <-sleepErr; !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "Abort handler of task "+itoa(id)+" cannot sleep") {
		t.Fatalf("abort handler sleep error=%v", err)
	}
}

func TestSleepTransitionForms(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	wake := now.Add(time.Minute)
	for _, tc := range []struct {
		name       string
		transition TaskTransition
		status     string
		err        string
	}{
		{name: "sleeping", transition: TaskTransition{Status: "sleeping", WakeAt: &wake}, status: "sleeping"},
		{name: "pending-with-deadline", transition: TaskTransition{Status: "pending", WakeAt: &wake}, status: "sleeping"},
		{name: "waiting-on-nothing-with-deadline", transition: TaskTransition{Status: "waiting", WakeAt: &wake}, status: "sleeping"},
		{name: "sleeping-without-deadline", transition: TaskTransition{Status: "sleeping"}, err: "cannot sleep without a time to wake"},
		{name: "waiting-with-deadline", transition: TaskTransition{Status: "waiting", WaitOn: []TaskID{2}, WakeAt: &wake}, err: "cannot wait with a deadline"},
		{name: "running-drops-deadline", transition: TaskTransition{Status: "running", WakeAt: &wake}, status: "running"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := waitState(&TaskRecord{ID: 1}, &TaskRecord{ID: 2})
			task := state.Tasks[1]
			err := applyTransition(&Tx{state: state, active: true}, task, tc.transition, now)
			if tc.err != "" {
				if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("error=%v, want %q", err, tc.err)
				}
				return
			}
			if err != nil || task.Status != tc.status {
				t.Fatalf("status=%q error=%v", task.Status, err)
			}
			if tc.status == "sleeping" && (task.WakeAt == nil || !task.WakeAt.Equal(wake) || len(task.WaitOn) != 0) {
				t.Fatalf("sleep=%+v", task)
			}
			if tc.status == "running" && task.WakeAt != nil {
				t.Fatal("a running task kept a deadline")
			}
		})
	}
}

func TestSleepIsNotCountedByACrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	definition := TaskDefinition{Kind: "test.sleep-crash", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		return r.SleepUntil(ctx, r.Now().Add(time.Hour), nil)
	}}
	registry := NewRegistry()
	installTasks(t, registry, "sleepy", definition)
	hosts := newCrashHosts(t, nil, HarnessOptions{Registry: registry})
	h := hosts.open(nil)
	id := createTaskForTest(t, h, definition)
	h.Resume()
	waitForTask(t, h, id, func(task TaskRecord) bool { return task.Status == "sleeping" })
	hosts.crash(h)
	h = hosts.open(nil)
	if task, err := h.Task(ctx, id); err != nil || task.Status != "sleeping" || task.Restarts != 0 {
		t.Fatalf("task=%+v error=%v", task, err)
	}
}
