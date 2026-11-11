package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type restartReopenStorage struct{ Storage }

func (restartReopenStorage) Close() error { return nil }

func openRestartTestHarness(t *testing.T, store Storage, registry *Registry) *Harness {
	t.Helper()
	h, err := Open(context.Background(), store, HarnessOptions{Registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return h
}

// unlimitedRestarts isolates crash accounting from restart-limit enforcement.
var unlimitedRestarts = HarnessOptions{Settings: Settings{RestartLimit: math.Inf(1)}}

func restartAccountingDefinition() TaskDefinition {
	return TaskDefinition{Kind: "test.restart-accounting", Version: 1,
		Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
}

func TestCrashLoopOpenCountsOnlyRunningTasksOnce(t *testing.T) {
	ctx := context.Background()
	memory := NewMemoryStorage()
	store := restartReopenStorage{Storage: memory}
	h := openRestartTestHarness(t, store, nil)
	definition := restartAccountingDefinition()
	root, err := h.Root(ctx, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wakeAt := time.Now().UTC().Add(time.Hour)
	statuses := []struct {
		name     string
		status   string
		restarts int
		abort    bool
	}{
		{name: "running-missing-counter", status: "running"},
		{name: "running-existing-counter", status: "running", restarts: 3},
		{name: "running-abort-invocation", status: "running", restarts: 4, abort: true},
		{name: "pending", status: "pending", restarts: 5},
		{name: "pending-future-wake", status: "pending", restarts: 5},
		{name: "waiting-future-wake", status: "waiting", restarts: 6},
		{name: "completing", status: "completing", restarts: 7},
		{name: "terminal", status: "terminal", restarts: 8},
	}
	ids := make(map[string]TaskID, len(statuses))
	if err := h.Commit(ctx, func(tx *Tx) error {
		for _, tc := range statuses {
			id, err := tx.CreateTask(root.ID(), definition, nil, TaskOptions{})
			if err != nil {
				return err
			}
			ids[tc.name] = id
			task := tx.state.Tasks[id]
			task.Status = tc.status
			task.Restarts = tc.restarts
			task.AbortRequested = tc.abort
			if tc.status == "waiting" || tc.name == "pending-future-wake" {
				task.WakeAt = &wakeAt
			}
			if tc.status == "completing" || tc.status == "terminal" {
				task.Outcome = &Outcome{Status: OutcomeCompleted}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}

	for open := 1; open <= 2; open++ {
		h = openRestartTestHarness(t, store, nil) // Open leaves scheduling paused.
		state, err := h.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if state.Version != stateVersion {
			t.Fatalf("open %d changed state version to %d", open, state.Version)
		}
		for _, tc := range statuses {
			got := state.Tasks[ids[tc.name]]
			wantStatus, wantRestarts := tc.status, tc.restarts
			if tc.status == "running" {
				wantStatus, wantRestarts = "pending", tc.restarts+1
			}
			if got.Status != wantStatus || got.Restarts != wantRestarts {
				t.Errorf("open %d %s: status=%q restarts=%d, want %q/%d", open, tc.name, got.Status, got.Restarts, wantStatus, wantRestarts)
			}
			if (tc.status == "waiting" || tc.name == "pending-future-wake") && (got.WakeAt == nil || !got.WakeAt.Equal(wakeAt)) {
				t.Errorf("open %d changed future WakeAt: %v", open, got.WakeAt)
			}
		}
		if err := h.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

type runtimeRestartCase struct {
	name      string
	configure func(*Tx, *TaskRecord, TaskDefinition, *TaskID) error
	commit    func(context.Context, *TaskRuntime, TaskID, TaskID) error
	want      int
}

func TestCrashLoopRuntimeCommitProgress(t *testing.T) {
	targets := func(tx *Tx, task *TaskRecord, definition TaskDefinition, second *TaskID) error {
		first, err := tx.CreateTask(task.Conversation, definition, nil, TaskOptions{})
		if err != nil {
			return err
		}
		id, err := tx.CreateTask(task.Conversation, definition, nil, TaskOptions{})
		if err != nil {
			return err
		}
		*second = id
		task.WaitOn = []TaskID{first}
		task.WaitPolicy = "allSettled"
		return nil
	}
	cases := []runtimeRestartCase{
		{name: "checkpoint-change", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				return sameRestartTransition(task, json.RawMessage(`{"step":2}`)), nil
			})
		}, want: 0},
		{name: "status-change", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				tr := sameRestartTransition(task, task.Checkpoint)
				tr.Status = "pending"
				return tr, nil
			})
		}, want: 0},
		{name: "waits-change", configure: targets, commit: func(ctx context.Context, r *TaskRuntime, _, second TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				tr := sameRestartTransition(task, task.Checkpoint)
				tr.WaitOn = []TaskID{second}
				return tr, nil
			})
		}, want: 0},
		{name: "wake-at-change", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				// A sleep with a new deadline is a new state.
				tr := sameRestartTransition(task, task.Checkpoint)
				wake := time.Now().UTC().Add(2 * time.Hour)
				tr.Status, tr.WakeAt = "sleeping", &wake
				return tr, nil
			})
		}, want: 0},
		{name: "terminal-outcome", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
		}, want: 0},
		{name: "held-completing-outcome", commit: func(ctx context.Context, r *TaskRuntime, child, _ TaskID) error {
			if child == 0 {
				return fmt.Errorf("missing owned child for held outcome")
			}
			return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
		}, want: 0},
		{name: "structurally-equal-checkpoint", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				return sameRestartTransition(task, json.RawMessage(`{"step":1.0}`)), nil
			})
		}, want: 9},
		{name: "memo-write", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			_, err := r.Memo(ctx, "memo", json.RawMessage(`true`))
			return err
		}, want: 9},
		{name: "document-write", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				if err := tx.PutDocument(DocumentDefinition{Kind: "test.restart-count", Version: 1, Scope: "session"}, 0, json.RawMessage(`{"saved":true}`)); err != nil {
					return TaskTransition{}, err
				}
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				return sameRestartTransition(task, task.Checkpoint), nil
			})
		}, want: 9},
		{name: "entry-write", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				if _, err := tx.Append(task.Conversation, Entry{Kind: "test.restart-count", Data: json.RawMessage(`{"saved":true}`)}); err != nil {
					return TaskTransition{}, err
				}
				return sameRestartTransition(task, task.Checkpoint), nil
			})
		}, want: 9},
		{name: "nil-checkpoint-keeps-state", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
				return TaskTransition{Status: "running"}, nil
			})
		}, want: 9},
		{name: "in-place-checkpoint-mutation", commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				task.Checkpoint[8] = '2' // {"step":1}
				return sameRestartTransition(task, nil), nil
			})
		}, want: 0},
		{name: "in-place-waits-mutation", configure: targets, commit: func(ctx context.Context, r *TaskRuntime, _, second TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				task.WaitOn[0] = second
				return sameRestartTransition(task, nil), nil
			})
		}, want: 0},
		{name: "in-place-deadline-mutation", configure: func(_ *Tx, task *TaskRecord, _ TaskDefinition, _ *TaskID) error {
			wake := time.Unix(1000, 0).UTC()
			task.WakeAt = &wake
			return nil
		}, commit: func(ctx context.Context, r *TaskRuntime, _, _ TaskID) error {
			return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				task, err := tx.Task(r.ID)
				if err != nil {
					return TaskTransition{}, err
				}
				*task.WakeAt = task.WakeAt.Add(time.Hour)
				return sameRestartTransition(task, nil), nil
			})
		}, want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := taskTestHarness(t, HarnessOptions{}) // Scheduler stays paused.
			definition := restartAccountingDefinition()
			id := createTaskForTest(t, h, definition)
			var child, second TaskID
			if err := h.Commit(context.Background(), func(tx *Tx) error {
				task := tx.state.Tasks[id]
				task.Status = "running"
				task.Restarts = 9
				task.Checkpoint = json.RawMessage(`{"step":1}`)
				if tc.name == "held-completing-outcome" {
					var err error
					child, err = tx.CreateTask(task.Conversation, definition, nil, TaskOptions{Owner: Owner{Task: id}})
					if err != nil {
						return err
					}
				}
				if tc.configure != nil {
					return tc.configure(tx, task, definition, &second)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			runtime := &TaskRuntime{h: h, ID: id}
			if err := tc.commit(context.Background(), runtime, child, second); err != nil {
				t.Fatal(err)
			}
			task, err := h.Task(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if task.Restarts != tc.want {
				t.Fatalf("restarts=%d, want %d (task=%+v)", task.Restarts, tc.want, task)
			}
			if tc.name == "held-completing-outcome" && task.Status != "completing" {
				t.Fatalf("outcome was not held: %+v", task)
			}
		})
	}
}

func sameRestartTransition(task *TaskRecord, checkpoint json.RawMessage) TaskTransition {
	return TaskTransition{Status: task.Status, Checkpoint: checkpoint, WaitOn: task.WaitOn,
		WaitPolicy: task.WaitPolicy, WakeAt: task.WakeAt}
}

func TestCrashLoopOutcomesClearCounter(t *testing.T) {
	t.Run("fault", func(t *testing.T) {
		h := taskTestHarness(t, HarnessOptions{})
		definition := restartAccountingDefinition()
		id := createTaskForTest(t, h, definition)
		if err := h.Commit(context.Background(), func(tx *Tx) error {
			task := tx.state.Tasks[id]
			task.Status, task.Restarts = "running", 6
			return h.scheduler.fault(tx, task, errors.New("task failed"))
		}); err != nil {
			t.Fatal(err)
		}
		task, err := h.Task(context.Background(), id)
		if err != nil || task.Outcome == nil || task.Outcome.Status != OutcomeFaulted || task.Restarts != 0 {
			t.Fatalf("faulted task=%+v error=%v", task, err)
		}
	})

	t.Run("orphan", func(t *testing.T) {
		h := taskTestHarness(t, HarnessOptions{})
		id := createTaskForTest(t, h, restartAccountingDefinition()) // Intentionally not installed.
		if err := h.Commit(context.Background(), func(tx *Tx) error {
			task := tx.state.Tasks[id]
			task.Restarts = 6
			var reports []Report
			_, _, err := h.scheduler.markAbort(tx, task, false, &reports)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		task, err := h.Task(context.Background(), id)
		if err != nil || task.Outcome == nil || task.Outcome.Status != OutcomeOrphaned || task.Restarts != 0 {
			t.Fatalf("orphaned task=%+v error=%v", task, err)
		}
	})
}

func TestCrashLoopAbortMarksResetOnChange(t *testing.T) {
	for _, tc := range []struct {
		name       string
		preMarked  bool
		operation  string
		wantParent int
		wantChild  int
	}{
		{name: "direct-mark", operation: "direct", wantParent: 0, wantChild: 0},
		{name: "restart-abandonment", operation: "abandon", wantParent: 0, wantChild: 0},
		{name: "cascade", preMarked: true, operation: "cascade", wantParent: 8, wantChild: 0},
		{name: "upgraded-mark", preMarked: true, operation: "upgrade", wantParent: 0, wantChild: 0},
		{name: "same-mark-reason", preMarked: true, operation: "same", wantParent: 8, wantChild: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := taskTestHarness(t, HarnessOptions{})
			definition := restartAccountingDefinition()
			parent := createTaskForTest(t, h, definition)
			var child TaskID
			if err := h.Commit(context.Background(), func(tx *Tx) error {
				p := tx.state.Tasks[parent]
				p.Restarts = 8
				id, err := tx.CreateTask(p.Conversation, definition, nil, TaskOptions{Owner: Owner{Task: parent}})
				if err != nil {
					return err
				}
				child = id
				c := tx.state.Tasks[child]
				c.Restarts = 8
				if tc.preMarked {
					p.AbortRequested, p.AbortReason = true, "restart"
					c.AbortRequested, c.AbortReason = true, "restart"
				}
				if tc.operation == "cascade" {
					p.AbortRequested = true
					p.AbortReason = ""
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := h.Commit(context.Background(), func(tx *Tx) error {
				switch tc.operation {
				case "direct":
					markAbortTreeReason(tx.state, parent, "")
				case "abandon":
					markAbortTreeReason(tx.state, parent, "restart")
				case "cascade":
					cascadeCancellation(tx.state)
				case "upgrade":
					markAbortTreeReason(tx.state, parent, "")
				case "same":
					markAbortTreeReason(tx.state, parent, "restart")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			state, err := h.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got := state.Tasks[parent].Restarts; got != tc.wantParent {
				t.Errorf("parent restarts=%d, want %d", got, tc.wantParent)
			}
			if got := state.Tasks[child].Restarts; got != tc.wantChild {
				t.Errorf("child restarts=%d, want %d", got, tc.wantChild)
			}
		})
	}
}

func TestCrashLoopReservationAndMigrationKeepCounter(t *testing.T) {
	for _, migrate := range []bool{false, true} {
		t.Run(map[bool]string{false: "reservation", true: "migration"}[migrate], func(t *testing.T) {
			h := taskTestHarness(t, unlimitedRestarts)
			started := make(chan struct{})
			release := make(chan struct{})
			var migrationCalls atomic.Int32
			definition := restartAccountingDefinition()
			if migrate {
				definition.Version = 2
				definition.Migrate = func(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
					migrationCalls.Add(1)
					if from != 1 {
						return MigratedTask{}, fmt.Errorf("migration from %d", from)
					}
					return MigratedTask{Input: input, Checkpoint: checkpoint}, nil
				}
			}
			definition.Run = func(ctx context.Context, _ *TaskRuntime, record TaskRecord) error {
				if record.Restarts != 12 {
					t.Errorf("reserved task count=%d, want 12", record.Restarts)
				}
				close(started)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return nil
				}
			}
			if err := h.registry.Install(Extension{Name: "restart-accounting", Tasks: []TaskDefinition{definition}}); err != nil {
				t.Fatal(err)
			}
			id := createTaskForTest(t, h, definition)
			if err := h.Commit(context.Background(), func(tx *Tx) error {
				task := tx.state.Tasks[id]
				task.Restarts = 12
				if migrate {
					task.Version = 1
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := h.scheduler.reserve(id); err != nil {
				t.Fatal(err)
			}
			h.Resume() // Keep normal shutdown joining for this manually reserved invocation.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			task, err := h.Task(ctx, id)
			if err != nil || task.Status != "running" || task.Restarts != 12 {
				t.Fatalf("reserved task=%+v error=%v", task, err)
			}
			if migrate && migrationCalls.Load() != 1 {
				t.Fatalf("migration calls=%d, want 1", migrationCalls.Load())
			}
			close(release)
		})
	}
}

func TestCrashLoopHandoverKeepsCounter(t *testing.T) {
	h := taskTestHarness(t, unlimitedRestarts)
	oldReady, releaseOld, newReady := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce atomic.Bool
	defer func() {
		if releaseOnce.CompareAndSwap(false, true) {
			close(releaseOld)
		}
	}()
	old := TaskDefinition{Kind: "test.restart-handover", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		if err := r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			return TaskTransition{Status: "running", Checkpoint: json.RawMessage(`{"phase":1}`)}, nil
		}); err != nil {
			return err
		}
		close(oldReady)
		select {
		case <-releaseOld:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	if err := h.registry.Install(Extension{Name: "restart-handover", Tasks: []TaskDefinition{old}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, old)
	h.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-oldReady:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := h.Commit(ctx, func(tx *Tx) error {
		tx.state.Tasks[id].Restarts = 13
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	next := old
	next.Run = func(ctx context.Context, r *TaskRuntime, record TaskRecord) error {
		if record.Restarts != 13 {
			t.Errorf("handed-over count=%d, want 13", record.Restarts)
		}
		close(newReady)
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}
	if err := h.registry.Install(Extension{Name: "restart-handover", Tasks: []TaskDefinition{next}}); err != nil {
		t.Fatal(err)
	}
	if releaseOnce.CompareAndSwap(false, true) {
		close(releaseOld)
	}
	select {
	case <-newReady:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	task, err := h.WaitTask(ctx, id)
	if err != nil || task.Outcome == nil || task.Restarts != 0 {
		t.Fatalf("handed-over task=%+v error=%v", task, err)
	}
}

func TestCrashLoopAbortCancellationReleaseKeepsCounter(t *testing.T) {
	h := taskTestHarness(t, unlimitedRestarts)
	runStarted := make(chan struct{})
	runCanceled := make(chan struct{})
	releaseRun := make(chan struct{})
	abortStarted := make(chan struct{})
	releaseAbort := make(chan struct{})
	var runRelease, abortRelease sync.Once
	defer runRelease.Do(func() { close(releaseRun) })
	defer abortRelease.Do(func() { close(releaseAbort) })
	definition := restartAccountingDefinition()
	definition.Run = func(ctx context.Context, _ *TaskRuntime, _ TaskRecord) error {
		close(runStarted)
		<-ctx.Done()
		close(runCanceled)
		<-releaseRun
		return ctx.Err()
	}
	definition.Abort = func(ctx context.Context, r *TaskRuntime, record TaskRecord) error {
		if record.Restarts != 17 {
			t.Errorf("abort invocation count=%d, want 17", record.Restarts)
		}
		close(abortStarted)
		select {
		case <-releaseAbort:
			return r.Complete(ctx, Outcome{Status: OutcomeAborted})
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := h.registry.Install(Extension{Name: "restart-abort-release", Tasks: []TaskDefinition{definition}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, definition)
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		tx.state.Tasks[id].Restarts = 6
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.scheduler.reserve(id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-runStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	abortDone := make(chan error, 1)
	go func() {
		_, err := h.AbortTask(ctx, id)
		abortDone <- err
	}()
	select {
	case <-runCanceled:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A fresh abort mark itself resets the counter. Seed a known value after
	// that decision while the run is still held, to isolate cancellation release.
	if err := h.Commit(ctx, func(tx *Tx) error {
		tx.state.Tasks[id].Restarts = 17
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runRelease.Do(func() { close(releaseRun) })
	select {
	case <-abortStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-abortDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	task, err := h.Task(ctx, id)
	if err != nil || task.Status != "running" || task.Restarts != 17 {
		t.Fatalf("after cancellation release task=%+v error=%v", task, err)
	}
	abortRelease.Do(func() { close(releaseAbort) })
	task, err = h.WaitTask(ctx, id)
	if err != nil || task.Outcome == nil || task.Restarts != 0 {
		t.Fatalf("after abort outcome task=%+v error=%v", task, err)
	}
}
