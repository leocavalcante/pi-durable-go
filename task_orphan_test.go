package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var orphanTaskDocument = DocumentDefinition{Kind: "test.orphan-state", Version: 1, Scope: "task"}

func orphanHarness(t *testing.T, options HarnessOptions) *Harness {
	t.Helper()
	if options.Model == nil {
		options.Model = echoModel{}
	}
	h, err := Open(context.Background(), NewMemoryStorage(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Errorf("close harness: %v", err)
		}
	})
	return h
}

func createTaskAndDocument(t *testing.T, h *Harness, c *Conversation, definition TaskDefinition) TaskID {
	t.Helper()
	var id TaskID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		id, err = tx.CreateTask(c.id, definition, nil, TaskOptions{})
		if err != nil {
			return err
		}
		return tx.PutDocument(orphanTaskDocument, uint64(id), json.RawMessage(`{"retire":true}`))
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func waitForOrphanTask(t *testing.T, h *Harness, id TaskID, predicate func(TaskRecord) bool) TaskRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task, err := h.Task(context.Background(), id)
		if err == nil && predicate(task) {
			return task
		}
		time.Sleep(time.Millisecond)
	}
	task, err := h.Task(context.Background(), id)
	t.Fatalf("task %d did not reach expected state: %+v, %v", id, task, err)
	return TaskRecord{}
}

func TestTaskOrphanDirectResolutionAndAtomicRetirement(t *testing.T) {
	for _, tc := range []struct {
		name           string
		storedVersion  int
		installed      func(TaskDefinition) TaskDefinition
		wantReason     string
		wantReport     bool
		migrationCalls int32
	}{
		{name: "missing", storedVersion: 1, installed: func(TaskDefinition) TaskDefinition { return TaskDefinition{} }, wantReason: "missing_task"},
		{name: "too_old", storedVersion: 2, installed: func(def TaskDefinition) TaskDefinition { def.Version = 1; return def }, wantReason: "task_too_old"},
		{name: "missing_migrate", storedVersion: 1, installed: func(def TaskDefinition) TaskDefinition { def.Version = 2; return def }, wantReason: "migration_failed", wantReport: true},
		{name: "failed_migrate", storedVersion: 1, installed: func(def TaskDefinition) TaskDefinition {
			def.Version = 2
			def.Migrate = func(int, json.RawMessage, json.RawMessage) (MigratedTask, error) {
				return MigratedTask{}, errors.New("migration declined")
			}
			return def
		}, wantReason: "migration_failed", wantReport: true, migrationCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reports := make(chan Report, 8)
			h := orphanHarness(t, HarnessOptions{OnReport: func(report Report) { reports <- report }})
			c := testRoot(t, h)
			var calls atomic.Int32
			stored := TaskDefinition{Kind: "test.orphan." + tc.name, Version: tc.storedVersion,
				Run:   func(context.Context, *TaskRuntime, TaskRecord) error { calls.Add(1); return nil },
				Abort: func(context.Context, *TaskRuntime, TaskRecord) error { calls.Add(1); return nil },
			}
			id := createTaskAndDocument(t, h, c, stored)
			installed := tc.installed(stored)
			if installed.Kind != "" {
				if installed.Migrate != nil {
					original := installed.Migrate
					installed.Migrate = func(version int, input, checkpoint json.RawMessage) (MigratedTask, error) {
						calls.Add(1)
						return original(version, input, checkpoint)
					}
				}
				if err := h.registry.Install(Extension{Name: "orphan-" + tc.name, Tasks: []TaskDefinition{installed}}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := h.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			result, err := h.AbortTask(context.Background(), id)
			if err != nil || result != AbortMarked {
				t.Fatalf("AbortTask=(%q, %v), want marked", result, err)
			}
			after, err := h.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			task := after.Tasks[id]
			if task.Status != "terminal" || task.Outcome == nil || task.Outcome.Status != OutcomeOrphaned || task.Outcome.Reason != tc.wantReason || task.EndedAt == nil {
				t.Fatalf("orphan receipt=%+v", task)
			}
			if _, ok := after.Documents[documentKey(orphanTaskDocument, uint64(id))]; ok {
				t.Fatal("orphaning did not retire its task-scoped document")
			}
			if after.Sequence != before.Sequence+1 {
				t.Fatalf("orphan and retirement used %d commits; want one (%d -> %d)", after.Sequence-before.Sequence, before.Sequence, after.Sequence)
			}
			if got := calls.Load(); got != tc.migrationCalls {
				t.Fatalf("Run/Abort/Migrate calls=%d, want %d", got, tc.migrationCalls)
			}
			if tc.wantReport {
				select {
				case report := <-reports:
					if report.Kind != ReportMigrationFailed || report.Reason != "" || report.Task == nil || report.Task.ID != id || report.Extension != "orphan-"+tc.name {
						t.Fatalf("migration report=%+v", report)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("migration failure was not reported")
				}
			}
			sequence := after.Sequence
			result, err = h.AbortTask(context.Background(), id)
			if err != nil || result != AbortTerminal {
				t.Fatalf("re-abort terminal task=(%q, %v)", result, err)
			}
			if state, err := h.Snapshot(context.Background()); err != nil || state.Sequence != sequence {
				t.Fatalf("terminal abort wrote state: sequence=%v error=%v", state, err)
			}
			if _, err := h.AbortTask(context.Background(), TaskID(^uint64(0))); !errors.Is(err, ErrNotFound) {
				t.Fatalf("unknown task error=%v, want ErrNotFound", err)
			}
			if tc.wantReport {
				select {
				case report := <-reports:
					t.Fatalf("duplicate migration report: %+v", report)
				case <-time.After(50 * time.Millisecond):
				}
			}
		})
	}
}

func TestTaskOrphanSuccessfulMigrationProbePersistsOnlyAtReservation(t *testing.T) {
	h := orphanHarness(t, HarnessOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var migrations atomic.Int32
	var abortStarted atomic.Bool
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	old := TaskDefinition{Kind: "test.orphan.migrate", Version: 1,
		Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`{"step":"old"}`), nil },
		Run:     func(context.Context, *TaskRuntime, TaskRecord) error { t.Error("orphaned task ran Run"); return nil },
	}
	id := createTaskForTest(t, h, old)
	newer := old
	newer.Version = 2
	newer.Migrate = func(version int, input, checkpoint json.RawMessage) (MigratedTask, error) {
		migrations.Add(1)
		if version != 1 {
			return MigratedTask{}, fmt.Errorf("unexpected source version %d", version)
		}
		return MigratedTask{Input: json.RawMessage(`{"step":"new-input"}`), Checkpoint: json.RawMessage(`{"step":"new-checkpoint"}`)}, nil
	}
	newer.Abort = func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		abortStarted.Store(true)
		if task.Version != 2 || string(task.Input) != `{"step":"new-input"}` || string(task.Checkpoint) != `{"step":"new-checkpoint"}` {
			t.Errorf("abort saw unmigrated task: %+v", task)
		}
		<-release
		return r.Complete(ctx, Outcome{Status: OutcomeAborted})
	}
	if err := h.registry.Install(Extension{Name: "orphan-migrate", Tasks: []TaskDefinition{newer}}); err != nil {
		t.Fatal(err)
	}
	var reports []Report
	if err := h.Commit(ctx, func(tx *Tx) error {
		task, err := tx.Task(id)
		if err != nil {
			return err
		}
		_, _, err = h.scheduler.markAbort(tx, task, false, &reports)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	h.scheduler.enqueueReports(reports)
	marked, err := h.Task(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !marked.AbortRequested || marked.Version != 1 || string(marked.Input) != "null" || string(marked.Checkpoint) != `{"step":"old"}` || migrations.Load() != 1 {
		t.Fatalf("successful marking probe persisted migration: %+v, calls=%d", marked, migrations.Load())
	}
	h.Resume()
	waitForOrphanTask(t, h, id, func(TaskRecord) bool { return abortStarted.Load() })
	reserved, err := h.Task(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if reserved.Status != "running" || reserved.Version != 2 || string(reserved.Input) != `{"step":"new-input"}` || string(reserved.Checkpoint) != `{"step":"new-checkpoint"}` || migrations.Load() != 2 {
		t.Fatalf("reservation did not atomically persist migration: %+v, calls=%d", reserved, migrations.Load())
	}
	releaseOnce.Do(func() { close(release) })
	task, err := h.WaitTask(ctx, id)
	if err != nil || task.Outcome == nil || task.Outcome.Status != OutcomeAborted {
		t.Fatalf("task=%+v error=%v", task, err)
	}
}

func TestTaskOrphanPausedRunnableAndActiveAbortAreNotJoined(t *testing.T) {
	t.Run("paused_run_never_starts_and_new_abort_is_not_joined", func(t *testing.T) {
		h := orphanHarness(t, HarnessOptions{})
		testRoot(t, h)
		started := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		var runs atomic.Int32
		def := TaskDefinition{Kind: "test.orphan.paused", Version: 1,
			Run: func(context.Context, *TaskRuntime, TaskRecord) error { runs.Add(1); return nil },
			Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
				close(started)
				<-release
				return r.Complete(ctx, Outcome{Status: OutcomeAborted})
			},
		}
		if err := h.registry.Install(Extension{Name: "orphan-paused", Tasks: []TaskDefinition{def}}); err != nil {
			t.Fatal(err)
		}
		id := createTaskForTest(t, h, def)
		result, err := h.AbortTask(context.Background(), id)
		if err != nil || result != AbortMarked {
			t.Fatalf("AbortTask=(%q, %v)", result, err)
		}
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("abort invocation did not start")
		}
		if got := runs.Load(); got != 0 {
			t.Fatalf("paused Run started before/after abort mark: %d calls", got)
		}
		task, err := h.Task(context.Background(), id)
		if err != nil || task.Status != "running" {
			t.Fatalf("AbortTask joined a newly started Abort handler: %+v, %v", task, err)
		}
		once.Do(func() { close(release) })
		if _, err := h.WaitTask(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("already_active_abort_is_not_joined", func(t *testing.T) {
		h := orphanHarness(t, HarnessOptions{})
		testRoot(t, h)
		started := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		def := TaskDefinition{Kind: "test.orphan.active-abort", Version: 1,
			Run: func(context.Context, *TaskRuntime, TaskRecord) error { t.Error("marked task ran Run"); return nil },
			Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
				close(started)
				<-release
				return r.Complete(ctx, Outcome{Status: OutcomeAborted})
			},
		}
		if err := h.registry.Install(Extension{Name: "orphan-active-abort", Tasks: []TaskDefinition{def}}); err != nil {
			t.Fatal(err)
		}
		id := createTaskForTest(t, h, def)
		if err := h.Commit(context.Background(), func(tx *Tx) error { markAbortTree(tx.state, id); return nil }); err != nil {
			t.Fatal(err)
		}
		h.Resume()
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("abort invocation did not start")
		}
		result, err := h.AbortTask(context.Background(), id)
		if err != nil || result != AbortMarked {
			t.Fatalf("AbortTask waited for active Abort: (%q, %v)", result, err)
		}
		task, err := h.Task(context.Background(), id)
		if err != nil || task.Status != "running" {
			t.Fatalf("task did not remain in active abort: %+v, %v", task, err)
		}
		once.Do(func() { close(release) })
		if _, err := h.WaitTask(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTaskOrphanAfterDefinitionDisappearsDuringRun(t *testing.T) {
	h := orphanHarness(t, HarnessOptions{})
	testRoot(t, h)
	entered := make(chan struct{})
	release := make(chan struct{})
	abortCalled := make(chan struct{}, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	def := TaskDefinition{Kind: "test.orphan.disappears", Version: 1,
		Run:   func(context.Context, *TaskRuntime, TaskRecord) error { close(entered); <-release; return nil },
		Abort: func(context.Context, *TaskRuntime, TaskRecord) error { abortCalled <- struct{}{}; return nil },
	}
	if err := h.registry.Install(Extension{Name: "orphan-disappears", Tasks: []TaskDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, def)
	h.Resume()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Run handler did not start")
	}
	h.registry.Uninstall("orphan-disappears")
	joined := make(chan struct {
		result AbortResult
		err    error
	}, 1)
	go func() {
		result, err := h.AbortTask(context.Background(), id)
		joined <- struct {
			result AbortResult
			err    error
		}{result, err}
	}()
	waitForOrphanTask(t, h, id, func(task TaskRecord) bool { return task.AbortRequested })
	select {
	case got := <-joined:
		t.Fatalf("AbortTask returned before active Run joined: %+v", got)
	default:
	}
	once.Do(func() { close(release) })
	select {
	case got := <-joined:
		if got.err != nil || got.result != AbortMarked {
			t.Fatalf("AbortTask=(%q, %v)", got.result, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AbortTask did not join the ended Run")
	}
	task, err := h.WaitTask(context.Background(), id)
	if err != nil || task.Outcome == nil || task.Outcome.Status != OutcomeOrphaned || task.Outcome.Reason != "missing_task" {
		t.Fatalf("task=%+v error=%v", task, err)
	}
	select {
	case <-abortCalled:
		t.Fatal("missing-definition orphan called Abort")
	default:
	}
}

func TestTaskOrphanWaitCancellationKeepsMarkAndCooperativeRunJoins(t *testing.T) {
	t.Run("caller_cancellation_does_not_undo_mark", func(t *testing.T) {
		h := orphanHarness(t, HarnessOptions{})
		testRoot(t, h)
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		def := TaskDefinition{Kind: "test.orphan.cancel-join", Version: 1,
			Run: func(context.Context, *TaskRuntime, TaskRecord) error { close(entered); <-release; return nil },
			Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
				return r.Complete(ctx, Outcome{Status: OutcomeAborted})
			},
		}
		if err := h.registry.Install(Extension{Name: "orphan-cancel-join", Tasks: []TaskDefinition{def}}); err != nil {
			t.Fatal(err)
		}
		id := createTaskForTest(t, h, def)
		h.Resume()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("Run handler did not start")
		}
		ctx, cancel := context.WithCancel(context.Background())
		joined := make(chan error, 1)
		go func() { _, err := h.AbortTask(ctx, id); joined <- err }()
		waitForOrphanTask(t, h, id, func(task TaskRecord) bool { return task.AbortRequested })
		cancel()
		select {
		case err := <-joined:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("join error=%v, want cancellation", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("cancelled join did not return")
		}
		if task, err := h.Task(context.Background(), id); err != nil || !task.AbortRequested {
			t.Fatalf("caller cancellation undid durable mark: %+v, %v", task, err)
		}
		once.Do(func() { close(release) })
		task, err := h.WaitTask(context.Background(), id)
		if err != nil || task.Outcome == nil || task.Outcome.Status != OutcomeAborted {
			t.Fatalf("task=%+v error=%v", task, err)
		}
	})

	t.Run("cooperative_run_is_joined", func(t *testing.T) {
		h := orphanHarness(t, HarnessOptions{})
		testRoot(t, h)
		runEntered := make(chan struct{})
		runExited := make(chan struct{})
		def := TaskDefinition{Kind: "test.orphan.cooperative", Version: 1,
			Run: func(ctx context.Context, _ *TaskRuntime, _ TaskRecord) error {
				close(runEntered)
				<-ctx.Done()
				close(runExited)
				return ctx.Err()
			},
			Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
				return r.Complete(ctx, Outcome{Status: OutcomeAborted})
			},
		}
		if err := h.registry.Install(Extension{Name: "orphan-cooperative", Tasks: []TaskDefinition{def}}); err != nil {
			t.Fatal(err)
		}
		id := createTaskForTest(t, h, def)
		h.Resume()
		select {
		case <-runEntered:
		case <-time.After(3 * time.Second):
			t.Fatal("cooperative Run did not enter")
		}
		result, err := h.AbortTask(context.Background(), id)
		if err != nil || result != AbortMarked {
			t.Fatalf("AbortTask=(%q, %v)", result, err)
		}
		select {
		case <-runExited:
		default:
			t.Fatal("AbortTask returned before cooperative Run exited")
		}
		if _, err := h.WaitTask(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTaskOrphanEndsJoinOnSessionSealWithoutWaitingForPublicDone(t *testing.T) {
	for _, failSession := range []bool{false, true} {
		name := "clean_seal"
		if failSession {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			h := orphanHarness(t, HarnessOptions{})
			testRoot(t, h)
			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			def := TaskDefinition{Kind: "test.orphan.seal." + name, Version: 1,
				Run: func(context.Context, *TaskRuntime, TaskRecord) error { close(entered); <-release; return nil },
				Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
					return r.Complete(ctx, Outcome{Status: OutcomeAborted})
				},
			}
			if err := h.registry.Install(Extension{Name: "orphan-seal-" + name, Tasks: []TaskDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			id := createTaskForTest(t, h, def)
			h.Resume()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("Run handler did not start")
			}
			joined := make(chan error, 1)
			go func() { _, err := h.AbortTask(context.Background(), id); joined <- err }()
			waitForOrphanTask(t, h, id, func(task TaskRecord) bool { return task.AbortRequested })
			var closeDone chan error
			if failSession {
				h.session.fail(errors.New("injected session failure"))
			} else {
				closeDone = make(chan error, 1)
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					closeDone <- h.Close(ctx)
				}()
			}
			deadline := time.Now().Add(3 * time.Second)
			for h.Err() == nil && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if h.Err() == nil {
				t.Fatal("Session did not seal")
			}
			select {
			case err := <-joined:
				if failSession {
					if !errors.Is(err, ErrPoisoned) {
						t.Fatalf("failure join error=%v", err)
					}
				} else if !errors.Is(err, ErrClosed) {
					t.Fatalf("clean-seal join error=%v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("AbortTask join did not end at Session seal")
			}
			select {
			case <-h.Done():
				t.Fatal("public Done closed while non-cooperative handler was still running")
			default:
			}
			once.Do(func() { close(release) })
			select {
			case <-h.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("Session did not finish after releasing task handler")
			}
			if closeDone != nil {
				if err := <-closeDone; err != nil {
					t.Fatalf("clean Close: %v", err)
				}
			}
		})
	}
}

func TestTaskOrphanOwnedWorkDrainsBeforeOwnerOrphan(t *testing.T) {
	h := orphanHarness(t, HarnessOptions{})
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	childAbortStarted := make(chan struct{})
	childRelease := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(childRelease) })
	parentDef := TaskDefinition{Kind: "test.orphan.owner-missing", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	childDef := TaskDefinition{Kind: "test.orphan.owner-child", Version: 1,
		Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
			wake := time.Now().Add(time.Hour)
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "waiting", WakeAt: &wake}, nil })
		},
		Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
			close(childAbortStarted)
			<-childRelease
			return r.Complete(ctx, Outcome{Status: OutcomeAborted})
		},
	}
	if err := h.registry.Install(Extension{Name: "orphan-owner-child", Tasks: []TaskDefinition{childDef}}); err != nil {
		t.Fatal(err)
	}
	var parent, child TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		parent, err = tx.CreateTask(c.id, parentDef, nil, TaskOptions{})
		if err != nil {
			return err
		}
		child, err = tx.CreateTask(c.id, childDef, nil, TaskOptions{Owner: Owner{Task: parent}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	waitForOrphanTask(t, h, child, func(task TaskRecord) bool { return task.Status == "sleeping" })
	result, err := h.AbortTask(ctx, parent)
	if err != nil || result != AbortMarked {
		t.Fatalf("AbortTask owner=(%q, %v)", result, err)
	}
	select {
	case <-childAbortStarted:
	case <-ctx.Done():
		t.Fatal("owned child abort did not start")
	}
	owner, err := h.Task(ctx, parent)
	if err != nil || owner.Status == "terminal" || !owner.AbortRequested {
		t.Fatalf("owner orphaned before live child drained: %+v, %v", owner, err)
	}
	once.Do(func() { close(childRelease) })
	owner, err = h.WaitTask(ctx, parent)
	if err != nil || owner.Outcome == nil || owner.Outcome.Status != OutcomeOrphaned || owner.Outcome.Reason != "missing_task" {
		t.Fatalf("owner=%+v error=%v", owner, err)
	}
	settledChild, err := h.Task(ctx, child)
	if err != nil || settledChild.Outcome == nil || settledChild.Outcome.Status != OutcomeAborted {
		t.Fatalf("child=%+v error=%v", settledChild, err)
	}
}

func TestTaskOrphanRestartMarkWaitsForHostAndAbortOwnedPreservesIt(t *testing.T) {
	t.Run("explicit_abort_upgrades_blocked_restart", func(t *testing.T) {
		h := orphanHarness(t, HarnessOptions{})
		testRoot(t, h)
		def := TaskDefinition{Kind: "test.orphan.restart", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { t.Error("blocked task ran Run"); return nil }}
		id := createTaskForTest(t, h, def)
		if err := h.Commit(context.Background(), func(tx *Tx) error { markAbortTreeReason(tx.state, id, "restart"); return nil }); err != nil {
			t.Fatal(err)
		}
		h.Resume()
		time.Sleep(50 * time.Millisecond)
		task, err := h.Task(context.Background(), id)
		if err != nil || task.Status == "terminal" || !task.AbortRequested || task.AbortReason != "restart" {
			t.Fatalf("restart mark alone did not remain blocked: %+v, %v", task, err)
		}
		result, err := h.AbortTask(context.Background(), id)
		if err != nil || result != AbortMarked {
			t.Fatalf("host abort upgrade=(%q, %v)", result, err)
		}
		task, err = h.Task(context.Background(), id)
		if err != nil || task.Outcome == nil || task.Outcome.Status != OutcomeOrphaned || task.Outcome.Reason != "missing_task" || task.AbortReason == "restart" {
			t.Fatalf("explicit abort did not upgrade and orphan: %+v, %v", task, err)
		}
	})

	t.Run("abort_owned_keeps_restart_reason_until_upgrade", func(t *testing.T) {
		h := orphanHarness(t, HarnessOptions{})
		c := testRoot(t, h)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var childID TaskID
		childDef := TaskDefinition{Kind: "test.orphan.abort-owned-child", Version: 1,
			Run: func(context.Context, *TaskRuntime, TaskRecord) error {
				t.Error("restart-marked child ran Run")
				return nil
			},
			Abort: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
				if task.AbortReason != "restart" {
					t.Errorf("AbortOwned replaced restart reason with %q", task.AbortReason)
				}
				return r.Complete(ctx, Outcome{Status: OutcomeAborted})
			},
		}
		abortOwnedStarted := make(chan struct{})
		parentDef := TaskDefinition{Kind: "test.orphan.abort-owned-parent", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
			close(abortOwnedStarted)
			if err := r.AbortOwned(ctx, childID); err != nil {
				return err
			}
			return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
		}}
		if err := h.registry.Install(Extension{Name: "orphan-abort-owned", Tasks: []TaskDefinition{parentDef}}); err != nil {
			t.Fatal(err)
		}
		var parent TaskID
		if err := h.Commit(ctx, func(tx *Tx) error {
			var err error
			parent, err = tx.CreateTask(c.id, parentDef, nil, TaskOptions{})
			if err != nil {
				return err
			}
			childID, err = tx.CreateTask(c.id, childDef, nil, TaskOptions{Owner: Owner{Task: parent}})
			if err != nil {
				return err
			}
			markAbortTreeReason(tx.state, childID, "restart")
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		before, err := h.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		h.Resume()
		select {
		case <-abortOwnedStarted:
		case <-ctx.Done():
			t.Fatal("parent did not attempt AbortOwned")
		}
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			state, snapshotErr := h.Snapshot(ctx)
			if snapshotErr != nil {
				t.Fatal(snapshotErr)
			}
			if state.Sequence >= before.Sequence+2 { // parent reservation plus AbortOwned's mark commit
				break
			}
			time.Sleep(time.Millisecond)
		}
		state, err := h.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if state.Sequence < before.Sequence+2 {
			t.Fatalf("AbortOwned mark was not committed: sequence=%d", state.Sequence)
		}
		child, err := h.Task(ctx, childID)
		if err != nil || child.Status == "terminal" || child.AbortReason != "restart" {
			t.Fatalf("AbortOwned orphaned/upgraded restart child: %+v, %v", child, err)
		}
		if err := h.registry.Install(Extension{Name: "orphan-abort-owned-child", Tasks: []TaskDefinition{childDef}}); err != nil {
			t.Fatal(err)
		}
		parentTask, err := h.WaitTask(ctx, parent)
		if err != nil || parentTask.Outcome == nil || parentTask.Outcome.Status != OutcomeCompleted {
			t.Fatalf("parent=%+v error=%v", parentTask, err)
		}
		child, err = h.Task(ctx, childID)
		if err != nil || child.Outcome == nil || child.Outcome.Status != OutcomeAborted || child.AbortReason != "restart" {
			t.Fatalf("child=%+v error=%v", child, err)
		}
	})
}

func TestConversationAbortOrphansBlockedTask(t *testing.T) {
	h := orphanHarness(t, HarnessOptions{})
	c := testRoot(t, h)
	def := TaskDefinition{Kind: "test.orphan.conversation", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { t.Error("missing task ran Run"); return nil }}
	id := createTaskForTest(t, h, def)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Abort(ctx, false); err != nil {
		t.Fatalf("Conversation.Abort did not settle blocked task: %v", err)
	}
	task, err := h.Task(ctx, id)
	if err != nil || task.Outcome == nil || task.Outcome.Status != OutcomeOrphaned || task.Outcome.Reason != "missing_task" {
		t.Fatalf("task=%+v error=%v", task, err)
	}
}

func TestTaskRuntimeOutcomesOrderValidationAndDetachment(t *testing.T) {
	h := orphanHarness(t, HarnessOptions{})
	c := testRoot(t, h)
	def := TaskDefinition{Kind: "test.orphan.outcomes", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	var first, second, live TaskID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		first, err = tx.CreateTask(c.id, def, nil, TaskOptions{})
		if err != nil {
			return err
		}
		second, err = tx.CreateTask(c.id, def, nil, TaskOptions{})
		if err != nil {
			return err
		}
		live, err = tx.CreateTask(c.id, def, nil, TaskOptions{})
		if err != nil {
			return err
		}
		tx.state.Tasks[first].Outcome = &Outcome{Status: OutcomeCompleted, Result: json.RawMessage(`{"value":"first"}`)}
		terminalizeTask(tx, tx.state.Tasks[first], time.Now())
		tx.state.Tasks[second].Outcome = &Outcome{Status: OutcomeFailed, Error: "second failed", Result: json.RawMessage(`{"value":"second"}`)}
		terminalizeTask(tx, tx.state.Tasks[second], time.Now())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runtime := &TaskRuntime{h: h, ID: first}
	outcomes, err := runtime.Outcomes(context.Background(), []TaskID{second, first})
	if err != nil || len(outcomes) != 2 || outcomes[0].Status != OutcomeFailed || outcomes[1].Status != OutcomeCompleted {
		t.Fatalf("outcomes=%+v error=%v", outcomes, err)
	}
	outcomes[0].Result[2] = 'X'
	outcomes[1].Result[2] = 'Y'
	stored, err := h.Task(context.Background(), second)
	if err != nil || string(stored.Outcome.Result) != `{"value":"second"}` {
		t.Fatalf("Outcomes leaked mutable result bytes: %+v, %v", stored, err)
	}
	if _, err := runtime.Outcomes(context.Background(), []TaskID{live}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nonterminal outcome read error=%v", err)
	}
	if _, err := runtime.Outcomes(context.Background(), []TaskID{TaskID(^uint64(0))}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing outcome read error=%v", err)
	}
}

func TestTaskOrphanBuiltinGenerationCleanupIsAtomic(t *testing.T) {
	h := orphanHarness(t, HarnessOptions{})
	c := testRoot(t, h)
	ctx := context.Background()
	def := generationTaskDefinition()
	input, _ := json.Marshal(generationInput{Submissions: []SubmissionID{77}})
	checkpoint, _ := json.Marshal(generationCheckpoint{Phase: "stream", Partial: "partial output"})
	var id TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		id, err = tx.CreateTask(c.id, def, input, TaskOptions{})
		if err != nil {
			return err
		}
		task := tx.state.Tasks[id]
		task.Version = def.Version + 1
		task.Checkpoint = checkpoint
		tx.state.Conversations[c.id].Busy = id
		tx.state.Submissions[77] = &SubmissionRecord{ID: 77, Conversation: c.id, Status: "running", Request: SubmissionRequest{Type: "input"}, CreatedAt: time.Now()}
		return tx.PutDocument(orphanTaskDocument, uint64(id), json.RawMessage(`{"retire":true}`))
	}); err != nil {
		t.Fatal(err)
	}
	before, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := h.AbortTask(ctx, id); err != nil || result != AbortMarked {
		t.Fatalf("AbortTask=(%q, %v)", result, err)
	}
	after, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := after.Tasks[id]
	if task.Outcome == nil || task.Outcome.Status != OutcomeOrphaned || task.Outcome.Reason != "task_too_old" {
		t.Fatalf("generation outcome=%+v", task.Outcome)
	}
	if after.Conversations[c.id].Busy != 0 {
		t.Fatal("generation cleanup did not clear Busy")
	}
	if submission := after.Submissions[77]; submission.Status != "unanswered" || submission.Reason != "task_too_old" {
		t.Fatalf("submission cleanup=%+v", submission)
	}
	if _, ok := after.Documents[documentKey(orphanTaskDocument, uint64(id))]; ok {
		t.Fatal("generation orphan retained task document")
	}
	if after.Sequence != before.Sequence+1 {
		t.Fatalf("orphan cleanup was not one atomic commit: %d -> %d", before.Sequence, after.Sequence)
	}
	var salvaged bool
	for _, entry := range after.Entries {
		if entry.Conversation == c.id && entry.Kind == "pi.assistant" && entry.Message != nil && entry.Message.Content == "partial output" && entry.Message.Aborted {
			salvaged = true
		}
	}
	if !salvaged {
		t.Fatal("generation partial was not salvaged as an aborted assistant entry")
	}
}
