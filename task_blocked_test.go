package durable

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type taskBlockedStorageSpy struct {
	*MemoryStorage
	mu      sync.Mutex
	commits []*State
}

func (s *taskBlockedStorageSpy) Commit(ctx context.Context, expected uint64, next *State) error {
	if err := s.MemoryStorage.Commit(ctx, expected, next); err != nil {
		return err
	}
	copy, err := cloneState(next)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.commits = append(s.commits, copy)
	s.mu.Unlock()
	return nil
}

func (s *taskBlockedStorageSpy) committedAfter(sequence uint64) []*State {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*State
	for _, state := range s.commits {
		if state.Sequence > sequence {
			copy, _ := cloneState(state)
			out = append(out, copy)
		}
	}
	return out
}

func openTaskBlockedHarness(t *testing.T, store Storage, registry *Registry, reports chan<- Report, now func() time.Time) *Harness {
	t.Helper()
	if store == nil {
		store = NewMemoryStorage()
	}
	if registry == nil {
		registry = NewRegistry()
	}
	options := HarnessOptions{Model: echoModel{}, Registry: registry}
	if reports != nil {
		options.OnReport = func(report Report) { reports <- report }
	}
	if now != nil {
		options.Now = now
	}
	h, err := Open(context.Background(), store, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return h
}

func installTaskBlockedDefinition(t *testing.T, registry *Registry, extension string, definition TaskDefinition) {
	t.Helper()
	if err := registry.Install(Extension{Name: extension, Tasks: []TaskDefinition{definition}}); err != nil {
		t.Fatal(err)
	}
}

func awaitTaskBlockedReport(t *testing.T, reports <-chan Report, kind ReportKind, id TaskID) Report {
	t.Helper()
	select {
	case report := <-reports:
		if report.Kind != kind || report.Task == nil || report.Task.ID != id {
			t.Fatalf("report=%+v, want kind=%q task=%d", report, kind, id)
		}
		return report
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %q report for task %d", kind, id)
		return Report{}
	}
}

func awaitTaskBlockedRun(t *testing.T, started <-chan TaskRecord) TaskRecord {
	t.Helper()
	select {
	case task := <-started:
		return task
	case <-time.After(5 * time.Second):
		t.Fatal("task Run did not start")
		return TaskRecord{}
	}
}

func assertNoTaskBlockedReport(t *testing.T, reports <-chan Report) {
	t.Helper()
	select {
	case report := <-reports:
		t.Fatalf("unexpected report: %+v", report)
	default:
	}
}

func waitForTaskBlockedTask(t *testing.T, h *Harness, id TaskID) TaskRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatalf("WaitTask(%d): %v", id, err)
	}
	return task
}

func TestTaskBlockedMissingAndNewerDefinitionsWakeOnInstall(t *testing.T) {
	t.Run("missing definition remains live and install wakes it", func(t *testing.T) {
		registry := NewRegistry()
		h := openTaskBlockedHarness(t, nil, registry, nil, nil)
		var runs atomic.Int32
		started := make(chan TaskRecord, 1)
		def := TaskDefinition{Kind: "blocked.missing", Version: 1, Run: func(ctx context.Context, rt *TaskRuntime, task TaskRecord) error {
			runs.Add(1)
			started <- task
			return rt.Complete(ctx, Outcome{Status: OutcomeCompleted})
		}}
		id := createTaskForTest(t, h, def)
		inspection, err := h.Inspect(context.Background())
		if err != nil || len(inspection.Tasks) != 1 || inspection.Tasks[0].Record.ID != id || inspection.Tasks[0].Record.Status != "pending" || inspection.Tasks[0].State.Kind != "blocked" || inspection.Tasks[0].State.Reason != "missing_task" {
			t.Fatalf("missing task inspection=%+v error=%v", inspection, err)
		}
		barrierStarted := make(chan TaskRecord, 1)
		barrierRelease := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-barrierRelease:
			default:
				close(barrierRelease)
			}
		})
		barrier := TaskDefinition{Kind: "blocked.missing-barrier", Version: 1, Run: func(ctx context.Context, rt *TaskRuntime, task TaskRecord) error {
			barrierStarted <- task
			<-barrierRelease
			return rt.Complete(ctx, Outcome{Status: OutcomeCompleted})
		}}
		installTaskBlockedDefinition(t, registry, "missing-barrier-extension", barrier)
		barrierID := createTaskForTest(t, h, barrier)

		h.Resume()
		awaitTaskBlockedRun(t, barrierStarted) // This later ID proves the missing task was examined first.
		installTaskBlockedDefinition(t, registry, "missing-extension", def)
		awaitTaskBlockedRun(t, started)
		task := waitForTaskBlockedTask(t, h, id)
		if task.Status != "terminal" || task.Outcome == nil || task.Outcome.Status != OutcomeCompleted || runs.Load() != 1 {
			t.Fatalf("task=%+v runs=%d", task, runs.Load())
		}
		close(barrierRelease)
		waitForTaskBlockedTask(t, h, barrierID)
	})

	t.Run("newer stored definition stays blocked until a fitting install", func(t *testing.T) {
		registry := NewRegistry()
		var runs atomic.Int32
		started := make(chan TaskRecord, 1)
		run := func(ctx context.Context, rt *TaskRuntime, task TaskRecord) error {
			runs.Add(1)
			started <- task
			return rt.Complete(ctx, Outcome{Status: OutcomeCompleted})
		}
		old := TaskDefinition{Kind: "blocked.newer", Version: 1, Run: run}
		stored := TaskDefinition{Kind: "blocked.newer", Version: 2, Run: run}
		installTaskBlockedDefinition(t, registry, "newer-extension", old)
		h := openTaskBlockedHarness(t, nil, registry, nil, nil)
		id := createTaskForTest(t, h, stored)
		inspection, err := h.Inspect(context.Background())
		if err != nil || len(inspection.Tasks) != 1 || inspection.Tasks[0].Record.Status != "pending" || inspection.Tasks[0].State.Kind != "blocked" || inspection.Tasks[0].State.Reason != "task_too_old" {
			t.Fatalf("newer task inspection=%+v error=%v", inspection, err)
		}
		before, err := h.Task(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		barrierStarted := make(chan TaskRecord, 1)
		barrierRelease := make(chan struct{})
		t.Cleanup(func() {
			select {
			case <-barrierRelease:
			default:
				close(barrierRelease)
			}
		})
		barrier := TaskDefinition{Kind: "blocked.newer-barrier", Version: 1, Run: func(ctx context.Context, rt *TaskRuntime, task TaskRecord) error {
			barrierStarted <- task
			<-barrierRelease
			return rt.Complete(ctx, Outcome{Status: OutcomeCompleted})
		}}
		installTaskBlockedDefinition(t, registry, "newer-barrier-extension", barrier)
		barrierID := createTaskForTest(t, h, barrier)

		h.Resume()
		awaitTaskBlockedRun(t, barrierStarted) // The scheduler has passed the too-new task.
		blocked, err := h.Inspect(context.Background())
		if err != nil || len(blocked.Tasks) != 2 || blocked.Tasks[0].Record.ID != id || blocked.Tasks[0].Record.Status != "pending" || blocked.Tasks[0].State.Kind != "blocked" || blocked.Tasks[0].State.Reason != "task_too_old" {
			t.Fatalf("task did not remain blocked while incompatible: inspection=%+v error=%v", blocked, err)
		}
		if err := registry.Install(Extension{Name: "newer-extension", Tasks: []TaskDefinition{stored}}); err != nil {
			t.Fatal(err)
		}
		awaitTaskBlockedRun(t, started)
		task := waitForTaskBlockedTask(t, h, id)
		if task.Status != "terminal" || task.Outcome == nil || task.Outcome.Status != OutcomeCompleted || runs.Load() != 1 {
			t.Fatalf("task=%+v runs=%d", task, runs.Load())
		}
		if before.Version != 2 {
			t.Fatalf("stored version unexpectedly changed before install: %+v", before)
		}
		close(barrierRelease)
		waitForTaskBlockedTask(t, h, barrierID)
	})
}

func TestTaskBlockedMigrationReservationIsOneAtomicStorageCommit(t *testing.T) {
	registry := NewRegistry()
	store := &taskBlockedStorageSpy{MemoryStorage: NewMemoryStorage()}
	h := openTaskBlockedHarness(t, store, registry, nil, nil)
	old := TaskDefinition{Kind: "blocked.atomic", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		t.Error("old handler ran after migration")
		return nil
	}}
	root := testRoot(t, h)
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		_, err = tx.CreateTask(root.ID(), old, json.RawMessage(`{"source":"old"}`), TaskOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var id TaskID
	for taskID := range state.Tasks {
		id = taskID
	}
	before := state.Tasks[id]
	started := make(chan TaskRecord, 1)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	newer := TaskDefinition{Kind: old.Kind, Version: 2,
		Migrate: func(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
			if from != 1 || string(input) != `{"source":"old"}` || string(checkpoint) != `{}` {
				t.Errorf("migration inputs: from=%d input=%s checkpoint=%s", from, input, checkpoint)
			}
			return MigratedTask{Input: json.RawMessage(`{"source":"new"}`), Checkpoint: json.RawMessage(`{"phase":"new"}`)}, nil
		},
		Run: func(ctx context.Context, rt *TaskRuntime, task TaskRecord) error {
			started <- task
			<-release
			return rt.Complete(ctx, Outcome{Status: OutcomeCompleted})
		},
	}
	installTaskBlockedDefinition(t, registry, "atomic-extension", newer)
	h.Resume()
	runRecord := awaitTaskBlockedRun(t, started)
	if runRecord.Version != 2 || string(runRecord.Input) != `{"source":"new"}` || string(runRecord.Checkpoint) != `{"phase":"new"}` || runRecord.Status != "running" || runRecord.StartedAt == nil {
		t.Fatalf("handler observed partially migrated reservation: %+v", runRecord)
	}

	commits := store.committedAfter(state.Sequence)
	if len(commits) != 1 {
		t.Fatalf("post-task storage commits=%d, want exactly one reservation commit", len(commits))
	}
	reserved := commits[0].Tasks[id]
	if reserved == nil || reserved.Status != "running" || reserved.Version != 2 || string(reserved.Input) != `{"source":"new"}` || string(reserved.Checkpoint) != `{"phase":"new"}` || reserved.StartedAt == nil {
		t.Fatalf("storage reservation was not atomic: %+v", reserved)
	}
	if before.Version != 1 || before.Status != "pending" || string(before.Input) != `{"source":"old"}` || string(before.Checkpoint) != `{}` {
		t.Fatalf("unexpected pre-migration record: %+v", before)
	}
	close(release)
	waitForTaskBlockedTask(t, h, id)
}

func TestTaskBlockedFailedMigrationPreservesSatisfiedWaitAndRetriesOnlyOnReinstall(t *testing.T) {
	registry := NewRegistry()
	reports := make(chan Report, 8)
	h := openTaskBlockedHarness(t, nil, registry, reports, nil)
	old := TaskDefinition{Kind: "blocked.retry", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		t.Error("blocked old handler ran")
		return nil
	}}
	childDef := TaskDefinition{Kind: "blocked.satisfied-child", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		t.Error("satisfied child unexpectedly ran")
		return nil
	}}
	var id TaskID
	root := testRoot(t, h)
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		childID, err := tx.CreateTask(root.ID(), childDef, nil, TaskOptions{})
		if err != nil {
			return err
		}
		id, err = tx.CreateTask(root.ID(), old, json.RawMessage(`{"keep":true}`), TaskOptions{})
		if err != nil {
			return err
		}
		child := tx.state.mutTask(childID)
		child.Status = "terminal"
		child.Outcome = &Outcome{Status: OutcomeCompleted}
		parent := tx.state.mutTask(id)
		parent.Status = "waiting"
		parent.WaitOn = []TaskID{childID}
		parent.WaitPolicy = "allSettled"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	failure := errors.New("migration declined")
	newer := TaskDefinition{Kind: old.Kind, Version: 2,
		Migrate: func(int, json.RawMessage, json.RawMessage) (MigratedTask, error) {
			attempts.Add(1)
			return MigratedTask{}, failure
		},
		Run: func(context.Context, *TaskRuntime, TaskRecord) error {
			t.Error("handler ran after failed migration")
			return nil
		},
	}
	installTaskBlockedDefinition(t, registry, "retry-extension", newer)
	before, err := h.Task(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	h.Resume()
	first := awaitTaskBlockedReport(t, reports, ReportMigrationFailed, id)
	if first.Extension != "retry-extension" || !errors.Is(first.Err, failure) {
		t.Fatalf("migration report=%+v", first)
	}
	assertTaskBlockedRecordUnchanged(t, h, id, before)
	if attempts.Load() != 1 || h.Err() != nil {
		t.Fatalf("attempts=%d Session.Err()=%v", attempts.Load(), h.Err())
	}

	// An unrelated registry replacement wakes the scheduler but must not clear
	// the cached failure for this task's effective definition identity.
	unrelated := TaskDefinition{Kind: "blocked.unrelated", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	installTaskBlockedDefinition(t, registry, "unrelated-extension", unrelated)
	barrierStarted := make(chan TaskRecord, 1)
	barrierRelease := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-barrierRelease:
		default:
			close(barrierRelease)
		}
	})
	barrier := TaskDefinition{Kind: "blocked.retry-barrier", Version: 1, Run: func(ctx context.Context, rt *TaskRuntime, task TaskRecord) error {
		barrierStarted <- task
		<-barrierRelease
		return rt.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	installTaskBlockedDefinition(t, registry, "barrier-extension", barrier)
	barrierID := createTaskForTest(t, h, barrier)
	awaitTaskBlockedRun(t, barrierStarted) // The pass examined the earlier failed task first.
	if attempts.Load() != 1 || h.Err() != nil {
		t.Fatalf("unrelated install retried migration: attempts=%d Session.Err()=%v", attempts.Load(), h.Err())
	}
	assertTaskBlockedRecordUnchanged(t, h, id, before)
	assertNoTaskBlockedReport(t, reports)

	// Reinstalling the same Go values intentionally creates a new identity and
	// gives the failed migration one new attempt.
	if err := registry.Install(Extension{Name: "retry-extension", Tasks: []TaskDefinition{newer}}); err != nil {
		t.Fatal(err)
	}
	second := awaitTaskBlockedReport(t, reports, ReportMigrationFailed, id)
	if second.Extension != "retry-extension" || !errors.Is(second.Err, failure) {
		t.Fatalf("retry report=%+v", second)
	}
	if attempts.Load() != 2 || h.Err() != nil {
		t.Fatalf("reinstall attempts=%d Session.Err()=%v", attempts.Load(), h.Err())
	}
	assertTaskBlockedRecordUnchanged(t, h, id, before)
	close(barrierRelease)
	waitForTaskBlockedTask(t, h, barrierID)
}

func assertTaskBlockedRecordUnchanged(t *testing.T, h *Harness, id TaskID, before TaskRecord) {
	t.Helper()
	after, err := h.Task(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("blocked task record changed:\nbefore=%+v\nafter =%+v", before, after)
	}
}

func TestTaskBlockedMissingMigrationIsOnlyCachedAndReportedOnScheduling(t *testing.T) {
	registry := NewRegistry()
	reports := make(chan Report, 4)
	h := openTaskBlockedHarness(t, nil, registry, reports, nil)
	old := TaskDefinition{Kind: "blocked.no-migrate", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		t.Error("handler ran without a migration")
		return nil
	}}
	var runs atomic.Int32
	newer := TaskDefinition{Kind: old.Kind, Version: 2, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		runs.Add(1)
		return nil
	}}
	id := createTaskForTest(t, h, old)
	installTaskBlockedDefinition(t, registry, "no-migrate-extension", newer)
	before, err := h.Task(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}

	inspection, err := h.Inspect(context.Background())
	if err != nil || len(inspection.Tasks) != 1 || inspection.Tasks[0].State.Kind != "blocked" || inspection.Tasks[0].State.Reason != "migration_failed" || inspection.Tasks[0].State.Err == nil {
		t.Fatalf("inspection=%+v error=%v", inspection, err)
	}
	h.scheduler.mu.Lock()
	_, cached := h.scheduler.migrations[id]
	h.scheduler.mu.Unlock()
	if cached {
		t.Fatal("inspection cached a missing migration")
	}
	assertNoTaskBlockedReport(t, reports)

	h.Resume()
	report := awaitTaskBlockedReport(t, reports, ReportMigrationFailed, id)
	if report.Extension != "no-migrate-extension" || report.Err == nil {
		t.Fatalf("migration report=%+v", report)
	}
	assertTaskBlockedRecordUnchanged(t, h, id, before)
	inspection, err = h.Inspect(context.Background())
	if err != nil || len(inspection.Tasks) != 1 || inspection.Tasks[0].State.Kind != "blocked" || inspection.Tasks[0].State.Reason != "migration_failed" || inspection.Tasks[0].State.Err == nil {
		t.Fatalf("post-attempt inspection=%+v error=%v", inspection, err)
	}
	h.scheduler.mu.Lock()
	failure, cached := h.scheduler.migrations[id]
	h.scheduler.mu.Unlock()
	if !cached || failure.err == nil || runs.Load() != 0 || h.Err() != nil {
		t.Fatalf("migration failure cache=%+v cached=%v runs=%d Session.Err()=%v", failure, cached, runs.Load(), h.Err())
	}
	assertNoTaskBlockedReport(t, reports)
}

func TestTaskBlockedInvalidAndPanickingMigrationsDoNotFailSession(t *testing.T) {
	cases := []struct {
		name    string
		migrate func() (MigratedTask, error)
	}{
		{name: "invalid JSON", migrate: func() (MigratedTask, error) {
			return MigratedTask{Input: json.RawMessage(`{`), Checkpoint: json.RawMessage(`{}`)}, nil
		}},
		{name: "panic", migrate: func() (MigratedTask, error) {
			panic("migration panic")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewRegistry()
			reports := make(chan Report, 4)
			h := openTaskBlockedHarness(t, nil, registry, reports, nil)
			var runs atomic.Int32
			old := TaskDefinition{Kind: "blocked.invalid-" + tc.name, Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
			var attempts atomic.Int32
			newer := TaskDefinition{Kind: old.Kind, Version: 2,
				Migrate: func(int, json.RawMessage, json.RawMessage) (MigratedTask, error) {
					attempts.Add(1)
					return tc.migrate()
				},
				Run: func(context.Context, *TaskRuntime, TaskRecord) error {
					runs.Add(1)
					return nil
				},
			}
			id := createTaskForTest(t, h, old)
			installTaskBlockedDefinition(t, registry, "invalid-migration-extension", newer)
			before, err := h.Task(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			h.Resume()
			report := awaitTaskBlockedReport(t, reports, ReportMigrationFailed, id)
			if report.Extension != "invalid-migration-extension" || report.Err == nil {
				t.Fatalf("migration report=%+v", report)
			}
			assertTaskBlockedRecordUnchanged(t, h, id, before)
			inspection, err := h.Inspect(context.Background())
			if err != nil || len(inspection.Tasks) != 1 || inspection.Tasks[0].State.Kind != "blocked" || inspection.Tasks[0].State.Reason != "migration_failed" || inspection.Tasks[0].State.Err == nil {
				t.Fatalf("inspection=%+v error=%v", inspection, err)
			}
			if attempts.Load() != 1 || runs.Load() != 0 || h.Err() != nil {
				t.Fatalf("attempts=%d runs=%d Session.Err()=%v", attempts.Load(), runs.Load(), h.Err())
			}
			assertNoTaskBlockedReport(t, reports)
		})
	}
}

func TestTaskBlockedInspectionDerivesStatesWithoutCallingUserCode(t *testing.T) {
	registry := NewRegistry()
	reports := make(chan Report, 4)
	fixedNow := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	h := openTaskBlockedHarness(t, nil, registry, reports, func() time.Time { return fixedNow })
	var runCalls, migrateCalls atomic.Int32
	definition := func(kind string, version int) TaskDefinition {
		return TaskDefinition{Kind: kind, Version: version, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
			runCalls.Add(1)
			return nil
		}}
	}
	currentMigration := definition("inspect.migrate", 2)
	currentMigration.Migrate = func(int, json.RawMessage, json.RawMessage) (MigratedTask, error) {
		migrateCalls.Add(1)
		return MigratedTask{Input: json.RawMessage(`{}`), Checkpoint: json.RawMessage(`{}`)}, nil
	}
	installed := []TaskDefinition{
		definition("inspect.running", 1), definition("inspect.waiting", 1), definition("inspect.sleeping", 1),
		definition("inspect.completing", 1), definition("inspect.child", 1), currentMigration,
		definition("inspect.too-old", 2), definition("inspect.abort", 1), definition("inspect.restart", 1),
	}
	if err := registry.Install(Extension{Name: "inspection", Tasks: installed}); err != nil {
		t.Fatal(err)
	}
	root := testRoot(t, h)
	var ids = map[string]TaskID{}
	oldMigration := definition("inspect.migrate", 1)
	storedTooOld := definition("inspect.too-old", 3)
	missing := definition("inspect.missing", 1)
	terminal := definition("inspect.terminal", 1)
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		create := func(name string, def TaskDefinition, opts TaskOptions) error {
			ids[name], err = tx.CreateTask(root.ID(), def, nil, opts)
			return err
		}
		for _, item := range []struct {
			name string
			def  TaskDefinition
		}{
			{"running", definition("inspect.running", 1)}, {"waiting", definition("inspect.waiting", 1)},
			{"sleeping", definition("inspect.sleeping", 1)}, {"completing", definition("inspect.completing", 1)},
			{"migrates", oldMigration}, {"cached-block", oldMigration}, {"missing", missing},
			{"too-old", storedTooOld}, {"abort", definition("inspect.abort", 1)},
			{"restart", definition("inspect.restart", 1)}, {"terminal", terminal},
		} {
			if err = create(item.name, item.def, TaskOptions{}); err != nil {
				return err
			}
		}
		ids["child"], err = tx.CreateTask(root.ID(), definition("inspect.child", 1), nil, TaskOptions{Owner: Owner{Task: ids["completing"]}})
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		tx.state.mutTask(ids["running"]).Status = "running"
		waiting := tx.state.mutTask(ids["waiting"])
		waiting.Status = "waiting"
		waiting.WaitOn = []TaskID{ids["migrates"]}
		waiting.WaitPolicy = "allSettled"
		sleeping := tx.state.mutTask(ids["sleeping"])
		sleeping.Status = "waiting"
		wake := fixedNow.Add(time.Hour)
		sleeping.WakeAt = &wake
		completing := tx.state.mutTask(ids["completing"])
		completing.Status = "completing"
		completing.Outcome = &Outcome{Status: OutcomeCompleted}
		for _, name := range []string{"abort", "restart"} {
			task := tx.state.mutTask(ids[name])
			task.Status = "waiting"
			wake := fixedNow.Add(time.Hour)
			task.WakeAt = &wake
			task.AbortRequested = true
			if name == "restart" {
				task.AbortReason = "restart"
			}
		}
		tx.state.mutTask(ids["terminal"]).Status = "terminal"
		tx.state.Submissions[90] = &SubmissionRecord{ID: 90, Status: "queued"}
		tx.state.Submissions[12] = &SubmissionRecord{ID: 12, Status: "placed"}
		tx.state.Submissions[3] = &SubmissionRecord{ID: 3, Status: "done"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	fakeCtx, cancel := context.WithCancel(context.Background())
	resolved, ok := registry.Snapshot().resolved("inspect.migrate")
	if !ok {
		t.Fatal("migration definition missing from registry")
	}
	h.scheduler.mu.Lock()
	h.scheduler.invocations[ids["running"]] = &taskInvocation{ctx: fakeCtx, cancel: cancel, done: make(chan struct{})}
	h.scheduler.migrations[ids["cached-block"]] = migrationFailure{identity: resolved.Identity, err: errors.New("previous migration failed")}
	h.scheduler.mu.Unlock()

	inspection, err := h.Inspect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantStates := map[string]TaskInspectionState{
		"running":      {Kind: "running"},
		"waiting":      {Kind: "waiting", On: []TaskID{ids["migrates"]}},
		"sleeping":     {Kind: "sleeping", Until: func() *time.Time { value := fixedNow.Add(time.Hour); return &value }()},
		"completing":   {Kind: "completing"},
		"migrates":     {Kind: "pending", Migrates: true},
		"cached-block": {Kind: "blocked", Reason: "migration_failed"},
		"missing":      {Kind: "blocked", Reason: "missing_task"},
		"too-old":      {Kind: "blocked", Reason: "task_too_old"},
		"abort":        {Kind: "pending"},
		"restart":      {Kind: "pending"},
		"child":        {Kind: "pending"},
	}
	wantIDs := make([]TaskID, 0, len(wantStates))
	for name := range wantStates {
		wantIDs = append(wantIDs, ids[name])
	}
	sort.Slice(wantIDs, func(i, j int) bool { return wantIDs[i] < wantIDs[j] })
	gotIDs := make([]TaskID, 0, len(inspection.Tasks))
	for _, item := range inspection.Tasks {
		gotIDs = append(gotIDs, item.Record.ID)
		name := ""
		for candidate, id := range ids {
			if item.Record.ID == id {
				name = candidate
				break
			}
		}
		want, ok := wantStates[name]
		if !ok || item.State.Kind != want.Kind || item.State.Reason != want.Reason || item.State.Migrates != want.Migrates || !reflect.DeepEqual(item.State.On, want.On) {
			t.Errorf("task %q inspection state=%+v, want %+v", name, item.State, want)
		}
		if want.Until != nil && (item.State.Until == nil || !item.State.Until.Equal(*want.Until)) {
			t.Errorf("sleeping task Until=%v, want %v", item.State.Until, want.Until)
		}
		if name == "cached-block" && item.State.Err == nil {
			t.Error("cached migration failure was omitted from inspection")
		}
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("inspection task IDs=%v, want sorted live IDs %v", gotIDs, wantIDs)
	}
	if len(inspection.Submissions) != 2 || inspection.Submissions[0].ID != 12 || inspection.Submissions[1].ID != 90 {
		t.Fatalf("inspection submissions=%+v, want sorted queued/running submissions [12, 90]", inspection.Submissions)
	}
	if runCalls.Load() != 0 || migrateCalls.Load() != 0 {
		t.Fatalf("inspection ran user code: Run calls=%d Migrate calls=%d", runCalls.Load(), migrateCalls.Load())
	}
	assertNoTaskBlockedReport(t, reports)

	h.scheduler.mu.Lock()
	delete(h.scheduler.invocations, ids["running"])
	h.scheduler.mu.Unlock()
	cancel()
}
