package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskHandoverPreservesMemosAndRejectsOldRuntime(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "same-version", 2: "migrated"}[version], func(t *testing.T) {
			h := testHarness(t)
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var active atomic.Int32
			var oldRuntime *TaskRuntime
			old := TaskDefinition{Kind: "test.handover", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, record TaskRecord) error {
				active.Add(1)
				defer active.Add(-1)
				oldRuntime = r
				if _, err := r.Memo(ctx, "memo", json.RawMessage(`{"kept":true}`)); err != nil {
					return err
				}
				if err := r.Commit(ctx, func(*Tx) (TaskTransition, error) {
					return TaskTransition{Status: "running", Checkpoint: json.RawMessage(`{"phase":1}`)}, nil
				}); err != nil {
					return err
				}
				before := r.Registry()
				close(started)
				<-release
				if before != r.Registry() {
					t.Error("registry snapshot changed within phase")
				}
				return nil
			}}
			if err := h.registry.Install(Extension{Name: "handover", Tasks: []TaskDefinition{old}}); err != nil {
				t.Fatal(err)
			}
			id := createTaskForTest(t, h, old)
			h.Resume()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			next := old
			next.Version = version
			next.Migrate = func(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
				if from != 1 || string(checkpoint) != `{"phase":1}` {
					t.Errorf("migration: from=%d checkpoint=%s", from, checkpoint)
				}
				return MigratedTask{Input: json.RawMessage(`{"migrated":true}`), Checkpoint: json.RawMessage(`{"phase":2}`)}, nil
			}
			next.Run = func(ctx context.Context, r *TaskRuntime, record TaskRecord) error {
				if active.Load() != 0 {
					t.Error("handover overlapped old handler")
				}
				if record.Version != version || version == 2 && string(record.Input) != `{"migrated":true}` {
					t.Errorf("bad migration: %+v", record)
				}
				if value, err := r.Memo(ctx, "memo", nil); err != nil || string(value) != `{"kept":true}` {
					t.Errorf("memo=%s error=%v", value, err)
				}
				if err := oldRuntime.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "running"}, nil }); !errors.Is(err, ErrClosed) {
					t.Errorf("old runtime accepted commit: %v", err)
				}
				if r.Registry() == oldRuntime.Registry() {
					t.Error("new invocation reused old snapshot")
				}
				return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
			}
			if err := h.registry.Install(Extension{Name: "handover", Tasks: []TaskDefinition{next}}); err != nil {
				t.Fatal(err)
			}
			once.Do(func() { close(release) })
			record, err := h.WaitTask(ctx, id)
			if err != nil || record.Outcome.Status != OutcomeCompleted {
				t.Fatalf("record=%+v error=%v", record, err)
			}
		})
	}
}

func TestTaskHandoverUnusableReplacementKeepsOldDefinition(t *testing.T) {
	for _, mode := range []string{"missing", "too-old", "no-migrate"} {
		t.Run(mode, func(t *testing.T) {
			reports := make(chan Report, 8)
			h := taskTestHarness(t, HarnessOptions{OnReport: func(r Report) { reports <- r }})
			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var calls atomic.Int32
			var phaseSnapshot *RegistrySnapshot
			old := TaskDefinition{Kind: "test.outdated", Version: 2, Run: func(ctx context.Context, r *TaskRuntime, record TaskRecord) error {
				n := calls.Add(1)
				if n == 1 {
					phaseSnapshot = r.Registry()
					close(started)
					<-release
					if r.Registry() != phaseSnapshot {
						t.Error("phase snapshot refreshed early")
					}
				} else if r.Registry() == phaseSnapshot {
					t.Error("next phase did not refresh snapshot")
				}
				if n == 3 {
					return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
				}
				data, _ := json.Marshal(n)
				return r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "running", Checkpoint: data}, nil })
			}}
			if err := h.registry.Install(Extension{Name: "outdated", Tasks: []TaskDefinition{old}}); err != nil {
				t.Fatal(err)
			}
			id := createTaskForTest(t, h, old)
			h.Resume()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			reason := "missing_task"
			if mode == "missing" {
				h.registry.Uninstall("outdated")
			} else {
				next := old
				next.Version = 1
				reason = "task_too_old"
				if mode == "no-migrate" {
					next.Version = 3
					reason = "migration_failed"
				}
				next.Run = func(context.Context, *TaskRuntime, TaskRecord) error { t.Error("unusable replacement ran"); return nil }
				if err := h.registry.Install(Extension{Name: "outdated", Tasks: []TaskDefinition{next}}); err != nil {
					t.Fatal(err)
				}
			}
			once.Do(func() { close(release) })
			record, err := h.WaitTask(ctx, id)
			if err != nil || record.Outcome.Status != OutcomeCompleted || calls.Load() != 3 {
				t.Fatalf("record=%+v calls=%d error=%v", record, calls.Load(), err)
			}
			if err := h.Close(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-h.reports.workerDone:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if len(reports) != 1 {
				t.Fatalf("reports=%d want 1", len(reports))
			}
			r := <-reports
			if r.Kind != ReportTaskOutdated || r.Reason != reason || r.Extension != "outdated" {
				t.Fatalf("report=%+v", r)
			}
		})
	}
}

func TestTaskHandoverFailedMigrationBlocks(t *testing.T) {
	reports := make(chan Report, 4)
	h := taskTestHarness(t, HarnessOptions{OnReport: func(r Report) { reports <- r }})
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	old := TaskDefinition{Kind: "test.handover-fail", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		if err := r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			return TaskTransition{Status: "running", Checkpoint: json.RawMessage(`1`)}, nil
		}); err != nil {
			return err
		}
		close(started)
		<-release
		return nil
	}}
	if err := h.registry.Install(Extension{Name: "handover-fail", Tasks: []TaskDefinition{old}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, old)
	h.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	next := old
	next.Version = 2
	next.Migrate = func(int, json.RawMessage, json.RawMessage) (MigratedTask, error) {
		return MigratedTask{}, errors.New("migration broken")
	}
	if err := h.registry.Install(Extension{Name: "handover-fail", Tasks: []TaskDefinition{next}}); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	select {
	case r := <-reports:
		if r.Kind != ReportMigrationFailed {
			t.Fatalf("report=%+v", r)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	inspection, err := h.Inspect(ctx)
	if err != nil || len(inspection.Tasks) != 1 || inspection.Tasks[0].State.Reason != "migration_failed" || inspection.Tasks[0].Record.Version != 1 {
		t.Fatalf("inspection=%+v error=%v", inspection, err)
	}
	if h.Err() != nil {
		t.Fatal(h.Err())
	}
	_ = id
}
