package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskSchedulerCommitDecisionsAndCancellation(t *testing.T) {
	h := testHarness(t)
	ctx := context.Background()
	before, _ := h.Snapshot(ctx)
	committed, err := h.scheduler.commitInternal(ctx, func(*Tx) error { return errNoChange })
	after, _ := h.Snapshot(ctx)
	if err != nil || committed || after.Sequence != before.Sequence {
		t.Fatalf("no-op committed=%v error=%v", committed, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	committed, err = h.scheduler.commitInternal(cancelled, nil)
	if committed || !errors.Is(err, context.Canceled) || h.Err() != nil {
		t.Fatalf("pre-admission cancellation: committed=%v error=%v Session=%v", committed, err, h.Err())
	}
	cancelled, cancel = context.WithCancel(ctx)
	committed, err = h.scheduler.commitInternal(cancelled, func(*Tx) error { cancel(); return nil })
	if committed || !errors.Is(err, context.Canceled) || h.Err() != nil {
		t.Fatalf("pre-storage cancellation: committed=%v error=%v Session=%v", committed, err, h.Err())
	}
}

func TestTaskSchedulerRejectedWriteFailsButCallerRejectionDoesNot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cause := &RejectedError{Err: ErrConflict}
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause}
	h, err := Open(ctx, store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	store.fail.Store(true)
	if err := h.Commit(ctx, nil); err != cause || h.Err() != nil {
		t.Fatalf("caller rejection: error=%v Session=%v", err, h.Err())
	}
	committed, err := h.scheduler.commitInternal(ctx, func(*Tx) error { return nil })
	if committed || err != cause || !errors.Is(h.Err(), ErrPoisoned) || !errors.Is(h.Err(), cause) {
		t.Fatalf("internal rejection: committed=%v error=%v Session=%v", committed, err, h.Err())
	}
	if _, err := h.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskSchedulerHealthyErrorsCannotMasqueradeAsSealedAdmission(t *testing.T) {
	for _, cause := range []error{fmt.Errorf("callback: %w", ErrClosed), fmt.Errorf("callback: %w", ErrPoisoned), context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			h := testHarness(t)
			committed, err := h.scheduler.commitInternal(context.Background(), func(*Tx) error { return cause })
			if committed || err != cause || !errors.Is(h.Err(), cause) || h.session.failure.Load() == nil {
				t.Fatalf("committed=%v error=%v Session=%v", committed, err, h.Err())
			}
		})
	}
}

type cancelledReconcileStorage struct {
	*MemoryStorage
	cancel  context.CancelFunc
	closed  chan struct{}
	release chan struct{}
}

func (s *cancelledReconcileStorage) Load(ctx context.Context) (*State, error) {
	state, err := s.MemoryStorage.Load(ctx)
	s.cancel()
	return state, err
}
func (s *cancelledReconcileStorage) Close() error {
	close(s.closed)
	<-s.release
	return s.MemoryStorage.Close()
}
func TestTaskSchedulerCancelledReconcileDoesNotFailOrWaitForBackend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &cancelledReconcileStorage{MemoryStorage: NewMemoryStorage(), cancel: cancel, closed: make(chan struct{}), release: make(chan struct{})}
	defer close(store.release)
	reports := make(chan Report, 1)
	opened := make(chan error, 1)
	go func() {
		_, err := Open(ctx, store, HarnessOptions{OnReport: func(report Report) { reports <- report }})
		opened <- err
	}()
	select {
	case err := <-opened:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Open error=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled Open waited for backend closure")
	}
	select {
	case <-store.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled Open did not start cleanup")
	}
	select {
	case report := <-reports:
		t.Fatalf("cancelled reconciliation reported failure: %+v", report)
	default:
	}
}

type cancelBookkeepingStorage struct {
	*MemoryStorage
	cancel   context.CancelFunc
	reject   bool
	writeErr error
}

func (s *cancelBookkeepingStorage) Commit(ctx context.Context, seq uint64, next *State) error {
	if s.cancel == nil {
		return s.MemoryStorage.Commit(ctx, seq, next)
	}
	s.cancel()
	cause := s.writeErr
	if cause == nil {
		cause = context.Canceled
	}
	if s.reject {
		return &RejectedError{Err: cause}
	}
	return fmt.Errorf("uncertain: %w", cause)
}
func TestTaskSchedulerStorageDecisionSentinelIsNotANoOp(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncertain", true: "rejected"}[rejected], func(t *testing.T) {
			var cause error = errNoChange
			if rejected {
				cause = &RejectedError{Err: cause}
			}
			store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause}
			h, err := Open(context.Background(), store, HarnessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close(context.Background())
			store.fail.Store(true)
			committed, err := h.scheduler.commitInternal(context.Background(), nil)
			if committed || err != cause || !errors.Is(h.Err(), ErrPoisoned) {
				t.Fatalf("storage error swallowed: committed=%v error=%v Session=%v", committed, err, h.Err())
			}
		})
	}
}

func TestTaskSchedulerCertifiedDeadlineIsCallerCancellation(t *testing.T) {
	store := &cancelBookkeepingStorage{MemoryStorage: NewMemoryStorage(), reject: true, writeErr: context.DeadlineExceeded}
	h, err := Open(context.Background(), store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.cancel = cancel
	committed, err := h.scheduler.commitInternal(ctx, nil)
	if committed || !errors.Is(err, context.DeadlineExceeded) || h.Err() != nil {
		t.Fatalf("certified cancellation: committed=%v error=%v Session=%v", committed, err, h.Err())
	}
}

func TestTaskSchedulerCancelledWriteRequiresCertifiedRejection(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncertain", true: "rejected"}[reject], func(t *testing.T) {
			store := &cancelBookkeepingStorage{MemoryStorage: NewMemoryStorage(), reject: reject}
			h, err := Open(context.Background(), store, HarnessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close(context.Background())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store.cancel = cancel
			committed, err := h.scheduler.commitInternal(ctx, func(*Tx) error { return nil })
			if committed || !errors.Is(err, context.Canceled) {
				t.Fatalf("committed=%v error=%v", committed, err)
			}
			if reject && h.Err() != nil || !reject && !errors.Is(h.Err(), ErrPoisoned) {
				t.Fatalf("reject=%v Session=%v", reject, h.Err())
			}
		})
	}
}

func TestTaskOrphanRejectedWriteFailsSession(t *testing.T) {
	cause := &RejectedError{Err: errors.New("orphan write rejected")}
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause}
	h, err := Open(context.Background(), store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	definition := TaskDefinition{Kind: "test.orphan-rejection", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	id := createTaskForTest(t, h, definition)
	if _, err := h.AbortTask(context.Background(), id+100); !errors.Is(err, ErrNotFound) || h.Err() != nil {
		t.Fatalf("invalid request failed Session: %v, %v", err, h.Err())
	}
	store.fail.Store(true)
	if _, err := h.AbortTask(context.Background(), id); err != cause || !errors.Is(h.Err(), cause) {
		t.Fatalf("orphan rejection: error=%v Session=%v", err, h.Err())
	}
}

func TestTaskMigrationCacheSurvivesCancelledRejectedOrphanWrite(t *testing.T) {
	store := &cancelBookkeepingStorage{MemoryStorage: NewMemoryStorage(), reject: true}
	reports := make(chan Report, 4)
	h, err := Open(context.Background(), store, HarnessOptions{OnReport: func(r Report) { reports <- r }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	definition := TaskDefinition{Kind: "test.cached-orphan", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	id := createTaskForTest(t, h, definition)
	var attempts atomic.Int32
	newer := definition
	newer.Version = 2
	newer.Migrate = func(int, json.RawMessage, json.RawMessage) (MigratedTask, error) {
		attempts.Add(1)
		return MigratedTask{}, errors.New("migration failed")
	}
	if err := h.registry.Install(Extension{Name: "cached-orphan", Tasks: []TaskDefinition{newer}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.cancel = cancel
	if _, err := h.AbortTask(ctx, id); !errors.Is(err, context.Canceled) || h.Err() != nil {
		t.Fatalf("cancelled orphan: error=%v Session=%v", err, h.Err())
	}
	store.cancel = nil
	if _, err := h.AbortTask(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.reports.workerDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if attempts.Load() != 1 || len(reports) != 1 {
		t.Fatalf("migration attempts=%d reports=%d", attempts.Load(), len(reports))
	}
	if report := <-reports; report.Kind != ReportMigrationFailed {
		t.Fatalf("report=%+v", report)
	}
}

func TestTaskFaultWriteFailureEndsSessionWithoutRerun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cause := &RejectedError{Err: errors.New("fault write rejected")}
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause}
	reports := make(chan Report, 4)
	h, err := Open(ctx, store, HarnessOptions{OnReport: func(r Report) { reports <- r }})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	var runs atomic.Int32
	def := TaskDefinition{Kind: "test.fault-write", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		runs.Add(1)
		store.fail.Store(true)
		return errors.New("handler failed")
	}}
	if err := h.registry.Install(Extension{Name: "fault-write", Tasks: []TaskDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, def)
	if _, err := h.WaitTask(ctx, id); !errors.Is(err, ErrPoisoned) || !errors.Is(err, cause) {
		t.Fatalf("WaitTask error=%v", err)
	}
	if _, err := h.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.reports.workerDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if runs.Load() != 1 || len(reports) != 1 {
		t.Fatalf("runs=%d reports=%d", runs.Load(), len(reports))
	}
	if r := <-reports; r.Kind != ReportStorageFailed || r.Err != cause {
		t.Fatalf("report=%+v", r)
	}
}

func TestTaskReportFromCommitCallbackIsQueuedAndReentrant(t *testing.T) {
	var h *Harness
	readDone := make(chan error, 1)
	h = taskTestHarness(t, HarnessOptions{OnReport: func(report Report) {
		if report.Kind != ReportTaskError {
			readDone <- fmt.Errorf("unexpected report: %v", report.Kind)
			return
		}
		if _, err := h.Snapshot(context.Background()); err != nil {
			readDone <- err
			return
		}
		_, err := h.Inspect(context.Background())
		readDone <- err
	}})
	def := TaskDefinition{Kind: "test.report-commit", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			r.Report(errors.New("queued, not inline"))
			return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted}}, nil
		})
	}}
	if err := h.registry.Install(Extension{Name: "report-commit", Tasks: []TaskDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, def)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.WaitTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("queued reporter deadlocked callback")
	}
}
