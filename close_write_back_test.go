package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Close releases only this host's wrapper, so a later host can reopen the
// committed memory. The wrappers are never owned by two live Sessions.
type closeWriteBackStorage struct {
	*MemoryStorage
	fail         atomic.Bool
	writeErr     error
	applyOnError bool
	closeErr     error
	writes       atomic.Int32
	closes       atomic.Int32
	blockNext    atomic.Bool
	entered      chan struct{}
	release      chan struct{}
	closed       chan struct{}
}

func (s *closeWriteBackStorage) Commit(ctx context.Context, seq uint64, next *State) error {
	s.writes.Add(1)
	if s.blockNext.Swap(false) {
		close(s.entered)
		<-s.release
	}
	if s.fail.Load() {
		if s.applyOnError {
			if err := s.MemoryStorage.Commit(ctx, seq, next); err != nil {
				return err
			}
		}
		return s.writeErr
	}
	return s.MemoryStorage.Commit(ctx, seq, next)
}

func (s *closeWriteBackStorage) Close() error {
	s.closes.Add(1)
	if s.closed != nil {
		close(s.closed)
	}
	return s.closeErr
}

func startCloseWriteBackTask(t *testing.T, ctx context.Context, store Storage, onReport func(Report), run func(context.Context) error) (*Harness, TaskID) {
	t.Helper()
	started := make(chan TaskRecord, 1)
	def := TaskDefinition{Kind: "test.close-write-back", Version: 1,
		Run: func(ctx context.Context, _ *TaskRuntime, task TaskRecord) error {
			started <- task
			if run != nil {
				return run(ctx)
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "close-write-back", Tasks: []TaskDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	h, err := Open(ctx, store, HarnessOptions{Registry: registry, OnReport: onReport})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.Close(cleanup); err != nil {
			// A test may intentionally give the backend a close error.
			if s, ok := store.(*closeWriteBackStorage); !ok || err != s.closeErr {
				t.Error(err)
			}
		}
	})
	id := createTaskForTest(t, h, def)
	if err := h.Commit(ctx, func(tx *Tx) error {
		task := tx.state.mutTask(id)
		task.Restarts = 2
		task.Checkpoint = json.RawMessage(`{"phase":"effect"}`)
		task.Memos = map[string]json.RawMessage{"receipt": json.RawMessage(`42`)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	select {
	case task := <-started:
		if task.Restarts != 2 {
			t.Fatalf("reservation changed count: %+v", task)
		}
	case <-ctx.Done():
		t.Fatal("task did not start")
	}
	return h, id
}

func loadCloseWriteBackState(t *testing.T, store *MemoryStorage) *State {
	t.Helper()
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestCloseWriteBackOnlyStoppedTasksWithoutPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &closeWriteBackStorage{MemoryStorage: NewMemoryStorage()}
	h, id := startCloseWriteBackTask(t, ctx, store, nil, nil)
	// A stored running record without an invocation is not one close stopped.
	var other TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		other, err = tx.CreateTask(tx.state.mutTask(id).Conversation, TaskDefinition{Kind: "test.missing", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}, nil, TaskOptions{})
		if err == nil {
			tx.state.mutTask(other).Status = "running"
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	watch, stop, err := h.session.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	<-watch
	before := loadCloseWriteBackState(t, store.MemoryStorage)
	writes := store.writes.Load()
	var afterCommits atomic.Int32
	afterCommit := h.session.afterCommit
	h.session.mu.Lock()
	h.session.afterCommit = func(st *State) { afterCommit(st); afterCommits.Add(1) }
	h.session.mu.Unlock()
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if next, ok := <-watch; ok {
		t.Fatalf("close write-back published: %+v", next)
	}
	got := loadCloseWriteBackState(t, store.MemoryStorage)
	before.Sequence++
	before.Tasks[id].Status = "pending"
	if !reflect.DeepEqual(got, before) || got.Tasks[other].Status != "running" {
		t.Fatalf("close changed more than stopped status: got=%+v, task=%+v", got, got.Tasks[id])
	}
	if store.writes.Load() != writes+1 || afterCommits.Load() != 1 || store.closes.Load() != 1 {
		t.Fatalf("writes=%d hook=%d closes=%d", store.writes.Load()-writes, afterCommits.Load(), store.closes.Load())
	}
	if err := h.Close(ctx); err != nil || store.writes.Load() != writes+1 {
		t.Fatalf("repeated close wrote again: %v", err)
	}
	// The next host counts only the record that close did not stop.
	next, err := Open(ctx, &closeWriteBackStorage{MemoryStorage: store.MemoryStorage}, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(ctx)
	state, err := next.Snapshot(ctx)
	if err != nil || state.Tasks[id].Restarts != 2 || state.Tasks[other].Restarts != 1 {
		t.Fatalf("reopen task=%+v other=%+v error=%v", state.Tasks[id], state.Tasks[other], err)
	}
}

func TestCloseWriteBackPreservesAdmittedAbortMark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &closeWriteBackStorage{MemoryStorage: NewMemoryStorage(), entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(store.release) })
	h, id := startCloseWriteBackTask(t, ctx, store, nil, nil)
	store.blockNext.Store(true)
	committed := make(chan error, 1)
	go func() {
		committed <- h.Commit(ctx, func(tx *Tx) error {
			markAbortTreeReason(tx.state, id, "restart")
			return nil
		})
	}()
	select {
	case <-store.entered:
	case <-ctx.Done():
		t.Fatal("abort mark did not enter storage")
	}
	// Sealing is independent of the commit mutex; write-back must follow the
	// admitted abort mark and use its new record, not a reservation snapshot.
	h.session.startClose()
	release.Do(func() { close(store.release) })
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	end, err := h.WaitClosed(ctx)
	if err != nil || end.Kind != "closed" {
		t.Fatalf("end=%+v error=%v", end, err)
	}
	task := loadCloseWriteBackState(t, store.MemoryStorage).Tasks[id]
	if task.Status != "pending" || !task.AbortRequested || task.AbortReason != "restart" || task.Restarts != 0 || task.Memos["receipt"] == nil || string(task.Checkpoint) != `{"phase":"effect"}` {
		t.Fatalf("close lost admitted state: %+v", task)
	}
}

func TestCloseWriteBackKeepsAdmittedNonRunningState(t *testing.T) {
	for _, status := range []string{"pending", "terminal"} {
		t.Run(status, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store := &closeWriteBackStorage{MemoryStorage: NewMemoryStorage(), entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(store.release) })
			h, id := startCloseWriteBackTask(t, ctx, store, nil, nil)
			writes := store.writes.Load()
			store.blockNext.Store(true)
			committed := make(chan error, 1)
			go func() {
				committed <- h.Commit(ctx, func(tx *Tx) error {
					tr := TaskTransition{Status: status}
					if status == "terminal" {
						tr.Outcome = &Outcome{Status: OutcomeCompleted}
					}
					return applyTransition(tx, tx.state.mutTask(id), tr, h.now())
				})
			}()
			select {
			case <-store.entered:
			case <-ctx.Done():
				t.Fatal("transition did not enter storage")
			}
			h.session.startClose()
			release.Do(func() { close(store.release) })
			if err := <-committed; err != nil {
				t.Fatal(err)
			}
			if _, err := h.WaitClosed(ctx); err != nil {
				t.Fatal(err)
			}
			task := loadCloseWriteBackState(t, store.MemoryStorage).Tasks[id]
			if task.Status != status || task.Outcome != nil && task.Outcome.Status != OutcomeCompleted {
				t.Fatalf("close undid admitted transition: %+v", task)
			}
			if store.writes.Load() != writes+1 {
				t.Fatalf("close wrote a redundant state: writes=%d", store.writes.Load()-writes)
			}
		})
	}
}

func TestCloseWriteBackFailuresAreSessionFailuresNotCloseErrors(t *testing.T) {
	cause := errors.New("write-back failed")
	backendClose := errors.New("backend close failed")
	for _, tc := range []struct {
		name     string
		writeErr error
		closeErr error
		applied  bool
	}{
		{name: "uncertain", writeErr: cause},
		{name: "rejected", writeErr: &RejectedError{Err: cause}},
		{name: "rejected_cancellation", writeErr: &RejectedError{Err: context.Canceled}},
		{name: "storage_decision_sentinel", writeErr: errNoChange},
		{name: "wrapped_storage_decision_sentinel", writeErr: fmt.Errorf("storage: %w", errNoChange)},
		{name: "also_backend_close", writeErr: cause, closeErr: backendClose},
		{name: "uncertain_applied", writeErr: cause, applied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store := &closeWriteBackStorage{MemoryStorage: NewMemoryStorage(), writeErr: tc.writeErr, closeErr: tc.closeErr, applyOnError: tc.applied}
			reports := make(chan Report, 2)
			h, id := startCloseWriteBackTask(t, ctx, store, func(r Report) { reports <- r }, nil)
			before := loadCloseWriteBackState(t, store.MemoryStorage)
			writes := store.writes.Load()
			store.fail.Store(true)
			if err := h.Close(ctx); err != tc.closeErr {
				t.Fatalf("Close=%v, want backend close error %v", err, tc.closeErr)
			}
			end, err := h.WaitClosed(ctx)
			if err != nil || end.Kind != "failed" || end.Err != tc.writeErr || !errors.Is(h.Err(), ErrPoisoned) {
				t.Fatalf("end=%+v error=%v Session=%v", end, err, h.Err())
			}
			select {
			case report := <-reports:
				if report.Kind != ReportStorageFailed || report.Err != tc.writeErr {
					t.Fatalf("report=%+v", report)
				}
			case <-ctx.Done():
				t.Fatal("no storage_failed report")
			}
			if store.writes.Load() != writes+1 || store.closes.Load() != 1 {
				t.Fatalf("writes=%d closes=%d", store.writes.Load()-writes, store.closes.Load())
			}
			// Never adopt a guess after an uncertain result, even if the backend
			// did apply it. Reopen, not the failed Session, decides that fact.
			if h.session.state.Sequence != before.Sequence || h.session.state.Tasks[id].Status != "running" {
				t.Fatal("failed write-back was adopted in memory")
			}
			next, err := Open(ctx, &closeWriteBackStorage{MemoryStorage: store.MemoryStorage}, HarnessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close(ctx)
			task, err := next.Task(ctx, id)
			wantCount := 3
			if tc.applied {
				wantCount = 2
			}
			if err != nil || task.Status != "pending" || task.Restarts != wantCount {
				t.Fatalf("recovered task=%+v error=%v", task, err)
			}
		})
	}
}

func TestCloseWriteBackFailureShutdownWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cause := errors.New("host crashed")
	store := &closeWriteBackStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause}
	h, id := startCloseWriteBackTask(t, ctx, store, nil, nil)
	before := loadCloseWriteBackState(t, store.MemoryStorage)
	writes := store.writes.Load()
	store.fail.Store(true)
	if err := h.Commit(ctx, nil); err != cause {
		t.Fatal(err)
	}
	if _, err := h.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
	if got := loadCloseWriteBackState(t, store.MemoryStorage); !reflect.DeepEqual(got, before) || store.writes.Load() != writes+1 {
		t.Fatal("failure shutdown wrote back running tasks")
	}
	next, err := Open(ctx, &closeWriteBackStorage{MemoryStorage: store.MemoryStorage}, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(ctx)
	task, err := next.Task(ctx, id)
	if err != nil || task.Restarts != 3 || task.Status != "pending" {
		t.Fatalf("recovered task=%+v error=%v", task, err)
	}
}

func TestCloseWriteBackCapturesInvocationBeforeSchedulerClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	store := &closeWriteBackStorage{MemoryStorage: NewMemoryStorage()}
	h, id := startCloseWriteBackTask(t, ctx, store, nil, func(context.Context) error { <-release; return nil })
	inv := h.scheduler.active(id)
	// Stop admission, but deliberately do not run beforeClose yet.
	h.session.sealAdmission()
	once.Do(func() { close(release) })
	select {
	case <-inv.done:
	case <-ctx.Done():
		t.Fatal("invocation did not finish after the seal")
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	task := loadCloseWriteBackState(t, store.MemoryStorage).Tasks[id]
	if task.Status != "pending" || task.Restarts != 2 {
		t.Fatalf("invocation in seal/join window was missed: %+v", task)
	}
}

type cancelOpenWriteBackStorage struct {
	*closeWriteBackStorage
	cancel context.CancelFunc
}

func (s *cancelOpenWriteBackStorage) Load(ctx context.Context) (*State, error) {
	state, err := s.MemoryStorage.Load(ctx)
	s.cancel()
	return state, err
}

func TestCloseWriteBackCancelledOpenPreservesCrashEvidence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	memory := NewMemoryStorage()
	memory.state = versionedState(2)
	memory.state.Tasks[1] = &TaskRecord{ID: 1, Kind: "test.cut-off", Version: 1, Status: "running", Restarts: 2}
	before := loadCloseWriteBackState(t, memory)
	store := &closeWriteBackStorage{MemoryStorage: memory, closed: make(chan struct{})}
	if _, err := Open(ctx, &cancelOpenWriteBackStorage{closeWriteBackStorage: store, cancel: cancel}, HarnessOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open=%v", err)
	}
	select {
	case <-store.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled open did not close")
	}
	if got := loadCloseWriteBackState(t, memory); !reflect.DeepEqual(got, before) || store.writes.Load() != 0 {
		t.Fatal("cancelled open erased evidence without counting it")
	}
}

func TestCloseWriteBackContinuesAfterCloseWaitCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	store := &closeWriteBackStorage{MemoryStorage: NewMemoryStorage()}
	h, id := startCloseWriteBackTask(t, ctx, store, nil, func(ctx context.Context) error {
		<-ctx.Done()
		<-release
		return ctx.Err()
	})
	wait, stop := context.WithCancel(ctx)
	closed := make(chan error, 1)
	go func() { closed <- h.Close(wait) }()
	<-h.session.closing
	stop()
	if err := <-closed; !errors.Is(err, context.Canceled) {
		t.Fatalf("Close wait=%v", err)
	}
	if store.closes.Load() != 0 {
		t.Fatal("backend closed before task joined")
	}
	once.Do(func() { close(release) })
	end, err := h.WaitClosed(ctx)
	if err != nil || end.Kind != "closed" {
		t.Fatalf("end=%+v error=%v", end, err)
	}
	if task := loadCloseWriteBackState(t, store.MemoryStorage).Tasks[id]; task.Status != "pending" || task.Restarts != 2 {
		t.Fatalf("cancelled Close wait cancelled write-back: %+v", task)
	}
}
