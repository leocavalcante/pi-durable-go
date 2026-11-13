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

type lifecycleStorage struct {
	*MemoryStorage
	fail          atomic.Bool
	writeErr      error
	closeErr      error
	applyOnError  bool
	closes        atomic.Int32
	closeStarted  chan struct{}
	closeReleased chan struct{}
}

func (s *lifecycleStorage) Commit(ctx context.Context, seq uint64, next *State) error {
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
func (s *lifecycleStorage) Close() error {
	s.closes.Add(1)
	if s.closeStarted != nil {
		close(s.closeStarted)
	}
	if s.closeReleased != nil {
		<-s.closeReleased
	}
	_ = s.MemoryStorage.Close()
	return s.closeErr
}

func TestSessionFailureEndsWatchesAndPreservesFirstCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cause := errors.New("private uncertain write failure")
	closeErr := errors.New("secondary close failure")
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause, closeErr: closeErr,
		applyOnError: true, closeStarted: make(chan struct{}), closeReleased: make(chan struct{})}
	store.fail.Store(true)
	s, err := NewSession(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	var release sync.Once
	defer func() { release.Do(func() { close(store.closeReleased) }); _ = s.Close() }()
	watch, stop, err := s.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	<-watch
	if err := s.Commit(ctx, func(tx *Tx) error {
		tx.state.Documents["uncertain"] = &DocumentRecord{Value: json.RawMessage(`{"applied":true}`)}
		return nil
	}); err != cause {
		t.Fatalf("triggering call did not return original failure: %v", err)
	}
	select {
	case _, ok := <-watch:
		if ok {
			t.Fatal("uncertain candidate published to a watch")
		}
	case <-ctx.Done():
		t.Fatal("watch survived a poisoned Session")
	}
	var failed *SessionFailedError
	if err := s.Err(); !errors.Is(err, ErrPoisoned) || !errors.Is(err, cause) || !errors.As(err, &failed) {
		t.Fatalf("failure=%v", err)
	}
	if s.Err().Error() != ErrPoisoned.Error() {
		t.Fatal("failure wrapper disclosed private cause")
	}
	callback := false
	if err := s.Commit(ctx, func(*Tx) error { callback = true; return nil }); !errors.Is(err, cause) || callback {
		t.Fatalf("failed admission: callback=%v error=%v", callback, err)
	}
	if _, err := s.Snapshot(ctx); !errors.Is(err, cause) {
		t.Fatalf("snapshot failure=%v", err)
	}
	if _, _, err := s.Watch(ctx); !errors.Is(err, cause) {
		t.Fatalf("watch failure=%v", err)
	}
	select {
	case <-store.closeStarted:
	case <-ctx.Done():
		t.Fatal("failed Session did not automatically close storage")
	}
	short, shortCancel := context.WithCancel(ctx)
	shortCancel()
	if _, err := s.WaitClosed(short); !errors.Is(err, context.Canceled) {
		t.Fatalf("close observation ignored caller cancellation: %v", err)
	}
	select {
	case <-s.Done():
		t.Fatal("Done closed before backend closure")
	default:
	}
	// The backend may have applied the write, but the poisoned Session's last
	// adopted value is unchanged. Recovery must reopen, not publish a guess.
	s.mu.Lock()
	seq := s.state.Sequence
	s.mu.Unlock()
	if seq != 0 {
		t.Fatal("uncertain write adopted in memory")
	}
	release.Do(func() { close(store.closeReleased) })
	end, err := s.WaitClosed(ctx)
	if err != nil || end.Kind != "failed" || end.Err != cause {
		t.Fatalf("end=%+v error=%v", end, err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Close(); err != closeErr {
				t.Errorf("close error=%v", err)
			}
		}()
	}
	wg.Wait()
	if store.closes.Load() != 1 || !errors.Is(s.Err(), cause) {
		t.Fatalf("closes=%d, cause=%v", store.closes.Load(), s.Err())
	}
}

func TestSessionCleanCloseAndCloseFailure(t *testing.T) {
	for _, closeErr := range []error{nil, errors.New("close failed")} {
		store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), closeErr: closeErr}
		s, err := NewSession(context.Background(), store, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Waiting for shutdown never initiates it.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := s.WaitClosed(ctx); !errors.Is(err, context.Canceled) || store.closes.Load() != 0 || s.Err() != nil {
			t.Fatalf("observation initiated shutdown: closes=%d error=%v", store.closes.Load(), err)
		}
		if err := s.Close(); err != closeErr {
			t.Fatalf("close=%v", err)
		}
		end, err := s.WaitClosed(context.Background())
		if err != nil || end.Err != closeErr {
			t.Fatalf("end=%+v error=%v", end, err)
		}
		if closeErr == nil {
			if end.Kind != "closed" || !errors.Is(s.Err(), ErrClosed) {
				t.Fatalf("end=%+v status=%v", end, s.Err())
			}
		} else if end.Kind != "failed" || !errors.Is(s.Err(), closeErr) || !errors.Is(s.Err(), ErrPoisoned) {
			t.Fatalf("end=%+v status=%v", end, s.Err())
		}
	}
}

// Storage failures are final (upstream): a rejection, certified or not,
// fails the Session.
func TestStorageRejectionFailsTheSession(t *testing.T) {
	cause := errors.New("certified not applied")
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: &RejectedError{Err: cause}}
	store.fail.Store(true)
	s, err := NewSession(context.Background(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Commit(context.Background(), touchStorage); !errors.Is(err, cause) {
		t.Fatalf("rejection=%v", err)
	}
	if !errors.Is(s.Err(), ErrPoisoned) || !errors.Is(s.Err(), cause) {
		t.Fatalf("Session=%v", s.Err())
	}
	end, err := s.WaitClosed(context.Background())
	if err != nil || end.Kind != "failed" || store.closes.Load() != 1 {
		t.Fatalf("end=%+v error=%v closes=%d", end, err, store.closes.Load())
	}
}

type cancelOnFailureModel struct {
	started   chan struct{}
	cancelled chan struct{}
}

func (m *cancelOnFailureModel) Generate(ctx context.Context, _ ModelRequest, _ func(string) error) (ModelResponse, error) {
	close(m.started)
	<-ctx.Done()
	close(m.cancelled)
	return ModelResponse{}, ctx.Err()
}

func TestHarnessFailureEndsPendingWaitsAndObservers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cause := errors.New("lost storage acknowledgement")
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause}
	model := &cancelOnFailureModel{started: make(chan struct{}), cancelled: make(chan struct{})}
	h, err := Open(ctx, store, HarnessOptions{Model: model})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "model"}}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmissionRequest{Content: "hold the model"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-model.started:
	case <-ctx.Done():
		t.Fatal("model did not start")
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	taskID := state.Conversations[root.ID()].Busy
	views, stopViews, err := root.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stopViews()
	<-views
	doc := DocumentDefinition{Kind: "test.failure-watch", Version: 1, Scope: "session"}
	documents, stopDocuments, err := WatchDocument(ctx, h, doc, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer stopDocuments()
	<-documents
	subResult, taskResult := make(chan error, 1), make(chan error, 1)
	go func() { _, err := sub.Wait(ctx); subResult <- err }()
	go func() { _, err := h.WaitTask(ctx, taskID); taskResult <- err }()
	// Both pending waits must have registered before the failure; this also
	// covers the existing observer paths, not only future calls.
	for {
		h.session.mu.Lock()
		registered := len(h.session.watchers)
		h.session.mu.Unlock()
		if registered == 4 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("waits did not register")
		case <-time.After(time.Millisecond):
		}
	}
	store.fail.Store(true)
	if err := h.Commit(ctx, func(tx *Tx) error {
		return tx.PutDocument(doc, 0, json.RawMessage(`{"notPublished":true}`))
	}); err != cause {
		t.Fatalf("triggering error=%v", err)
	}
	for _, result := range []<-chan error{subResult, taskResult} {
		select {
		case err := <-result:
			if !errors.Is(err, ErrPoisoned) || !errors.Is(err, cause) {
				t.Fatalf("wait lost first failure: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("wait hung after Session failure")
		}
	}
	select {
	case _, ok := <-views:
		if ok {
			t.Fatal("view remained open after failure")
		}
	case <-ctx.Done():
		t.Fatal("view watch did not end")
	}
	select {
	case _, ok := <-documents:
		if ok {
			t.Fatal("uncertain document update published")
		}
	case <-ctx.Done():
		t.Fatal("document watch did not end")
	}
	select {
	case <-model.cancelled:
	case <-ctx.Done():
		t.Fatal("fatal Session did not signal running model")
	}
	end, err := h.WaitClosed(ctx)
	if err != nil || end.Kind != "failed" || end.Err != cause || store.closes.Load() != 1 {
		t.Fatalf("end=%+v closes=%d error=%v", end, store.closes.Load(), err)
	}
	if _, err := sub.Wait(ctx); !errors.Is(err, cause) {
		t.Fatalf("future submission wait=%v", err)
	}
	store.MemoryStorage.mu.Lock()
	stored := store.MemoryStorage.state.Tasks[taskID]
	outcome := stored.Outcome
	store.MemoryStorage.mu.Unlock()
	if outcome != nil {
		t.Fatal("storage failure wrote a task outcome")
	}
}

func TestFailureShutdownJoinsNonCooperativeCode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cause := errors.New("uncertain write")
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause}
	model := &nonCooperativeModel{started: make(chan struct{}), release: make(chan struct{})}
	h, err := Open(ctx, store, HarnessOptions{Model: model})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	defer func() { once.Do(func() { close(model.release) }); _ = h.Close(context.Background()) }()
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "model"}}})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmissionRequest{Content: "hold"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-model.started:
	case <-ctx.Done():
		t.Fatal("model did not start")
	}
	store.fail.Store(true)
	if err := h.Commit(ctx, touchStorage); err != cause {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); !errors.Is(err, cause) {
		t.Fatalf("failed wait did not reject promptly: %v", err)
	}
	short, shortCancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer shortCancel()
	if err := h.Close(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close=%v", err)
	}
	select {
	case <-h.Done():
		t.Fatal("shutdown settled before task code returned")
	default:
	}
	if store.closes.Load() != 0 {
		t.Fatal("backend closed while non-cooperative task code was live")
	}
	once.Do(func() { close(model.release) })
	end, err := h.WaitClosed(ctx)
	if err != nil || end.Kind != "failed" || end.Err != cause || store.closes.Load() != 1 {
		t.Fatalf("end=%+v closes=%d error=%v", end, store.closes.Load(), err)
	}
}

type blockedCommitStorage struct {
	*MemoryStorage
	block   atomic.Bool
	started chan struct{}
	release chan struct{}
	closes  atomic.Int32
}

func (s *blockedCommitStorage) Commit(ctx context.Context, seq uint64, next *State) error {
	if s.block.Load() {
		close(s.started)
		<-s.release // Deliberately ignores cancellation while settling an admitted write.
	}
	return s.MemoryStorage.Commit(ctx, seq, next)
}
func (s *blockedCommitStorage) Close() error {
	s.closes.Add(1)
	return s.MemoryStorage.Close()
}

func TestCloseSealsWithoutWaitingOnAdmittedCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &blockedCommitStorage{MemoryStorage: NewMemoryStorage(), started: make(chan struct{}), release: make(chan struct{})}
	h, err := Open(ctx, store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	defer func() { once.Do(func() { close(store.release) }); _ = h.Close(context.Background()) }()
	store.block.Store(true)
	committed := make(chan error, 1)
	go func() { committed <- h.Commit(context.Background(), touchStorage) }()
	select {
	case <-store.started:
	case <-ctx.Done():
		t.Fatal("commit did not enter storage")
	}
	short, shortCancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer shortCancel()
	closed := make(chan error, 1)
	go func() { closed <- h.Close(short) }()
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("close=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("Close could not time out while a commit held the mutation mutex")
	}
	called := false
	if err := h.Commit(ctx, func(*Tx) error { called = true; return nil }); !errors.Is(err, ErrClosed) || called {
		t.Fatalf("new admission after timed-out close: callback=%v error=%v", called, err)
	}
	if store.closes.Load() != 0 {
		t.Fatal("backend closed before admitted commit settled")
	}
	once.Do(func() { close(store.release) })
	select {
	case err := <-committed:
		if err != nil {
			t.Fatalf("admitted commit interrupted by close: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("admitted commit did not settle")
	}
	end, err := h.WaitClosed(ctx)
	if err != nil || end.Kind != "closed" || end.Err != nil || store.closes.Load() != 1 {
		t.Fatalf("end=%+v closes=%d error=%v", end, store.closes.Load(), err)
	}
}
