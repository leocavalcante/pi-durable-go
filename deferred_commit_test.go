package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

var progressDocument = DocumentDefinition{Kind: "test.progress", Version: 1, Scope: "conversation", History: "latest", Fork: "initial"}

// deferredHarness opens a Harness over memory storage with one conversation
// that has a test.progress document stored.
func deferredHarness(t *testing.T, store Storage) (*Harness, ConversationID) {
	t.Helper()
	h, err := Open(context.Background(), store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	var cid ConversationID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		created, err := tx.CreateConversation(ConversationOptions{})
		if err != nil {
			return err
		}
		cid = created
		return tx.PutDocument(progressDocument, uint64(cid), json.RawMessage(`{"n":0}`))
	}); err != nil {
		t.Fatal(err)
	}
	return h, cid
}

func setProgress(cid ConversationID, n int) func(*Tx) error {
	return func(tx *Tx) error {
		return tx.PutDocument(progressDocument, uint64(cid), json.RawMessage(fmt.Sprintf(`{"n":%d}`, n)))
	}
}

func storedProgress(t *testing.T, store *MemoryStorage, cid ConversationID) string {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	doc := store.state.Documents[documentKey(progressDocument, uint64(cid))]
	if doc == nil {
		return ""
	}
	return string(doc.Value)
}

func shownProgress(t *testing.T, h *Harness, cid ConversationID) string {
	t.Helper()
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return string(state.Documents[documentKey(progressDocument, uint64(cid))].Value)
}

func deferredWithin(ms float64) CommitOptions { return CommitOptions{Deferred: true, WithinMs: &ms} }

func TestDeferredCommitIsShownAtOnceAndStoredWithACommitThatReadsIt(t *testing.T) {
	store := NewMemoryStorage()
	h, cid := deferredHarness(t, store)
	sequence := func() uint64 {
		state, _ := h.Snapshot(context.Background())
		return state.Sequence
	}
	before := sequence()
	if err := h.CommitWith(context.Background(), deferredWithin(math.Inf(1)), setProgress(cid, 1)); err != nil {
		t.Fatal(err)
	}
	if got := shownProgress(t, h, cid); got != `{"n":1}` {
		t.Fatalf("shown=%s", got)
	}
	if got := storedProgress(t, store, cid); got != `{"n":0}` {
		t.Fatalf("stored before a stored commit=%s", got)
	}
	if after := sequence(); after != before {
		t.Fatalf("a held commit took sequence %d (was %d)", after, before)
	}
	// A stored commit that neither reads nor writes it leaves it held.
	if err := h.Commit(context.Background(), touchStorage); err != nil {
		t.Fatal(err)
	}
	if got := storedProgress(t, store, cid); got != `{"n":0}` {
		t.Fatalf("stored with an unrelated commit=%s", got)
	}
	// One that reads it stores it with itself (upstream).
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		if _, err := tx.Document(progressDocument, uint64(cid)); err != nil {
			return err
		}
		return touchStorage(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if got := storedProgress(t, store, cid); got != `{"n":1}` {
		t.Fatalf("stored with a commit that read it=%s", got)
	}
}

func TestDeferredCommitIsStoredWithinItsWindow(t *testing.T) {
	store := NewMemoryStorage()
	h, cid := deferredHarness(t, store)
	if err := h.CommitWith(context.Background(), deferredWithin(20), setProgress(cid, 2)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for storedProgress(t, store, cid) != `{"n":2}` {
		if time.Now().After(deadline) {
			t.Fatalf("stored=%s after the window", storedProgress(t, store, cid))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDeferredCommitIsStoredAtClose(t *testing.T) {
	store := NewMemoryStorage()
	h, cid := deferredHarness(t, store)
	if err := h.CommitWith(context.Background(), deferredWithin(math.Inf(1)), setProgress(cid, 3)); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := storedProgress(t, store, cid); got != `{"n":3}` {
		t.Fatalf("stored at close=%s", got)
	}
}

func TestDeferredCommitIsLostInACrash(t *testing.T) {
	backend := NewMemoryStorage()
	store := &crashStorage{Storage: backend}
	h, cid := deferredHarness(t, store)
	if err := h.CommitWith(context.Background(), deferredWithin(math.Inf(1)), setProgress(cid, 4)); err != nil {
		t.Fatal(err)
	}
	store.crashed.Store(true)
	if err := h.Commit(context.Background(), touchStorage); err == nil {
		t.Fatal("commit succeeded on a crashed host")
	}
	if got := storedProgress(t, backend, cid); got != `{"n":0}` {
		t.Fatalf("stored after a crash=%s", got)
	}
}

func TestDeferredCommitThatCannotBeHeldIsStored(t *testing.T) {
	store := NewMemoryStorage()
	h, cid := deferredHarness(t, store)
	other := DocumentDefinition{Kind: "test.other", Version: 1, Scope: "conversation"}
	for name, change := range map[string]func(*Tx) error{
		"a new document": func(tx *Tx) error {
			return tx.PutDocument(other, uint64(cid), json.RawMessage(`{}`))
		},
		"an entry": func(tx *Tx) error {
			if err := setProgress(cid, 5)(tx); err != nil {
				return err
			}
			_, err := tx.Append(cid, Entry{Kind: "app.note", Data: json.RawMessage(`{}`)})
			return err
		},
		"a session document": touchStorage,
	} {
		before, _ := store.Load(context.Background())
		if err := h.CommitWith(context.Background(), deferredWithin(math.Inf(1)), change); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		after, _ := store.Load(context.Background())
		if after.Sequence != before.Sequence+1 {
			t.Fatalf("%s: stored sequence %d, was %d", name, after.Sequence, before.Sequence)
		}
	}
}

func TestDeferredCommitWithinMustBeZeroOrMore(t *testing.T) {
	h, cid := deferredHarness(t, NewMemoryStorage())
	for _, within := range []float64{-1, math.NaN()} {
		err := h.CommitWith(context.Background(), deferredWithin(within), setProgress(cid, 6))
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "0 or more") {
			t.Fatalf("WithinMs %v: %v", within, err)
		}
	}
	// 0 stores it at once.
	store := NewMemoryStorage()
	h, cid = deferredHarness(t, store)
	if err := h.CommitWith(context.Background(), deferredWithin(0), setProgress(cid, 7)); err != nil {
		t.Fatal(err)
	}
	if got := storedProgress(t, store, cid); got != `{"n":7}` {
		t.Fatalf("stored=%s", got)
	}
}

func TestDeferredCommitsArePublishedInLineOrder(t *testing.T) {
	store := &groupStorage{MemoryStorage: NewMemoryStorage()}
	h, cid := deferredHarness(t, store)
	started, release := make(chan struct{}), make(chan struct{})
	store.started, store.release = started, release
	watch, stop, err := h.session.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	<-watch
	stored := make(chan error, 1)
	go func() { stored <- h.Commit(context.Background(), touchStorage) }()
	<-started
	// While the stored commit is in flight, a held commit waits for it.
	held := make(chan error, 1)
	go func() { held <- h.CommitWith(context.Background(), deferredWithin(math.Inf(1)), setProgress(cid, 8)) }()
	select {
	case err := <-held:
		t.Fatalf("a held commit was published before the commit in front of it: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-stored; err != nil {
		t.Fatal(err)
	}
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if got := shownProgress(t, h, cid); got != `{"n":8}` {
		t.Fatalf("shown=%s", got)
	}
}

// storedLive reads pi.live of a conversation from memory storage.
func storedLive(t *testing.T, store *MemoryStorage, cid ConversationID) LiveState {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	var live LiveState
	if doc := store.state.Documents[documentKey(liveDocument, uint64(cid))]; doc != nil {
		if err := json.Unmarshal(doc.Value, &live); err != nil {
			t.Fatal(err)
		}
	}
	return live
}

func TestPartialAnswersAreHeldAndStoredAtClose(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
		for i := 0; i < 20; i++ {
			if err := delta("x"); err != nil {
				return ModelResponse{}, err
			}
		}
		select {
		case <-release:
		case <-ctx.Done():
			return ModelResponse{}, ctx.Err()
		}
		return ModelResponse{Content: strings.Repeat("x", 20)}, nil
	})
	store := NewMemoryStorage()
	h, err := Open(context.Background(), store, HarnessOptions{Model: model, Settings: Settings{Progress: ProgressSettings{PersistWithinMs: ptr(math.Inf(1))}}})
	if err != nil {
		t.Fatal(err)
	}
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	submit(t, root, SubmissionRequest{Content: "hi"})
	partial := strings.Repeat("x", 20)
	waitPartial(t, h, root.ID(), partial)
	// The partial is shown, not stored: a crash now would lose it.
	if live := storedLive(t, store, root.ID()); live.Generation != nil && live.Generation.Message != nil {
		t.Fatalf("stored a held partial: %+v", live.Generation.Message)
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	live := storedLive(t, store, root.ID())
	if live.Generation == nil || live.Generation.Message == nil || live.Generation.Message.Content != partial {
		t.Fatalf("stored at close=%+v", live.Generation)
	}
}

func TestTheAttemptShownIsStoredLikeProgress(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return ModelResponse{}, ctx.Err()
		}
		return ModelResponse{Content: "done"}, nil
	})
	store := NewMemoryStorage()
	h, err := Open(context.Background(), store, HarnessOptions{Model: model, Settings: Settings{Progress: ProgressSettings{PersistWithinMs: ptr(math.Inf(1))}}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	submit(t, root, SubmissionRequest{Content: "hi"})
	deadline := time.Now().Add(5 * time.Second)
	for {
		live := readLiveState(t, h, root.ID())
		if live.Generation != nil && live.Generation.Attempt == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no attempt shown")
		}
		time.Sleep(time.Millisecond)
	}
	// Shown, not stored: a crash now would leave no attempt behind.
	if live := storedLive(t, store, root.ID()); live.Generation != nil {
		t.Fatalf("stored the attempt marker: %+v", live.Generation)
	}
}

func TestHeldDeadlinesUseTheHostTimer(t *testing.T) {
	timers := &fakeTimers{}
	store := NewMemoryStorage()
	h, err := Open(context.Background(), store, HarnessOptions{SetTimer: timers.SetTimer})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	var cid ConversationID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		created, err := tx.CreateConversation(ConversationOptions{})
		cid = created
		if err != nil {
			return err
		}
		return tx.PutDocument(progressDocument, uint64(cid), json.RawMessage(`{"n":0}`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.CommitWith(context.Background(), deferredWithin(1500), setProgress(cid, 1)); err != nil {
		t.Fatal(err)
	}
	// The deadline is a host timer: nothing is stored until it fires.
	deadline := timers.waitArmed(t, func(timer *fakeTimer) bool {
		return timer.delay > 1400*time.Millisecond && timer.delay <= 1500*time.Millisecond
	})
	if got := storedProgress(t, store, cid); got != `{"n":0}` {
		t.Fatalf("stored before the deadline=%s", got)
	}
	timers.fire(deadline)
	end := time.Now().Add(5 * time.Second)
	for storedProgress(t, store, cid) != `{"n":1}` {
		if time.Now().After(end) {
			t.Fatalf("stored after the deadline=%s", storedProgress(t, store, cid))
		}
		time.Sleep(time.Millisecond)
	}
}
