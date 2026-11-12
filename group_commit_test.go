package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// groupStorage blocks its first store until released and records the size
// of every store call.
type groupStorage struct {
	*MemoryStorage
	mu      sync.Mutex
	calls   []int
	started chan struct{}
	release chan struct{}
	fail    error
}

func (g *groupStorage) record(n int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, n)
}

func (g *groupStorage) CommitDelta(ctx context.Context, expected uint64, delta *StateDelta) error {
	if g.started != nil {
		close(g.started)
		g.started = nil
		<-g.release
	}
	g.record(1)
	return g.MemoryStorage.CommitDelta(ctx, expected, delta)
}

func (g *groupStorage) CommitDeltas(ctx context.Context, expected uint64, deltas []*StateDelta) (int, error) {
	g.record(len(deltas))
	if g.fail != nil {
		return 0, g.fail
	}
	return g.MemoryStorage.CommitDeltas(ctx, expected, deltas)
}

func groupDocument(n int) func(*Tx) error {
	def := DocumentDefinition{Kind: fmt.Sprintf("test.group-%d", n), Version: 1, Scope: "session"}
	return func(tx *Tx) error { return tx.PutDocument(def, 0, json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))) }
}

func TestGroupCommitStoresCommitsAdmittedDuringAStoreTogether(t *testing.T) {
	store := &groupStorage{MemoryStorage: NewMemoryStorage(), started: make(chan struct{}), release: make(chan struct{})}
	s, err := NewSession(context.Background(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	watch, stop, err := s.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	<-watch
	started := store.started
	first := make(chan error, 1)
	go func() { first <- s.Commit(context.Background(), groupDocument(0)) }()
	<-started
	// The line is free while the first commit is stored: three more are
	// admitted at once and wait for their outcomes.
	const later = 3
	outcomes := make(chan error, later)
	for i := 1; i <= later; i++ {
		i := i
		go func() { outcomes <- s.Commit(context.Background(), groupDocument(i)) }()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		admitted := len(s.pending)
		s.mu.Unlock()
		if admitted == later {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d commits admitted while a store was in flight", admitted)
		}
		time.Sleep(time.Millisecond)
	}
	close(store.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < later; i++ {
		if err := <-outcomes; err != nil {
			t.Fatal(err)
		}
	}
	store.mu.Lock()
	calls := append([]int(nil), store.calls...)
	store.mu.Unlock()
	if len(calls) != 2 || calls[0] != 1 || calls[1] != later {
		t.Fatalf("store calls=%v, want [1 %d]", calls, later)
	}
	// Each commit has its own sequence, published in line order.
	var sequences []uint64
	for len(sequences) < 1+later {
		select {
		case state := <-watch:
			sequences = append(sequences, state.Sequence)
		case <-time.After(5 * time.Second):
			t.Fatalf("published sequences=%v", sequences)
		}
		if len(sequences) > 0 && sequences[len(sequences)-1] == 1+later {
			break
		}
	}
	if last := sequences[len(sequences)-1]; last != 1+later {
		t.Fatalf("published sequences=%v", sequences)
	}
}

func TestGroupCommitFailureFailsEveryCommitOfTheCall(t *testing.T) {
	cause := errors.New("disk gone")
	store := &groupStorage{MemoryStorage: NewMemoryStorage(), started: make(chan struct{}), release: make(chan struct{}), fail: cause}
	s, err := NewSession(context.Background(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	started := store.started
	first := make(chan error, 1)
	go func() { first <- s.Commit(context.Background(), groupDocument(0)) }()
	<-started
	outcomes := make(chan error, 2)
	for i := 1; i <= 2; i++ {
		i := i
		go func() { outcomes <- s.Commit(context.Background(), groupDocument(i)) }()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		admitted := len(s.pending)
		s.mu.Unlock()
		if admitted == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("commits were not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	close(store.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := <-outcomes; err != cause {
			t.Fatalf("outcome=%v, want the call's error", err)
		}
	}
	if !errors.Is(s.Err(), cause) {
		t.Fatalf("Session=%v", s.Err())
	}
}
