package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestSessionRollbackSequenceAndDetach(t *testing.T) {
	ctx := context.Background()
	s, err := NewSession(ctx, NewMemoryStorage(), func() time.Time { return time.Unix(1, 0) })
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Commit(ctx, func(tx *Tx) error {
		tx.state.Documents["a"] = &DocumentRecord{Kind: "before"}
		return errors.New("rollback")
	}); err == nil {
		t.Fatal("expected rollback error")
	}
	initial, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if initial.Sequence != 0 || len(initial.Documents) != 0 {
		t.Fatalf("rollback leaked: %#v", initial)
	}
	var escaped *State
	if err = s.Commit(ctx, func(tx *Tx) error {
		tx.state.Documents["a"] = &DocumentRecord{Kind: "saved"}
		escaped = tx.state
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	escaped.Documents["a"].Kind = "mutated"
	got, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Sequence != 1 || got.Documents["a"].Kind != "saved" {
		t.Fatalf("state not detached: %#v", got)
	}
}

func TestSessionRejectsNonJSONWithoutPoison(t *testing.T) {
	ctx := context.Background()
	s, _ := NewSession(ctx, NewMemoryStorage(), nil)
	err := s.Commit(ctx, func(tx *Tx) error {
		tx.state.Documents["x"] = &DocumentRecord{Value: json.RawMessage("NaN")}
		return nil
	})
	if err == nil {
		t.Fatal("expected strict JSON rejection")
	}
	if _, err = s.Snapshot(ctx); err != nil {
		t.Fatalf("pre-storage failure poisoned Session: %v", err)
	}
}

func TestSessionPoisonsUncertainWrite(t *testing.T) {
	store := &failingStorage{MemoryStorage: NewMemoryStorage(), err: errors.New("uncertain")}
	s, err := NewSession(context.Background(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Commit(context.Background(), func(*Tx) error { return nil }); err == nil {
		t.Fatal("expected storage failure")
	}
	if _, err = s.Snapshot(context.Background()); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("got %v", err)
	}
}

type failingStorage struct {
	*MemoryStorage
	err error
}

func (f *failingStorage) Commit(context.Context, uint64, *State) error { return f.err }

func TestWatchLatestAndDetached(t *testing.T) {
	ctx := context.Background()
	s, _ := NewSession(ctx, NewMemoryStorage(), nil)
	ch, cancel, err := s.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	<-ch
	for n := 1; n <= 3; n++ {
		if err = s.Commit(ctx, func(tx *Tx) error { tx.state.Documents["x"] = &DocumentRecord{Kind: string(rune('0' + n))}; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	last := <-ch
	if last.Sequence != 3 {
		t.Fatalf("latest sequence = %d", last.Sequence)
	}
	last.Documents["x"].Kind = "external"
	snapshot, _ := s.Snapshot(ctx)
	if snapshot.Documents["x"].Kind != "3" {
		t.Fatal("watch snapshot aliases session state")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-ch; ok {
		t.Fatal("watch did not close")
	}
}
