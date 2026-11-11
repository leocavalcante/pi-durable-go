package durable

import (
	"context"
	"errors"
	"testing"
)

func versionedState(version int) *State {
	return &State{
		Version:       version,
		NextID:        10,
		Conversations: map[ConversationID]*ConversationRecord{},
		Entries:       map[EntryID]*Entry{},
		Tasks:         map[TaskID]*TaskRecord{},
		Submissions:   map[SubmissionID]*SubmissionRecord{},
		Documents:     map[string]*DocumentRecord{"kept": {Kind: "test.value", Version: 1}},
	}
}

func TestSessionAcceptsAndPersistsStateVersionsOneAndTwo(t *testing.T) {
	for _, storedVersion := range []int{1, 2} {
		t.Run(map[int]string{1: "v1", 2: "v2"}[storedVersion], func(t *testing.T) {
			ctx := context.Background()
			store := NewMemoryStorage()
			store.state = versionedState(storedVersion)

			s, err := NewSession(ctx, store, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			snapshot, err := s.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Version != 2 || snapshot.Documents["kept"] == nil {
				t.Fatalf("in-memory upgraded snapshot=%+v", snapshot)
			}
			store.mu.Lock()
			backendVersion := store.state.Version
			store.mu.Unlock()
			if backendVersion != storedVersion {
				t.Fatalf("opening v%d unexpectedly wrote backend version %d", storedVersion, backendVersion)
			}

			if err := s.Commit(ctx, nil); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			persistedVersion := store.state.Version
			store.mu.Unlock()
			if persistedVersion != 2 {
				t.Fatalf("next commit persisted version %d, want 2", persistedVersion)
			}
		})
	}
}

func TestSessionDefaultsNewStateToVersionTwo(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStorage()
	s, err := NewSession(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	state, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 2 {
		t.Fatalf("new Session version=%d, want 2", state.Version)
	}
	if err := s.Commit(ctx, nil); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.state.Version != 2 {
		t.Fatalf("new store persisted version %d, want 2", store.state.Version)
	}
}

func TestSessionRejectsLaterStateVersion(t *testing.T) {
	store := NewMemoryStorage()
	store.state = versionedState(3)
	if _, err := NewSession(context.Background(), store, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("version 3 error=%v, want ErrInvalid", err)
	}
}
