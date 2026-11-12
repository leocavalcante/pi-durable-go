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

func TestSessionAcceptsAndPersistsEarlierStateVersions(t *testing.T) {
	for _, storedVersion := range []int{1, 2, 3} {
		t.Run(map[int]string{1: "v1", 2: "v2", 3: "v3"}[storedVersion], func(t *testing.T) {
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
			if snapshot.Version != stateVersion || snapshot.Documents["kept"] == nil {
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
			if persistedVersion != stateVersion {
				t.Fatalf("next commit persisted version %d, want %d", persistedVersion, stateVersion)
			}
		})
	}
}

func TestSessionDefaultsNewStateToCurrentVersion(t *testing.T) {
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
	if state.Version != stateVersion {
		t.Fatalf("new Session version=%d, want %d", state.Version, stateVersion)
	}
	// The first commit with a write stores the new State.
	if err := s.Commit(ctx, touchStorage); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.state.Version != stateVersion {
		t.Fatalf("new store persisted version %d, want %d", store.state.Version, stateVersion)
	}
}

func TestSessionRejectsLaterStateVersion(t *testing.T) {
	store := NewMemoryStorage()
	store.state = versionedState(stateVersion + 1)
	if _, err := NewSession(context.Background(), store, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("version %d error=%v, want ErrInvalid", stateVersion+1, err)
	}
}
