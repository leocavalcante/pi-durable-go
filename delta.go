package durable

import (
	"context"
	"fmt"
	"sort"
)

// StateDelta is one commit's changes to a State (plan 0006 M2): the header
// fields after the commit, the records the commit added or changed in each
// table, and the keys it removed. Applying it to the State at Sequence-1
// gives the State at Sequence.
type StateDelta struct {
	Version       int                                    `json:"version"`
	Sequence      uint64                                 `json:"sequence"`
	NextID        uint64                                 `json:"nextId"`
	Root          ConversationID                         `json:"root"`
	Conversations map[ConversationID]*ConversationRecord `json:"conversations,omitempty"`
	Entries       map[EntryID]*Entry                     `json:"entries,omitempty"`
	Tasks         map[TaskID]*TaskRecord                 `json:"tasks,omitempty"`
	Submissions   map[SubmissionID]*SubmissionRecord     `json:"submissions,omitempty"`
	Documents     map[string]*DocumentRecord             `json:"documents,omitempty"`
	Removed       RemovedKeys                            `json:"removed,omitempty"`
}

// RemovedKeys lists the keys a commit removed from each table, sorted.
type RemovedKeys struct {
	Conversations []ConversationID `json:"conversations,omitempty"`
	Entries       []EntryID        `json:"entries,omitempty"`
	Tasks         []TaskID         `json:"tasks,omitempty"`
	Submissions   []SubmissionID   `json:"submissions,omitempty"`
	Documents     []string         `json:"documents,omitempty"`
}

// DeltaStorage is a Storage that can store one commit's changes instead of
// the whole State. The Session uses CommitDelta when its storage implements
// it, with the same contract as Commit: apply atomically only when the stored
// sequence is expected, and return a RejectedError only when nothing was
// applied. The delta's records are shared with the Session's committed
// State: read them, never mutate or retain them (Clone the delta to keep it).
type DeltaStorage interface {
	Storage
	CommitDelta(ctx context.Context, expected uint64, delta *StateDelta) error
}

// Clone returns a detached copy of the delta.
func (d *StateDelta) Clone() (*StateDelta, error) {
	return strictClone(d)
}

// ApplyTo applies the delta to st in place, after checking that it follows
// st. st takes the delta's records: pass a delta st may own.
func (d *StateDelta) ApplyTo(st *State) error {
	if d.Sequence != st.Sequence+1 {
		return fmt.Errorf("%w: delta sequence %d does not follow %d", ErrInvalid, d.Sequence, st.Sequence)
	}
	st.Version, st.Sequence, st.NextID, st.Root = d.Version, d.Sequence, d.NextID, d.Root
	ensureTables(st)
	for id, v := range d.Conversations {
		st.Conversations[id] = v
	}
	for id, v := range d.Entries {
		st.Entries[id] = v
	}
	for id, v := range d.Tasks {
		st.Tasks[id] = v
	}
	for id, v := range d.Submissions {
		st.Submissions[id] = v
	}
	for key, v := range d.Documents {
		st.Documents[key] = v
	}
	for _, id := range d.Removed.Conversations {
		delete(st.Conversations, id)
	}
	for _, id := range d.Removed.Entries {
		delete(st.Entries, id)
	}
	for _, id := range d.Removed.Tasks {
		delete(st.Tasks, id)
	}
	for _, id := range d.Removed.Submissions {
		delete(st.Submissions, id)
	}
	for _, key := range d.Removed.Documents {
		delete(st.Documents, key)
	}
	return nil
}

// ensureTables gives a State its empty tables.
func ensureTables(st *State) {
	if st.Conversations == nil {
		st.Conversations = map[ConversationID]*ConversationRecord{}
	}
	if st.Entries == nil {
		st.Entries = map[EntryID]*Entry{}
	}
	if st.Tasks == nil {
		st.Tasks = map[TaskID]*TaskRecord{}
	}
	if st.Submissions == nil {
		st.Submissions = map[SubmissionID]*SubmissionRecord{}
	}
	if st.Documents == nil {
		st.Documents = map[string]*DocumentRecord{}
	}
}

// removedKeys lists base's keys the candidate no longer has, sorted.
func removedKeys[K interface{ ~uint64 | ~string }, V any](base, candidate map[K]*V) []K {
	var removed []K
	for key := range base {
		if _, ok := candidate[key]; !ok {
			removed = append(removed, key)
		}
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i] < removed[j] })
	return removed
}
