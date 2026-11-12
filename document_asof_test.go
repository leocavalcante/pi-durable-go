package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestReadDocumentAsOfAnEntry(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	notes := DocumentDefinition{Kind: "test.notes", Version: 1, Scope: "conversation", History: "rewindable", Fork: "asOf"}
	family := DocumentDefinition{Kind: "test.files", Version: 1, Scope: "conversation", History: "rewindable", Family: true}
	latest := DocumentDefinition{Kind: "test.latest", Version: 1, Scope: "conversation"}
	root, err := h.Root(ctx, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cid := uint64(root.ID())
	// Each step sets the documents and appends an entry in one commit.
	var ids []EntryID
	for n := 1; n <= 2; n++ {
		if err := h.Commit(ctx, func(tx *Tx) error {
			value := json.RawMessage(fmt.Sprintf(`{"n":%d}`, n))
			if err := tx.PutDocument(notes, cid, value); err != nil {
				return err
			}
			if err := tx.PutFamilyDocument(family, cid, "a.txt", value); err != nil {
				return err
			}
			id, err := tx.Append(root.ID(), Entry{Kind: "app.step", Data: value})
			ids = append(ids, id)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	type doc struct{ N int }
	for i, id := range ids {
		got, err := ReadDocumentAsOf[doc](ctx, h, notes, cid, id)
		if err != nil || got.N != i+1 {
			t.Fatalf("notes as of %d=%+v, %v", id, got, err)
		}
		member, err := ReadFamilyDocumentAsOf[doc](ctx, h, family, cid, "a.txt", id)
		if err != nil || member.N != i+1 {
			t.Fatalf("family member as of %d=%+v, %v", id, member, err)
		}
	}
	// An entry before the document existed: no value then.
	head := ids[0] - 1
	if _, err := ReadDocumentAsOf[doc](ctx, h, notes, cid, head); !errors.Is(err, ErrNotFound) {
		t.Fatalf("before the first value: %v", err)
	}
	// From a fork, a parent's entry reads the parent's document then.
	child, err := root.Fork(ctx, ids[0], ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ReadDocumentAsOf[doc](ctx, h, notes, uint64(child.ID()), ids[0]); err != nil || got.N != 1 {
		t.Fatalf("parent entry from the fork=%+v, %v", got, err)
	}
	if _, err := ReadDocumentAsOf[doc](ctx, h, notes, uint64(child.ID()), ids[1]); !errors.Is(err, ErrInvalid) {
		t.Fatalf("an entry the fork does not see: %v", err)
	}
	if _, err := ReadDocumentAsOf[doc](ctx, h, latest, cid, ids[0]); !errors.Is(err, ErrInvalid) {
		t.Fatalf("a latest document: %v", err)
	}
}

func TestRetiredDocumentsKeepTheirHistory(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	notes := DocumentDefinition{Kind: "test.history", Version: 1, Scope: "conversation", History: "rewindable"}
	root, err := h.Root(ctx, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cid := uint64(root.ID())
	var before, after EntryID
	var deltas []*StateDelta
	detach := h.ObserveCommits(func(delta *StateDelta) { deltas = append(deltas, delta) })
	if err := h.Commit(ctx, func(tx *Tx) error {
		if err := tx.PutDocument(notes, cid, json.RawMessage(`{"n":1}`)); err != nil {
			return err
		}
		var err error
		before, err = tx.Append(root.ID(), Entry{Kind: "app.mark"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(ctx, func(tx *Tx) error {
		if err := tx.RetireDocument(notes, cid); err != nil {
			return err
		}
		var err error
		after, err = tx.Append(root.ID(), Entry{Kind: "app.mark"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	detach()
	type doc struct{ N int }
	if got, err := ReadDocumentAsOf[doc](ctx, h, notes, cid, before); err != nil || got.N != 1 {
		t.Fatalf("before the retirement=%+v error=%v", got, err)
	}
	if _, err := ReadDocumentAsOf[doc](ctx, h, notes, cid, after); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after the retirement: %v", err)
	}
	// Each write stores one revision of its own; the document carries none.
	for _, delta := range deltas {
		if len(delta.Revisions) != 1 {
			t.Fatalf("a write stored %d revisions", len(delta.Revisions))
		}
		for _, d := range delta.Documents {
			if len(d.Revisions) != 0 {
				t.Fatalf("a document record carried %d revisions", len(d.Revisions))
			}
		}
	}
}

func TestVersionThreeRevisionsMoveOut(t *testing.T) {
	store := NewMemoryStorage()
	store.state = &State{Version: 3, NextID: 10, Root: 1,
		Conversations: map[ConversationID]*ConversationRecord{1: {ID: 1, Head: 2}},
		Entries:       map[EntryID]*Entry{2: {ID: 2, Conversation: 1, Kind: "app.mark", Sequence: 5}},
		Tasks:         map[TaskID]*TaskRecord{}, Submissions: map[SubmissionID]*SubmissionRecord{},
		Documents: map[string]*DocumentRecord{"conversation:1:test.old": {Kind: "test.old", Version: 1, Scope: "conversation", ScopeID: 1, History: "rewindable", Fork: "initial",
			Value: json.RawMessage(`{"n":2}`), Revisions: []DocumentRevision{{Version: 1, Sequence: 4, Value: json.RawMessage(`{"n":1}`)}, {Version: 1, Sequence: 6, Value: json.RawMessage(`{"n":2}`)}}}}}
	h, err := Open(context.Background(), store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	old := DocumentDefinition{Kind: "test.old", Version: 1, Scope: "conversation", History: "rewindable"}
	type doc struct{ N int }
	if got, err := ReadDocumentAsOf[doc](context.Background(), h, old, 1, 2); err != nil || got.N != 1 {
		t.Fatalf("as of entry 2=%+v error=%v", got, err)
	}
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Revisions) != 2 || len(state.Documents["conversation:1:test.old"].Revisions) != 0 {
		t.Fatalf("revisions=%d in record=%d", len(state.Revisions), len(state.Documents["conversation:1:test.old"].Revisions))
	}
}

func TestAsOfForksCopyDocumentsRetiredSince(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	notes := DocumentDefinition{Kind: "test.fork-notes", Version: 1, Scope: "conversation", History: "rewindable", Fork: "asOf"}
	root, err := h.Root(ctx, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cid := uint64(root.ID())
	var at EntryID
	if err := h.Commit(ctx, func(tx *Tx) error {
		if err := tx.PutDocument(notes, cid, json.RawMessage(`{"n":1}`)); err != nil {
			return err
		}
		var err error
		at, err = tx.Append(root.ID(), Entry{Kind: "app.mark"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(ctx, func(tx *Tx) error { return tx.RetireDocument(notes, cid) }); err != nil {
		t.Fatal(err)
	}
	child, err := root.Fork(ctx, at, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	type doc struct{ N int }
	if got, err := ReadDocument[doc](ctx, h, notes, uint64(child.ID())); err != nil || got.N != 1 {
		t.Fatalf("a document retired after the fork point=%+v error=%v", got, err)
	}
	if _, err := ReadDocument[doc](ctx, h, notes, cid); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the parent's document came back: %v", err)
	}
}
