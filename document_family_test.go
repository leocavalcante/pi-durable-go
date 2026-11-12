package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestDocumentFamilies(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root := testRoot(t, h)
	family := DocumentDefinition{Kind: "app.cursor", Version: 1, Scope: "conversation", Family: true,
		InitialSeed: func(seed json.RawMessage) any { return map[string]json.RawMessage{"position": seed} }}
	if err := h.Commit(ctx, func(tx *Tx) error {
		a, err := tx.FamilyDocument(family, uint64(root.ID()), "a", json.RawMessage(`1`))
		if err != nil {
			return err
		}
		if string(a) != `{"position":1}` {
			t.Errorf("seeded member=%s", a)
		}
		if err := tx.PutFamilyDocument(family, uint64(root.ID()), "b", json.RawMessage(`{"position":2}`)); err != nil {
			return err
		}
		if _, err := tx.Document(family, uint64(root.ID())); err == nil {
			t.Error("a family was read as a singleton")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	type cursor struct{ Position int }
	for member, want := range map[string]int{"a": 1, "b": 2} {
		got, err := ReadFamilyDocument[cursor](ctx, h, family, uint64(root.ID()), member)
		if err != nil || got.Position != want {
			t.Fatalf("member %s=%+v error=%v", member, got, err)
		}
	}
	// A fork copies members as their fork policy says; a retired member is gone.
	if err := h.Commit(ctx, func(tx *Tx) error { return tx.RetireFamilyDocument(family, uint64(root.ID()), "a") }); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFamilyDocument[cursor](ctx, h, family, uint64(root.ID()), "a"); err != ErrNotFound {
		t.Fatalf("retired member error=%v", err)
	}
	view, err := root.View(ctx)
	if err != nil || string(view.Documents["app.cursor/b"]) != `{"position":2}` {
		t.Fatalf("view documents=%v error=%v", view.Documents, err)
	}
}

func TestNestedCallsReadMembersAndTheEarlierSingleDocument(t *testing.T) {
	legacy, _ := json.Marshal(map[string]nestedCallState{"old": {TaskID: 7}})
	member, _ := json.Marshal(nestedCallState{TaskID: 9})
	state := &State{Documents: map[string]*DocumentRecord{
		documentKey(nestedCallsDocument, 3):     {Kind: nestedCallsDocument.Kind, Version: 1, Scope: "task", ScopeID: 3, History: "latest", Value: legacy},
		memberKey(nestedCallDocument, 3, "new"): {Kind: nestedCallDocument.Kind, Version: 1, Scope: "task", ScopeID: 3, History: "latest", Value: member, Member: "new"},
	}}
	for key, want := range map[string]TaskID{"old": 7, "new": 9, "missing": 0} {
		if call, err := readNestedCall(state, 3, key); err != nil || call.TaskID != want {
			t.Fatalf("%s=%+v error=%v", key, call, err)
		}
	}
}

func TestConversationsByOwnerAreDetached(t *testing.T) {
	h := testHarness(t)
	root := testRoot(t, h)
	def := TaskDefinition{Kind: "app.owner", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	var owner TaskID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		if owner, err = tx.CreateTask(root.ID(), def, nil, TaskOptions{}); err != nil {
			return err
		}
		_, err = tx.CreateConversation(ConversationOptions{Owner: Owner{Task: owner}, Agent: Agent{Tools: []string{"read"}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Changing a returned record must not reach the committed one, even in a
	// commit that rolls back (the committed-state check would panic).
	_ = h.Commit(context.Background(), func(tx *Tx) error {
		children, err := tx.ConversationsByOwner(owner)
		if err != nil || len(children) != 1 {
			t.Fatalf("children=%+v error=%v", children, err)
		}
		children[0].Agent.Tools[0] = "changed"
		return errors.New("roll back")
	})
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range state.Conversations {
		if c.Owner.Task == owner && c.Agent.Tools[0] != "read" {
			t.Fatalf("the committed agent changed: %+v", c.Agent)
		}
	}
}
