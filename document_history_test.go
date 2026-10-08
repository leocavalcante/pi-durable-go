package durable

import (
	"context"
	"encoding/json"
	"testing"
)

func TestForkHistoricalDocumentRetainsItsSchemaVersion(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx := context.Background()
	def := DocumentDefinition{Kind: "app.versioned", Version: 1, Scope: "conversation", History: "rewindable", Fork: "asOf", Initial: func() any { return map[string]int{"old": 1} }}
	var at EntryID
	if err := h.Commit(ctx, func(tx *Tx) error {
		if _, err := tx.Document(def, uint64(c.id)); err != nil {
			return err
		}
		var err error
		at, err = tx.Append(c.id, Entry{Kind: "app.note"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	newer := def
	newer.Version = 2
	newer.Migrate = func(version int, data json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"new":2}`), nil
	}
	if err := h.Commit(ctx, func(tx *Tx) error { _, err := tx.Document(newer, uint64(c.id)); return err }); err != nil {
		t.Fatal(err)
	}
	child, err := c.Fork(ctx, at, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Documents[documentKey(def, uint64(child.id))].Version != 1 {
		t.Fatal("historical data mislabeled as current schema")
	}
	value, err := ReadDocument[map[string]int](ctx, h, newer, uint64(child.id))
	if err != nil {
		t.Fatal(err)
	}
	if value["new"] != 2 {
		t.Fatalf("historical migration %+v", value)
	}
}
func TestForkCreationHookSeesInheritedDocument(t *testing.T) {
	def := DocumentDefinition{Kind: "app.created", Version: 1, Scope: "conversation", History: "latest", Fork: "current", Initial: func() any { return map[string]int{"n": 0} }}
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{Model: echoModel{}, ConversationCreated: func(tx *Tx, id ConversationID) error {
		return UpdateDocument(tx, def, uint64(id), func(value *map[string]int) error { (*value)["n"]++; return nil })
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	c := testRoot(t, h)
	ctx := context.Background()
	var at EntryID
	if err := h.Commit(ctx, func(tx *Tx) error {
		if err := UpdateDocument(tx, def, uint64(c.id), func(value *map[string]int) error { (*value)["n"] = 10; return nil }); err != nil {
			return err
		}
		var err error
		at, err = tx.Append(c.id, Entry{Kind: "app.note"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	child, err := c.Fork(ctx, at, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "child"}}})
	if err != nil {
		t.Fatal(err)
	}
	value, err := ReadDocument[map[string]int](ctx, h, def, uint64(child.id))
	if err != nil {
		t.Fatal(err)
	}
	if value["n"] != 11 {
		t.Fatalf("hook did not see inherited data: %+v", value)
	}
	var childAt EntryID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		childAt, err = tx.Append(child.id, Entry{Kind: "app.note"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	grandchild, err := child.Fork(ctx, childAt, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := grandchild.Agent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Model.ID != "child" {
		t.Fatalf("fork choices not journaled: %+v", agent)
	}
}
