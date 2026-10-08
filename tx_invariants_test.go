package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestEscapedTransactionCannotAlterPublishedState(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	var escaped *ConversationRecord
	var txSaved *Tx
	if err := h.Commit(context.Background(), func(tx *Tx) error { txSaved = tx; var err error; escaped, err = tx.Conversation(c.id); return err }); err != nil {
		t.Fatal(err)
	}
	escaped.Agent.Instructions = "outside mutation"
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if state.Conversations[c.id].Agent.Instructions != "" {
		t.Fatal("escaped pointer mutated committed state")
	}
	if _, err := txSaved.Conversation(c.id); !errors.Is(err, ErrClosed) {
		t.Fatalf("saved tx still active: %v", err)
	}
}
func TestInvalidCheckpointAndOwnershipRejected(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	def := TaskDefinition{Kind: "test.task", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(`invalid`), nil }}
	err := h.Commit(context.Background(), func(tx *Tx) error { _, err := tx.CreateTask(c.id, def, nil, TaskOptions{}); return err })
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("checkpoint %v", err)
	}
	def.Initial = nil
	err = h.Commit(context.Background(), func(tx *Tx) error {
		_, err := tx.CreateTask(c.id, def, nil, TaskOptions{Owner: Owner{Task: 999}, Background: true})
		return err
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("ownership %v", err)
	}
}
