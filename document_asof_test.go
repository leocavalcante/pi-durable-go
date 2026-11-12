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
