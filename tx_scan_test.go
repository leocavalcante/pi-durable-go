package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestTxScansPageThroughRecords(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root := testRoot(t, h)
	definition := TaskDefinition{Kind: "test.scanned", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	var tasks []TaskID
	var owned ConversationID
	if err := h.Commit(ctx, func(tx *Tx) error {
		for i := 0; i < 5; i++ {
			id, err := tx.CreateTask(root.ID(), definition, nil, TaskOptions{Background: i%2 == 0})
			if err != nil {
				return err
			}
			tasks = append(tasks, id)
		}
		var err error
		owned, err = tx.CreateConversation(ConversationOptions{Owner: Owner{Task: tasks[0]}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	background := true
	if err := h.Commit(ctx, func(tx *Tx) error {
		// Two at a time, newest first, background ones only.
		page, err := tx.ScanTasks(TaskQuery{Kind: "test.scanned", Background: &background, Order: "descending"}, 2, nil)
		if err != nil {
			return err
		}
		if len(page.Items) != 2 || page.Items[0].ID != tasks[4] || page.Items[1].ID != tasks[2] || page.Next == nil {
			t.Fatalf("first page=%+v", page)
		}
		page, err = tx.ScanTasks(TaskQuery{Kind: "test.scanned", Background: &background}, 2, page.Next)
		if err != nil {
			return err
		}
		if len(page.Items) != 1 || page.Items[0].ID != tasks[0] || page.Next != nil {
			t.Fatalf("second page=%+v", page)
		}
		conversations, err := tx.ScanConversations(ConversationQuery{OwnerConversation: root.ID()}, 10, nil)
		if err != nil || len(conversations.Items) != 1 || conversations.Items[0].ID != owned {
			t.Fatalf("owned conversations=%+v error=%v", conversations, err)
		}
		if byTask, _ := tx.ScanConversations(ConversationQuery{OwnerTask: tasks[1]}, 10, nil); len(byTask.Items) != 0 {
			t.Fatalf("conversations of another task=%+v", byTask)
		}
		// Scanned records are copies: changing one changes nothing.
		page.Items[0].Kind = "changed"
		return errNoChange
	}); err != nil && !errors.Is(err, errNoChange) {
		t.Fatal(err)
	}
	if task, _ := h.Task(ctx, tasks[0]); task.Kind != "test.scanned" {
		t.Fatalf("a scanned copy changed the task: %+v", task)
	}
}

func TestTxPointReadsAndForkInsideACommit(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root := testRoot(t, h)
	if _, err := root.Submit(ctx, SubmissionRequest{Type: "write", RequestID: "note-1", Entry: &Entry{Kind: "app.note", Data: json.RawMessage(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	var child ConversationID
	if err := h.Commit(ctx, func(tx *Tx) error {
		sub, err := tx.SubmissionByRequest(root.ID(), "note-1")
		if err != nil || sub.Request.RequestID != "note-1" {
			t.Fatalf("submission=%+v error=%v", sub, err)
		}
		if _, err := tx.SubmissionByRequest(root.ID(), "missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing request: %v", err)
		}
		latest, err := tx.LatestEntryID(root.ID())
		if err != nil || latest == 0 {
			t.Fatalf("latest=%d error=%v", latest, err)
		}
		entries, err := tx.ScanEntries(root.ID(), EntryQuery{}, 1, nil)
		if err != nil || len(entries.Items) != 1 || entries.Items[0].ID != latest {
			t.Fatalf("newest entry=%+v error=%v", entries, err)
		}
		// Fork at the newest entry and write to the fork in the same commit.
		if child, err = tx.Fork(root.ID(), latest, ConversationOptions{}); err != nil {
			return err
		}
		_, err = tx.Append(child, Entry{Kind: "app.note", Data: json.RawMessage(`{"in":"fork"}`)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	entries, err := (&Conversation{h: h, id: child}).Entries(ctx)
	if err != nil || len(entries) == 0 || string(entries[len(entries)-1].Data) != `{"in":"fork"}` {
		t.Fatalf("fork entries=%+v error=%v", entries, err)
	}
}
