package durable

import (
	"context"
	"testing"
	"time"
)

func appendNotes(t *testing.T, h *Harness, c *Conversation, n int) []EntryID {
	t.Helper()
	var ids []EntryID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		for i := 0; i < n; i++ {
			id, err := tx.Append(c.id, Entry{Kind: "app.note"})
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return ids
}

func nextView(t *testing.T, views <-chan ConversationView, ok func(ConversationView) bool) ConversationView {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case view := <-views:
			if ok(view) {
				return view
			}
		case <-deadline:
			t.Fatal("no matching view")
		}
	}
}

func TestConversationViewWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := testHarness(t)
	c := testRoot(t, h)
	ids := appendNotes(t, h, c, 5)
	views, stop, err := c.WatchWindow(ctx, ConversationWindow{Min: 2, Max: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	view := nextView(t, views, func(ConversationView) bool { return true })
	if len(view.Entries) != 2 || view.Entries[0].ID != ids[3] || view.Before != ids[2] {
		t.Fatalf("first window=%+v before=%d", view.Entries, view.Before)
	}
	more := appendNotes(t, h, c, 2)
	view = nextView(t, views, func(v ConversationView) bool { return v.Entries[len(v.Entries)-1].ID == more[1] })
	if len(view.Entries) != 4 || view.Entries[0].ID != ids[3] {
		t.Fatalf("grown window=%+v", view.Entries)
	}
	last := appendNotes(t, h, c, 1)
	view = nextView(t, views, func(v ConversationView) bool { return v.Entries[len(v.Entries)-1].ID == last[0] })
	if len(view.Entries) != 2 || view.Entries[0].ID != more[1] || view.Before != more[0] {
		t.Fatalf("spliced window=%+v before=%d", view.Entries, view.Before)
	}

	from, stopFrom, err := c.WatchWindow(ctx, ConversationWindow{From: ids[1], Min: 2, Max: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer stopFrom()
	view = nextView(t, from, func(ConversationView) bool { return true })
	if len(view.Entries) != 7 || view.Entries[0].ID != ids[1] || view.Before != ids[0] {
		t.Fatalf("from window=%+v before=%d", view.Entries, view.Before)
	}
}
