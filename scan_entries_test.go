package durable

import (
	"context"
	"testing"
)

func TestScanEntriesPagesHistory(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	var ids []EntryID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		for i := 0; i < 5; i++ {
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
	page, err := c.ScanEntries(context.Background(), EntryQuery{}, 2, nil)
	if err != nil || len(page.Items) != 2 || page.Items[0].ID != ids[4] || page.Next == nil {
		t.Fatalf("first page=%+v error=%v", page, err)
	}
	page, err = c.ScanEntries(context.Background(), EntryQuery{}, 2, page.Next)
	if err != nil || len(page.Items) != 2 || page.Items[0].ID != ids[2] {
		t.Fatalf("second page=%+v error=%v", page, err)
	}
	page, err = c.ScanEntries(context.Background(), EntryQuery{Order: "ascending", MinEntryID: ids[1], MaxEntryID: ids[3]}, 10, nil)
	if err != nil || len(page.Items) != 3 || page.Items[0].ID != ids[1] || page.Next != nil {
		t.Fatalf("bounded page=%+v error=%v", page, err)
	}
}
