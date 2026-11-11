package durable

import (
	"context"
	"errors"
	"testing"
)

func TestSubmitTxAdmitsInsideTheCallersCommit(t *testing.T) {
	ctx := context.Background()
	h, root := runHarness(t, echoModel{}, Settings{})
	var id SubmissionID
	if err := h.Commit(ctx, func(tx *Tx) error {
		if _, err := tx.Append(root.ID(), Entry{Kind: "app.note", Data: []byte(`{"n":1}`)}); err != nil {
			return err
		}
		var err error
		id, err = root.SubmitTx(tx, SubmissionRequest{Content: "hi"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if record := waitSettled(t, h, id); record.Status != "done" {
		t.Fatalf("submission=%+v", record)
	}
	// A rejection writes nothing and lets the callback recover.
	if err := h.Commit(ctx, func(tx *Tx) error {
		_, err := root.SubmitTx(tx, SubmissionRequest{Content: "x", WhenIdle: "reject"})
		if !errors.Is(err, ErrIdle) {
			t.Errorf("idle reject error=%v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A failing commit drops the admission.
	before, _ := h.Snapshot(ctx)
	_ = h.Commit(ctx, func(tx *Tx) error {
		if _, err := root.SubmitTx(tx, SubmissionRequest{Content: "dropped"}); err != nil {
			return err
		}
		return errors.New("caller failed")
	})
	after, _ := h.Snapshot(ctx)
	if len(after.Submissions) != len(before.Submissions) {
		t.Fatal("a failed commit kept its admission")
	}
	if err := root.Configure(ctx, Agent{MaxTurns: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative MaxTurns error=%v", err)
	}
}
