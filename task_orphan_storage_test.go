package durable_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

func TestAbortMarkedTaskReopensAsMissingDefinitionOrphanInSQLite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "orphan.sqlite")
	var runs, aborts atomic.Int32
	definition := durable.TaskDefinition{Kind: "test.sqlite-orphan", Version: 1,
		Run:   func(context.Context, *durable.TaskRuntime, durable.TaskRecord) error { runs.Add(1); return nil },
		Abort: func(context.Context, *durable.TaskRuntime, durable.TaskRecord) error { aborts.Add(1); return nil },
	}
	first := openTaskStore(t, ctx, path, durable.NewRegistry())
	root, err := first.Root(ctx, durable.ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var id durable.TaskID
	if err := first.Commit(ctx, func(tx *durable.Tx) error {
		var err error
		id, err = tx.CreateTask(root.ID(), definition, json.RawMessage(`{"input":true}`), durable.TaskOptions{})
		if err != nil {
			return err
		}
		task, err := tx.Task(id)
		if err != nil {
			return err
		}
		// Model a committed host abort request whose worker stopped before the
		// next scheduler pass. The record, including its mark, is SQLite state.
		task.AbortRequested = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}

	reopened := openTaskStore(t, ctx, path, durable.NewRegistry())
	persisted, err := reopened.Task(ctx, id)
	if err != nil || !persisted.AbortRequested || persisted.AbortReason != "" {
		t.Fatalf("reopened abort mark=%+v error=%v", persisted, err)
	}
	reopened.Resume()
	task, err := reopened.WaitTask(ctx, id)
	if err != nil || task.Outcome == nil || task.Outcome.Status != durable.OutcomeOrphaned || task.Outcome.Reason != "missing_task" {
		t.Fatalf("reopened task=%+v error=%v", task, err)
	}
	if runs.Load() != 0 || aborts.Load() != 0 {
		t.Fatalf("orphan ran task code: Run=%d Abort=%d", runs.Load(), aborts.Load())
	}
}
