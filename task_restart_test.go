package durable_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/storage"
)

func openTaskStore(t *testing.T, ctx context.Context, path string, registry *durable.Registry) *durable.Harness {
	t.Helper()
	store, err := storage.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, store, durable.HarnessOptions{Registry: registry})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})
	return h
}

func TestAbandonOnRestartMarksBeforeAnyReservation(t *testing.T) {
	for _, status := range []string{"pending", "running", "waiting", "completing"} {
		t.Run(status, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			path := filepath.Join(t.TempDir(), "tasks.sqlite")
			var mu sync.Mutex
			runs := make(map[durable.TaskID]int)
			aborts := make(map[durable.TaskID]string)
			def := durable.TaskDefinition{Kind: "test.restart", Version: 1,
				Run: func(ctx context.Context, r *durable.TaskRuntime, task durable.TaskRecord) error {
					mu.Lock()
					runs[task.ID]++
					mu.Unlock()
					return r.Complete(ctx, durable.Outcome{Status: "completed"})
				},
				Abort: func(ctx context.Context, r *durable.TaskRuntime, task durable.TaskRecord) error {
					mu.Lock()
					aborts[task.ID] = task.AbortReason
					mu.Unlock()
					return r.Complete(ctx, durable.Outcome{Status: "aborted"})
				},
			}
			registry := durable.NewRegistry()
			if err := registry.Install(durable.Extension{Name: "restart", Tasks: []durable.TaskDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			h := openTaskStore(t, ctx, path, registry)
			root, err := h.Root(ctx, durable.ConversationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var parent, child, grandchild, foreground, background, belowBackground durable.TaskID
			create := func(tx *durable.Tx, cid durable.ConversationID, opts durable.TaskOptions) durable.TaskID {
				id, err := tx.CreateTask(cid, def, json.RawMessage(`{}`), opts)
				if err != nil {
					t.Error(err)
				}
				return id
			}
			if err := h.Commit(ctx, func(tx *durable.Tx) error {
				parent = create(tx, root.ID(), durable.TaskOptions{AbandonOnRestart: true})
				child = create(tx, root.ID(), durable.TaskOptions{Owner: durable.Owner{Task: parent}})
				grandchild = create(tx, root.ID(), durable.TaskOptions{Owner: durable.Owner{Task: child}})
				cid, err := tx.CreateConversation(durable.ConversationOptions{Owner: durable.Owner{Task: parent}})
				if err != nil {
					return err
				}
				foreground = create(tx, cid, durable.TaskOptions{})
				background = create(tx, cid, durable.TaskOptions{Background: true})
				belowBackground = create(tx, cid, durable.TaskOptions{Owner: durable.Owner{Task: background}})
				stored, err := tx.Task(parent)
				if err != nil {
					return err
				}
				// Model an interrupted stored phase/wait/held outcome without
				// running any external effect in the first host.
				stored.Status = status
				if status == "waiting" {
					wake := time.Now().Add(time.Hour)
					stored.WakeAt = &wake
					stored.WaitOn = []durable.TaskID{child}
				}
				if status == "completing" {
					stored.Outcome = &durable.Outcome{Status: "completed"}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := h.Close(ctx); err != nil {
				t.Fatal(err)
			}
			h = openTaskStore(t, ctx, path, registry)
			// Inspection and registry installation never initiate abandonment.
			task, err := h.Task(ctx, parent)
			if err != nil || !task.AbandonOnRestart || task.AbortRequested {
				t.Fatalf("inspection task=%+v error=%v", task, err)
			}
			if _, err := h.Inspect(ctx); err != nil {
				t.Fatal(err)
			}
			var newTask durable.TaskID
			if err := h.Commit(ctx, func(tx *durable.Tx) error {
				newTask = create(tx, root.ID(), durable.TaskOptions{AbandonOnRestart: true})
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for _, id := range []durable.TaskID{parent, child, grandchild, foreground, background, belowBackground, newTask} {
				task, err := h.WaitTask(ctx, id)
				if err != nil || task.Outcome == nil {
					state, _ := h.Snapshot(context.Background())
					if state != nil {
						t.Logf("unfinished task %d: %+v", id, state.Tasks[id])
					}
					t.Fatalf("task=%+v error=%v", task, err)
				}
				if id == background || id == belowBackground || id == newTask {
					if task.AbortRequested || task.Outcome.Status != "completed" {
						t.Fatalf("unrelated/new work abandoned: %+v", task)
					}
					continue
				}
				if !task.AbortRequested || task.AbortReason != "restart" {
					t.Fatalf("missing restart mark: %+v", task)
				}
				want := "aborted"
				if id == parent && status == "completing" {
					want = "completed" // An already decided outcome is retained.
				}
				if task.Outcome.Status != want {
					t.Fatalf("outcome=%+v, want %s", task, want)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range []durable.TaskID{parent, child, grandchild, foreground} {
				if runs[id] != 0 {
					t.Fatalf("abandoned task %d replayed Run", id)
				}
				if id != parent || status != "completing" {
					if aborts[id] != "restart" {
						t.Fatalf("abort handler %d reason=%q", id, aborts[id])
					}
				}
			}
		})
	}
}

func TestRestartAbandonmentWaitsForMissingDefinition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "tasks.sqlite")
	def := durable.TaskDefinition{Kind: "test.missing-restart", Version: 1,
		Run: func(context.Context, *durable.TaskRuntime, durable.TaskRecord) error {
			t.Error("abandoned task replayed its run handler")
			return nil
		},
		Abort: func(ctx context.Context, r *durable.TaskRuntime, task durable.TaskRecord) error {
			if task.AbortReason != "restart" {
				t.Errorf("reason=%q", task.AbortReason)
			}
			return r.Complete(ctx, durable.Outcome{Status: "aborted"})
		},
	}
	h := openTaskStore(t, ctx, path, durable.NewRegistry())
	root, err := h.Root(ctx, durable.ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var id durable.TaskID
	if err := h.Commit(ctx, func(tx *durable.Tx) error {
		var err error
		id, err = tx.CreateTask(root.ID(), def, nil, durable.TaskOptions{AbandonOnRestart: true})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	registry := durable.NewRegistry()
	h = openTaskStore(t, ctx, path, registry)
	h.Resume()
	for {
		task, err := h.Task(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if task.AbortRequested {
			if task.Status == "terminal" || task.AbortReason != "restart" {
				t.Fatalf("restart-marked task did not wait: %+v", task)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if err := registry.Install(durable.Extension{Name: "restored", Tasks: []durable.TaskDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	task, err := h.WaitTask(ctx, id)
	if err != nil || task.Outcome == nil || task.Outcome.Status != "aborted" {
		t.Fatalf("task=%+v error=%v", task, err)
	}
}
