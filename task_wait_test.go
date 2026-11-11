package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTaskWaitCancellationDoesNotJoinBlockedStorage(t *testing.T) {
	for _, registered := range []bool{false, true} {
		for _, seal := range []bool{false, true} {
			if !registered && seal {
				continue
			} // registration after an existing seal rejects synchronously
			name := "initial_cancelled"
			if registered {
				name = "registered_cancelled"
				if seal {
					name = "registered_sealed"
				}
			}
			t.Run(name, func(t *testing.T) {
				store := &blockedCommitStorage{MemoryStorage: NewMemoryStorage(), started: make(chan struct{}), release: make(chan struct{})}
				h, err := Open(context.Background(), store, HarnessOptions{})
				if err != nil {
					t.Fatal(err)
				}
				var release sync.Once
				defer func() { release.Do(func() { close(store.release) }); _ = h.Close(context.Background()) }()
				definition := TaskDefinition{Kind: "test.wait-blocked", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
				id := createTaskForTest(t, h, definition)
				var sid SubmissionID
				if err := h.Commit(context.Background(), func(tx *Tx) error {
					sid = SubmissionID(tx.id())
					tx.state.Submissions[sid] = &SubmissionRecord{ID: sid, Conversation: tx.state.Tasks[id].Conversation, Request: SubmissionRequest{Type: "input", Content: "queued"}, Status: "queued"}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				waits := make(chan error, 2)
				startWaits := func() {
					go func() { _, err := h.WaitTask(ctx, id); waits <- err }()
					go func() { _, err := (&Submission{h: h, id: sid}).Wait(ctx); waits <- err }()
				}
				if registered {
					startWaits()
					deadline := time.After(3 * time.Second)
					for {
						h.session.mu.Lock()
						count := len(h.session.watchers)
						h.session.mu.Unlock()
						if count == 2 {
							break
						}
						select {
						case <-deadline:
							t.Fatal("waits did not register")
						case <-time.After(time.Millisecond):
						}
					}
				}
				store.block.Store(true)
				committed := make(chan error, 1)
				go func() { committed <- h.Commit(context.Background(), nil) }()
				select {
				case <-store.started:
				case <-time.After(3 * time.Second):
					t.Fatal("write did not enter storage")
				}
				want := context.Canceled
				if seal {
					h.session.startClose()
					want = ErrClosed
				} else {
					cancel()
				}
				if !registered {
					startWaits()
				}
				for range 2 {
					select {
					case err := <-waits:
						if !errors.Is(err, want) {
							t.Fatalf("wait error=%v, want %v", err, want)
						}
					case <-time.After(3 * time.Second):
						t.Fatal("wait cancellation/seal joined an admitted storage call")
					}
				}
				store.block.Store(false)
				release.Do(func() { close(store.release) })
				select {
				case err := <-committed:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("write did not settle")
				}
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer closeCancel()
				if err := h.Close(closeCtx); err != nil {
					t.Fatal(err)
				}
				h.session.mu.Lock()
				remaining := len(h.session.watchers)
				h.session.mu.Unlock()
				if remaining != 0 {
					t.Fatalf("waits leaked %d watches", remaining)
				}
			})
		}
	}
}
