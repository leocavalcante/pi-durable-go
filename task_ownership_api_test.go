package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestOwnershipCascadeKeepsTerminalEdgesAndBackgroundBoundaries(t *testing.T) {
	state := &State{
		Tasks: map[TaskID]*TaskRecord{
			1: {ID: 1, Conversation: 10, Status: "waiting"},
			2: {ID: 2, Conversation: 10, Owner: Owner{Task: 1}, Status: "terminal"},
			3: {ID: 3, Conversation: 11, Status: "pending"},
			4: {ID: 4, Conversation: 11, Background: true, Status: "pending"},
			5: {ID: 5, Conversation: 11, Owner: Owner{Task: 4}, Status: "pending"},
			6: {ID: 6, Conversation: 12, Status: "pending"},
		},
		Conversations: map[ConversationID]*ConversationRecord{
			10: {ID: 10}, 11: {ID: 11, Owner: Owner{Task: 2}}, 12: {ID: 12, Owner: Owner{Task: 4}},
		},
		Submissions: map[SubmissionID]*SubmissionRecord{
			20: {Conversation: 10, Status: "queued", Request: SubmissionRequest{Type: "input"}},
			21: {Conversation: 11, Status: "queued", Request: SubmissionRequest{Type: "input"}},
			22: {Conversation: 11, Status: "queued", Request: SubmissionRequest{Type: "write"}},
			23: {Conversation: 12, Status: "queued", Request: SubmissionRequest{Type: "input"}},
		},
	}
	markAbortTreeReason(state, 1, "restart")
	if state.Tasks[3].AbortReason != "restart" || !belowCancellationIntent(state, state.Tasks[3]) {
		t.Fatal("cancellation did not follow the terminal owner's retained edges")
	}
	for _, id := range []TaskID{2, 4, 5, 6} {
		if state.Tasks[id].AbortRequested || belowCancellationIntent(state, state.Tasks[5]) || belowCancellationIntent(state, state.Tasks[6]) {
			t.Fatalf("mark crossed a terminal/background boundary: %+v", state.Tasks[id])
		}
	}
	if state.Submissions[21].Status != "unanswered" || state.Submissions[21].Reason != "aborted" {
		t.Fatal("owned conversation input not withdrawn")
	}
	for _, id := range []SubmissionID{20, 22, 23} {
		if state.Submissions[id].Status != "queued" {
			t.Fatalf("unrelated input/write withdrawn: %+v", state.Submissions[id])
		}
	}
	markAbortTree(state, 1)
	markAbortTreeReason(state, 1, "restart") // An abandonment cannot downgrade a request.
	if state.Tasks[1].AbortReason != "" || state.Tasks[3].AbortReason != "" {
		t.Fatal("request did not take priority over restart marks")
	}
	// Addressing a background task directly enters its ordinary owned subtree.
	markAbortTreeReason(state, 4, "restart")
	if !state.Tasks[5].AbortRequested || !state.Tasks[6].AbortRequested || state.Submissions[23].Status != "unanswered" {
		t.Fatal("direct background abort did not enter its subtree")
	}
}

func TestRuntimeCommitRejectsDurableAbortBeforeSignal(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx := context.Background()
	def := TaskDefinition{Kind: "test.abort-gate", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	var parent, child TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		parent, err = tx.CreateTask(c.id, def, nil, TaskOptions{})
		if err != nil {
			return err
		}
		child, err = tx.CreateTask(c.id, def, nil, TaskOptions{Owner: Owner{Task: parent}})
		if err != nil {
			return err
		}
		tx.state.Tasks[parent].AbortRequested = true
		tx.state.Tasks[child].Status = "running"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := &TaskRuntime{h: h, ID: child}
	called := false
	change := func(*Tx) (TaskTransition, error) {
		called = true
		return TaskTransition{Status: "running"}, nil
	}
	if err := r.Commit(ctx, change); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("run commit after owner intent: error=%v called=%v", err, called)
	}
	if err := h.Commit(ctx, func(tx *Tx) error {
		tx.state.Tasks[parent].AbortRequested = false
		tx.state.Tasks[child].AbortRequested = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Commit(ctx, change); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("run commit after own mark: error=%v called=%v", err, called)
	}
	r.aborting = true
	if err := r.Commit(ctx, change); err != nil || !called {
		t.Fatalf("abort invocation could not commit: %v", err)
	}
}

func TestAbortInvocationModeIsPublishedBeforeReservation(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	def := TaskDefinition{Kind: "test.abort-mode", Version: 1,
		Run: func(context.Context, *TaskRuntime, TaskRecord) error {
			t.Error("marked task ran its Run handler")
			return nil
		},
		Abort: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
			return r.Complete(ctx, Outcome{Status: "aborted"})
		},
	}
	var id TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		id, err = tx.CreateTask(c.id, def, nil, TaskOptions{})
		if err == nil {
			markAbortTree(tx.state, id)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	task, err := h.Task(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	// Hold admission so the invocation cannot yet reserve/read its record.
	// A concurrent cascade must already know it is an abort invocation, or
	// cancelMarked/dispatch can repeatedly cancel it before cleanup starts.
	h.session.mu.Lock()
	h.Resume()
	h.scheduler.start(task, def)
	h.scheduler.mu.Lock()
	inv := h.scheduler.invocations[id]
	mode := inv != nil && inv.aborting
	h.scheduler.mu.Unlock()
	h.session.mu.Unlock()
	if !mode {
		t.Fatal("abort invocation published as a run invocation")
	}
	if _, err := h.WaitTask(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedTasksAndAbortOwned(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var parentID, first, second, terminal, inConversation, foreign TaskID
	var captured *TaskRuntime
	firstRelease := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(firstRelease) })
	child := TaskDefinition{Kind: "test.owned-child", Version: 1,
		Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
			wake := time.Now().Add(time.Hour)
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
				return TaskTransition{Status: "waiting", WakeAt: &wake}, nil
			})
		},
		Abort: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
			if task.ID == first {
				if task.AbortReason != "restart" {
					t.Errorf("AbortOwned replaced restart reason with %q", task.AbortReason)
				}
				select {
				case <-firstRelease:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return r.Complete(ctx, Outcome{Status: "aborted"})
		},
	}
	parent := TaskDefinition{Kind: "test.owned-parent", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		captured = r
		owned, err := r.OwnedTasks(ctx)
		if err != nil {
			return err
		}
		if len(owned) != 2 || owned[0].ID != first || owned[1].ID != second {
			t.Errorf("direct live children=%+v", owned)
		}
		if len(owned) > 0 {
			owned[0].Checkpoint[0] = 'x'
			stored, err := h.Task(ctx, owned[0].ID)
			if err != nil || !json.Valid(stored.Checkpoint) {
				t.Errorf("OwnedTasks leaked a live reference: %+v, %v", stored, err)
			}
		}
		for _, id := range []TaskID{foreign, inConversation} {
			if err := r.AbortOwned(ctx, id); !errors.Is(err, ErrInvalid) {
				t.Errorf("abort unowned %d: %v", id, err)
			}
		}
		if err := r.AbortOwned(ctx, 99999); !errors.Is(err, ErrNotFound) {
			t.Errorf("abort missing child: %v", err)
		}
		for _, id := range []TaskID{terminal, first, second} {
			if id == first {
				releaseOnce.Do(func() { close(firstRelease) })
			}
			if err := r.AbortOwned(ctx, id); err != nil {
				return err
			}
			stored, err := h.Task(ctx, id)
			if err != nil || stored.Status != "terminal" {
				t.Errorf("AbortOwned returned before terminal: %+v, %v", stored, err)
			}
			if id == first && stored.AbortReason != "restart" {
				t.Errorf("AbortOwned changed durable restart reason: %+v", stored)
			}
		}
		if owned, err := r.OwnedTasks(ctx); err != nil || len(owned) != 0 {
			t.Errorf("terminal children still listed: %+v, %v", owned, err)
		}
		// A task in an owned conversation is ordinary owned work, but not a
		// direct task child accepted by AbortOwned.
		if err := h.AbortTask(ctx, inConversation); err != nil {
			return err
		}
		if _, err := h.WaitTask(ctx, inConversation); err != nil {
			return err
		}
		return r.Complete(ctx, Outcome{Status: "completed"})
	}}
	if err := h.registry.Install(Extension{Name: "ownership-api", Tasks: []TaskDefinition{parent, child}}); err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		parentID, err = tx.CreateTask(c.id, parent, nil, TaskOptions{})
		if err != nil {
			return err
		}
		// These children predate this invocation. No current checkpoint lists
		// them; OwnedTasks discovers them from the committed ownership edges.
		first, err = tx.CreateTask(c.id, child, nil, TaskOptions{Owner: Owner{Task: parentID}})
		if err != nil {
			return err
		}
		second, err = tx.CreateTask(c.id, child, nil, TaskOptions{Owner: Owner{Task: parentID}})
		if err != nil {
			return err
		}
		terminal, err = tx.CreateTask(c.id, child, nil, TaskOptions{Owner: Owner{Task: parentID}})
		if err != nil {
			return err
		}
		tx.state.Tasks[terminal].Outcome = &Outcome{Status: "completed"}
		terminalizeTask(tx, tx.state.Tasks[terminal], time.Now())
		markAbortTreeReason(tx.state, first, "restart")
		cid, err := tx.CreateConversation(ConversationOptions{Owner: Owner{Task: parentID}})
		if err != nil {
			return err
		}
		inConversation, err = tx.CreateTask(cid, child, nil, TaskOptions{})
		if err != nil {
			return err
		}
		foreign, err = tx.CreateTask(c.id, child, nil, TaskOptions{Background: true})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	record, err := h.WaitTask(ctx, parentID)
	if err != nil || record.Outcome == nil || record.Outcome.Status != "completed" {
		t.Fatalf("parent=%+v, error=%v", record, err)
	}
	<-captured.invocation.done
	if _, err := captured.OwnedTasks(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("OwnedTasks after invocation end: %v", err)
	}
	if err := captured.AbortOwned(ctx, first); !errors.Is(err, ErrClosed) {
		t.Fatalf("AbortOwned after invocation end: %v", err)
	}
}

func TestAbortOwnedWaitIsBoundToCallerAndInvocation(t *testing.T) {
	for _, mode := range []string{"caller", "invocation"} {
		t.Run(mode, func(t *testing.T) {
			h := testHarness(t)
			c := testRoot(t, h)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			abortStarted := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			waitResult := make(chan error, 1)
			child := TaskDefinition{Kind: "test.bound-child", Version: 1,
				Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
					wake := time.Now().Add(time.Hour)
					return r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "waiting", WakeAt: &wake}, nil })
				},
				Abort: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
					close(abortStarted)
					select {
					case <-release:
						return r.Complete(ctx, Outcome{Status: "aborted"})
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			}
			parent := TaskDefinition{Kind: "test.bound-parent", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
				if string(task.Checkpoint) == `{"phase":"finish"}` {
					return r.Complete(ctx, Outcome{Status: "completed"})
				}
				var childID TaskID
				if err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
					var err error
					childID, err = tx.CreateTask(c.id, child, nil, TaskOptions{Owner: Owner{Task: r.ID}})
					return TaskTransition{Status: "running"}, err
				}); err != nil {
					return err
				}
				caller, stop := context.WithCancel(context.Background())
				go func() {
					defer stop()
					waitResult <- r.AbortOwned(caller, childID)
				}()
				select {
				case <-abortStarted:
				case <-ctx.Done():
					return ctx.Err()
				}
				if mode == "caller" {
					stop()
				}
				return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
					return TaskTransition{Status: "waiting", WaitOn: []TaskID{childID}, Checkpoint: json.RawMessage(`{"phase":"finish"}`)}, nil
				})
			}}
			if err := h.registry.Install(Extension{Name: "bound", Tasks: []TaskDefinition{parent, child}}); err != nil {
				t.Fatal(err)
			}
			var id TaskID
			if err := h.Commit(ctx, func(tx *Tx) error {
				var err error
				id, err = tx.CreateTask(c.id, parent, nil, TaskOptions{})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			h.Resume()
			select {
			case err := <-waitResult:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("wait error=%v", err)
				}
			case <-ctx.Done():
				t.Fatal("AbortOwned did not end with its caller/invocation")
			}
			once.Do(func() { close(release) })
			record, err := h.WaitTask(ctx, id)
			if err != nil || record.Outcome == nil || record.Outcome.Status != "completed" {
				t.Fatalf("cancelled wait undid durable child abort: %+v, %v", record, err)
			}
		})
	}
}
