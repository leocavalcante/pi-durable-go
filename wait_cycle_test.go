package durable

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// waitState builds tasks as owner/conversation/status/wait specifications.
func waitState(tasks ...*TaskRecord) *State {
	state := &State{Tasks: map[TaskID]*TaskRecord{}, Conversations: map[ConversationID]*ConversationRecord{1: {ID: 1}}}
	for _, task := range tasks {
		if task.Conversation == 0 {
			task.Conversation = 1
		}
		if task.Status == "" {
			task.Status = "running"
		}
		state.Tasks[task.ID] = task
	}
	return state
}

func TestWaitCycleValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  *State
		waiter TaskID
		on     []TaskID
		policy string
		err    string
	}{
		{name: "self", state: waitState(&TaskRecord{ID: 1}), waiter: 1, on: []TaskID{1},
			err: "Task 1 cannot wait on 1: 1 -> 1 is a wait cycle"},
		{name: "owner-holding-it", state: waitState(&TaskRecord{ID: 1}, &TaskRecord{ID: 2, Owner: Owner{Task: 1}}), waiter: 2, on: []TaskID{1},
			err: "Task 2 cannot wait on 1: 2 -> 1 -> 2 is a wait cycle"},
		{name: "mutual-wait", state: waitState(&TaskRecord{ID: 1, Status: "waiting", WaitOn: []TaskID{2}}, &TaskRecord{ID: 2}), waiter: 2, on: []TaskID{1},
			err: "Task 2 cannot wait on 1: 2 -> 1 -> 2 is a wait cycle"},
		{name: "through-owned-work", state: waitState(
			&TaskRecord{ID: 1}, &TaskRecord{ID: 2, Owner: Owner{Task: 1}},
			&TaskRecord{ID: 3, Status: "waiting", WaitOn: []TaskID{1}}), waiter: 2, on: []TaskID{3},
			err: "Task 2 cannot wait on 3: 2 -> 3 -> 1 -> 2 is a wait cycle"},
		{name: "through-owned-conversation", state: func() *State {
			s := waitState(&TaskRecord{ID: 1}, &TaskRecord{ID: 2, Conversation: 7}, &TaskRecord{ID: 3, Status: "waiting", WaitOn: []TaskID{1}})
			s.Conversations[7] = &ConversationRecord{ID: 7, Owner: Owner{Task: 1}}
			return s
		}(), waiter: 2, on: []TaskID{3}, err: "Task 2 cannot wait on 3: 2 -> 3 -> 1 -> 2 is a wait cycle"},
		{name: "abort-marked-waiter-has-no-wait-edges", state: waitState(&TaskRecord{ID: 1, Status: "waiting", WaitOn: []TaskID{2}, AbortRequested: true}, &TaskRecord{ID: 2}),
			waiter: 2, on: []TaskID{1}},
		{name: "background-cuts-ownership", state: waitState(&TaskRecord{ID: 1}, &TaskRecord{ID: 2, Conversation: 7, Background: true}), waiter: 2, on: []TaskID{1}},
		{name: "live-child-adds-no-edge", state: waitState(&TaskRecord{ID: 1}, &TaskRecord{ID: 2, Owner: Owner{Task: 1}}), waiter: 1, on: []TaskID{2}},
		{name: "ended-owned-work-adds-no-edge", state: waitState(&TaskRecord{ID: 1}, &TaskRecord{ID: 2, Status: "terminal", Owner: Owner{Task: 1}}, &TaskRecord{ID: 3}),
			waiter: 3, on: []TaskID{1}},
		{name: "through-ended-owner", state: waitState(&TaskRecord{ID: 1, Status: "terminal"}, &TaskRecord{ID: 2, Owner: Owner{Task: 1}}, &TaskRecord{ID: 3, Status: "waiting", WaitOn: []TaskID{2}}),
			waiter: 2, on: []TaskID{3}, err: "Task 2 cannot wait on 3: 2 -> 3 -> 2 is a wait cycle"},
		{name: "terminal-member", state: waitState(&TaskRecord{ID: 1, Status: "terminal", WaitOn: []TaskID{2}}, &TaskRecord{ID: 2}), waiter: 2, on: []TaskID{1}},
		{name: "missing", state: waitState(&TaskRecord{ID: 1}), waiter: 1, on: []TaskID{9}, err: "Task 9 does not exist"},
		{name: "fail-fast-not-owned", state: waitState(&TaskRecord{ID: 1}, &TaskRecord{ID: 2}), waiter: 1, on: []TaskID{2}, policy: "failFast",
			err: "Task 1 can wait failFast only on tasks it owns; 2 is not one"},
		{name: "fail-fast-owned", state: waitState(&TaskRecord{ID: 1}, &TaskRecord{ID: 2, Owner: Owner{Task: 1}}), waiter: 1, on: []TaskID{2}, policy: "failFast"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkWait(tc.state, tc.waiter, tc.on, tc.policy)
			if tc.err == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, ErrInvalid) || !strings.HasSuffix(err.Error(), tc.err) {
				t.Fatalf("error=%v, want %q", err, tc.err)
			}
		})
	}
}

func TestWaitCycleRejectedAtCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := testHarness(t)
	commitErr := make(chan error, 1)
	definition := TaskDefinition{Kind: "test.wait-self", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		err := r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			return TaskTransition{Status: "waiting", WaitOn: []TaskID{task.ID}}, nil
		})
		commitErr <- err
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	if err := h.registry.Install(Extension{Name: "wait-self", Tasks: []TaskDefinition{definition}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, definition)
	h.Resume()
	select {
	case err := <-commitErr:
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "is a wait cycle") {
			t.Fatalf("self wait error=%v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if record, err := h.WaitTask(ctx, id); err != nil || record.Outcome.Status != OutcomeCompleted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
}
