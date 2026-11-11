package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// compactionConversation has one completed turn to compact, appended directly
// so that every model call is a compaction request.
func compactionConversation(t *testing.T, h *Harness) (*Conversation, EntryID) {
	t.Helper()
	c := testRoot(t, h)
	var cut EntryID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		if _, err := tx.Append(c.id, Entry{Kind: "pi.user", Message: &Message{Role: "user", Content: "question"}}); err != nil {
			return err
		}
		var err error
		cut, err = tx.Append(c.id, Entry{Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: "done"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return c, cut
}

func hasCompactionEntry(t *testing.T, c *Conversation) bool {
	t.Helper()
	entries, err := c.Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Kind == "pi.compaction" {
			return true
		}
	}
	return false
}

func TestRetryPolicyCompactionRetriesTransientError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := newTestClock()
	model := &scriptedModel{steps: []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error){
		failWith(errors.New("503 overloaded"), ""), answerWith("summary"),
	}}
	h := taskTestHarness(t, HarnessOptions{Model: model, Now: clock.Now})
	c, _ := compactionConversation(t, h)
	id, err := c.Compact(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	backoff := waitForTask(t, h, id, func(task TaskRecord) bool { return task.WakeAt != nil })
	var cp compactionCheckpoint
	if err := json.Unmarshal(backoff.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	until := clock.unixMilliAfter(2000)
	if backoff.Status != "pending" || cp.Phase != "retry" || cp.Attempt != 1 || cp.Until != until || backoff.WakeAt.UnixMilli() != until {
		t.Fatalf("backoff=%+v checkpoint=%+v", backoff, cp)
	}
	time.Sleep(50 * time.Millisecond)
	if len(model.calls()) != 1 {
		t.Fatal("compaction retried before its deadline")
	}
	clock.advance(2 * time.Second)
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeCompleted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	calls := model.calls()
	if len(calls) != 2 {
		t.Fatalf("model calls=%d, want 2", len(calls))
	}
	first, _ := json.Marshal(calls[0])
	second, _ := json.Marshal(calls[1])
	if string(first) != string(second) {
		t.Fatalf("retry changed the pinned summary request:\n%s\n%s", first, second)
	}
	if !hasCompactionEntry(t, c) {
		t.Fatal("summary not placed after the retry")
	}
}

func TestRetryPolicyCompactionEndings(t *testing.T) {
	for _, tc := range []struct {
		name      string
		settings  Settings
		errors    []error
		calls     int
		completed bool
	}{
		{name: "not-transient", errors: []error{errors.New("invalid request")}, calls: 1},
		{name: "disabled", settings: Settings{Retry: RetrySettings{Enabled: ptr(false)}}, errors: []error{errors.New("503")}, calls: 1},
		{name: "exhausted", settings: Settings{Retry: RetrySettings{MaxRetries: ptr(1.0)}}, errors: []error{errors.New("503"), errors.New("503")}, calls: 2},
		// Compaction does not check for an overflow before retrying (§8.7).
		{name: "overflow-text-retries", errors: []error{errors.New("503 prompt is too long")}, calls: 2, completed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			clock := newTestClock()
			model := &scriptedModel{}
			for _, err := range tc.errors {
				model.steps = append(model.steps, failWith(err, ""))
			}
			model.steps = append(model.steps, answerWith("summary"))
			h := taskTestHarness(t, HarnessOptions{Model: model, Now: clock.Now, Settings: tc.settings})
			c, _ := compactionConversation(t, h)
			id, err := c.Compact(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan TaskRecord, 1)
			go func() {
				record, _ := h.WaitTask(ctx, id)
				done <- record
			}()
			var record TaskRecord
		loop:
			for {
				select {
				case record = <-done:
					break loop
				case <-time.After(5 * time.Millisecond):
					clock.advance(time.Minute)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			want := OutcomeFailed
			if tc.completed {
				want = OutcomeCompleted
			}
			if record.Outcome == nil || record.Outcome.Status != want || len(model.calls()) != tc.calls {
				t.Fatalf("outcome=%+v calls=%d, want %s after %d", record.Outcome, len(model.calls()), want, tc.calls)
			}
			if hasCompactionEntry(t, c) != tc.completed {
				t.Fatal("summary placement does not match the outcome")
			}
		})
	}
}

func TestBuiltinVersionCompactionMigration(t *testing.T) {
	migrated, err := migrateCompaction(1, json.RawMessage(`{"cut":1}`), json.RawMessage(`{"phase":"request"}`))
	if err != nil || string(migrated.Checkpoint) != `{"phase":"request"}` || string(migrated.Input) != `{"cut":1}` {
		t.Fatalf("migration=%+v error=%v", migrated, err)
	}
	if _, err := migrateCompaction(3, nil, nil); err == nil {
		t.Fatal("migrated from an unknown version")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	model := &scriptedModel{steps: []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error){answerWith("summary")}}
	h := taskTestHarness(t, HarnessOptions{Model: model})
	c, cut := compactionConversation(t, h)
	input, err := json.Marshal(compactionInput{Request: ModelRequest{Model: ModelRef{Provider: "fake", ID: "m"}}, Cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	seed := func(version int, checkpoint string) TaskID {
		var id TaskID
		if err := h.Commit(ctx, func(tx *Tx) error {
			var err error
			if id, err = tx.CreateTask(c.id, compactionTaskDefinition(), input, TaskOptions{Background: true}); err != nil {
				return err
			}
			task := tx.state.Tasks[id]
			task.Version, task.Checkpoint = version, json.RawMessage(checkpoint)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	request := seed(1, `{"phase":"request"}`)
	newer := seed(compactionVersion+1, `{"phase":"request","attempt":1}`)
	publish := seed(1, `{"phase":"publish","summary":"stored summary"}`)
	inspection, err := h.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range inspection.Tasks {
		switch task.Record.ID {
		case request, publish:
			if task.State.Kind != "pending" || !task.State.Migrates {
				t.Fatalf("version-1 inspection=%+v", task.State)
			}
		case newer:
			if task.State.Kind != "blocked" || task.State.Reason != "task_too_old" {
				t.Fatalf("newer inspection=%+v", task.State)
			}
		}
	}
	h.Resume()
	for _, id := range []TaskID{request, publish} {
		record, err := h.WaitTask(ctx, id)
		if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeCompleted || record.Version != compactionVersion {
			t.Fatalf("version-1 record=%+v error=%v", record, err)
		}
	}
	if len(model.calls()) != 1 {
		t.Fatalf("model calls=%d, want only the request-phase record's", len(model.calls()))
	}
	if task, err := h.Task(ctx, newer); err != nil || task.Status != "pending" {
		t.Fatalf("newer record ran: %+v error=%v", task, err)
	}
}
