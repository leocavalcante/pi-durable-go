package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testClock is a settable Harness clock.
type testClock struct{ nanos atomic.Int64 }

func newTestClock() *testClock {
	c := &testClock{}
	c.nanos.Store(time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}
func (c *testClock) Now() time.Time                { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *testClock) advance(d time.Duration)       { c.nanos.Add(int64(d)) }
func (c *testClock) unixMilliAfter(ms int64) int64 { return c.Now().UnixMilli() + ms }

// scriptedModel answers call n with steps[n], repeating the last step.
type scriptedModel struct {
	mu       sync.Mutex
	steps    []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error)
	requests []ModelRequest
}

func (m *scriptedModel) Generate(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
	m.mu.Lock()
	step := m.steps[min(len(m.requests), len(m.steps)-1)]
	m.requests = append(m.requests, request)
	m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, err
	}
	return step(ctx, request, delta)
}

func (m *scriptedModel) calls() []ModelRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ModelRequest(nil), m.requests...)
}

func failWith(err error, partial string) func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
	return func(_ context.Context, _ ModelRequest, delta func(string) error) (ModelResponse, error) {
		if partial != "" {
			if e := delta(partial); e != nil {
				return ModelResponse{}, e
			}
		}
		return ModelResponse{}, err
	}
}

func answerWith(content string) func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
	return func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
		return ModelResponse{Content: content}, nil
	}
}

// waitForTask polls until a task matches, failing the test at the deadline.
func waitForTask(t *testing.T, h *Harness, id TaskID, match func(TaskRecord) bool) TaskRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		task, err := h.Task(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if match(task) {
			return task
		}
		if time.Now().After(deadline) {
			t.Fatalf("task never matched: %+v checkpoint=%s", task, task.Checkpoint)
		}
		time.Sleep(time.Millisecond)
	}
}

func generationPhase(task TaskRecord) generationCheckpoint {
	var cp generationCheckpoint
	_ = json.Unmarshal(task.Checkpoint, &cp)
	return cp
}

func busyTask(t *testing.T, h *Harness, cid ConversationID) TaskID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, err := h.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if busy := state.Conversations[cid].Busy; busy != 0 {
			return busy
		}
		if time.Now().After(deadline) {
			t.Fatal("conversation never became busy")
		}
		time.Sleep(time.Millisecond)
	}
}

type transientTestError struct{}

func (transientTestError) Error() string   { return "sanitized adapter failure" }
func (transientTestError) Retryable() bool { return true }

func TestRetryPolicyGenerationRetriesTransientErrorAndPreparesAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := newTestClock()
	var mode atomic.Value
	mode.Store("before")
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "modes", Sections: []Section{{Name: "mode", Render: func(context.Context, ConversationRecord, *Harness) (string, error) {
		return mode.Load().(string), nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	model := &scriptedModel{steps: []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error){
		failWith(errors.New("503 Service Unavailable"), "par"),
		answerWith("ok"),
	}}
	// MaxTurns 1 proves the retry prepares the same turn instead of a new one.
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, Now: clock.Now, Settings: Settings{MaxTurns: 1}})
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}, Extensions: []string{"modes"}}})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	id := busyTask(t, h, root.ID())
	backoff := waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "retry" })
	cp := generationPhase(backoff)
	until := clock.unixMilliAfter(2000)
	if backoff.Status != "sleeping" || cp.Attempt != 1 || cp.Until != until || backoff.WakeAt == nil || backoff.WakeAt.UnixMilli() != until || cp.Turns != 1 {
		t.Fatalf("backoff=%+v checkpoint=%+v, want attempt 1 until %d", backoff, cp, until)
	}
	view, err := root.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if last := view.Entries[len(view.Entries)-1]; last.Kind != "pi.assistant" || last.Message.Content != "par" || !last.Message.Aborted {
		t.Fatalf("failed attempt's partial not kept as an aborted entry: %+v", last)
	}
	// Changes made during the backoff apply to the next attempt.
	mode.Store("after")
	time.Sleep(50 * time.Millisecond) // a scheduler pass before the deadline must not run it
	if calls := model.calls(); len(calls) != 1 {
		t.Fatalf("retried before the deadline: %d calls", len(calls))
	}
	clock.advance(2 * time.Second)
	h.Resume() // a fake clock needs a kick; timers run on real time
	settled, err := submission.Wait(ctx)
	if err != nil || settled.Status != "done" {
		t.Fatalf("submission=%+v error=%v", settled, err)
	}
	calls := model.calls()
	if len(calls) != 2 {
		t.Fatalf("model calls=%d, want 2", len(calls))
	}
	if !strings.Contains(calls[0].Messages[0].Content, "before") || !strings.Contains(calls[1].Messages[0].Content, "after") {
		t.Fatalf("retry did not prepare again: %q / %q", calls[0].Messages[0].Content, calls[1].Messages[0].Content)
	}
	for _, message := range calls[1].Messages {
		if message.Content == "par" {
			t.Fatal("the failed attempt's partial reached the next request")
		}
	}
}

func TestRetryPolicyGenerationEndings(t *testing.T) {
	overflow := errors.New("503 prompt is too long: 9000 tokens > 8192 maximum")
	for _, tc := range []struct {
		name     string
		settings Settings
		errors   []error
		calls    int
		answered bool
	}{
		{name: "not-transient", errors: []error{errors.New("invalid request")}, calls: 1},
		{name: "overflow", errors: []error{overflow}, calls: 1},
		{name: "disabled", settings: Settings{Retry: RetrySettings{Enabled: ptr(false)}}, errors: []error{errors.New("503")}, calls: 1},
		{name: "zero-retries", settings: Settings{Retry: RetrySettings{MaxRetries: ptr(0.0)}}, errors: []error{errors.New("503")}, calls: 1},
		{name: "exhausted", settings: Settings{Retry: RetrySettings{MaxRetries: ptr(1.0)}}, errors: []error{errors.New("timeout"), errors.New("timeout")}, calls: 2},
		{name: "adapter-classified", errors: []error{transientTestError{}}, calls: 2, answered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			clock := newTestClock()
			model := &scriptedModel{}
			for _, err := range tc.errors {
				model.steps = append(model.steps, failWith(err, ""))
			}
			model.steps = append(model.steps, answerWith("ok"))
			h := taskTestHarness(t, HarnessOptions{Model: model, Now: clock.Now, Settings: tc.settings})
			root := testRoot(t, h)
			submission, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
			if err != nil {
				t.Fatal(err)
			}
			id := busyTask(t, h, root.ID())
			done := make(chan SubmissionRecord, 1)
			go func() {
				record, _ := submission.Wait(ctx)
				done <- record
			}()
			var settled SubmissionRecord
		loop:
			for {
				select {
				case settled = <-done:
					break loop
				case <-time.After(5 * time.Millisecond):
					if task, err := h.Task(ctx, id); err == nil && generationPhase(task).Phase == "retry" {
						clock.advance(time.Minute)
						h.Resume() // a fake clock needs a kick; timers run on real time
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if got := len(model.calls()); got != tc.calls {
				t.Fatalf("model calls=%d, want %d", got, tc.calls)
			}
			if tc.answered != (settled.Status == "done") || !tc.answered && settled.Status != "unanswered" {
				t.Fatalf("submission=%+v", settled)
			}
			record, err := h.WaitTask(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			want := OutcomeCompleted
			if !tc.answered {
				want = OutcomeFailed
			}
			if record.Outcome == nil || record.Outcome.Status != want {
				t.Fatalf("outcome=%+v", record.Outcome)
			}
		})
	}
}

// seedGeneration stores a running submission and its generation record with
// the given version and checkpoint, as an earlier host would have left them.
func seedGeneration(t *testing.T, h *Harness, cid ConversationID, version int, checkpoint string, wakeAt *time.Time) (TaskID, SubmissionID) {
	t.Helper()
	var id TaskID
	var sub SubmissionID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		sub = SubmissionID(tx.id())
		request := SubmissionRequest{Type: "input", Content: "hi"}
		tx.state.Submissions[sub] = &SubmissionRecord{ID: sub, Conversation: cid, Request: request, Status: "running", CreatedAt: tx.clock()}
		if _, err := tx.Append(cid, inputEntry(request)); err != nil {
			return err
		}
		var err error
		if id, err = tx.createGeneration(cid, []SubmissionID{sub}); err != nil {
			return err
		}
		task := tx.state.Tasks[id]
		task.Version, task.Checkpoint, task.WakeAt = version, json.RawMessage(checkpoint), wakeAt
		tx.state.Conversations[cid].Busy = id
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return id, sub
}

func TestRetryPolicyGenerationRetryBeforeUntilBacksOffAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := newTestClock()
	model := &scriptedModel{steps: []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error){answerWith("ok")}}
	h := taskTestHarness(t, HarnessOptions{Model: model, Now: clock.Now})
	root := testRoot(t, h)
	until := clock.unixMilliAfter(3_600_000)
	checkpoint := `{"phase":"retry","attempt":2,"until":` + itoa(TaskID(until)) + `,"turns":1}`
	// No WakeAt: the record resumes early, as after a clock that moved backward.
	id, sub := seedGeneration(t, h, root.ID(), generationVersion, checkpoint, nil)
	h.Resume()
	waiting := waitForTask(t, h, id, func(task TaskRecord) bool { return task.WakeAt != nil })
	if waiting.Status != "sleeping" || waiting.WakeAt.UnixMilli() != until || !jsonEqual(waiting.Checkpoint, json.RawMessage(checkpoint)) {
		t.Fatalf("early retry=%+v checkpoint=%s", waiting, waiting.Checkpoint)
	}
	if len(model.calls()) != 0 {
		t.Fatal("an early retry phase sent a request")
	}
	clock.advance(time.Hour)
	h.Resume() // a fake clock needs a kick; timers run on real time
	settled, err := (&Submission{h: h, id: sub}).Wait(ctx)
	if err != nil || settled.Status != "done" || len(model.calls()) != 1 {
		t.Fatalf("submission=%+v calls=%d error=%v", settled, len(model.calls()), err)
	}
}

func TestBuiltinVersionGenerationMigration(t *testing.T) {
	for _, tc := range []struct {
		from, want string
	}{
		{`{"phase":"prepare","turns":2}`, `{"phase":"prepare","turns":2,"attempt":1}`},
		{`{"phase":"request","request":{"model":{"provider":"p","id":"m"},"sessionId":"s","messages":[]},"partial":"x","turns":1,"retry":2}`,
			`{"phase":"request","request":{"model":{"provider":"p","id":"m"},"sessionId":"s","messages":[]},"partial":"x","turns":1,"attempt":3}`},
		{`{"phase":"tools","turns":1,"children":[4,5],"assistant":3}`, `{"phase":"tools","turns":1,"children":[4,5],"assistant":3,"attempt":1}`},
	} {
		migrated, err := migrateGeneration(1, json.RawMessage(`{"submissions":[1]}`), json.RawMessage(tc.from))
		if err != nil || !jsonEqual(migrated.Checkpoint, json.RawMessage(tc.want)) || string(migrated.Input) != `{"submissions":[1]}` {
			t.Errorf("migrate %s = %s, %v; want %s", tc.from, migrated.Checkpoint, err, tc.want)
		}
	}
	for _, bad := range []string{`[]`, `{"phase":"request","retry":"two"}`} {
		if _, err := migrateGeneration(1, nil, json.RawMessage(bad)); err == nil {
			t.Errorf("migrated malformed %s", bad)
		}
	}
	if _, err := migrateGeneration(2, nil, json.RawMessage(`{}`)); err == nil {
		t.Error("migrated from an unknown version")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	model := &scriptedModel{steps: []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error){answerWith("ok")}}
	h := taskTestHarness(t, HarnessOptions{Model: model})
	root := testRoot(t, h)
	pinned := `{"model":{"provider":"fake","id":"pinned"},"sessionId":"s","messages":[{"role":"user","content":"pinned"}]}`
	id, sub := seedGeneration(t, h, root.ID(), 1, `{"phase":"request","request":`+pinned+`,"turns":1,"retry":1}`, nil)
	newer, _ := seedGeneration(t, h, root.ID(), generationVersion+1, `{"phase":"prepare","attempt":1}`, nil)
	inspection, err := h.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	states := map[TaskID]TaskInspectionState{}
	for _, task := range inspection.Tasks {
		states[task.Record.ID] = task.State
	}
	if state := states[id]; state.Kind != "pending" || !state.Migrates {
		t.Fatalf("version-1 inspection=%+v", state)
	}
	if state := states[newer]; state.Kind != "blocked" || state.Reason != "task_too_old" {
		t.Fatalf("newer-version inspection=%+v", state)
	}
	h.Resume()
	settled, err := (&Submission{h: h, id: sub}).Wait(ctx)
	if err != nil || settled.Status != "done" {
		t.Fatalf("submission=%+v error=%v", settled, err)
	}
	calls := model.calls()
	if len(calls) != 1 || calls[0].Model.ID != "pinned" || len(calls[0].Messages) != 1 || calls[0].Messages[0].Content != "pinned" {
		t.Fatalf("migrated request was not resent as pinned: %+v", calls)
	}
	if task, err := h.Task(ctx, newer); err != nil || task.Status != "pending" || task.Version != generationVersion+1 {
		t.Fatalf("newer record ran: %+v error=%v", task, err)
	}
}
