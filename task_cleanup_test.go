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

type taskModelFunc func(context.Context, ModelRequest, func(string) error) (ModelResponse, error)

func (f taskModelFunc) Generate(ctx context.Context, r ModelRequest, delta func(string) error) (ModelResponse, error) {
	return f(ctx, r, delta)
}

func TestTaskFaultGenerationCleanupWaitsForOwnedWork(t *testing.T) {
	partial, releaseModel, childAborting, releaseChild := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var modelOnce, childOnce sync.Once
	defer modelOnce.Do(func() { close(releaseModel) })
	defer childOnce.Do(func() { close(releaseChild) })
	h := taskTestHarness(t, HarnessOptions{Model: taskModelFunc(func(ctx context.Context, _ ModelRequest, delta func(string) error) (ModelResponse, error) {
		if err := delta("committed partial"); err != nil {
			return ModelResponse{}, err
		}
		close(partial)
		<-releaseModel
		panic("generation bug")
	})})
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, err := c.Submit(ctx, SubmissionRequest{Content: "first"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-partial:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitPartial(t, h, c.id, "committed partial")
	queued, err := c.Submit(ctx, SubmissionRequest{Content: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	child := TaskDefinition{Kind: "test.generation-owned", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		wake := time.Now().Add(time.Hour)
		return r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "waiting", WakeAt: &wake}, nil })
	}, Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		close(childAborting)
		<-releaseChild
		return r.Complete(ctx, Outcome{Status: OutcomeAborted})
	}}
	if err := h.registry.Install(Extension{Name: "owned", Tasks: []TaskDefinition{child}}); err != nil {
		t.Fatal(err)
	}
	var generation TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		generation = tx.state.Conversations[c.id].Busy
		_, err := tx.CreateTask(c.id, child, nil, TaskOptions{Owner: Owner{Task: generation}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	modelOnce.Do(func() { close(releaseModel) })
	select {
	case <-childAborting:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Tasks[generation].Status != "completing" || state.Conversations[c.id].Busy != generation || state.Submissions[first.id].Status != "placed" {
		t.Fatalf("cleanup ran before owned work drained: task=%+v sub=%+v", state.Tasks[generation], state.Submissions[first.id])
	}
	childOnce.Do(func() { close(releaseChild) })
	receipt, err := first.Wait(ctx)
	if err != nil || receipt.Status != "unanswered" || receipt.Reason != "faulted" || string(receipt.Detail) != `"panic: generation bug"` {
		t.Fatalf("receipt=%+v error=%v", receipt, err)
	}
	state, err = h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Conversations[c.id].Busy != 0 || state.Submissions[queued.id].Status != "queued" {
		t.Fatal("failed run advanced inbox or left Busy set")
	}
	entries, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if entry.Message != nil && entry.Message.Aborted && entry.Message.Content == "committed partial" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("aborted partials=%d", count)
	}
}

func TestTaskFaultMalformedGenerationCheckpointDoesNotFailSession(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	var id TaskID
	var sid SubmissionID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		sid = SubmissionID(tx.id())
		tx.state.Submissions[sid] = &SubmissionRecord{ID: sid, Conversation: c.id, Status: "placed"}
		input, _ := json.Marshal(generationInput{Submissions: []SubmissionID{sid}})
		var err error
		id, err = tx.CreateTask(c.id, generationTaskDefinition(), input, TaskOptions{})
		if err != nil {
			return err
		}
		tx.state.Tasks[id].Checkpoint = json.RawMessage(`{"phase":23,"partial":"salvaged"}`)
		tx.state.Conversations[c.id].Busy = id
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome.Status != OutcomeFaulted || h.Err() != nil {
		t.Fatalf("record=%+v error=%v Session=%v", record, err, h.Err())
	}
	state, _ := h.Snapshot(ctx)
	if state.Submissions[sid].Status != "unanswered" || state.Conversations[c.id].Busy != 0 {
		t.Fatal("malformed checkpoint prevented cleanup")
	}
	entries, _ := c.Entries(ctx)
	found := false
	for _, e := range entries {
		if e.Message != nil && e.Message.Aborted && e.Message.Content == "salvaged" {
			found = true
		}
	}
	if !found {
		t.Fatal("independently decodable partial not salvaged")
	}
}

func TestTaskFaultToolCleanupAndMissingResultContext(t *testing.T) {
	var calls atomic.Int32
	request := make(chan ModelRequest, 1)
	h := taskTestHarness(t, HarnessOptions{Model: taskModelFunc(func(ctx context.Context, r ModelRequest, _ func(string) error) (ModelResponse, error) {
		if calls.Add(1) == 1 {
			return ModelResponse{ToolCalls: []ToolCall{{ID: "broken-call", Name: "broken", Arguments: json.RawMessage(`{}`)}}}, nil
		}
		request <- r
		return ModelResponse{Content: "continued"}, nil
	})})
	if err := h.registry.Install(Extension{Name: "broken", Tools: []ToolDefinition{{ToolSpec: ToolSpec{Name: "broken", Parameters: json.RawMessage(`{"type":"object"}`)}, Execute: func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		if err := r.Output(ctx, "persisted output"); err != nil {
			return ToolResult{}, err
		}
		panic("broken tool")
	}}}}); err != nil {
		t.Fatal(err)
	}
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sub, err := c.Submit(ctx, SubmissionRequest{Content: "run broken tool"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := sub.Wait(ctx)
	if err != nil || receipt.Status != "done" {
		t.Fatalf("receipt=%+v error=%v", receipt, err)
	}
	select {
	case r := <-request:
		found := false
		for _, m := range r.Messages {
			if m.Role == "tool" && m.ToolCallID == "broken-call" && m.ToolName == "broken" && m.IsError {
				found = true
			}
		}
		if !found {
			t.Fatal("tool fault left unanswered provider call")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	entries, _ := c.Entries(ctx)
	for _, e := range entries {
		if e.Kind == "pi.tool-result" {
			t.Fatal("scheduler fault appended a result entry")
		}
	}
	state, _ := h.Snapshot(ctx)
	for _, task := range state.Tasks {
		if task.Kind == "pi.tool" && task.Outcome.Status != OutcomeFaulted {
			t.Fatalf("tool outcome=%+v", task)
		}
	}
	doc := state.Documents[documentKey(liveDocument, uint64(c.id))]
	if doc == nil {
		t.Fatal("tool wrote no live document")
	}
	var live map[string]any
	_ = json.Unmarshal(doc.Value, &live)
	if tools, ok := live["tools"].(map[string]any); ok && len(tools) > 0 {
		t.Fatalf("fault left live slot: %s", doc.Value)
	}
}

func TestTaskCompactionModelErrorsAreApplicationFailures(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "missing"}[missing], func(t *testing.T) {
			options := HarnessOptions{Settings: Settings{Compaction: compactEverything}}
			if !missing {
				options.Model = taskModelFunc(func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
					return ModelResponse{}, errors.New(strings.Repeat("x", 9000))
				})
			}
			h := taskTestHarness(t, options)
			c := testRoot(t, h)
			if err := h.Commit(context.Background(), func(tx *Tx) error {
				if _, err := tx.Append(c.id, Entry{Kind: "pi.user", Message: &Message{Role: "user", Content: "question"}}); err != nil {
					return err
				}
				_, err := tx.Append(c.id, Entry{Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: "done"}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			id, err := c.Compact(ctx, "")
			if err != nil {
				t.Fatal(err)
			}
			record, err := h.WaitTask(ctx, id)
			if err != nil || record.Outcome.Status != OutcomeFailed || record.Outcome.Error == "" || len(record.Outcome.Error) > 4096 {
				t.Fatalf("record=%+v outcome=%+v error=%v", record, record.Outcome, err)
			}
		})
	}
}
