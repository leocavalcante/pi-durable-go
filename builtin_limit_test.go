package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func assertRestartLimitRun(t *testing.T, h *Harness, sub SubmissionID, task TaskID, text string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	settled, err := (&Submission{h: h, id: sub}).Wait(ctx)
	detail, _ := json.Marshal(text)
	if err != nil || settled.Status != "unanswered" || settled.Reason != reasonRestartLimit || string(settled.Detail) != string(detail) {
		t.Fatalf("submission=%+v error=%v, want restart_limit %q", settled, err, text)
	}
	record, err := h.WaitTask(ctx, task)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeFailed || record.Outcome.Error != text ||
		!jsonEqual(record.Outcome.ErrorDetail, restartLimitDetail) {
		t.Fatalf("task=%+v outcome=%+v error=%v", record, record.Outcome, err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if busy := state.Conversations[record.Conversation].Busy; busy == task {
		t.Fatal("cut-off generation kept the conversation busy")
	}
}

func assertInterruptedReport(t *testing.T, log *reportLog, task TaskID, kind, reason string, restarts int) {
	t.Helper()
	reports := log.of(ReportTaskInterrupted)
	if len(reports) != 1 || reports[0].Task == nil || reports[0].Task.ID != task || reports[0].Task.Kind != kind ||
		reports[0].Reason != reason || reports[0].Restarts != restarts || reports[0].Extension != "" {
		t.Fatalf("task_interrupted reports=%+v", reports)
	}
}

func TestCrashLoopGenerationCutOffInPrepare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	log := &reportLog{}
	entered := make(chan struct{}, 4)
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "slow", Sections: []Section{{Name: "slow", Render: func(ctx context.Context, _ ConversationRecord, _ *Harness) (string, error) {
		entered <- struct{}{}
		<-ctx.Done() // a section that never finishes preparing
		return "", ctx.Err()
	}}}}); err != nil {
		t.Fatal(err)
	}
	model := &scriptedModel{steps: []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error){answerWith("never")}}
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, Registry: registry, OnReport: log.add, Settings: Settings{RestartLimit: 1}})
	h := hosts.open(nil)
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}, Extensions: []string{"slow"}}})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	id := busyTask(t, h, root.ID())
	<-entered
	hosts.crash(h)
	h = hosts.open(nil)
	h.Resume()
	assertRestartLimitRun(t, h, submission.ID(), id, "The generation was interrupted once in its prepare phase")
	if len(model.calls()) != 0 {
		t.Fatal("a cut-off generation sent a request")
	}
	drained(t, h)
	assertInterruptedReport(t, log, id, "pi.generation", reasonRestartLimit, 1)
}

func TestCrashLoopGenerationCutOffInEveryPhase(t *testing.T) {
	for _, tc := range []struct {
		name, checkpoint string
		restarts         int
		limit            float64
		text             string
		partialKept      bool
	}{
		{name: "retry", checkpoint: `{"phase":"retry","attempt":1,"until":1,"turns":1}`, restarts: 1, limit: 1,
			text: "The generation was interrupted once in its retry phase"},
		{name: "tools", checkpoint: `{"phase":"tools","attempt":1,"turns":1}`, restarts: 2, limit: 2,
			text: "The generation was interrupted 2 times in a row in its tools phase"},
		{name: "request-at-limit", checkpoint: `{"phase":"request","attempt":1,"request":{"model":{"provider":"fake","id":"m"},"sessionId":"s","messages":[]},"partial":"half","turns":1}`,
			restarts: 3, limit: 3, text: "The generation was interrupted 3 times in a row in its request phase", partialKept: true},
		{name: "request-past-limit", checkpoint: `{"phase":"request","attempt":1,"request":{"model":{"provider":"fake","id":"m"},"sessionId":"s","messages":[]},"partial":"half","turns":1}`,
			restarts: 3, limit: 2, text: "The generation was interrupted 3 times in a row in its request phase"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &reportLog{}
			model := &scriptedModel{steps: []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error){answerWith("never")}}
			h := taskTestHarness(t, HarnessOptions{Model: model, OnReport: log.add, Settings: Settings{RestartLimit: tc.limit}})
			root := testRoot(t, h)
			id, sub := seedGeneration(t, h, root.ID(), generationVersion, tc.checkpoint, nil)
			if err := h.Commit(context.Background(), func(tx *Tx) error { tx.state.Tasks[id].Restarts = tc.restarts; return nil }); err != nil {
				t.Fatal(err)
			}
			// Submit resumes scheduling, so it follows the seeded count.
			queued, err := root.Submit(context.Background(), SubmissionRequest{Content: "queued"})
			if err != nil {
				t.Fatal(err)
			}
			assertRestartLimitRun(t, h, sub, id, tc.text)
			if len(model.calls()) != 0 {
				t.Fatal("a cut-off generation sent a request")
			}
			if later, err := (&Submission{h: h, id: queued.ID()}).Record(context.Background()); err != nil || later.Status != "queued" {
				t.Fatalf("queued input=%+v error=%v", later, err)
			}
			state, err := h.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			entries, err := entriesFromState(state, root.ID())
			if err != nil {
				t.Fatal(err)
			}
			kept := false
			for _, entry := range entries {
				if entry.Kind == "pi.assistant" && entry.Message.Content == "half" && entry.Message.Aborted {
					kept = true
				}
			}
			if kept != tc.partialKept {
				t.Fatalf("partial kept=%v, want %v", kept, tc.partialKept)
			}
			drained(t, h)
			assertInterruptedReport(t, log, id, "pi.generation", reasonRestartLimit, tc.restarts)
		})
	}
}

func TestCrashLoopToolCutOffEndsWithErrorResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := &reportLog{}
	var attempts atomic.Int32
	blocked := make(chan struct{}, 4)
	registry := toolExtension("stream", true, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		n := attempts.Add(1)
		if err := r.Output(ctx, "partial "+itoa(TaskID(n))); err != nil {
			return ToolResult{}, err
		}
		blocked <- struct{}{}
		<-ctx.Done()
		return ToolResult{}, ctx.Err()
	})
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: &toolCallingModel{name: "stream"}, Registry: registry, OnReport: log.add, Settings: Settings{RestartLimit: 2}})
	h := hosts.open(nil)
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	for crash := 1; crash <= 2; crash++ {
		select {
		case <-blocked:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		hosts.crash(h)
		h = hosts.open(nil)
		if tool := toolTask(t, h); tool.Restarts != crash {
			t.Fatalf("after crash %d: tool restarts=%d", crash, tool.Restarts)
		}
		h.Resume()
	}
	settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx)
	if err != nil || settled.Status != "done" {
		t.Fatalf("the run did not continue to done: %+v error=%v", settled, err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("tool ran %d times, want 2", attempts.Load())
	}
	message := "Tool stream was interrupted 2 times in a row; it is not run again"
	if result := toolResultEntry(t, h, root.ID()); result.Message.Content != "partial 2\n<harness>\n[error] "+message+"\n</harness>" || !result.Message.IsError {
		t.Fatalf("result=%+v", result.Message)
	}
	tool := toolTask(t, h)
	if tool.Outcome == nil || tool.Outcome.Status != OutcomeFailed || tool.Outcome.Error != message || !jsonEqual(tool.Outcome.ErrorDetail, restartLimitDetail) {
		t.Fatalf("tool outcome=%+v", tool.Outcome)
	}
	drained(t, h)
	assertInterruptedReport(t, log, tool.ID, "pi.tool", reasonRestartLimit, 2)
}

func TestCrashLoopUnsafeToolCutOffIsNotAnInterruption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	blocked := make(chan struct{}, 1)
	registry := toolExtension("effect", false, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		blocked <- struct{}{}
		<-ctx.Done()
		return ToolResult{}, ctx.Err()
	})
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: &toolCallingModel{name: "effect"}, Registry: registry, Settings: Settings{RestartLimit: 1}})
	h := hosts.open(nil)
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	<-blocked
	hosts.crash(h)
	h = hosts.open(nil)
	h.Resume()
	if settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx); err != nil || settled.Status != "done" {
		t.Fatalf("submission=%+v error=%v", settled, err)
	}
	if result := toolResultEntry(t, h, root.ID()); result.Message.Content != "<harness>\n[error] Tool effect was interrupted once; it is not run again\n</harness>" {
		t.Fatalf("result=%q", result.Message.Content)
	}
}

func TestCrashLoopCompactionCutOff(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		log := &reportLog{}
		entered := make(chan struct{}, 1)
		model := taskModelFunc(func(ctx context.Context, _ ModelRequest, _ func(string) error) (ModelResponse, error) {
			entered <- struct{}{}
			<-ctx.Done()
			return ModelResponse{}, ctx.Err()
		})
		hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, OnReport: log.add, Settings: Settings{RestartLimit: 1, Compaction: compactEverything}})
		h := hosts.open(nil)
		c, _ := compactionConversation(t, h)
		id, err := c.Compact(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		<-entered
		hosts.crash(h)
		h = hosts.open(nil)
		record, err := h.WaitTask(ctx, id)
		if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeFailed || record.Outcome.Error != "Compaction was interrupted once" ||
			!jsonEqual(record.Outcome.ErrorDetail, restartLimitDetail) {
			t.Fatalf("record=%+v error=%v", record, err)
		}
		drained(t, h)
		assertInterruptedReport(t, log, id, "pi.compaction", reasonRestartLimit, 1)
	})
	t.Run("retry", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		h := taskTestHarness(t, HarnessOptions{Model: taskModelFunc(func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
			return ModelResponse{}, errors.New("not called")
		}), Settings: Settings{RestartLimit: 2}})
		c, cut := compactionConversation(t, h)
		input, _ := json.Marshal(compactionInput{Request: ModelRequest{Model: ModelRef{Provider: "fake", ID: "m"}}, Cut: cut})
		var id TaskID
		if err := h.Commit(ctx, func(tx *Tx) error {
			var err error
			if id, err = tx.CreateTask(c.id, compactionTaskDefinition(), input, TaskOptions{Background: true}); err != nil {
				return err
			}
			task := tx.state.Tasks[id]
			task.Checkpoint, task.Restarts = json.RawMessage(`{"phase":"retry","attempt":1,"until":1}`), 3
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		record, err := h.WaitTask(ctx, id)
		if err != nil || record.Outcome == nil || record.Outcome.Error != "Compaction was interrupted 3 times in a row" {
			t.Fatalf("record=%+v error=%v", record, err)
		}
	})
}
