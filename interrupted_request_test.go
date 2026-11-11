package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// crashingStreamModel streams "partial <n>" on call n and then blocks until
// cancelled when block(n) is true; other calls answer "answer <n>".
type crashingStreamModel struct {
	mu       sync.Mutex
	block    func(int) bool
	streamed chan int
	requests []ModelRequest
}

func newCrashingStreamModel(block func(int) bool) *crashingStreamModel {
	return &crashingStreamModel{block: block, streamed: make(chan int, 8)}
}

func (m *crashingStreamModel) Generate(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
	m.mu.Lock()
	m.requests = append(m.requests, request)
	n := len(m.requests)
	m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, err
	}
	if m.block(n) {
		if err := delta(fmt.Sprintf("partial %d", n)); err != nil {
			return ModelResponse{}, err
		}
		m.streamed <- n
		<-ctx.Done()
		return ModelResponse{}, ctx.Err()
	}
	return ModelResponse{Content: fmt.Sprintf("answer %d", n)}, nil
}

func (m *crashingStreamModel) calls() []ModelRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ModelRequest(nil), m.requests...)
}

func (m *crashingStreamModel) waitStreamed(t *testing.T, ctx context.Context) int {
	t.Helper()
	select {
	case n := <-m.streamed:
		return n
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return 0
	}
}

func abortedContents(t *testing.T, h *Harness, cid ConversationID) []string {
	t.Helper()
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entries, err := entriesFromState(state, cid)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, entry := range entries {
		if entry.Kind == "pi.assistant" && entry.Message != nil && entry.Message.Aborted {
			out = append(out, entry.Message.Content)
		}
	}
	return out
}

func TestInterruptedRequestChargedAndRetried(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	log := &reportLog{}
	clock := newTestClock()
	model := newCrashingStreamModel(func(n int) bool { return n == 1 })
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, Now: clock.Now, OnReport: log.add})
	h := hosts.open(nil)
	root := testRoot(t, h)
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	id := busyTask(t, h, root.ID())
	model.waitStreamed(t, ctx)
	waitPartial(t, h, root.ID(), "partial 1")
	hosts.crash(h)
	h = hosts.open(nil)
	if task, err := h.Task(ctx, id); err != nil || task.Restarts != 1 || task.Status != "pending" || generationPhase(task).Phase != "request" {
		t.Fatalf("before resume: task=%+v error=%v", task, err)
	}
	h.Resume()
	backoff := waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "retry" })
	cp := generationPhase(backoff)
	if cp.Attempt != 1 || cp.Until != clock.unixMilliAfter(2000) || backoff.Restarts != 0 {
		t.Fatalf("charge=%+v checkpoint=%+v", backoff, cp)
	}
	if got := abortedContents(t, h, root.ID()); len(got) != 1 || got[0] != "partial 1" {
		t.Fatalf("aborted entries=%q", got)
	}
	if len(model.calls()) != 1 {
		t.Fatal("the interrupted request was resent")
	}
	clock.advance(2 * time.Second)
	h.Resume() // a fake clock needs a kick; timers run on real time
	settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx)
	if err != nil || settled.Status != "done" {
		t.Fatalf("submission=%+v error=%v", settled, err)
	}
	calls := model.calls()
	if len(calls) != 2 {
		t.Fatalf("model calls=%d, want 2", len(calls))
	}
	for _, message := range calls[1].Messages {
		if message.Content == "partial 1" {
			t.Fatal("the charged attempt's partial reached the next request")
		}
	}
	drained(t, h)
	if reports := log.of(ReportTaskInterrupted); len(reports) != 0 {
		t.Fatalf("a retried charge reported task_interrupted: %+v", reports)
	}
}

func TestInterruptedRequestEndsWhenNoRetryLeft(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := &reportLog{}
	clock := newTestClock()
	model := newCrashingStreamModel(func(n int) bool { return n <= 2 })
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, Now: clock.Now, OnReport: log.add, Settings: Settings{Retry: RetrySettings{MaxRetries: ptr(1.0)}}})
	h := hosts.open(nil)
	root := testRoot(t, h)
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	id := busyTask(t, h, root.ID())
	model.waitStreamed(t, ctx)
	waitPartial(t, h, root.ID(), "partial 1")
	hosts.crash(h)
	h = hosts.open(nil)
	h.Resume()
	waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "retry" })
	clock.advance(time.Minute)
	h.Resume() // a fake clock needs a kick; timers run on real time
	if n := model.waitStreamed(t, ctx); n != 2 {
		t.Fatalf("second attempt is call %d", n)
	}
	waitPartial(t, h, root.ID(), "partial 2")
	hosts.crash(h)
	h = hosts.open(nil)
	h.Resume()
	text := "The host stopped while the model was answering, on attempt 2"
	settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx)
	detail, _ := json.Marshal(text)
	if err != nil || settled.Status != "unanswered" || settled.Reason != reasonInterrupted || string(settled.Detail) != string(detail) {
		t.Fatalf("submission=%+v error=%v", settled, err)
	}
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeFailed || record.Outcome.Error != text ||
		!jsonEqual(record.Outcome.ErrorDetail, interruptedDetail) {
		t.Fatalf("record=%+v error=%v", record.Outcome, err)
	}
	if got := abortedContents(t, h, root.ID()); len(got) != 2 || got[0] != "partial 1" || got[1] != "partial 2" {
		t.Fatalf("aborted entries=%q", got)
	}
	if len(model.calls()) != 2 {
		t.Fatalf("a request was sent after the last crash: %d calls", len(model.calls()))
	}
	drained(t, h)
	assertInterruptedReport(t, log, id, "pi.generation", reasonInterrupted, 1)
	if reports := log.of(ReportTaskInterrupted); reports[0].Err == nil || reports[0].Err.Error() != text {
		t.Fatalf("report=%+v", reports[0])
	}
}

func TestInterruptedRequestEndsAtOnceWhenRetriesDisabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	model := newCrashingStreamModel(func(n int) bool { return n == 1 })
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, Settings: Settings{Retry: RetrySettings{Enabled: ptr(false)}}})
	h := hosts.open(nil)
	root := testRoot(t, h)
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	model.waitStreamed(t, ctx)
	hosts.crash(h)
	h = hosts.open(nil)
	h.Resume()
	settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx)
	if err != nil || settled.Status != "unanswered" || settled.Reason != reasonInterrupted || len(model.calls()) != 1 {
		t.Fatalf("submission=%+v calls=%d error=%v", settled, len(model.calls()), err)
	}
}

func TestInterruptedRequestChargedOnceAcrossCleanClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	clock := newTestClock()
	model := newCrashingStreamModel(func(n int) bool { return n == 1 })
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, Now: clock.Now})
	h := hosts.open(nil)
	root := testRoot(t, h)
	submission, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	id := busyTask(t, h, root.ID())
	model.waitStreamed(t, ctx)
	waitPartial(t, h, root.ID(), "partial 1")
	hosts.crash(h)
	// Open and close cleanly without resuming: the count stays, nothing more.
	h = hosts.open(nil)
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	h = hosts.open(nil)
	if task, err := h.Task(ctx, id); err != nil || task.Restarts != 1 {
		t.Fatalf("task=%+v error=%v", task, err)
	}
	h.Resume()
	waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "retry" })
	clock.advance(time.Minute)
	h.Resume() // a fake clock needs a kick; timers run on real time
	if settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx); err != nil || settled.Status != "done" {
		t.Fatalf("submission=%+v error=%v", settled, err)
	}
	if got := abortedContents(t, h, root.ID()); len(got) != 1 || len(model.calls()) != 2 {
		t.Fatalf("aborted=%q calls=%d, want one charge", got, len(model.calls()))
	}
}

func TestInterruptedRequestNotChargedWhileBackingOffOrWaiting(t *testing.T) {
	t.Run("backing-off", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		clock := newTestClock()
		model := &scriptedModel{steps: []func(context.Context, ModelRequest, func(string) error) (ModelResponse, error){
			failWith(errors.New("503 overloaded"), ""), answerWith("ok"),
		}}
		hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, Now: clock.Now})
		h := hosts.open(nil)
		root := testRoot(t, h)
		submission, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
		if err != nil {
			t.Fatal(err)
		}
		id := busyTask(t, h, root.ID())
		waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "retry" })
		hosts.crash(h)
		h = hosts.open(nil)
		if task, err := h.Task(ctx, id); err != nil || task.Restarts != 0 || generationPhase(task).Phase != "retry" {
			t.Fatalf("a backing-off generation was counted: %+v error=%v", task, err)
		}
		clock.advance(time.Minute)
		h.Resume() // a fake clock needs a kick; timers run on real time
		if settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx); err != nil || settled.Status != "done" {
			t.Fatalf("submission=%+v error=%v", settled, err)
		}
		if len(model.calls()) != 2 {
			t.Fatalf("model calls=%d", len(model.calls()))
		}
	})
	t.Run("waiting-for-tools", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		var attempts atomic.Int32
		blocked := make(chan struct{}, 1)
		registry := toolExtension("slow", true, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
			if attempts.Add(1) == 1 {
				blocked <- struct{}{}
				<-ctx.Done()
				return ToolResult{}, ctx.Err()
			}
			return ToolResult{Content: "slow done"}, nil
		})
		hosts := newCrashHosts(t, nil, HarnessOptions{Model: &toolCallingModel{name: "slow"}, Registry: registry})
		h := hosts.open(nil)
		root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
		if err != nil {
			t.Fatal(err)
		}
		submission, err := root.Submit(ctx, SubmissionRequest{Content: "go"})
		if err != nil {
			t.Fatal(err)
		}
		id := busyTask(t, h, root.ID())
		<-blocked
		hosts.crash(h)
		h = hosts.open(nil)
		if task, err := h.Task(ctx, id); err != nil || task.Restarts != 0 || task.Status != "waiting" {
			t.Fatalf("a waiting generation was counted: %+v error=%v", task, err)
		}
		if tool := toolTask(t, h); tool.Restarts != 1 {
			t.Fatalf("the running tool was not counted: %+v", tool)
		}
		h.Resume()
		if settled, err := (&Submission{h: h, id: submission.ID()}).Wait(ctx); err != nil || settled.Status != "done" {
			t.Fatalf("submission=%+v error=%v", settled, err)
		}
		if got := abortedContents(t, h, root.ID()); len(got) != 0 {
			t.Fatalf("a waiting generation was charged: %q", got)
		}
	})
}
