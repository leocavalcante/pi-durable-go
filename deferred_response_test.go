package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// deferringModel answers every request with a deferred handle and answers
// fetches from a script: "later" stays deferred, "answer" answers, and
// "block" waits for cancellation.
type deferringModel struct {
	mu        sync.Mutex
	fetches   []DeferredHandle
	cancelled []DeferredHandle
	script    []string
	requests  int
	fetching  chan struct{}
}

func (m *deferringModel) Generate(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
	m.mu.Lock()
	m.requests++
	m.mu.Unlock()
	return ModelResponse{StopReason: "deferred", Deferred: &DeferredHandle{Provider: "fake", ModelID: "m", ID: "job-1", PollAfterMs: 60_000}}, nil
}

func (m *deferringModel) FetchDeferred(ctx context.Context, model ModelRef, handle DeferredHandle) (ModelResponse, error) {
	m.mu.Lock()
	step := m.script[min(len(m.fetches), len(m.script)-1)]
	m.fetches = append(m.fetches, handle)
	fetching := m.fetching
	m.mu.Unlock()
	switch step {
	case "later":
		return ModelResponse{StopReason: "deferred", Deferred: &DeferredHandle{ID: handle.ID, PollAfterMs: 1}}, nil
	case "block":
		if fetching != nil {
			close(fetching)
		}
		<-ctx.Done()
		return ModelResponse{}, ctx.Err()
	default:
		return ModelResponse{Content: "deferred answer"}, nil
	}
}

func (m *deferringModel) CancelDeferred(_ context.Context, _ ModelRef, handle DeferredHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancelled = append(m.cancelled, handle)
	return nil
}

func (m *deferringModel) counts() (requests, fetches, cancelled int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests, len(m.fetches), len(m.cancelled)
}

func TestDeferredAnswerIsPolledUntilItIsReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clock := newTestClock()
	model := &deferringModel{script: []string{"later", "answer"}}
	var hooked []ModelResponse
	var mu sync.Mutex
	extension := Extension{Name: "observer", Hooks: Hooks{AfterResponse: func(_ context.Context, response *ModelResponse, _ *TaskRuntime) error {
		mu.Lock()
		defer mu.Unlock()
		hooked = append(hooked, *response)
		return nil
	}}}
	h, root := deferredRunHarness(t, model, clock, extension)
	sub := submit(t, root, SubmissionRequest{Content: "hi"})
	id := busyTask(t, h, root.ID())
	sleeping := waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "poll" && task.Status == "sleeping" })
	cp := generationPhase(sleeping)
	if cp.Handle == nil || cp.Handle.ID != "job-1" || cp.PollAt != clock.unixMilliAfter(60_000) {
		t.Fatalf("poll checkpoint=%+v", cp)
	}
	if live := readLiveState(t, h, root.ID()); live.Generation == nil || live.Generation.Deferred == nil || live.Generation.Deferred.PollAt != cp.PollAt {
		t.Fatalf("live=%+v", live.Generation)
	}
	// Not ready at the first poll: polled again, never at or before the
	// previous poll.
	clock.advance(time.Minute)
	h.Resume()
	again := waitForTask(t, h, id, func(task TaskRecord) bool {
		next := generationPhase(task)
		return next.Phase == "poll" && task.Status == "sleeping" && next.PollAt != cp.PollAt
	})
	if next := generationPhase(again); next.PollAt != cp.PollAt+1 {
		t.Fatalf("second poll at %d, first at %d", next.PollAt, cp.PollAt)
	}
	clock.advance(time.Second)
	h.Resume()
	if record := waitSettled(t, h, sub); record.Status != "done" {
		t.Fatalf("submission=%+v", record)
	}
	state, _ := h.Snapshot(ctx)
	if answer := state.Entries[waitSettled(t, h, sub).Answer].Message; answer.Content != "deferred answer" {
		t.Fatalf("answer=%+v", answer)
	}
	if requests, fetches, _ := model.counts(); requests != 1 || fetches != 2 {
		t.Fatalf("requests=%d fetches=%d", requests, fetches)
	}
	// Hooks see the answer, not the deferred responses.
	mu.Lock()
	defer mu.Unlock()
	if len(hooked) != 1 || hooked[0].Content != "deferred answer" {
		t.Fatalf("hooks saw %+v", hooked)
	}
}

func TestAbortingAPolledGenerationCancelsItsDeferredAnswer(t *testing.T) {
	clock := newTestClock()
	model := &deferringModel{script: []string{"answer"}}
	h, root := deferredRunHarness(t, model, clock)
	submit(t, root, SubmissionRequest{Content: "hi"})
	id := busyTask(t, h, root.ID())
	waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "poll" && task.Status == "sleeping" })
	if err := root.Abort(context.Background(), false); err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	waitForTask(t, h, id, func(task TaskRecord) bool { return task.Status == "terminal" })
	if _, fetches, cancelled := model.counts(); fetches != 0 || cancelled != 1 {
		t.Fatalf("fetches=%d cancelled=%d", fetches, cancelled)
	}
}

func TestAnInterruptedPollPollsAgainAfterItsBackoff(t *testing.T) {
	clock := newTestClock()
	model := &deferringModel{script: []string{"block", "answer"}, fetching: make(chan struct{})}
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, Now: clock.Now})
	h := hosts.open(nil)
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	sub := submit(t, root, SubmissionRequest{Content: "hi"})
	id := busyTask(t, h, root.ID())
	waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "poll" && task.Status == "sleeping" })
	clock.advance(time.Minute)
	h.Resume()
	<-model.fetching
	hosts.crash(h)
	h = hosts.open(nil)
	h.Resume()
	backoff := waitForTask(t, h, id, func(task TaskRecord) bool { return generationPhase(task).Phase == "retry" })
	if cp := generationPhase(backoff); cp.Poll == nil || cp.Poll.ID != "job-1" {
		t.Fatalf("retry checkpoint=%+v", cp)
	}
	clock.advance(time.Minute)
	h.Resume()
	if record := waitSettled(t, h, sub); record.Status != "done" {
		t.Fatalf("submission=%+v", record)
	}
	// The answer was fetched again, not requested again.
	if requests, fetches, _ := model.counts(); requests != 1 || fetches != 2 {
		t.Fatalf("requests=%d fetches=%d", requests, fetches)
	}
}

// deferredRunHarness is runHarness on a test clock.
func deferredRunHarness(t *testing.T, model Model, clock *testClock, extensions ...Extension) (*Harness, *Conversation) {
	t.Helper()
	registry := NewRegistry()
	for _, extension := range extensions {
		if err := registry.Install(extension); err != nil {
			t.Fatal(err)
		}
	}
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, Now: clock.Now})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	return h, root
}
