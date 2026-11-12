package durable

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// windowModel answers summaries and requests, and knows a context window.
type windowModel struct {
	window   int
	overflow atomic.Int32 // requests to fail with an overflow first
	mu       sync.Mutex
	requests []ModelRequest
}

func (m *windowModel) ContextWindow(ModelRef) int { return m.window }

func (m *windowModel) Generate(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
	m.mu.Lock()
	m.requests = append(m.requests, request)
	m.mu.Unlock()
	if strings.HasPrefix(request.Messages[0].Content, "You are a context summarization assistant") {
		return ModelResponse{Content: "the summary"}, nil
	}
	if m.overflow.Add(-1) >= 0 {
		return ModelResponse{}, errors.New("400 prompt is too long: 9000 tokens > 8192 maximum")
	}
	return ModelResponse{Content: "answered"}, nil
}

func (m *windowModel) summaries() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, request := range m.requests {
		if strings.HasPrefix(request.Messages[0].Content, "You are a context summarization assistant") {
			n++
		}
	}
	return n
}

func lastRequest(m *windowModel) ModelRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests[len(m.requests)-1]
}

func compactionTasks(t *testing.T, h *Harness) []TaskRecord {
	t.Helper()
	state, _ := h.Snapshot(context.Background())
	var out []TaskRecord
	for _, id := range sortedTaskIDs(state) {
		if task := state.Tasks[id]; task.Kind == "pi.compaction" {
			out = append(out, *task)
		}
	}
	return out
}

func TestCompactionBlockingThresholdCompactsBeforeTheRequest(t *testing.T) {
	model := &windowModel{window: 60}
	settings := Settings{Compaction: CompactionSettings{ReserveTokens: ptr(10.0), KeepRecentTokens: ptr(0.0), BackgroundTokens: ptr(0.0)}}
	h, root := runHarness(t, model, settings)
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: strings.Repeat("long ", 40)}))
	record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "next"}))
	if record.Status != "done" {
		t.Fatalf("submission=%+v", record)
	}
	tasks := compactionTasks(t, h)
	if len(tasks) == 0 || tasks[0].Owner.Task == 0 || tasks[0].Outcome.Status != OutcomeCompleted {
		t.Fatalf("blocking compaction=%+v", tasks)
	}
	// After the head marker, the request's prompt is a new baseline, then the summary.
	if messages := withoutSystem(lastRequest(model).Messages); !strings.HasPrefix(messages[0].Content, summaryPrefix) {
		t.Fatalf("the request after compacting does not start with the summary: %+v", messages[0])
	}
}

func TestCompactionBackgroundThreshold(t *testing.T) {
	model := &windowModel{window: 100}
	// Blocking above 100-10=90 tokens, background above 90-80=10.
	settings := Settings{Compaction: CompactionSettings{ReserveTokens: ptr(10.0), KeepRecentTokens: ptr(0.0), BackgroundTokens: ptr(80.0)}}
	h, root := runHarness(t, model, settings)
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: strings.Repeat("word ", 20)}))
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "second"}))
	if err := h.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	tasks := compactionTasks(t, h)
	if len(tasks) == 0 || tasks[0].Owner.Task != 0 || !tasks[0].Background {
		t.Fatalf("background compaction=%+v", tasks)
	}
}

func TestCompactionAfterContextOverflow(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "compacts-and-answers", false: "disabled-fails"}[enabled], func(t *testing.T) {
			model := &windowModel{}
			settings := Settings{Compaction: CompactionSettings{Enabled: ptr(enabled), KeepRecentTokens: ptr(0.0)}}
			h, root := runHarness(t, model, settings)
			waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "first"}))
			model.overflow.Store(1)
			record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "second"}))
			if !enabled {
				if record.Reason != RunModelError || model.summaries() != 0 {
					t.Fatalf("submission=%+v summaries=%d", record, model.summaries())
				}
				return
			}
			if record.Status != "done" || model.summaries() != 1 {
				t.Fatalf("submission=%+v summaries=%d", record, model.summaries())
			}
			tasks := compactionTasks(t, h)
			live := readLiveState(t, h, root.ID())
			if len(tasks) != 1 || tasks[0].Owner.Task == 0 || live.Compactions != nil {
				t.Fatalf("overflow compaction=%+v live=%+v", tasks, live.Compactions)
			}
		})
	}
}

func TestCompactionPolicyResolution(t *testing.T) {
	ref := ModelRef{Provider: "p", ID: "m"}
	harness := CompactionSettings{KeepRecentTokens: ptr(100.0), ModelOverrides: map[string]CompactionSettings{"p/m": {ReserveTokens: ptr(5.0), Enabled: ptr(true)}}}
	conversation := &CompactionSettings{Enabled: ptr(false), BackgroundTokens: ptr(7.0)}
	policy := resolveCompaction(harness, conversation, ref)
	if policy.enabled || policy.reserveTokens != 5 || policy.keepRecentTokens != 100 || policy.backgroundTokens != 7 {
		t.Fatalf("policy=%+v", policy)
	}
	if policy := resolveCompaction(CompactionSettings{}, nil, ref); !policy.enabled || policy.reserveTokens != 16384 || policy.keepRecentTokens != 20000 || policy.backgroundTokens != 32768 {
		t.Fatalf("defaults=%+v", policy)
	}
}
