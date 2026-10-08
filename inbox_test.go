package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type boundaryModel struct {
	calls  atomic.Int32
	toolAt int32
}

func (m *boundaryModel) Generate(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, err
	}
	if m.calls.Add(1) == m.toolAt {
		return ModelResponse{ToolCalls: []ToolCall{{ID: "boundary-tool", Name: "gate", Arguments: json.RawMessage(`{}`)}}}, nil
	}
	text := "answer"
	for i := len(request.Messages) - 1; i >= 0; i-- {
		if request.Messages[i].Role == "user" {
			text = request.Messages[i].Content
			break
		}
	}
	return ModelResponse{Content: text}, nil
}
func boundaryHarness(t *testing.T, toolAt int32) (*Harness, *Conversation, <-chan struct{}, chan<- struct{}) {
	t.Helper()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{Model: &boundaryModel{toolAt: toolAt}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	})
	if err := h.registry.Install(Extension{Name: "gate", Tools: []ToolDefinition{{ToolSpec: ToolSpec{Name: "gate", Parameters: json.RawMessage(`{"type":"object"}`)}, Execute: func(ctx context.Context, args json.RawMessage, api *ToolRuntime) (ToolResult, error) {
		started <- struct{}{}
		select {
		case <-release:
			return ToolResult{Content: "tool complete"}, nil
		case <-ctx.Done():
			return ToolResult{}, ctx.Err()
		}
	}}}}); err != nil {
		t.Fatal(err)
	}
	return h, testRoot(t, h), started, release
}
func TestSteerJoinsRunAndFollowUpStartsNext(t *testing.T) {
	h, root, started, release := boundaryHarness(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := root.Submit(ctx, SubmissionRequest{Content: "first"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("tool not started")
	}
	if _, err := root.Submit(ctx, SubmissionRequest{Content: "reject", WhenBusy: "reject"}); !errors.Is(err, ErrBusy) {
		t.Fatalf("reject %v", err)
	}
	steer, err := root.Submit(ctx, SubmissionRequest{Content: "steer", WhenBusy: "steer"})
	if err != nil {
		t.Fatal(err)
	}
	follow, err := root.Submit(ctx, SubmissionRequest{Content: "follow"})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	a, err := first.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := steer.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	next, err := follow.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != "done" || b.Status != "done" || a.Answer != b.Answer || next.Answer == a.Answer {
		t.Fatalf("first %+v steer %+v follow %+v", a, b, next)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Entries[a.Answer].Message.Content != "steer" || state.Entries[next.Answer].Message.Content != "follow" {
		t.Fatal("input order changed")
	}
}
func TestQueuedCompactionBecomesStaleAtResetPlacement(t *testing.T) {
	_, root, started, release := boundaryHarness(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := root.Submit(ctx, SubmissionRequest{Content: "baseline"})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := first.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	active, err := root.Submit(ctx, SubmissionRequest{Content: "active"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("tool not started")
	}
	reset, err := root.Reset(ctx, "handoff")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(struct {
		Summary string  `json:"summary"`
		Cut     EntryID `json:"cut"`
	}{"obsolete", baseline.Answer})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := root.Submit(ctx, SubmissionRequest{Type: "write", Entry: &Entry{Kind: "pi.compaction", Data: data}})
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	stale, err := summary.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Status != "unanswered" || stale.Reason != "stale" {
		t.Fatalf("summary %+v", stale)
	}
	if rec, err := reset.Wait(ctx); err != nil || rec.Status != "done" {
		t.Fatalf("reset %+v %v", rec, err)
	}
	rec, err := active.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != "unanswered" || rec.Reason != "reset" {
		t.Fatalf("run %+v", rec)
	}
	messages, err := root.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Content != "handoff" {
		t.Fatalf("reset context %+v", messages)
	}
}
