package durable

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type contextRoundsModel struct {
	mu       sync.Mutex
	requests []ModelRequest
}

func (m *contextRoundsModel) Generate(ctx context.Context, request ModelRequest, onDelta func(string) error) (ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, err
	}
	m.mu.Lock()
	m.requests = append(m.requests, request)
	m.mu.Unlock()
	return ModelResponse{Content: "summary"}, nil
}

func (m *contextRoundsModel) capturedRequests() []ModelRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]ModelRequest(nil), m.requests...)
}

func newContextRoundsConversation(t *testing.T) (*Harness, *Conversation, *contextRoundsModel) {
	t.Helper()
	model := &contextRoundsModel{}
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{Model: model, Settings: Settings{Compaction: compactEverything}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	c, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "context-rounds"}}})
	if err != nil {
		t.Fatal(err)
	}
	return h, c, model
}

func appendContextRoundEntries(t *testing.T, h *Harness, c *Conversation, entries ...Entry) {
	t.Helper()
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		for _, entry := range entries {
			if _, err := tx.Append(c.ID(), entry); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func messageEntry(kind string, message Message) Entry {
	return Entry{Kind: kind, Message: &message}
}

func TestContextToolResultsMatchWithinAssistantWindow(t *testing.T) {
	h, c, _ := newContextRoundsConversation(t)
	callA := ToolCall{ID: "call-a", Name: "alpha", Arguments: json.RawMessage(`null`)}
	callB := ToolCall{ID: "call-b", Name: "beta", Arguments: json.RawMessage(`null`)}
	abortedCall := ToolCall{ID: "aborted-call", Name: "aborted", Arguments: json.RawMessage(`null`)}
	missingCall := ToolCall{ID: "missing-call", Name: "missing", Arguments: json.RawMessage(`null`)}
	appendContextRoundEntries(t, h, c,
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: "orphan", Content: "before any assistant"}),
		messageEntry("pi.user", Message{Role: "user", Content: "question"}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "working", ToolCalls: []ToolCall{callA, callB}}),
		// Not a pi.system baseline: a system message another entry carries
		// stays where it was committed.
		messageEntry("app.system", Message{Role: "system", Content: "intervening system"}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: callB.ID, ToolName: "stored-beta", Content: "first beta", IsError: true}),
		messageEntry("pi.user", Message{Role: "user", Content: "intervening user"}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: "unknown", Content: "unmatched"}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: callB.ID, Content: "duplicate beta"}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: callA.ID, ToolName: "alpha", Content: "alpha result"}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "aborted partial", Aborted: true, ToolCalls: []ToolCall{abortedCall}}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: abortedCall.ID, Content: "aborted result", Aborted: true}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "missing result", ToolCalls: []ToolCall{missingCall}}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: missingCall.ID, Content: "aborted result", Aborted: true}),
	)

	got, err := c.Context(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Message{
		{Role: "user", Content: "question"},
		{Role: "assistant", Content: "working", ToolCalls: []ToolCall{callA, callB}},
		{Role: "tool", ToolCallID: callA.ID, ToolName: "alpha", Content: "alpha result"},
		{Role: "tool", ToolCallID: callB.ID, ToolName: "stored-beta", Content: "first beta", IsError: true},
		{Role: "system", Content: "intervening system"},
		{Role: "user", Content: "intervening user"},
		{Role: "assistant", Content: "missing result", ToolCalls: []ToolCall{missingCall}},
		{Role: "tool", ToolCallID: missingCall.ID, ToolName: missingCall.Name, IsError: true, Content: "Tool result unavailable: history ends before this call completed.", Details: json.RawMessage(`{"reason":"missing_result"}`)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("context mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestContextToolResultsDoNotCrossAssistantRoundsWithReusedIDs(t *testing.T) {
	h, c, _ := newContextRoundsConversation(t)
	first := ToolCall{ID: "reused", Name: "first-round", Arguments: json.RawMessage(`null`)}
	second := ToolCall{ID: "reused", Name: "second-round", Arguments: json.RawMessage(`null`)}
	third := ToolCall{ID: "third", Name: "unfinished", Arguments: json.RawMessage(`null`)}
	appendContextRoundEntries(t, h, c,
		messageEntry("pi.user", Message{Role: "user", Content: "start"}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "round one", ToolCalls: []ToolCall{first}}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "round two", ToolCalls: []ToolCall{second}}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: "reused", ToolName: "second-round", Content: "second round result"}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: "reused", Content: "duplicate second result"}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "round three", ToolCalls: []ToolCall{third}}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: "reused", Content: "too late for round one or two"}),
	)

	got, err := c.Context(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Message{
		{Role: "user", Content: "start"},
		{Role: "assistant", Content: "round one", ToolCalls: []ToolCall{first}},
		{Role: "tool", ToolCallID: first.ID, ToolName: first.Name, IsError: true, Content: "Tool result unavailable: history ends before this call completed.", Details: json.RawMessage(`{"reason":"missing_result"}`)},
		{Role: "assistant", Content: "round two", ToolCalls: []ToolCall{second}},
		{Role: "tool", ToolCallID: second.ID, ToolName: "second-round", Content: "second round result"},
		{Role: "assistant", Content: "round three", ToolCalls: []ToolCall{third}},
		{Role: "tool", ToolCallID: third.ID, ToolName: third.Name, IsError: true, Content: "Tool result unavailable: history ends before this call completed.", Details: json.RawMessage(`{"reason":"missing_result"}`)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("context mismatch\n got: %#v\nwant: %#v", got, want)
	}
}

func TestContextToolRoundsRespectResetAndKeepStoredEntries(t *testing.T) {
	h, c, _ := newContextRoundsConversation(t)
	oldCall := ToolCall{ID: "before-reset", Name: "old", Arguments: json.RawMessage(`null`)}
	newCall := ToolCall{ID: "after-reset", Name: "new", Arguments: json.RawMessage(`null`)}
	appendContextRoundEntries(t, h, c,
		messageEntry("pi.user", Message{Role: "user", Content: "old history"}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "old call", ToolCalls: []ToolCall{oldCall}}),
		messageEntry("pi.reset", Message{Role: "user", Content: "reset note"}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "new call", ToolCalls: []ToolCall{newCall}}),
		messageEntry("pi.user", Message{Role: "user", Content: "after call"}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: newCall.ID, ToolName: "new", Content: "new result"}),
	)
	before, err := c.Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Context(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []Message{
		{Role: "user", Content: "reset note"},
		{Role: "assistant", Content: "new call", ToolCalls: []ToolCall{newCall}},
		{Role: "tool", ToolCallID: newCall.ID, ToolName: "new", Content: "new result"},
		{Role: "user", Content: "after call"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reset context mismatch\n got: %#v\nwant: %#v", got, want)
	}
	after, err := c.Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("context derivation changed stored entries")
	}
	if len(after) < 6 {
		t.Fatalf("reset discarded transcript entries: %d", len(after))
	}
}

func TestGenerationRequestUsesNormalizedToolRoundContext(t *testing.T) {
	h, c, model := newContextRoundsConversation(t)
	call := ToolCall{ID: "generation-call", Name: "lookup", Arguments: json.RawMessage(`null`)}
	appendContextRoundEntries(t, h, c,
		messageEntry("pi.user", Message{Role: "user", Content: "prior question"}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "calling lookup", ToolCalls: []ToolCall{call}}),
		messageEntry("pi.user", Message{Role: "user", Content: "intervening note"}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: "unmatched", Content: "discard this"}),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	submission, err := c.Submit(ctx, SubmissionRequest{Content: "continue"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := submission.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	requests := model.capturedRequests()
	if len(requests) != 1 {
		t.Fatalf("generation model requests: %d", len(requests))
	}
	want := []Message{
		{Role: "user", Content: "prior question"},
		{Role: "assistant", Content: "calling lookup", ToolCalls: []ToolCall{call}},
		{Role: "tool", ToolCallID: call.ID, ToolName: call.Name, IsError: true, Content: "Tool result unavailable: history ends before this call completed.", Details: json.RawMessage(`{"reason":"missing_result"}`)},
		{Role: "user", Content: "intervening note"},
		{Role: "user", Content: "continue"},
	}
	if !reflect.DeepEqual(requests[0].Messages, want) {
		t.Fatalf("generation request context mismatch\n got: %#v\nwant: %#v", requests[0].Messages, want)
	}
}

func TestCompactionInputUsesNormalizedToolRoundContextWithoutRewritingHistory(t *testing.T) {
	h, c, model := newContextRoundsConversation(t)
	call := ToolCall{ID: "compact-call", Name: "lookup", Arguments: json.RawMessage(`null`)}
	appendContextRoundEntries(t, h, c,
		messageEntry("pi.user", Message{Role: "user", Content: "question for compaction"}),
		messageEntry("pi.assistant", Message{Role: "assistant", Content: "calling lookup", ToolCalls: []ToolCall{call}}),
		messageEntry("pi.user", Message{Role: "user", Content: "intervening note"}),
		messageEntry("pi.tool-result", Message{Role: "tool", ToolCallID: "unmatched", Content: "discard this"}),
	)
	beforeEntries, err := c.Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	beforeContext, err := c.Context(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taskID, err := c.Compact(ctx, "keep tool call identity")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitTask(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	requests := model.capturedRequests()
	if len(requests) != 1 || len(requests[0].Messages) != 2 {
		t.Fatalf("compaction model requests: %+v", requests)
	}
	// The summarized range is normalized like model context: the call's
	// missing result is synthesized after it, and the unmatched one dropped.
	prompt := requests[0].Messages[1].Content
	want := "[User]: question for compaction\n\n[Assistant]: calling lookup\n\n[Assistant tool calls]: lookup()\n\n[Tool result]: Tool result unavailable: history ends before this call completed."
	if !strings.HasPrefix(prompt, "<conversation>\n"+want+"\n</conversation>") || !strings.HasSuffix(prompt, "Additional focus: keep tool call identity") {
		t.Fatalf("summary prompt=%q", prompt)
	}
	_ = beforeContext

	afterEntries, err := c.Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entriesByID := make(map[EntryID]Entry, len(afterEntries))
	for _, entry := range afterEntries {
		entriesByID[entry.ID] = entry
	}
	for _, entry := range beforeEntries {
		if got, ok := entriesByID[entry.ID]; !ok || !reflect.DeepEqual(got, entry) {
			t.Fatalf("stored entry %d changed during context reduction", entry.ID)
		}
	}
}
