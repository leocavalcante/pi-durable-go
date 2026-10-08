package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type echoModel struct{}

func (echoModel) Generate(ctx context.Context, r ModelRequest, onDelta func(string) error) (ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return ModelResponse{}, err
	}
	text := "answer"
	for i := len(r.Messages) - 1; i >= 0; i-- {
		if r.Messages[i].Role == "user" {
			text = "echo: " + r.Messages[i].Content
			break
		}
	}
	if err := onDelta(text); err != nil {
		return ModelResponse{}, err
	}
	return ModelResponse{Content: text, Usage: Usage{InputTokens: 3, OutputTokens: 2}}, nil
}
func testHarness(t *testing.T) *Harness {
	t.Helper()
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{Model: echoModel{}})
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
	return h
}
func testRoot(t *testing.T, h *Harness) *Conversation {
	t.Helper()
	c, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "echo"}}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestRootOptionsApplyOnlyOnCreation(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	again, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Instructions: "changed"}})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID() != c.ID() {
		t.Fatal("root changed")
	}
	agent, err := again.Agent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if agent.Instructions != "" {
		t.Fatal("existing root reconfigured")
	}
}
func TestSubmissionDedupAndAnswer(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := c.Submit(ctx, SubmissionRequest{Type: "input", Content: "hello", RequestID: "one"})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != "done" || settled.Answer == 0 {
		t.Fatalf("unsettled: %+v", settled)
	}
	again, err := c.Submit(ctx, SubmissionRequest{Type: "input", Content: "different", RequestID: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID() != sub.ID() {
		t.Fatal("duplicate admission")
	}
	_, err = c.Submit(ctx, SubmissionRequest{Type: "write", RequestID: "one", Entry: &Entry{Kind: "app.note"}})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("type mismatch: %v", err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	entry := state.Entries[settled.Answer]
	if entry == nil || entry.Message == nil || entry.Message.Content != "echo: hello" {
		t.Fatalf("answer %+v", entry)
	}
}
func TestCancelledWaitDoesNotWithdrawSubmission(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	sub, err := c.Submit(context.Background(), SubmissionRequest{Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = sub.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != "done" {
		t.Fatalf("%+v", rec)
	}
}
func TestDocumentForkUsesWholeEntryCommitAndHistoricalAgent(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx := context.Background()
	def := DocumentDefinition{Kind: "app.todos", Version: 1, Scope: "conversation", History: "rewindable", Fork: "asOf", Initial: func() any { return map[string]any{"items": []string{}} }}
	var at EntryID
	err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		at, err = tx.Append(c.id, Entry{Kind: "app.note"})
		if err != nil {
			return err
		}
		return UpdateDocument(tx, def, uint64(c.id), func(v *struct {
			Items []string `json:"items"`
		}) error {
			v.Items = append(v.Items, "at")
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	err = h.Commit(ctx, func(tx *Tx) error {
		return UpdateDocument(tx, def, uint64(c.id), func(v *struct {
			Items []string `json:"items"`
		}) error {
			v.Items = append(v.Items, "later")
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Configure(ctx, Agent{Model: ModelRef{Provider: "fake", ID: "other"}, Instructions: "later"}); err != nil {
		t.Fatal(err)
	}
	child, err := c.Fork(ctx, at, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	todos, err := ReadDocument[struct {
		Items []string `json:"items"`
	}](ctx, h, def, uint64(child.id))
	if err != nil {
		t.Fatal(err)
	}
	if len(todos.Items) != 1 || todos.Items[0] != "at" {
		t.Fatalf("fork state %+v", todos)
	}
	agent, err := child.Agent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Model.ID != "echo" || agent.Instructions != "" {
		t.Fatalf("fork agent %+v", agent)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Conversations[c.id].ProviderSessionID == state.Conversations[child.id].ProviderSessionID {
		t.Fatal("fork shared provider identity")
	}
}
func TestDocumentReadDoesNotCreateAndMigrationPersistsOnAccess(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx := context.Background()
	def := DocumentDefinition{Kind: "app.counter", Version: 1, Scope: "conversation", Initial: func() any { return map[string]int{"n": 1} }}
	_, err := ReadDocument[map[string]int](ctx, h, def, uint64(c.id))
	if !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := h.Commit(ctx, func(tx *Tx) error { _, err := tx.Document(def, uint64(c.id)); return err }); err != nil {
		t.Fatal(err)
	}
	newer := def
	newer.Version = 2
	newer.Migrate = func(version int, data json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`{"n":2}`), nil
	}
	value, err := ReadDocument[map[string]int](ctx, h, newer, uint64(c.id))
	if err != nil {
		t.Fatal(err)
	}
	if value["n"] != 2 {
		t.Fatal(value)
	}
	state, _ := h.Snapshot(ctx)
	if state.Documents[documentKey(def, uint64(c.id))].Version != 1 {
		t.Fatal("read persisted migration")
	}
	if err := h.Commit(ctx, func(tx *Tx) error { _, err := tx.Document(newer, uint64(c.id)); return err }); err != nil {
		t.Fatal(err)
	}
	state, _ = h.Snapshot(ctx)
	if state.Documents[documentKey(def, uint64(c.id))].Version != 2 {
		t.Fatal("commit did not persist migration")
	}
}
func TestContextResetPreservesTranscript(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := c.Submit(ctx, SubmissionRequest{Content: "before"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	reset, err := c.Reset(ctx, "handoff")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reset.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	messages, err := c.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Content != "handoff" {
		t.Fatalf("context %+v", messages)
	}
	entries, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 4 {
		t.Fatal("history lost")
	}
}
func TestCompactionPreservesTranscript(t *testing.T) {
	h := testHarness(t)
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := c.Submit(ctx, SubmissionRequest{Content: "before"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := c.Compact(ctx, "retain goals")
	if err != nil {
		t.Fatal(err)
	}
	task, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if task.Outcome == nil || task.Outcome.Status != "completed" {
		t.Fatalf("%+v", task)
	}
	after, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) <= len(before) {
		t.Fatal("compaction entry absent")
	}
	messages, err := c.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("summary context %+v", messages)
	}
}
