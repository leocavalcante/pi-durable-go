package durable

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// Plan 0010: any entry with a head starts a new context, as upstream's
// EntryRecord.head does; entries appended by a task are attributed to it.

func TestEntryHeadsStartTheContext(t *testing.T) {
	h := taskTestHarness(t, HarnessOptions{Model: echoModel{}})
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := func(request SubmissionRequest) SubmissionRecord {
		t.Helper()
		sub, err := c.Submit(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		record, err := sub.Wait(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	wait(SubmissionRequest{Content: "before"})
	entries, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Entries a run appends carry its generation's task; a host's carry none.
	var answer Entry
	for _, e := range entries {
		if e.Kind == "pi.assistant" {
			answer = e
		}
	}
	if answer.ByTask == 0 {
		t.Fatalf("the answer has no task: %+v", answer)
	}
	if task, err := h.Task(ctx, answer.ByTask); err != nil || task.Kind != "pi.generation" {
		t.Fatalf("the answer's task %+v, %v", task, err)
	}
	// A host's head entry: the context starts at it.
	topic := wait(SubmissionRequest{Type: "write", Entry: &Entry{Kind: "app.topic", Head: HeadSelf, Message: &Message{Role: "user", Content: "New topic"}}})
	if topic.Status != "done" {
		t.Fatalf("topic %+v", topic)
	}
	placed, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	marker := placed[len(placed)-1]
	if marker.Kind != "app.topic" || marker.Head != marker.ID || marker.ByTask != 0 {
		t.Fatalf("marker %+v", marker)
	}
	messages, err := c.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Content != "New topic" {
		t.Fatalf("context %+v", messages)
	}
	// A head before the active range would bring back cut history: stale.
	stale := wait(SubmissionRequest{Type: "write", Entry: &Entry{Kind: "app.topic", Head: answer.ID, Message: &Message{Role: "user", Content: "old"}}})
	if stale.Status != "unanswered" || stale.Reason != "stale" {
		t.Fatalf("stale write %+v", stale)
	}
	// A head at or after it is placed: the context runs from the head, with
	// the newest marker first.
	kept := wait(SubmissionRequest{Type: "write", Entry: &Entry{Kind: "app.summary", Head: marker.ID, Message: &Message{Role: "user", Content: "Summary"}}})
	if kept.Status != "done" {
		t.Fatalf("kept write %+v", kept)
	}
	messages, err = c.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Content != "Summary" {
		t.Fatalf("context %+v", messages)
	}
}

func TestCompactionEntriesHaveHeads(t *testing.T) {
	h := taskTestHarness(t, HarnessOptions{Model: echoModel{}, Settings: Settings{Compaction: compactEverything}})
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sub, err := c.Submit(ctx, SubmissionRequest{Content: "before"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := c.Compact(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.WaitTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	entries, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	summary := entries[len(entries)-1]
	// Upstream's shape: the first kept entry as its head, why it ran as its
	// data, the summary as its message.
	if summary.Kind != "pi.compaction" || summary.Head != entries[len(entries)-2].ID || string(summary.Data) != `{"reason":"manual"}` || summary.Message == nil {
		t.Fatalf("summary %+v", summary)
	}
}

// Summaries and resets stored before entries had heads still start the
// context: a summary keeps the entries after its cut.
func TestEntriesFromBeforeHeadsStillStartTheContext(t *testing.T) {
	h := taskTestHarness(t, HarnessOptions{Model: echoModel{}})
	c := testRoot(t, h)
	ctx := context.Background()
	var first, second EntryID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		if first, err = tx.Append(c.ID(), Entry{Kind: "pi.user", Message: &Message{Role: "user", Content: "one"}}); err != nil {
			return err
		}
		if second, err = tx.Append(c.ID(), Entry{Kind: "pi.user", Message: &Message{Role: "user", Content: "two"}}); err != nil {
			return err
		}
		data, _ := json.Marshal(map[string]any{"summary": "S", "cut": first})
		_, err = tx.Append(c.ID(), Entry{Kind: "pi.compaction", Data: data})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	messages, err := c.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0].Content != "Conversation summary:\nS" || messages[1].Content != "two" {
		t.Fatalf("context %+v", messages)
	}
	_ = second
	if err := h.Commit(ctx, func(tx *Tx) error {
		_, err := tx.Append(c.ID(), Entry{Kind: "pi.reset"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if messages, err := c.Context(ctx); err != nil || len(messages) != 0 {
		t.Fatalf("after a reset %+v, %v", messages, err)
	}
}

// A write with edits changes what the model sees: while the conversation is
// busy it waits for a boundary, as upstream's isDisplayOnly decides; a write
// without a message, head, or edits is placed at once.
func TestWritesWithEditsWaitForABoundary(t *testing.T) {
	release := make(chan struct{})
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return ModelResponse{}, ctx.Err()
		}
		return ModelResponse{Content: "answer"}, nil
	})
	h := taskTestHarness(t, HarnessOptions{Model: model})
	c := testRoot(t, h)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sub, err := c.Submit(ctx, SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	input := entries[len(entries)-1].ID
	edit, err := c.Submit(ctx, SubmissionRequest{Type: "write", Entry: &Entry{Kind: "app.edit", Edits: []ContextEdit{{Target: input, Action: "omit"}}}})
	if err != nil {
		t.Fatal(err)
	}
	note, err := c.Submit(ctx, SubmissionRequest{Type: "write", Entry: &Entry{Kind: "app.note", Data: json.RawMessage(`{"n":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if record, err := h.Submission(ctx, edit.ID()); err != nil || mustRecord(t, ctx, record).Status != "queued" {
		t.Fatalf("the edit write is not queued")
	}
	if record, err := h.Submission(ctx, note.ID()); err != nil || mustRecord(t, ctx, record).Status != "done" {
		t.Fatalf("the display-only write is not placed")
	}
	close(release)
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if record, err := edit.Wait(ctx); err != nil || record.Status != "done" {
		t.Fatalf("edit %+v, %v", record, err)
	}
}

func mustRecord(t *testing.T, ctx context.Context, s *Submission) SubmissionRecord {
	t.Helper()
	record, err := s.Record(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return record
}
