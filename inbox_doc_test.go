package durable

import (
	"context"
	"testing"
)

func TestInboxDocumentMirrorsQueuedSubmissions(t *testing.T) {
	gate := newGatedTool()
	h, root := runHarness(t, toolThenEcho(), Settings{}, gate.extension())
	first := submit(t, root, SubmissionRequest{Content: "tool"})
	<-gate.started
	steer := submit(t, root, SubmissionRequest{Content: "steered", WhenBusy: "steer"})
	follow := submit(t, root, SubmissionRequest{Content: "later"})
	write := submit(t, root, SubmissionRequest{Type: "write", Entry: &Entry{Kind: "pi.reset", Message: &Message{Role: "user", Content: "fresh"}}})
	inbox, err := root.Inbox(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox.Items) != 3 || inbox.Items[0].ID != steer || inbox.Items[0].Mode != "steer" || inbox.Items[1].ID != follow ||
		inbox.Items[1].Mode != "followUp" || inbox.Items[1].Content != "later" || inbox.Items[2].ID != write || inbox.Items[2].Mode != "write" {
		t.Fatalf("inbox=%+v", inbox.Items)
	}
	gate.release <- struct{}{}
	for _, id := range []SubmissionID{first, steer, follow} {
		waitSettled(t, h, id)
	}
	if err := root.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if inbox, _ := root.Inbox(context.Background()); len(inbox.Items) != 0 {
		t.Fatalf("placed items stayed in the inbox: %+v", inbox.Items)
	}
}
