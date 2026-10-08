package durable

import (
	"context"
	"errors"
	"testing"
	"time"
)

type nonCooperativeModel struct {
	started chan struct{}
	release chan struct{}
}

func (m *nonCooperativeModel) Generate(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
	close(m.started)
	<-m.release
	return ModelResponse{Content: "late"}, nil
}
func TestCloseTimeoutCanBeJoinedAndSealsAdmission(t *testing.T) {
	m := &nonCooperativeModel{started: make(chan struct{}), release: make(chan struct{})}
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{Model: m})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-m.release:
		default:
			close(m.release)
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.Close(cleanupCtx)
	}()
	root, err := h.Root(context.Background(), ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Submit(context.Background(), SubmissionRequest{Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.started:
	case <-time.After(3 * time.Second):
		t.Fatal("model did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := h.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close %v", err)
	}
	if _, err := root.Submit(context.Background(), SubmissionRequest{Content: "after close"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("admission not sealed: %v", err)
	}
	close(m.release)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if err := h.Close(ctx2); err != nil {
		t.Fatal(err)
	}
	if _, err := h.session.Snapshot(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("storage not closed: %v", err)
	}
}
