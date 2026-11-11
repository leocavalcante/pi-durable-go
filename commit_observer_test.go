package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

func TestObserveCommitsKeepsAReplicaEqualToStorage(t *testing.T) {
	store := NewMemoryStorage()
	log := &reportLog{}
	h, err := Open(context.Background(), store, HarnessOptions{Model: echoModel{}, OnReport: log.add})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	replica := initial
	detach := h.ObserveCommits(func(delta *StateDelta) {
		mu.Lock()
		defer mu.Unlock()
		if err := delta.ApplyTo(replica); err != nil {
			t.Error(err)
		}
	})
	panicking := h.ObserveCommits(func(*StateDelta) { panic("observer bug") })
	root := testRoot(t, h)
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	if err := root.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	panicking()
	detach()
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	stored := store.storedStateJSON()
	mu.Lock()
	got, _ := json.Marshal(replica)
	mu.Unlock()
	if string(got) != string(stored) {
		t.Fatalf("replica diverged:\n%s\nstored\n%s", got, stored)
	}
	drained(t, h)
	if reports := log.of(ReportListenerError); len(reports) == 0 {
		t.Fatal("an observer panic was not reported")
	}
}
