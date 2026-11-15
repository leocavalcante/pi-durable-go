package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// The pacer follows upstream chord's Progress: the first change commits at
// once, changes while a commit is in flight coalesce into the next one, and
// each commit delays the next by the minimum interval or its size at 100
// KiB/s, rechecking the clock when its timer fires.
func TestProgressPacerAsChord(t *testing.T) {
	clock := newTestClock()
	timers := &fakeTimers{}
	var mu sync.Mutex
	var writes int
	release := make(chan struct{})
	started := make(chan int, 10)
	p := &progressPacer{minInterval: 50 * time.Millisecond, now: clock.Now, setTimer: timers.SetTimer,
		write: func() (int, error) {
			mu.Lock()
			writes++
			n := writes
			mu.Unlock()
			started <- n
			<-release
			return 20 * 1024, nil // 200 ms at 100 KiB/s
		}}
	p.mark()
	if n := <-started; n != 1 {
		t.Fatalf("first write %d", n)
	}
	// In flight: these coalesce.
	p.mark()
	waiter := p.markAndWait()
	release <- struct{}{}
	// The next commit waits for its pause: 200 ms, more than the minimum.
	timer := timers.waitArmed(t, func(timer *fakeTimer) bool { return !timer.stopped })
	if timer.delay != 200*time.Millisecond {
		t.Fatalf("pause %v, want 200ms", timer.delay)
	}
	// A timer that fires early waits for the rest.
	clock.advance(150 * time.Millisecond)
	timer.callback()
	rest := timers.waitArmed(t, func(other *fakeTimer) bool { return other != timer && !other.stopped })
	if rest.delay != 50*time.Millisecond {
		t.Fatalf("rest %v, want 50ms", rest.delay)
	}
	clock.advance(50 * time.Millisecond)
	rest.callback()
	if n := <-started; n != 2 {
		t.Fatalf("second write %d", n)
	}
	release <- struct{}{}
	if err := <-waiter; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if writes != 2 {
		t.Fatalf("%d writes, want 2", writes)
	}
	mu.Unlock()
	// Stopped: what is still waiting goes to the caller.
	late := p.markAndWait()
	waiters := p.stop()
	if len(waiters) != 1 || (<-chan error)(waiters[0]) != late {
		t.Fatalf("waiters %d", len(waiters))
	}
}

// A tool's first output after an idle period commits at once, without
// waiting for the interval; the next waits for its pause (upstream).
func TestFirstToolOutputCommitsAtOnce(t *testing.T) {
	clock := newTestClock()
	timers := &fakeTimers{}
	second := make(chan struct{})
	release := make(chan struct{})
	registry := toolExtension("talker", true, func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
		if err := r.Output(ctx, "first "); err != nil {
			return ToolResult{}, err
		}
		<-second
		if err := r.Output(ctx, "second"); err != nil {
			return ToolResult{}, err
		}
		<-release
		return ToolResult{}, nil
	})
	h := taskTestHarness(t, HarnessOptions{Model: &toolCallingModel{name: "talker"}, Registry: registry, Now: clock.Now, SetTimer: timers.SetTimer})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	sub := submit(t, root, SubmissionRequest{Content: "go"})
	var task TaskID
	deadline := time.Now().Add(5 * time.Second)
	for task == 0 && time.Now().Before(deadline) {
		if state, err := h.Snapshot(context.Background()); err == nil {
			for _, record := range state.Tasks {
				if record.Kind == "pi.tool" {
					task = record.ID
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	if slot := awaitSlot(t, h, root.ID(), task, func(slot ToolSlot) bool { return slot.Output != "" }); slot.Output != "first " {
		t.Fatalf("first output: %q", slot.Output)
	}
	close(second)
	// The pause after the first commit: the default 100 ms.
	timer := timers.waitArmed(t, func(timer *fakeTimer) bool { return timer.delay == 100*time.Millisecond && !timer.stopped })
	if slot := toolSlot(t, h, root.ID(), task); slot.Output != "first " {
		t.Fatalf("committed before its pause: %q", slot.Output)
	}
	clock.advance(100 * time.Millisecond)
	timer.callback()
	if slot := awaitSlot(t, h, root.ID(), task, func(slot ToolSlot) bool { return slot.Output == "first second" }); slot.Output != "first second" {
		t.Fatalf("second output: %q", slot.Output)
	}
	close(release)
	waitSettled(t, h, sub)
}
