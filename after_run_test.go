package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestAfterRunHooksHoldTheRunForTheirTasks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var mu sync.Mutex
	var summaries []RunSummary
	followUp := TaskDefinition{Kind: "test.follow-up", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		if string(task.Input) != `{"review":true}` {
			t.Errorf("input=%s", task.Input)
		}
		started <- struct{}{}
		<-release
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	log := &reportLog{}
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "reviewer", Tasks: []TaskDefinition{followUp}, Hooks: Hooks{
		AfterRun: func(_ context.Context, run RunSummary, _ *TaskRuntime) ([]RunEndTask, error) {
			mu.Lock()
			summaries = append(summaries, run)
			first := len(summaries) == 1
			mu.Unlock()
			if !first {
				return nil, nil
			}
			return []RunEndTask{{Kind: "test.follow-up", Input: json.RawMessage(`{"review":true}`)}, {Kind: "pi.generation"}, {Kind: "test.missing"}}, nil
		}}}); err != nil {
		t.Fatal(err)
	}
	h := taskTestHarness(t, HarnessOptions{Model: echoModel{}, Registry: registry, OnReport: log.add})
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	first := submit(t, root, SubmissionRequest{Content: "first"})
	record := waitSettled(t, h, first)
	if record.Status != "done" {
		t.Fatalf("an answered run settles its inputs at once: %+v", record)
	}
	<-started
	queued := submit(t, root, SubmissionRequest{Content: "second"})
	live := readLiveState(t, h, root.ID())
	if live.Run == nil || live.Run.AfterRun == nil || len(live.Run.AfterRun.Tasks) != 1 || len(live.Run.Inputs) != 0 {
		t.Fatalf("held run=%+v", live.Run)
	}
	if sub, _ := (&Submission{h: h, id: queued}).Record(ctx); sub.Status != "queued" {
		t.Fatalf("an input queued behind the after-run tasks=%+v", sub)
	}
	close(release)
	if record := waitSettled(t, h, queued); record.Status != "done" {
		t.Fatalf("the next run=%+v", record)
	}
	// Inputs settle when the run answers; its hooks run after.
	if err := root.WaitForIdle(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(summaries) != 2 || summaries[0].Reason != "answer" {
		t.Fatalf("summaries=%+v", summaries)
	}
	state, _ := h.Snapshot(ctx)
	if summaries[0].First != state.Submissions[first].Entry || summaries[0].Last != summaries[0].Answer {
		t.Fatalf("summary=%+v first entry=%d", summaries[0], state.Submissions[first].Entry)
	}
	drained(t, h)
	if reports := log.of(ReportExtensionError); len(reports) != 2 || reports[0].Extension != "reviewer" {
		t.Fatalf("rejected requests=%+v", reports)
	}
}

func TestAfterRunHooksSeeATerminatingRound(t *testing.T) {
	summaries := make(chan RunSummary, 1)
	registry := toolExtension("finish", true, func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
		return ToolResult{Content: "finished", Terminate: true}, nil
	})
	if err := registry.Install(Extension{Name: "after", Hooks: Hooks{AfterRun: func(_ context.Context, run RunSummary, _ *TaskRuntime) ([]RunEndTask, error) {
		summaries <- run
		return nil, nil
	}}}); err != nil {
		t.Fatal(err)
	}
	h := taskTestHarness(t, HarnessOptions{Model: &toolCallingModel{name: "finish"}, Registry: registry})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	run := <-summaries
	if err := root.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	result := toolResultEntry(t, h, root.ID())
	if record.Status != "done" || run.Reason != "terminate" || run.Answer != record.Answer || run.Last != result.ID {
		t.Fatalf("record=%+v run=%+v result=%d", record, run, result.ID)
	}
	if live := readLiveState(t, h, root.ID()); live.Run != nil {
		t.Fatalf("run not ended: %+v", live.Run)
	}
}
