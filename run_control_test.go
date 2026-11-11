package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func lastUser(request ModelRequest) string {
	for i := len(request.Messages) - 1; i >= 0; i-- {
		if request.Messages[i].Role == "user" {
			return request.Messages[i].Content
		}
	}
	return ""
}

func hasToolResult(request ModelRequest) bool {
	return len(request.Messages) > 0 && request.Messages[len(request.Messages)-1].Role == "tool"
}

func readLiveState(t *testing.T, h *Harness, cid ConversationID) LiveState {
	t.Helper()
	live, err := ReadDocument[LiveState](context.Background(), h, liveDocument, uint64(cid))
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	return live
}

func waitSettled(t *testing.T, h *Harness, id SubmissionID) SubmissionRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	record, err := (&Submission{h: h, id: id}).Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func submit(t *testing.T, c *Conversation, request SubmissionRequest) SubmissionID {
	t.Helper()
	sub, err := c.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return sub.ID()
}

// gatedTool blocks each call until released, so tests can queue submissions
// while a run waits for its tool round.
type gatedTool struct {
	started chan struct{}
	release chan struct{}
}

func newGatedTool() *gatedTool {
	return &gatedTool{started: make(chan struct{}, 8), release: make(chan struct{}, 8)}
}

func (g *gatedTool) extension() Extension {
	return Extension{Name: "gated", Tools: []ToolDefinition{{ToolSpec: ToolSpec{Name: "gate", Parameters: json.RawMessage(`{"type":"object"}`)},
		Execute: func(ctx context.Context, _ json.RawMessage, _ *ToolRuntime) (ToolResult, error) {
			g.started <- struct{}{}
			select {
			case <-g.release:
				return ToolResult{Content: "gate done"}, nil
			case <-ctx.Done():
				return ToolResult{}, ctx.Err()
			}
		}}}}
}

// toolThenEcho calls the gate tool for prompts starting with "tool", then
// answers each user message by echoing it.
func toolThenEcho() taskModelFunc {
	return func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		if err := ctx.Err(); err != nil {
			return ModelResponse{}, err
		}
		if !hasToolResult(request) && strings.HasPrefix(lastUser(request), "tool") {
			return ModelResponse{ToolCalls: []ToolCall{{ID: "call-" + lastUser(request), Name: "gate", Arguments: json.RawMessage(`{}`)}}}, nil
		}
		return ModelResponse{Content: "echo: " + lastUser(request)}, nil
	}
}

func runHarness(t *testing.T, model Model, settings Settings, extensions ...Extension) (*Harness, *Conversation) {
	t.Helper()
	registry := NewRegistry()
	for _, extension := range extensions {
		if err := registry.Install(extension); err != nil {
			t.Fatal(err)
		}
	}
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, Settings: settings})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	return h, root
}

func TestRunControlHandsTheRunToEachTurn(t *testing.T) {
	gate := newGatedTool()
	h, root := runHarness(t, toolThenEcho(), Settings{}, gate.extension())
	sub := submit(t, root, SubmissionRequest{Content: "tool please"})
	<-gate.started
	live := readLiveState(t, h, root.ID())
	if live.Run == nil || len(live.Run.Inputs) != 1 || live.Run.Inputs[0] != sub || live.Run.Turns != 1 || live.Run.First == 0 {
		t.Fatalf("run during the round=%+v", live.Run)
	}
	if len(live.Tools) != 1 || live.Tools[0].Status != "running" || live.Tools[0].Name != "gate" {
		t.Fatalf("slots=%+v", live.Tools)
	}
	first := live.Run.TaskID
	gate.release <- struct{}{}
	record := waitSettled(t, h, sub)
	if record.Status != "done" || record.Entry == 0 || record.Answer == 0 {
		t.Fatalf("submission=%+v", record)
	}
	live = readLiveState(t, h, root.ID())
	if live.Run != nil || live.Tools != nil || live.Generation != nil || live.RunFailure != nil {
		t.Fatalf("idle live state=%+v", live)
	}
	state, _ := h.Snapshot(context.Background())
	generations := 0
	for _, task := range state.Tasks {
		if task.Kind == "pi.generation" {
			generations++
			if task.Outcome == nil || task.Outcome.Status != OutcomeCompleted {
				t.Fatalf("generation=%+v", task)
			}
		}
	}
	if generations != 2 || state.Tasks[first].Outcome == nil {
		t.Fatalf("a run of one tool round has %d generations", generations)
	}
}

func TestRunControlFailureReasons(t *testing.T) {
	t.Run("model_error", func(t *testing.T) {
		h, root := runHarness(t, taskModelFunc(func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
			return ModelResponse{}, errors.New("invalid request: bad schema")
		}), Settings{})
		sub := submit(t, root, SubmissionRequest{Content: "hi"})
		record := waitSettled(t, h, sub)
		if record.Status != "unanswered" || record.Reason != RunModelError || string(record.Detail) != `"invalid request: bad schema"` {
			t.Fatalf("submission=%+v", record)
		}
		live := readLiveState(t, h, root.ID())
		if live.RunFailure == nil || live.RunFailure.Reason != RunModelError || live.RunFailure.Detail != "invalid request: bad schema" ||
			len(live.RunFailure.Inputs) != 1 || live.RunFailure.First != record.Entry {
			t.Fatalf("runFailure=%+v", live.RunFailure)
		}
		task, _ := h.Task(context.Background(), live.RunFailure.TaskID)
		if task.Outcome == nil || task.Outcome.Status != OutcomeFailed || !jsonEqual(task.Outcome.ErrorDetail, json.RawMessage(`{"reason":"model_error"}`)) {
			t.Fatalf("outcome=%+v", task.Outcome)
		}
	})
	t.Run("no_model", func(t *testing.T) {
		h, root := runHarness(t, nil, Settings{})
		record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
		if record.Status != "unanswered" || record.Reason != RunNoModel || record.Detail != nil {
			t.Fatalf("submission=%+v", record)
		}
		if live := readLiveState(t, h, root.ID()); live.RunFailure == nil || live.RunFailure.Detail != "No model is configured" {
			t.Fatalf("runFailure=%+v", live.RunFailure)
		}
	})
	t.Run("nothing_to_answer", func(t *testing.T) {
		h, root := runHarness(t, toolThenEcho(), Settings{})
		if record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"})); record.Status != "done" {
			t.Fatalf("first=%+v", record)
		}
		record := waitSettled(t, h, submit(t, root, SubmissionRequest{Type: "continue"}))
		if record.Status != "unanswered" || record.Reason != RunNothingToAnswer || record.Request.Type != "continue" {
			t.Fatalf("continue after an answer=%+v", record)
		}
	})
	t.Run("turn_limit", func(t *testing.T) {
		gate := newGatedTool()
		always := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
			return ModelResponse{ToolCalls: []ToolCall{{ID: "c" + itoa(TaskID(len(request.Messages))), Name: "gate", Arguments: json.RawMessage(`{}`)}}}, nil
		})
		registry := NewRegistry()
		if err := registry.Install(gate.extension()); err != nil {
			t.Fatal(err)
		}
		h := taskTestHarness(t, HarnessOptions{Model: always, Registry: registry})
		root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}, MaxTurns: 2}})
		if err != nil {
			t.Fatal(err)
		}
		gate.release <- struct{}{}
		gate.release <- struct{}{}
		record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "loop"}))
		if record.Status != "unanswered" || record.Reason != RunTurnLimit || string(record.Detail) != `"2 turns"` {
			t.Fatalf("submission=%+v", record)
		}
		// A continue resumes on the round's results.
		gate.release <- struct{}{}
		gate.release <- struct{}{}
		if record := waitSettled(t, h, submit(t, root, SubmissionRequest{Type: "continue"})); record.Reason != RunTurnLimit {
			t.Fatalf("continued run=%+v", record)
		}
	})
	t.Run("aborted", func(t *testing.T) {
		gate := newGatedTool()
		h, root := runHarness(t, toolThenEcho(), Settings{}, gate.extension())
		sub := submit(t, root, SubmissionRequest{Content: "tool"})
		<-gate.started
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := root.Abort(ctx, false); err != nil {
			t.Fatal(err)
		}
		record := waitSettled(t, h, sub)
		if record.Status != "unanswered" || record.Reason != RunAborted {
			t.Fatalf("submission=%+v", record)
		}
		if live := readLiveState(t, h, root.ID()); live.RunFailure == nil || live.RunFailure.Reason != RunAborted || live.Run != nil {
			t.Fatalf("live=%+v", live)
		}
	})
}

func TestRunControlResetAtPostToolsBoundary(t *testing.T) {
	gate := newGatedTool()
	h, root := runHarness(t, toolThenEcho(), Settings{}, gate.extension())
	first := submit(t, root, SubmissionRequest{Content: "tool"})
	<-gate.started
	steer := submit(t, root, SubmissionRequest{Content: "steered", WhenBusy: "steer"})
	if _, err := root.Reset(context.Background(), "fresh context"); err != nil {
		t.Fatal(err)
	}
	gate.release <- struct{}{}
	if record := waitSettled(t, h, first); record.Status != "unanswered" || record.Reason != RunReset {
		t.Fatalf("first=%+v", record)
	}
	// The steer was queued before the reset, so it runs in the new context.
	if record := waitSettled(t, h, steer); record.Status != "done" {
		t.Fatalf("steer=%+v", record)
	}
	live := readLiveState(t, h, root.ID())
	if live.RunFailure == nil || live.RunFailure.Reason != RunReset {
		t.Fatalf("a run started in the commit that ended the failed one keeps its failure: %+v", live.RunFailure)
	}
	context, err := root.Context(context.Background())
	if err != nil || context[0].Content != "fresh context" {
		t.Fatalf("context=%+v error=%v", context, err)
	}
}

func TestRunControlAfterFailedRun(t *testing.T) {
	for _, policy := range []string{AfterFailedRunKeep, AfterFailedRunContinue, AfterFailedRunWithdraw} {
		t.Run(policy, func(t *testing.T) {
			release := make(chan struct{})
			var calls atomic.Int32
			model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
				if calls.Add(1) == 1 {
					<-release
					return ModelResponse{}, errors.New("bad request")
				}
				return ModelResponse{Content: "echo: " + lastUser(request)}, nil
			})
			h, root := runHarness(t, model, Settings{AfterFailedRun: policy})
			failing := submit(t, root, SubmissionRequest{Content: "fails"})
			queued := submit(t, root, SubmissionRequest{Content: "queued"})
			close(release)
			if record := waitSettled(t, h, failing); record.Reason != RunModelError {
				t.Fatalf("failing=%+v", record)
			}
			switch policy {
			case AfterFailedRunKeep:
				time.Sleep(20 * time.Millisecond)
				record, _ := (&Submission{h: h, id: queued}).Record(context.Background())
				if record.Status != "queued" {
					t.Fatalf("kept input=%+v", record)
				}
				// The next input places the kept one with it.
				next := submit(t, root, SubmissionRequest{Content: "next"})
				if record := waitSettled(t, h, queued); record.Status != "done" {
					t.Fatalf("kept input after the next submission=%+v", record)
				}
				waitSettled(t, h, next)
			case AfterFailedRunContinue:
				if record := waitSettled(t, h, queued); record.Status != "done" {
					t.Fatalf("continued input=%+v", record)
				}
			case AfterFailedRunWithdraw:
				if record := waitSettled(t, h, queued); record.Status != "unanswered" || record.Reason != "run_failed" {
					t.Fatalf("withdrawn input=%+v", record)
				}
			}
		})
	}
}

func TestRunControlSubmissionRules(t *testing.T) {
	gate := newGatedTool()
	h, root := runHarness(t, toolThenEcho(), Settings{}, gate.extension())
	ctx := context.Background()
	if _, err := root.Submit(ctx, SubmissionRequest{Content: "hi", WhenIdle: "reject"}); !errors.Is(err, ErrIdle) {
		t.Fatalf("idle reject error=%v", err)
	}
	if _, err := root.Submit(ctx, SubmissionRequest{Type: "continue", Content: "x"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("continue with content error=%v", err)
	}
	busy := submit(t, root, SubmissionRequest{Content: "tool"})
	<-gate.started
	if _, err := root.Submit(ctx, SubmissionRequest{Type: "continue"}); !errors.Is(err, ErrBusy) {
		t.Fatalf("busy continue error=%v", err)
	}
	if _, err := root.Submit(ctx, SubmissionRequest{Content: "x", WhenBusy: "reject"}); !errors.Is(err, ErrBusy) {
		t.Fatalf("busy reject error=%v", err)
	}
	queuedIdle := submit(t, root, SubmissionRequest{Content: "joins", WhenIdle: "reject"}) // busy: queued
	legacy := submit(t, root, SubmissionRequest{Content: "legacy", WhenBusy: "follow-up"})
	if record, _ := (&Submission{h: h, id: legacy}).Record(ctx); record.Request.WhenBusy != "followUp" || record.Status != "queued" {
		t.Fatalf("legacy follow-up=%+v", record)
	}
	gate.release <- struct{}{}
	for _, id := range []SubmissionID{busy, queuedIdle, legacy} {
		if record := waitSettled(t, h, id); record.Status != "done" {
			t.Fatalf("submission %d=%+v", id, record)
		}
	}
}

func TestRunControlContinueAfterFailure(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		if fail.Load() {
			return ModelResponse{}, errors.New("bad request")
		}
		return ModelResponse{Content: "recovered: " + lastUser(request)}, nil
	})
	h, root := runHarness(t, model, Settings{})
	failed := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "question"}))
	fail.Store(false)
	cont := waitSettled(t, h, submit(t, root, SubmissionRequest{Type: "continue"}))
	if cont.Status != "done" {
		t.Fatalf("continue=%+v", cont)
	}
	state, _ := h.Snapshot(context.Background())
	if answer := state.Entries[cont.Answer]; answer == nil || answer.Message.Content != "recovered: question" {
		t.Fatalf("answer=%+v", answer)
	}
	if live := readLiveState(t, h, root.ID()); live.RunFailure != nil {
		t.Fatalf("a run that started removed the failure: %+v", live.RunFailure)
	}
	_ = failed
}

func TestRunControlQueueModes(t *testing.T) {
	for _, mode := range []string{QueueOneAtATime, QueueAll} {
		t.Run(mode, func(t *testing.T) {
			gate := newGatedTool()
			var mu sync.Mutex
			var prompts [][]string
			model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
				var users []string
				for _, message := range request.Messages {
					if message.Role == "user" {
						users = append(users, message.Content)
					}
				}
				mu.Lock()
				prompts = append(prompts, users)
				mu.Unlock()
				if !hasToolResult(request) && lastUser(request) == "tool" {
					return ModelResponse{ToolCalls: []ToolCall{{ID: "c1", Name: "gate", Arguments: json.RawMessage(`{}`)}}}, nil
				}
				return ModelResponse{Content: "ok"}, nil
			})
			h, root := runHarness(t, model, Settings{SteeringMode: mode, FollowUpMode: mode}, gate.extension())
			first := submit(t, root, SubmissionRequest{Content: "tool"})
			<-gate.started
			s1 := submit(t, root, SubmissionRequest{Content: "s1", WhenBusy: "steer"})
			s2 := submit(t, root, SubmissionRequest{Content: "s2", WhenBusy: "steer"})
			f1 := submit(t, root, SubmissionRequest{Content: "f1"})
			f2 := submit(t, root, SubmissionRequest{Content: "f2"})
			gate.release <- struct{}{}
			for _, id := range []SubmissionID{first, s1, s2, f1, f2} {
				if record := waitSettled(t, h, id); record.Status != "done" {
					t.Fatalf("submission %d=%+v", id, record)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			// The post-tools boundary places one steer, or all of them.
			second := strings.Join(prompts[1], ",")
			if mode == QueueOneAtATime && second != "tool,s1" || mode == QueueAll && second != "tool,s1,s2" {
				t.Fatalf("post-tools prompt=%q", second)
			}
		})
	}
}

func TestRunControlCompactionStatus(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		if strings.HasPrefix(request.Messages[0].Content, "Summarize") {
			if calls.Add(1) == 1 {
				<-release
				return ModelResponse{}, errors.New("bad summary request")
			}
			return ModelResponse{Content: "summary"}, nil
		}
		return ModelResponse{Content: "answer"}, nil
	})
	h, root := runHarness(t, model, Settings{})
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	failing, err := root.Compact(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	live := readLiveState(t, h, root.ID())
	if len(live.Compactions) != 1 || live.Compactions[0].TaskID != failing || live.Compactions[0].Trigger != "manual" || live.Compactions[0].Attempt != 1 {
		t.Fatalf("compactions=%+v", live.Compactions)
	}
	close(release)
	if record, err := h.WaitTask(ctx, failing); err != nil || record.Outcome.Status != OutcomeFailed {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	live = readLiveState(t, h, root.ID())
	if live.Compactions != nil || live.CompactionFailure == nil || live.CompactionFailure.TaskID != failing || live.CompactionFailure.Error != "bad summary request" {
		t.Fatalf("after failure=%+v", live)
	}
	succeeding, err := root.Compact(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if record, err := h.WaitTask(ctx, succeeding); err != nil || record.Outcome.Status != OutcomeCompleted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
	if live = readLiveState(t, h, root.ID()); live.Compactions != nil || live.CompactionFailure != nil {
		t.Fatalf("a summary did not clear the failure: %+v", live)
	}
}

func TestRunControlStateVersionThreeUpgrade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store := NewMemoryStorage()
	h, err := Open(ctx, store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Rewrite the store as a version-2 build left it mid-run.
	store.mu.Lock()
	state, err := cloneState(store.state)
	store.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	state.Version = 2
	const sub, task, write = SubmissionID(90), TaskID(91), SubmissionID(92)
	state.NextID = 100
	state.Entries[93] = &Entry{ID: 93, Conversation: root.ID(), Kind: "pi.user", Message: &Message{Role: "user", Content: "legacy question"}, Sequence: state.Sequence}
	state.Conversations[root.ID()].Head = 93
	state.Submissions[sub] = &SubmissionRecord{ID: sub, Conversation: root.ID(), Request: SubmissionRequest{Type: "input", Content: "legacy question", WhenBusy: "follow-up"}, Status: "running"}
	state.Submissions[write] = &SubmissionRecord{ID: write, Conversation: root.ID(), Request: SubmissionRequest{Type: "write", Entry: &Entry{Kind: "app.note"}}, Status: "done", Answer: 93}
	input, _ := json.Marshal(generationInput{Submissions: []SubmissionID{sub}})
	state.Tasks[task] = &TaskRecord{ID: task, Conversation: root.ID(), Kind: "pi.generation", Version: 2, Status: "pending", Input: input, Checkpoint: json.RawMessage(`{"phase":"prepare","attempt":1}`)}
	state.Conversations[root.ID()].Busy = task
	state.Documents[documentKey(liveDocument, uint64(root.ID()))] = &DocumentRecord{Kind: "pi.live", Version: 1, Scope: "conversation", ScopeID: uint64(root.ID()), History: "latest", Fork: "initial",
		Value: json.RawMessage(`{"tools":{"7":{"callId":"c","name":"gate","output":"old"}}}`)}
	store.mu.Lock()
	store.closed = false
	store.state = state
	store.mu.Unlock()
	h, err = Open(ctx, store, HarnessOptions{Model: echoModel{}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	upgraded, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.Version != stateVersion || upgraded.Submissions[sub].Status != "placed" || upgraded.Submissions[sub].Request.WhenBusy != "followUp" ||
		upgraded.Submissions[write].Entry != 93 || upgraded.Submissions[write].Answer != 0 {
		t.Fatalf("upgraded submissions=%+v %+v", upgraded.Submissions[sub], upgraded.Submissions[write])
	}
	live := readLiveState(t, h, root.ID())
	if live.Run == nil || live.Run.TaskID != task || len(live.Run.Inputs) != 1 || live.Run.Inputs[0] != sub ||
		len(live.Tools) != 1 || live.Tools[0].TaskID != 7 || live.Tools[0].Output != "old" {
		t.Fatalf("upgraded live=%+v", live)
	}
	record := waitSettled(t, h, sub)
	if record.Status != "done" {
		t.Fatalf("the upgraded run did not finish: %+v", record)
	}
}
