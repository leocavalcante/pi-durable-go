package durable

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// Expectations are pi-ai 1.5.2's ToolCallArgumentsStream readings of the
// same texts (TestCrosscheckStreamedArguments compares more).
func TestPartialArgumentsFollowTheStreamingRules(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{``, `{}`},
		{`{`, `{}`},
		{`{"pa`, `{}`},
		{`{"path"`, `{}`},
		{`{"path":`, `{}`},
		{`{"path": "src/ma`, `{"path":"src/ma"}`},
		{`{"path": "a\`, `{"path":"a"}`},
		{`{"path": "a\u00`, `{"path":"a"}`},
		{`{"path": "a\u00e9`, `{"path":"aé"}`},
		{`{"path": "\ud83d`, `{"path":"\ud83d"}`},
		{`{"path": "\ud83d\ude00"`, `{"path":"😀"}`},
		{`{"path": "a\d"}`, `{"path":"a\\d"}`},
		{`{"n": 12`, `{"n":12}`},
		{`{"n": -`, `{}`},
		{`{"n": 1.`, `{}`},
		{`{"n": 1e`, `{"n":1}`},
		{`{"ok": tr`, `{"ok":true}`},
		{`{"ok": nul`, `{"ok":null}`},
		{`{"ok": maybe}`, `{}`},
		{`{"list": [1, 2, "th`, `{"list":[1,2,"th"]}`},
		{`{"a": {"b": "x`, `{"a":{"b":"x"}}`},
		{`{"a": 1 "b": 2}`, `{"a":1,"b":2}`},
		{`{"a" 1}`, `{"a":1}`},
		{`{"a": 1} trailing`, `{"a":1}`},
		{`{"html": "<b>&</b>"}`, `{"html":"<b>&</b>"}`},
		{`[1, 2`, `{}`},
		{`{"n": 01`, `{}`},
		{`}{"a": 1`, `{"a":1}`},
		{`{"n": -0`, `{"n":0}`},
		{`{"n": 1e5}`, `{"n":100000}`},
		{`{"2": "b", "1": "a"`, `{"1":"a","2":"b"}`},
	} {
		if got := partialArguments(tc.in); string(got) != tc.want {
			t.Errorf("partialArguments(%q)=%s, want %s", tc.in, got, tc.want)
		}
	}
}

// eventModel streams events and waits for release before answering.
type eventModel struct {
	events  []StreamEvent
	release chan struct{}
}

func (m *eventModel) Generate(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
	return ModelResponse{}, nil
}

func (m *eventModel) GenerateEvents(ctx context.Context, request ModelRequest, onEvent func(StreamEvent) error) (ModelResponse, error) {
	for _, event := range m.events {
		if err := onEvent(event); err != nil {
			return ModelResponse{}, err
		}
	}
	select {
	case <-m.release:
	case <-ctx.Done():
		return ModelResponse{}, ctx.Err()
	}
	return ModelResponse{Content: "done"}, nil
}

func TestPartialsShowThinkingAndToolCallArguments(t *testing.T) {
	model := &eventModel{release: make(chan struct{}), events: []StreamEvent{
		{Type: "thinking_delta", Index: 0, Delta: "Let me "},
		{Type: "thinking_delta", Index: 0, Delta: "look."},
		{Type: "text_delta", Index: 1, Delta: "Reading"},
		{Type: "toolcall_start", Index: 2, ID: "call-1", Name: "read"},
		{Type: "toolcall_delta", Index: 2, Delta: `{"path": "src/`},
		{Type: "toolcall_delta", Index: 2, Delta: `main.go", "limit": 2`},
	}}
	defer close(model.release)
	h, root := runHarness(t, model, Settings{Progress: ProgressSettings{PartialIntervalMs: ptr(0.0)}})
	submit(t, root, SubmissionRequest{Content: "hi"})
	deadline := time.Now().Add(5 * time.Second)
	for {
		live := readLiveState(t, h, root.ID())
		if message := live.Generation; message != nil && message.Message != nil && len(message.Message.ToolCalls) == 1 &&
			string(message.Message.ToolCalls[0].Arguments) == `{"path":"src/main.go","limit":2}` {
			m := message.Message
			if m.Thinking != "Let me look." || m.Content != "Reading" || m.ToolCalls[0].ID != "call-1" || m.ToolCalls[0].Name != "read" {
				t.Fatalf("partial=%+v", m)
			}
			break
		}
		if time.Now().After(deadline) {
			data, _ := json.Marshal(live.Generation)
			t.Fatalf("partial never showed the call: %s", data)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAPartialWithToolCallsIsKeptWhenItsAttemptIsCutOff(t *testing.T) {
	model := &eventModel{release: make(chan struct{}), events: []StreamEvent{
		{Type: "thinking_delta", Delta: "hmm"},
		{Type: "toolcall_start", Index: 1, ID: "call-1", Name: "read"},
		{Type: "toolcall_delta", Index: 1, Delta: `{"path": "a`},
	}}
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, Settings: Settings{Progress: ProgressSettings{PartialIntervalMs: ptr(0.0)}}})
	h := hosts.open(nil)
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	submit(t, root, SubmissionRequest{Content: "hi"})
	deadline := time.Now().Add(5 * time.Second)
	for {
		live := readLiveState(t, h, root.ID())
		// The last event's partial: its arguments are in.
		if live.Generation != nil && live.Generation.Message != nil && len(live.Generation.Message.ToolCalls) == 1 &&
			string(live.Generation.Message.ToolCalls[0].Arguments) == `{"path":"a"}` {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no partial")
		}
		time.Sleep(time.Millisecond)
	}
	hosts.crash(h)
	close(model.release)
	model.events = nil
	h = hosts.open(nil)
	h.Resume()
	root, err = h.Root(context.Background(), ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		entries, _ := root.Entries(context.Background())
		for _, entry := range entries {
			if m := entry.Message; m != nil && m.StopReason == "aborted" {
				if m.Thinking != "hmm" || len(m.ToolCalls) != 1 || string(m.ToolCalls[0].Arguments) != `{"path":"a"}` {
					t.Fatalf("aborted entry=%+v", m)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no aborted entry: %+v", entries)
		}
		time.Sleep(time.Millisecond)
	}
}
