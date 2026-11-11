package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

type traceKey struct{}

// tracingTelemetry names each span and records its parent: the span ctx
// carries, or one a carrier names.
type tracingTelemetry struct {
	mu      sync.Mutex
	next    int
	parents map[string]string // span name#id -> parent span
	names   map[string]string // span id -> span name
	links   map[string][]string
}

type tracedSpan struct {
	t  *tracingTelemetry
	id string
}

func (t *tracingTelemetry) StartSpan(ctx context.Context, name string, attributes map[string]any) (context.Context, TelemetrySpan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next++
	id := fmt.Sprintf("%s#%d", name, t.next)
	parent, _ := ctx.Value(traceKey{}).(string)
	if t.parents == nil {
		t.parents, t.names, t.links = map[string]string{}, map[string]string{}, map[string][]string{}
	}
	t.parents[id] = parent
	return context.WithValue(ctx, traceKey{}, id), &tracedSpan{t: t, id: id}
}

func (s *tracedSpan) SetAttributes(map[string]any)    {}
func (s *tracedSpan) AddEvent(string, map[string]any) {}
func (s *tracedSpan) End(error)                       {}
func (s *tracedSpan) AddLink(carrier json.RawMessage) {
	s.t.mu.Lock()
	defer s.t.mu.Unlock()
	var id string
	_ = json.Unmarshal(carrier, &id)
	s.t.links[s.id] = append(s.t.links[s.id], id)
}

func (t *tracingTelemetry) Inject(ctx context.Context) json.RawMessage {
	id, _ := ctx.Value(traceKey{}).(string)
	if id == "" {
		return nil
	}
	data, _ := json.Marshal(id)
	return data
}

func (t *tracingTelemetry) Extract(ctx context.Context, carrier json.RawMessage) context.Context {
	var id string
	_ = json.Unmarshal(carrier, &id)
	return context.WithValue(ctx, traceKey{}, id)
}

func TestTracesContinueFromSubmissionsIntoTheirRuns(t *testing.T) {
	telemetry := &tracingTelemetry{}
	h := taskTestHarness(t, HarnessOptions{Model: echoModel{}, Telemetry: telemetry})
	root := testRoot(t, h)
	id := submit(t, root, SubmissionRequest{Content: "hi"})
	waitSettled(t, h, id)
	if err := root.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var submitSpan string
	_ = json.Unmarshal(state.Submissions[id].Trace, &submitSpan)
	if submitSpan == "" {
		t.Fatal("the submission stored no carrier")
	}
	var generation *TaskRecord
	for _, task := range state.Tasks {
		if task.Kind == "pi.generation" {
			generation = task
		}
	}
	if generation == nil || string(generation.Trace) != string(state.Submissions[id].Trace) {
		t.Fatalf("generation trace=%s, want %s", generation.Trace, state.Submissions[id].Trace)
	}
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	phases := 0
	for span, parent := range telemetry.parents {
		if len(span) > len("pi.durable.phase") && span[:len("pi.durable.phase")] == "pi.durable.phase" {
			phases++
			if parent != submitSpan {
				t.Fatalf("phase %s has parent %q, want %q", span, parent, submitSpan)
			}
		}
	}
	if phases == 0 {
		t.Fatal("no phase spans")
	}
}

func TestTaskTraceOptions(t *testing.T) {
	telemetry := &tracingTelemetry{}
	h := taskTestHarness(t, HarnessOptions{Telemetry: telemetry})
	root := testRoot(t, h)
	def := TaskDefinition{Kind: "app.traced", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
	ctx := context.WithValue(context.Background(), traceKey{}, "caller#0")
	var inherited, explicit, rooted TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		if inherited, err = tx.CreateTask(root.ID(), def, nil, TaskOptions{}); err != nil {
			return err
		}
		if explicit, err = tx.CreateTask(root.ID(), def, nil, TaskOptions{Trace: json.RawMessage(`"given#9"`), Links: []json.RawMessage{json.RawMessage(`"link#1"`)}}); err != nil {
			return err
		}
		rooted, err = tx.CreateTask(root.ID(), def, nil, TaskOptions{TraceRoot: true})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[TaskID]string{inherited: `"caller#0"`, explicit: `"given#9"`, rooted: ``} {
		task, err := h.Task(context.Background(), id)
		if err != nil || string(task.Trace) != want {
			t.Fatalf("task %d trace=%s, want %s (error %v)", id, task.Trace, want, err)
		}
	}
	if task, _ := h.Task(context.Background(), explicit); len(task.Links) != 1 || string(task.Links[0]) != `"link#1"` {
		t.Fatalf("links=%s", task.Links)
	}
}
