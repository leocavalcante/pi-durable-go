package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type recordedSpan struct {
	name       string
	attributes map[string]any
	events     []string
	ended      bool
}

type recordingTelemetry struct {
	mu      sync.Mutex
	spans   []*recordedSpan
	panic   bool
	active  map[string]float64
	phases  int
	metrics bool
}

func (r *recordingTelemetry) Add(name string, delta float64, attributes map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active == nil {
		r.active = map[string]float64{}
	}
	r.active[attributes["task"].(string)] += delta
}

func (r *recordingTelemetry) Record(name string, value float64, attributes map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if name == "pi.durable.phase.duration" && value >= 0 && attributes["outcome"] != nil {
		r.phases++
	}
}

func (r *recordingTelemetry) StartSpan(ctx context.Context, name string, attributes map[string]any) (context.Context, TelemetrySpan) {
	if r.panic {
		panic("adapter bug")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	span := &recordedSpan{name: name, attributes: map[string]any{}}
	for k, v := range attributes {
		span.attributes[k] = v
	}
	r.spans = append(r.spans, span)
	return ctx, &recordingSpan{owner: r, span: span}
}

type recordingSpan struct {
	owner *recordingTelemetry
	span  *recordedSpan
}

func (s *recordingSpan) SetAttributes(attributes map[string]any) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	for k, v := range attributes {
		s.span.attributes[k] = v
	}
}
func (s *recordingSpan) AddEvent(name string, attributes map[string]any) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	switch name {
	case "pi.durable.entry":
		s.span.events = append(s.span.events, name+":"+attributes["entry.kind"].(string))
	case "pi.durable.report":
		s.span.events = append(s.span.events, name+":"+attributes["type"].(string))
	case "pi.durable.submission":
		s.span.events = append(s.span.events, name+":"+attributes["status"].(string))
	case "pi.durable.checkpoint":
		s.span.events = append(s.span.events, name+":"+attributes["phase"].(string))
	}
}
func (s *recordingSpan) End(error) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	s.span.ended = true
}

func TestTelemetrySpansForSubmissionsAndPhases(t *testing.T) {
	telemetry := &recordingTelemetry{}
	h := taskTestHarness(t, HarnessOptions{Model: echoModel{}, Telemetry: telemetry})
	root := testRoot(t, h)
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	if err := root.WaitForIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A phase span ends after its decision commit; wait for every span.
	deadline := time.Now().Add(5 * time.Second)
	for {
		telemetry.mu.Lock()
		ended := true
		for _, span := range telemetry.spans {
			ended = ended && span.ended
		}
		if ended || time.Now().After(deadline) {
			break
		}
		telemetry.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	defer telemetry.mu.Unlock()
	var submitSpan, prepare, request *recordedSpan
	for _, span := range telemetry.spans {
		switch {
		case span.name == "pi.durable.submit":
			submitSpan = span
		case span.name == "pi.durable.phase" && span.attributes["phase"] == "prepare":
			prepare = span
		case span.name == "pi.durable.phase" && span.attributes["phase"] == "request":
			request = span
		}
	}
	if submitSpan == nil || !submitSpan.ended || submitSpan.attributes["result"] != "placed" || submitSpan.attributes["submission.type"] != "input" {
		t.Fatalf("submit span=%+v", submitSpan)
	}
	if prepare == nil || prepare.attributes["task.name"] != "pi.generation" || prepare.attributes["kind"] != "run" || prepare.attributes["outcome"] != "next=request" {
		t.Fatalf("prepare span=%+v", prepare)
	}
	if request != nil && request.attributes["outcome"] != "done" {
		t.Fatalf("request span=%+v", request)
	}
	if len(submitSpan.events) == 0 || submitSpan.events[0] != "pi.durable.entry:pi.user" {
		t.Fatalf("submit events=%v", submitSpan.events)
	}
	var settledEvent bool
	for _, span := range telemetry.spans {
		for _, event := range span.events {
			settledEvent = settledEvent || event == "pi.durable.submission:done"
		}
	}
	if !settledEvent {
		t.Fatal("no pi.durable.submission event")
	}
	if telemetry.phases == 0 {
		t.Fatal("no phase durations")
	}
	for kind, n := range telemetry.active {
		if n != 0 {
			t.Fatalf("%s still active: %v", kind, n)
		}
	}
	if len(telemetry.active) == 0 {
		t.Fatal("tasks.active never counted")
	}
}

func TestTelemetryAdapterPanicsAreHostErrors(t *testing.T) {
	log := &reportLog{}
	h := taskTestHarness(t, HarnessOptions{Model: echoModel{}, Telemetry: &recordingTelemetry{panic: true}, OnReport: log.add})
	root := testRoot(t, h)
	if record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"})); record.Status != "done" {
		t.Fatalf("record=%+v", record)
	}
	drained(t, h)
	reports := log.of(ReportHostError)
	if len(reports) == 0 || reports[0].Callback != "telemetry" {
		t.Fatalf("reports=%+v", reports)
	}
}

func TestTelemetryReportEventsOnThePhaseSpan(t *testing.T) {
	telemetry := &recordingTelemetry{}
	definition := TaskDefinition{Kind: "app.reporting", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		r.Report(errors.New("noted"))
		return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted}}, nil
		})
	}}
	registry := NewRegistry()
	installTasks(t, registry, "reporting", definition)
	h := taskTestHarness(t, HarnessOptions{Telemetry: telemetry, Registry: registry})
	id := createTaskForTest(t, h, definition)
	h.Resume()
	waitForTask(t, h, id, func(task TaskRecord) bool { return task.Status == "terminal" })
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	for _, span := range telemetry.spans {
		for _, event := range span.events {
			if event == "pi.durable.report:task_error" && span.attributes["task.name"] == "app.reporting" {
				return
			}
		}
	}
	t.Fatal("no pi.durable.report event on the phase span")
}
