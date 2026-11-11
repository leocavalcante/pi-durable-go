package durable

import (
	"context"
	"sync"
	"testing"
)

type recordedSpan struct {
	name       string
	attributes map[string]any
	ended      bool
}

type recordingTelemetry struct {
	mu    sync.Mutex
	spans []*recordedSpan
	panic bool
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
func (s *recordingSpan) AddEvent(string, map[string]any) {}
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
	telemetry.mu.Lock()
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
