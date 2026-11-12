package durable

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
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
	// violations are what the Harness emitted outside the schema
	// (DurableTelemetrySchema); conforms fails a test on them.
	violations []string
	mu         sync.Mutex
	spans      []*recordedSpan
	panic      bool
	active     map[string]float64
	phases     int
	metrics    bool
	reports    map[string]float64
}

func (r *recordingTelemetry) Add(name string, delta float64, attributes map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkMetric(name, "counter", attributes)
	if name == "pi.durable.reports" {
		if r.reports == nil {
			r.reports = map[string]float64{}
		}
		r.reports[attributes["type"].(string)] += delta
		return
	}
	if r.active == nil {
		r.active = map[string]float64{}
	}
	r.active[attributes["task"].(string)] += delta
}

func (r *recordingTelemetry) Record(name string, value float64, attributes map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkMetric(name, "histogram", attributes)
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
	if schema, ok := telemetrySchema.Spans[name]; !ok {
		r.violations = append(r.violations, fmt.Sprintf("undeclared span %q", name))
	} else {
		r.violations = append(r.violations, schemaViolations(name+" start", schema.StartAttributes, attributes, true)...)
	}
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
	s.owner.violations = append(s.owner.violations, schemaViolations(s.span.name+" end", telemetrySchema.Spans[s.span.name].EndAttributes, attributes, false)...)
	for k, v := range attributes {
		s.span.attributes[k] = v
	}
}
func (s *recordingSpan) AddEvent(name string, attributes map[string]any) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if event, ok := telemetrySchema.Spans[s.span.name].Events[name]; !ok {
		s.owner.violations = append(s.owner.violations, fmt.Sprintf("undeclared event %q on %s", name, s.span.name))
	} else {
		s.owner.violations = append(s.owner.violations, schemaViolations(s.span.name+" event "+name, event.Attributes, attributes, true)...)
	}
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

var telemetrySchema = DurableTelemetrySchema()

// checkMetric records what a metric emission does outside the schema; the
// caller holds r.mu.
func (r *recordingTelemetry) checkMetric(name, call string, attributes map[string]any) {
	metric, ok := telemetrySchema.Metrics[name]
	switch {
	case !ok:
		r.violations = append(r.violations, fmt.Sprintf("undeclared metric %q", name))
		return
	case call == "histogram" && metric.Kind != "histogram", call == "counter" && metric.Kind == "histogram":
		r.violations = append(r.violations, fmt.Sprintf("metric %q of kind %s recorded as a %s", name, metric.Kind, call))
	}
	r.violations = append(r.violations, schemaViolations("metric "+name, metric.Attributes, attributes, true)...)
}

// schemaViolations lists the attributes outside declared: undeclared names,
// wrong types, values outside the declared ones, and, when complete, missing
// required ones.
func schemaViolations(where string, declared map[string]TelemetryAttribute, attributes map[string]any, complete bool) []string {
	var out []string
	for name, value := range attributes {
		attribute, ok := declared[name]
		if !ok {
			out = append(out, fmt.Sprintf("%s: undeclared attribute %q", where, name))
			continue
		}
		if !schemaType(attribute.Type, value) {
			out = append(out, fmt.Sprintf("%s: %q is %T, not a %s", where, name, value, attribute.Type))
		}
		if len(attribute.Values) > 0 {
			if text, _ := value.(string); !slices.Contains(attribute.Values, text) {
				out = append(out, fmt.Sprintf("%s: %q is %v, not one of %v", where, name, value, attribute.Values))
			}
		}
	}
	if complete {
		for name, attribute := range declared {
			if _, ok := attributes[name]; attribute.Required && !ok {
				out = append(out, fmt.Sprintf("%s: required attribute %q is missing", where, name))
			}
		}
	}
	return out
}

func schemaType(kind string, value any) bool {
	switch kind {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "number":
		switch reflect.ValueOf(value).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			return true
		}
	}
	return false
}

// conforms fails the test on emissions outside the schema.
func (r *recordingTelemetry) conforms(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, violation := range r.violations {
		t.Errorf("telemetry outside the schema: %s", violation)
	}
}

func TestTelemetrySpansForSubmissionsAndPhases(t *testing.T) {
	telemetry := &recordingTelemetry{}
	defer telemetry.conforms(t)
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
	defer telemetry.conforms(t)
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

func TestTelemetryCountsReports(t *testing.T) {
	telemetry := &recordingTelemetry{}
	defer telemetry.conforms(t)
	definition := TaskDefinition{Kind: "app.counted", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		r.Report(errors.New("noted"))
		return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted}}, nil
		})
	}}
	registry := NewRegistry()
	installTasks(t, registry, "counted", definition)
	h := taskTestHarness(t, HarnessOptions{Telemetry: telemetry, Registry: registry})
	id := createTaskForTest(t, h, definition)
	h.Resume()
	waitForTask(t, h, id, func(task TaskRecord) bool { return task.Status == "terminal" })
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	if telemetry.reports["task_error"] != 1 {
		t.Fatalf("reports=%v", telemetry.reports)
	}
}

func TestTelemetryFailedPhaseStatus(t *testing.T) {
	ended := make(chan error, 4)
	telemetry := &endRecordingTelemetry{ended: ended}
	definition := TaskDefinition{Kind: "app.failing", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeFailed, Error: "no luck"}}, nil
		})
	}}
	registry := NewRegistry()
	installTasks(t, registry, "failing", definition)
	h := taskTestHarness(t, HarnessOptions{Telemetry: telemetry, Registry: registry})
	id := createTaskForTest(t, h, definition)
	h.Resume()
	waitForTask(t, h, id, func(task TaskRecord) bool { return task.Status == "terminal" })
	select {
	case err := <-ended:
		var status interface{ Category() string }
		if !errors.As(err, &status) || status.Category() != "task_failed" || err.Error() != "task_failed: no luck" {
			t.Fatalf("span error=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no phase span ended")
	}
}

type endRecordingTelemetry struct{ ended chan error }

func (e *endRecordingTelemetry) StartSpan(ctx context.Context, name string, _ map[string]any) (context.Context, TelemetrySpan) {
	if name != "pi.durable.phase" {
		return ctx, nopSpan{}
	}
	return ctx, endSpan{ended: e.ended}
}

type nopSpan struct{}

func (nopSpan) SetAttributes(map[string]any)    {}
func (nopSpan) AddEvent(string, map[string]any) {}
func (nopSpan) End(error)                       {}

type endSpan struct{ ended chan error }

func (endSpan) SetAttributes(map[string]any)    {}
func (endSpan) AddEvent(string, map[string]any) {}
func (s endSpan) End(err error)                 { s.ended <- err }

func TestTelemetryToolPhasesNameTheirCall(t *testing.T) {
	telemetry := &recordingTelemetry{}
	defer telemetry.conforms(t)
	gate := newGatedTool()
	registry := NewRegistry()
	if err := registry.Install(gate.extension()); err != nil {
		t.Fatal(err)
	}
	h := taskTestHarness(t, HarnessOptions{Model: toolThenEcho(), Registry: registry, Telemetry: telemetry})
	root := testRoot(t, h)
	sub := submit(t, root, SubmissionRequest{Content: "tool please"})
	<-gate.started
	close(gate.release)
	waitSettled(t, h, sub)
	deadline := time.Now().Add(5 * time.Second)
	for {
		telemetry.mu.Lock()
		for _, span := range telemetry.spans {
			if span.name == "pi.durable.phase" && span.attributes["task.name"] == "pi.tool" && span.attributes["tool.name"] != nil {
				name, id := span.attributes["tool.name"], span.attributes["tool.call_id"]
				telemetry.mu.Unlock()
				if name == "" || id == "" {
					t.Fatalf("tool phase attributes=%v", span.attributes)
				}
				return
			}
		}
		telemetry.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("no tool phase span named its call")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
