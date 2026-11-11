package durable

import (
	"context"
	"encoding/json"
	"runtime/debug"
)

// Telemetry is an optional host tracing adapter (upstream's content-free
// telemetry vocabulary): spans named "pi.durable.submit" for each public
// submission and "pi.durable.phase" for each task phase, with IDs, kinds,
// phases, and results as attributes, never content. A panicking adapter is
// reported as host_error with callback "telemetry" and ignored.
type Telemetry interface {
	StartSpan(ctx context.Context, name string, attributes map[string]any) (context.Context, TelemetrySpan)
}

// TelemetrySpan is one span of a Telemetry adapter.
type TelemetrySpan interface {
	SetAttributes(attributes map[string]any)
	AddEvent(name string, attributes map[string]any)
	End(err error)
}

// span starts a span through the adapter, containing its failures.
func (h *Harness) span(ctx context.Context, name string, attributes map[string]any) (context.Context, *span) {
	telemetry := h.options.Telemetry
	if telemetry == nil {
		return ctx, nil
	}
	var started TelemetrySpan
	next := ctx
	if !h.telemetryCall(func() { next, started = telemetry.StartSpan(ctx, name, attributes) }) || started == nil {
		return ctx, nil
	}
	// Keep the caller's cancellation: an adapter context that is not
	// derived from it is not used.
	if next == nil || next.Done() != ctx.Done() {
		next = ctx
	}
	sp := &span{h: h, span: started}
	return context.WithValue(next, spanKey{}, sp), sp
}

type spanKey struct{}

// spanOf returns the span a context carries, or nil.
func spanOf(ctx context.Context) *span {
	if ctx == nil {
		return nil
	}
	sp, _ := ctx.Value(spanKey{}).(*span)
	return sp
}

// TelemetryMetrics is optionally implemented by a Telemetry adapter to
// receive upstream's metrics: the up-down counter "pi.durable.tasks.active"
// (attribute task, the kind), the counter "pi.durable.reports" (attribute
// type, the report kind), and the histogram "pi.durable.phase.duration" in
// milliseconds (attributes task, phase, outcome).
type TelemetryMetrics interface {
	Add(name string, delta float64, attributes map[string]any)
	Record(name string, value float64, attributes map[string]any)
}

func (h *Harness) metrics() TelemetryMetrics {
	metrics, _ := h.options.Telemetry.(TelemetryMetrics)
	return metrics
}

// countActive keeps pi.durable.tasks.active equal to the live tasks per
// kind of the latest snapshot this Harness committed or loaded.
func (h *Harness) countActive(state *State) {
	metrics := h.metrics()
	if metrics == nil {
		return
	}
	counts := map[string]int{}
	if state != nil {
		for _, task := range state.Tasks {
			if task.Status != "terminal" {
				counts[task.Kind]++
			}
		}
	}
	h.activeMu.Lock()
	defer h.activeMu.Unlock()
	if h.active == nil {
		h.active = map[string]int{}
	}
	for kind, n := range counts {
		if delta := n - h.active[kind]; delta != 0 {
			h.telemetryCall(func() { metrics.Add("pi.durable.tasks.active", float64(delta), map[string]any{"task": kind}) })
		}
	}
	for kind, n := range h.active {
		if _, ok := counts[kind]; !ok && n != 0 {
			h.telemetryCall(func() { metrics.Add("pi.durable.tasks.active", float64(-n), map[string]any{"task": kind}) })
		}
	}
	h.active = counts
}

func (h *Harness) telemetryCall(call func()) (ok bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			ok = false
			h.report(Report{Kind: ReportHostError, Err: &panicError{value: recovered, stack: debug.Stack()}, Callback: "telemetry"})
		}
	}()
	call()
	return true
}

// span wraps an adapter span; a nil *span records nothing.
type span struct {
	h    *Harness
	span TelemetrySpan
}

func (s *span) set(attributes map[string]any) {
	if s != nil {
		s.h.telemetryCall(func() { s.span.SetAttributes(attributes) })
	}
}

func (s *span) event(name string, attributes map[string]any) {
	if s != nil {
		s.h.telemetryCall(func() { s.span.AddEvent(name, attributes) })
	}
}

func (s *span) end(err error) {
	if s != nil {
		s.h.telemetryCall(func() { s.span.End(err) })
	}
}

// checkpointPhase reads a checkpoint's phase for span attributes.
func checkpointPhase(checkpoint json.RawMessage) (string, int) {
	var cp struct {
		Phase   string `json:"phase"`
		Attempt int    `json:"attempt"`
	}
	_ = json.Unmarshal(checkpoint, &cp)
	return cp.Phase, cp.Attempt
}
