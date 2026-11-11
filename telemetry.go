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
	if next == nil {
		next = ctx
	}
	return next, &span{h: h, span: started}
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
