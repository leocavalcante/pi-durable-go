// Package codemode ports Pi Durable's code mode: a tool that runs
// model-written JavaScript in a sandbox whose only capability is calling the
// conversation's tools, and whose script survives a restart by deterministic
// replay (upstream src/codemode).
package codemode

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
)

// Sandbox runs one script deterministically, with the caller answering its
// calls (upstream CodemodeSandboxBase.start): the same script, start time,
// seed, tools, globals, and store, answered with the same results in the same
// seq order, makes the same calls with the same arguments, on any VM with the
// same fingerprint.
type Sandbox interface {
	// Start fails at once for options the sandbox rejects or a closed
	// sandbox; script failures come back in the run's Result.
	Start(code string, options RunOptions) (Run, error)
}

// RunOptions are upstream's CodemodeRunOptions. The callbacks are called
// one at a time, in the order the VM reports.
type RunOptions struct {
	// StartedAt is the VM clock's start, in milliseconds since the epoch.
	StartedAt float64
	Seed      uint32
	Tools     []DeclaredTool
	Globals   []DeclaredGlobal
	// Store holds the values load() reads, as JSON.
	Store map[string]json.RawMessage
	// TimeoutMs bounds the run; nil runs without a deadline.
	TimeoutMs *float64
	// OnStarted gets the VM's fingerprint before the script runs.
	OnStarted func(fingerprint string)
	// OnCall gets each call the script makes; Run.Deliver answers it.
	OnCall func(HostCall)
	// OnIdle reports, after the script's first run (seq 0) and after each
	// delivered result, how many calls the script has issued.
	OnIdle func(seq, issued int)
}

// Run is one started script (upstream CodemodeRun).
type Run interface {
	// Deliver hands the VM the outcome of call id as result number seq,
	// with the clock value now; results are applied in seq order.
	Deliver(seq int, now float64, id int, outcome Outcome) error
	// Abort ends the run with an "aborted" error the script cannot catch.
	Abort(message string)
	// Finished reports whether the run has its result.
	Finished() bool
	// Wait returns the run's result once it has one; it never fails for
	// script failures, only when ctx ends first.
	Wait(ctx context.Context) (Result, error)
}

// DeclaredTool is a tool as scripts see it: tools.<JSName>.
type DeclaredTool struct {
	Name        string `json:"name"`
	JSName      string `json:"jsName,omitempty"`
	Description string `json:"description,omitempty"`
}

// DeclaredGlobal is a global function scripts call; Spread passes its
// arguments as a list.
type DeclaredGlobal struct {
	Name   string `json:"name"`
	Spread bool   `json:"spread,omitempty"`
}

// HostCall is a call the script made: a tool's (Target "tool") or a
// global's. Args is nil when the script passed undefined.
type HostCall struct {
	ID     int             `json:"id"`
	Target string          `json:"target"`
	Name   string          `json:"name"`
	Args   json.RawMessage `json:"args,omitempty"`
}

// Outcome answers a call (upstream CodemodeOutcome): a value (nil is
// undefined) or an error message with optional properties for the script's
// Error object.
type Outcome struct {
	OK         bool
	Value      json.RawMessage
	Message    string
	Properties map[string]json.RawMessage
}

// MarshalJSON writes upstream's shape: {ok:true, value?} or {ok:false,
// message, properties?}.
func (o Outcome) MarshalJSON() ([]byte, error) {
	if o.OK {
		return json.Marshal(struct {
			OK    bool            `json:"ok"`
			Value json.RawMessage `json:"value,omitempty"`
		}{true, o.Value})
	}
	return json.Marshal(struct {
		OK         bool                       `json:"ok"`
		Message    string                     `json:"message"`
		Properties map[string]json.RawMessage `json:"properties,omitempty"`
	}{false, o.Message, o.Properties})
}

// UnmarshalJSON reads upstream's shape.
func (o *Outcome) UnmarshalJSON(data []byte) error {
	var raw struct {
		OK         bool                       `json:"ok"`
		Value      json.RawMessage            `json:"value"`
		Message    string                     `json:"message"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*o = Outcome{OK: raw.OK, Value: raw.Value, Message: raw.Message, Properties: raw.Properties}
	return nil
}

// Result is how a run ended (upstream CodemodeResult).
type Result struct {
	OK bool `json:"ok"`
	// Value is the script's return value; nil is undefined.
	Value  json.RawMessage `json:"value,omitempty"`
	Error  *Error          `json:"error,omitempty"`
	Output []OutputItem    `json:"output"`
	Calls  []CallRecord    `json:"calls"`
	// StoreWrites are the store() calls of a script that succeeded.
	StoreWrites StoreWrites `json:"storeWrites"`
	// During says where the script ended: the result it was processing
	// (0 for its first run) and the calls it had issued.
	During *During `json:"during,omitempty"`
}

// Error is a failed run's error. Kind is script, timeout, aborted, or
// sandbox.
type Error struct {
	Kind    string `json:"kind"`
	Name    string `json:"name,omitempty"`
	Message string `json:"message"`
	Stack   string `json:"stack,omitempty"`
}

// OutputItem is a text (Console for console lines) or image item the script
// produced.
type OutputItem struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	Console  bool   `json:"console,omitempty"`
	Data     string `json:"data,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
}

// CallRecord is a tool call of the run as the sandbox saw it.
type CallRecord struct {
	Name       string  `json:"name"`
	Status     string  `json:"status"`
	DurationMs float64 `json:"durationMs"`
}

// StoreWrites are the values a script set and the keys it deleted.
type StoreWrites struct {
	Set    map[string]json.RawMessage `json:"set"`
	Delete []string                   `json:"delete"`
}

// During is where a deterministic run ended.
type During struct {
	Seq    int `json:"seq"`
	Issued int `json:"issued"`
}

// checkDelivery is upstream's synchronous deliver() check.
func checkDelivery(seq int, now float64, delivered map[int]bool, pending map[int]bool, id int) error {
	if math.IsNaN(now) || math.IsInf(now, 0) {
		return fmt.Errorf("The clock value of result %d is not a finite number", seq)
	}
	if seq < 1 || delivered[seq] {
		return fmt.Errorf("Result number %d is not a positive integer used once", seq)
	}
	if !pending[id] {
		return fmt.Errorf("Call %d is not waiting for a result", id)
	}
	return nil
}
