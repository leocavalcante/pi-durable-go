package durable

import (
	"context"
	"encoding/json"
	"time"
)

// ResolvedAgent is a conversation's agent resolved against the phase's
// registry snapshot and the settings (upstream Agent): the tools a request
// offers, those nested calls resolve among, the deferred tools not offered
// yet, and the prompt sections.
type ResolvedAgent struct {
	Agent
	// Tools are offered to the model, in order: enabled, callable by the
	// model, and selected by ModelTools.
	Tools []ToolDefinition
	// Callable are the tools nested calls resolve among: enabled and
	// callable by tools.
	Callable []ToolDefinition
	// Deferred are enabled deferred tools the model may call and is not
	// offered yet, in registry order.
	Deferred []ToolDefinition
	// Sections are the selected extensions' sections, in order.
	Sections []Section
}

// Agent resolves the task's conversation agent (upstream runtime.agent).
func (r *TaskRuntime) Agent(ctx context.Context) (ResolvedAgent, error) {
	if err := r.check(); err != nil {
		return ResolvedAgent{}, err
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return ResolvedAgent{}, err
	}
	conv := state.Conversations[r.conversation]
	if conv == nil {
		return ResolvedAgent{}, ErrNotFound
	}
	agent := cloneRecord(conv).Agent
	registry := r.Registry()
	enabled := registry.Tools(agent)
	resolved := ResolvedAgent{Agent: agent, Tools: offeredTools(enabled, agent.ModelTools), Sections: registry.Sections(agent)}
	offered := map[string]bool{}
	for _, tool := range resolved.Tools {
		offered[tool.Name] = true
	}
	for _, tool := range enabled {
		if callableBy(tool, "tools") {
			resolved.Callable = append(resolved.Callable, tool)
		}
		if tool.Deferred && callableBy(tool, "model") && !offered[tool.Name] {
			resolved.Deferred = append(resolved.Deferred, tool)
		}
	}
	return resolved, nil
}

// Task reads a task record (upstream runtime.getTask); detached.
func (r *TaskRuntime) Task(ctx context.Context, id TaskID) (TaskRecord, error) {
	if err := r.check(); err != nil {
		return TaskRecord{}, err
	}
	return r.h.Task(ctx, id)
}

// WaitTask waits in memory until a task is terminal (upstream
// runtime.waitForTask). It is not a durable wait: a restart loses it, so a
// task that must wait across restarts commits a waiting transition instead.
func (r *TaskRuntime) WaitTask(ctx context.Context, id TaskID) (TaskRecord, error) {
	if err := r.check(); err != nil {
		return TaskRecord{}, err
	}
	if r.invocation != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-r.invocation.ctx.Done():
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	return r.h.WaitTask(ctx, id)
}

// Conversation returns a handle to a conversation (upstream
// runtime.conversation).
func (r *TaskRuntime) Conversation(ctx context.Context, id ConversationID) (*Conversation, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	return r.h.Conversation(ctx, id)
}

// Entry reads an entry of the task's conversation history (upstream
// runtime.entry); ErrNotFound when it is not in that history.
func (r *TaskRuntime) Entry(ctx context.Context, id EntryID) (Entry, error) {
	if err := r.check(); err != nil {
		return Entry{}, err
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return Entry{}, err
	}
	entries, err := entriesFromState(state, r.conversation)
	if err != nil {
		return Entry{}, err
	}
	for _, e := range entries {
		if e.ID == id {
			return e, nil
		}
	}
	return Entry{}, ErrNotFound
}

// Diagnostic adds a diagnostic to the result of this call (upstream
// api.diagnostic): diagnostics reported while the call runs come before the
// result's own. They are stored in the call's pi.live slot with its throttled
// output, so a result built from the slot after an interruption keeps them.
func (r *ToolRuntime) Diagnostic(diagnostic ToolDiagnostic) {
	r.diagnosticsMu.Lock()
	r.diagnostics = append(r.diagnostics, diagnostic)
	r.diagnosticsMu.Unlock()
	if r.check() != nil {
		return
	}
	interval := 100 * time.Millisecond
	if ms := r.h.settings().Progress.OutputIntervalMs; ms != nil && *ms >= 0 {
		interval = time.Duration(*ms * float64(time.Millisecond))
	}
	if interval == 0 {
		if err := r.commitOutput(context.Background(), "", diagnostic); err != nil && r.check() == nil {
			r.Report(err)
		}
		return
	}
	r.output.mu.Lock()
	defer r.output.mu.Unlock()
	r.output.diagnostics = append(r.output.diagnostics, diagnostic)
	if !r.output.armed && !r.output.stopped {
		r.output.armed = true
		r.output.disarm = r.SetTimer(r.flushOutput, interval)
	}
}

// takeDiagnostics returns and clears the diagnostics added so far.
func (r *ToolRuntime) takeDiagnostics() []ToolDiagnostic {
	r.diagnosticsMu.Lock()
	defer r.diagnosticsMu.Unlock()
	out := r.diagnostics
	r.diagnostics = nil
	return out
}

// RetainedOutput is the running output the slot keeps, as the model would
// see it if the call ended now (upstream api.retainedOutput): its trailing
// 50 KiB, including output not committed yet, and whether earlier output was
// dropped.
func (r *ToolRuntime) RetainedOutput(ctx context.Context) (string, bool) {
	var text string
	var dropped bool
	if state, err := r.h.read(ctx); err == nil {
		if doc := state.Documents[documentKey(liveDocument, uint64(r.ConversationID))]; doc != nil {
			var live LiveState
			if value, err := migratedDocument(liveDocument, doc); err == nil && json.Unmarshal(value, &live) == nil {
				if slot := live.toolSlot(r.ID); slot != nil {
					text, dropped = slot.Output, slot.DroppedBytes > 0
				}
			}
		}
	}
	r.output.mu.Lock()
	pending := r.output.pending.String()
	r.output.mu.Unlock()
	combined := text + pending
	kept := tailBytes(combined, maxToolOutputBytes)
	return kept, dropped || len(kept) < len(combined)
}
