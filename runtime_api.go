package durable

import (
	"context"
	"fmt"
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
	return resolveAgent(r.Registry(), cloneRecord(conv).Agent), nil
}

// resolveAgent resolves an agent's tools and sections in a registry.
func resolveAgent(registry *RegistrySnapshot, agent Agent) ResolvedAgent {
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
	return resolved
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
// result's own. They are published to the call's pi.live slot with its
// paced progress, so a result built from the slot after an interruption
// keeps them.
func (r *ToolRuntime) Diagnostic(diagnostic ToolDiagnostic) {
	if r.check() != nil {
		return
	}
	r.reported.mu.Lock()
	r.reported.diagnostics = append(r.reported.diagnostics, diagnostic)
	r.reported.mu.Unlock()
	r.progress().mark()
}

// takeDiagnostics returns the diagnostics the call reported.
func (r *ToolRuntime) takeDiagnostics() []ToolDiagnostic {
	r.reported.mu.Lock()
	defer r.reported.mu.Unlock()
	return append([]ToolDiagnostic(nil), r.reported.diagnostics...)
}

// RetainedOutput is the running output the call keeps, as the model would
// see it if the call ended now (upstream api.retainedOutput): the window of
// its tool's output limits, sanitized, and whether the limits dropped
// output.
func (r *ToolRuntime) RetainedOutput(ctx context.Context) (string, bool) {
	snapshot := r.reported.snapshot()
	return snapshot.text, snapshot.droppedBytes > 0
}

// Model is the Harness model for requests a task, hook, or tool makes itself,
// with the same catalog, credentials, and identity as generation (upstream
// models); read at each call, so ModelFunc changes apply. Nil without a model.
func (r *TaskRuntime) Model() Model {
	if r.h.model() == nil {
		return nil
	}
	return harnessModel{h: r.h}
}

// EachTaskHook calls invoke with each handler of type H that the extensions
// of the task's conversation register for the task's kind
// (Extension.TaskHooks), in extension order (upstream runtime.hooks.each).
// An error or panic from invoke is reported as extension_error and the next
// handler runs; once the invocation is cancelled, its error is returned.
// Handlers of another type are skipped.
func EachTaskHook[H any](ctx context.Context, r *TaskRuntime, invoke func(H) error) error {
	if err := r.check(); err != nil {
		return err
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return err
	}
	task := state.Tasks[r.ID]
	if task == nil {
		return ErrNotFound
	}
	conv := state.Conversations[task.Conversation]
	if conv == nil {
		return ErrNotFound
	}
	for _, hook := range r.Registry().taskHooks(conv.Agent, task.Kind) {
		handlers, ok := hook.handlers.(H)
		if !ok {
			continue
		}
		if err := containPanic(func() error { return invoke(handlers) }); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.reportExtension(err, hook.extension)
		}
	}
	return nil
}

// retainedResult is the output the call kept, for a result that omits its
// content, and the note that the limits dropped some, worded with the
// tool's retention (upstream truncated(retained, limits.retain)).
func (r *ToolRuntime) retainedResult(def *ToolDefinition) (string, *ToolDiagnostic) {
	snapshot := r.reported.snapshot()
	if snapshot.droppedBytes == 0 {
		return snapshot.text, nil
	}
	kept := "beginning"
	if outputLimitsOf(def).Retain == "tail" {
		kept = "end"
	}
	return snapshot.text, &ToolDiagnostic{Severity: "warn", Code: "truncated",
		Message: fmt.Sprintf("Output truncated to its %s: %d lines, %d bytes dropped", kept, snapshot.droppedLines, snapshot.droppedBytes)}
}
