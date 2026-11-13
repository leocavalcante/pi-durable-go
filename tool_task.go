package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

type toolTaskInput struct {
	// Kind is "nested" for a call another tool made through ExecuteTool;
	// empty for a model-issued call.
	Kind         string   `json:"kind,omitempty"`
	Parent       TaskID   `json:"parent,omitempty"`
	ParentCallID string   `json:"parentCallId,omitempty"`
	Key          string   `json:"key,omitempty"`
	NoProgress   bool     `json:"noProgress,omitempty"`
	Assistant    EntryID  `json:"assistant,omitempty"`
	CallID       string   `json:"callId"`
	Call         ToolCall `json:"call"`
	// Unoffered marks a call to a tool its request did not offer: it settles
	// tool_unavailable without resolving the name.
	Unoffered bool `json:"unoffered,omitempty"`
}

// toolCheckpoint is pi.tool version 2: phases call and execute. Running
// output lives in the call's pi.live slot; Output is only read from version-1
// records, which stored it here.
type toolCheckpoint struct {
	Phase      string          `json:"phase"`
	Arguments  json.RawMessage `json:"arguments,omitempty"`
	ReplaySafe bool            `json:"replaySafe,omitempty"`
	Output     string          `json:"output,omitempty"` // legacy version 1 only
}

// toolVersion is Go's own numbering of pi.tool's stored form. Version 2 moved
// output to the pi.live slot only; a version-1 Harness would drop committed
// output from interrupted and aborted results, so it leaves version 2 blocked.
const toolVersion = 2

func migrateTool(from int, input, checkpoint json.RawMessage) (MigratedTask, error) {
	if from != 1 {
		return MigratedTask{}, fmt.Errorf("pi.tool has no migration from version %d", from)
	}
	return MigratedTask{Input: input, Checkpoint: checkpoint}, nil
}

type toolTaskResult struct {
	EntryID EntryID    `json:"entryId"`
	Result  ToolResult `json:"result"`
}

// finishSlot marks a slot done and clears its running output.
func finishSlot(slot *ToolSlot) {
	slot.Status = "done"
	slot.Output, slot.Details, slot.DroppedBytes, slot.DroppedLines, slot.Diagnostics = "", nil, 0, 0, nil
}

func toolTaskDefinition() TaskDefinition {
	return TaskDefinition{Kind: "pi.tool", Version: toolVersion, Initial: func(json.RawMessage) (json.RawMessage, error) { return json.Marshal(toolCheckpoint{Phase: "call"}) }, Run: runTool, Abort: abortTool, Migrate: migrateTool}
}

// harnessError is an error result the Harness writes itself: no content and
// one error diagnostic with a code.
func harnessError(code, message string) ToolResult {
	return ToolResult{IsError: true, Diagnostics: []ToolDiagnostic{{Severity: "error", Code: code, Message: message}}}
}

// toolCall resolves what a tool task's runtime and hooks need.
type toolCall struct {
	input   toolTaskInput
	cp      toolCheckpoint
	def     *ToolDefinition
	runtime *ToolRuntime
	hooks   []namedHooks
}

func resolveToolCall(ctx context.Context, r *TaskRuntime, task TaskRecord) (*toolCall, error) {
	call := &toolCall{}
	if err := json.Unmarshal(task.Input, &call.input); err != nil {
		return nil, err
	}
	// The phase span says which call it runs (upstream readCall).
	callID := call.input.Call.ID
	if callID == "" {
		callID = call.input.CallID
	}
	spanOf(ctx).set(map[string]any{"tool.name": call.input.Call.Name, "tool.call_id": callID})
	if err := json.Unmarshal(task.Checkpoint, &call.cp); err != nil {
		return nil, err
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return nil, err
	}
	c := state.Conversations[task.Conversation]
	if c == nil {
		return nil, ErrNotFound
	}
	c = cloneRecord(c)
	registry := r.Registry()
	// A nested call resolves among the tools callable by tools.
	caller := "model"
	if call.input.Kind == "nested" {
		caller = "tools"
	}
	for _, tool := range registry.Tools(c.Agent) {
		if tool.Name == call.input.Call.Name && callableBy(tool, caller) {
			copy := tool
			call.def = &copy
			break
		}
	}
	call.runtime = &ToolRuntime{TaskRuntime: r, ConversationID: task.Conversation, Call: call.input.Call,
		nested: call.input.Kind == "nested", noProgress: call.input.NoProgress}
	call.hooks = registry.hooks(c.Agent)
	return call, nil
}

// afterTool runs AfterTool hooks on a result, as on every result the tool
// task writes; a failing hook is reported as extension_error and skipped.
func (call *toolCall) afterTool(ctx context.Context, result ToolResult) ToolResult {
	for _, hook := range call.hooks {
		if hook.AfterTool == nil {
			continue
		}
		replaced := result
		if err := containPanic(func() error { return hook.AfterTool(ctx, &replaced, call.runtime) }); err != nil {
			call.runtime.reportExtension(err, hook.extension)
			continue
		}
		result = replaced
	}
	return result
}

// fromSlot builds an error result from the call's slot: what it published,
// and an error diagnostic. A version-1 record kept its output in the
// checkpoint instead.
func (call *toolCall) fromSlot(ctx context.Context, r *TaskRuntime, task TaskRecord, code, message string) (ToolResult, error) {
	result := harnessError(code, message)
	result.Content = call.cp.Output
	state, err := r.h.read(ctx)
	if err != nil {
		return ToolResult{}, err
	}
	if doc, _ := state.document(documentKey(liveDocument, uint64(task.Conversation))); doc != nil {
		var live LiveState
		if value, err := migratedDocument(liveDocument, doc); err == nil && json.Unmarshal(value, &live) == nil {
			if slot := live.toolSlot(task.ID); slot != nil {
				if slot.Output != "" {
					result.Content = slot.Output
				}
				result.Details = slot.Details
				// What the tool reported while it ran comes before the error.
				result.Diagnostics = append(append([]ToolDiagnostic{}, slot.Diagnostics...), result.Diagnostics...)
				if slot.DroppedBytes > 0 {
					result.Diagnostics = append(result.Diagnostics, ToolDiagnostic{Severity: "warn", Code: "truncated",
						Message: fmt.Sprintf("Output truncated: %d lines, %d bytes dropped", slot.DroppedLines, slot.DroppedBytes)})
				}
			}
		}
	}
	return result, nil
}

// abortTool is the abort handler. An abandonment after a restart says
// whether the call may have run. The scheduler gives an abort handler its
// last attempt at restarts limit-1; that attempt skips the hooks, which may
// be what cuts it off.
func abortTool(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	call, err := resolveToolCall(ctx, r, task)
	if err != nil {
		return err
	}
	name := call.input.Call.Name
	code, message := "aborted", fmt.Sprintf("Tool %s was aborted", name)
	if task.AbortReason == "restart" {
		if call.cp.Phase == "execute" {
			code, message = "interrupted", fmt.Sprintf("Tool %s was interrupted by a restart and may have partially run", name)
		} else {
			code, message = "abandoned", fmt.Sprintf("Tool %s was not started: its caller ended with a restart", name)
		}
	}
	result, err := call.fromSlot(ctx, r, task, code, message)
	if err != nil {
		return err
	}
	if float64(task.Restarts) < r.h.restartLimit()-1 {
		result = call.afterTool(ctx, result)
	}
	return finishTool(ctx, r, task, result, OutcomeAborted)
}

func runTool(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
	call, err := resolveToolCall(ctx, r, task)
	if err != nil {
		return err
	}
	input, cp, def, runtime := call.input, call.cp, call.def, call.runtime
	name := input.Call.Name
	// After reading its call, and before the replay decision, a call cut off
	// restartLimit times in a row ends with an error result built from its
	// slot instead of running again, even for a replay-unsafe tool. Past the
	// limit the hooks may be what keeps cutting it off.
	if cp.Phase == "call" || cp.Phase == "execute" {
		if limit := r.h.restartLimit(); cutOffTooOften(task.Restarts, limit) {
			message := fmt.Sprintf("Tool %s was interrupted %s; it is not run again", name, cutOffCount(task.Restarts))
			result, err := call.fromSlot(ctx, r, task, reasonRestartLimit, message)
			if err != nil {
				return err
			}
			if salvageable(task.Restarts, limit) {
				result = call.afterTool(ctx, result)
			}
			return finishToolEnding(ctx, r, task, result, OutcomeFailed, message, restartLimitDetail, false)
		}
	}
	reject := func(result ToolResult) error {
		return finishTool(ctx, r, task, call.afterTool(ctx, result), OutcomeCompleted)
	}
	if cp.Phase == "execute" {
		// Recovery after intent: rerun only when the stored and the current
		// policy both say safe.
		if !cp.ReplaySafe || def == nil || !def.ReplaySafe || def.Execute == nil {
			message := fmt.Sprintf("Tool %s was interrupted and may have partially run", name)
			result, err := call.fromSlot(ctx, r, task, "interrupted", message)
			if err != nil {
				return err
			}
			return finishToolEnding(ctx, r, task, call.afterTool(ctx, result), OutcomeFailed, message, nil, false)
		}
		// The rerun reports from scratch: clear what the interrupted attempt
		// published. A version-1 record's checkpoint output goes too.
		if err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			if err := runtime.clearProgress(tx); err != nil {
				return TaskTransition{}, err
			}
			checkpoint := task.Checkpoint
			if cp.Output != "" {
				cleared := toolCheckpoint{Phase: cp.Phase, Arguments: cp.Arguments, ReplaySafe: cp.ReplaySafe}
				var err error
				if checkpoint, err = json.Marshal(cleared); err != nil {
					return TaskTransition{}, err
				}
			}
			return TaskTransition{Status: "running", Checkpoint: checkpoint}, nil
		}); err != nil {
			return err
		}
		runtime.Call.Arguments = cp.Arguments
	} else if cp.Phase == "call" {
		if input.Unoffered || def == nil || def.Execute == nil {
			return reject(harnessError("tool_unavailable", fmt.Sprintf("Tool %s is not available", name)))
		}
		// Repaired, then coerced and checked (pi-ai validateToolArguments):
		// hooks and the tool see the coerced arguments; the transcript keeps
		// the model's.
		prepared, err := prepareArguments(def, runtime.Call.Arguments)
		if err != nil {
			return reject(harnessError("invalid_arguments", err.Error()))
		}
		checked, err := validateToolArguments(def.ToolSpec, prepared)
		if err != nil {
			return reject(harnessError("invalid_arguments", err.Error()))
		}
		runtime.Call.Arguments = checked
		for _, hook := range call.hooks {
			if hook.BeforeTool == nil {
				continue
			}
			// The first block wins; a throw blocks.
			if err := containPanic(func() error { return hook.BeforeTool(ctx, &runtime.Call, runtime) }); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return reject(harnessError("blocked", "Tool call blocked: "+err.Error()))
			}
		}
		if runtime.Call.Name != input.Call.Name || runtime.Call.ID != input.Call.ID {
			return reject(harnessError("blocked", "Tool call blocked: hooks cannot change the call's identity"))
		}
		if checked, err = validateToolArguments(def.ToolSpec, runtime.Call.Arguments); err != nil {
			return reject(harnessError("invalid_arguments", err.Error()))
		}
		runtime.Call.Arguments = checked
		cp = toolCheckpoint{Phase: "execute", Arguments: runtime.Call.Arguments, ReplaySafe: def.ReplaySafe}
		// The slot's status and arguments are presentation, stored like
		// progress; the intent is durable before the tool runs (upstream).
		if err := r.CommitWith(ctx, progressCommit(), func(tx *Tx) (TaskTransition, error) {
			current, err := tx.Task(r.ID)
			if err != nil {
				return TaskTransition{}, err
			}
			if err := runtime.updateLive(tx, func(slot *ToolSlot) { slot.Status = "running" }); err != nil {
				return TaskTransition{}, err
			}
			// A nested slot shows the arguments the call runs with, which
			// hooks may have changed.
			if runtime.nested && !runtime.noProgress {
				if err := updateLive(tx, runtime.ConversationID, func(live *LiveState) error {
					for i := range live.NestedTools {
						if slot := &live.NestedTools[i]; slot.TaskID == r.ID && !jsonEqual(slot.Arguments, cp.Arguments) {
							slot.Arguments = append(json.RawMessage(nil), cp.Arguments...)
						}
					}
					return nil
				}); err != nil {
					return TaskTransition{}, err
				}
			}
			return TaskTransition{Status: "running", Checkpoint: current.Checkpoint}, nil
		}); err != nil {
			return err
		}
		if err := r.Commit(ctx, func(*Tx) (TaskTransition, error) {
			data, err := json.Marshal(cp)
			return TaskTransition{Status: "running", Checkpoint: data}, err
		}); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("%w: tool phase", ErrInvalid)
	}
	started := time.Now()
	result, executeErr := def.Execute(ctx, cp.Arguments, runtime)
	runtime.executed.Store(true)
	if err := runtime.stopOutput(ctx); err != nil && ctx.Err() == nil {
		runtime.Report(err)
	}
	durationMs := time.Since(started).Milliseconds()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	status, outcomeError := "completed", ""
	if executeErr != nil {
		// A throw keeps what the tool output as the result; its message is
		// a tool_error diagnostic, and the task fails (upstream).
		status, outcomeError = "failed", fmt.Sprintf("Tool %s threw", def.Name)
		result = ToolResult{IsError: true, Diagnostics: []ToolDiagnostic{{Severity: "error", Code: "tool_error", Message: diagnostic(executeErr.Error())}}}
	}
	// Diagnostics the tool reported while it ran come before its result's own.
	result.Diagnostics = append(runtime.takeDiagnostics(), result.Diagnostics...)
	// A result without content shows the output the call kept, to the
	// afterTool chain too, with a note while the chain keeps it when that
	// output dropped some (upstream).
	retained, truncation := "", (*ToolDiagnostic)(nil)
	if result.Content == "" && len(result.Images) == 0 {
		retained, truncation = runtime.retainedResult(ctx, def)
		result.Content = retained
	}
	result.DurationMs = &durationMs
	result = call.afterTool(ctx, result)
	if truncation != nil && retained != "" && result.Content == retained {
		result.Diagnostics = append(result.Diagnostics, *truncation)
	}
	// The contract holds for what the afterTool chain leaves (upstream).
	result = call.checkStructuredOutput(ctx, result)
	// A schema tool's error result without structured output has its text
	// only in the content, which nested callers never get: it becomes an
	// error diagnostic, as a throw's message does, unless one says what went
	// wrong already (upstream).
	if runtime.nested && len(def.StructuredOutputSchema) > 0 && result.IsError && len(result.StructuredOutput) == 0 && result.Content != "" {
		explained := false
		for _, d := range result.Diagnostics {
			explained = explained || d.Severity == "error"
		}
		if !explained {
			result.Diagnostics = append(result.Diagnostics, ToolDiagnostic{Severity: "error", Code: "tool_error", Message: result.Content})
		}
	}
	result.DurationMs = &durationMs
	return finishToolEnding(ctx, r, task, result, status, outcomeError, nil, true)
}

// checkStructuredOutput holds a result to its tool's structured output
// contract (upstream): a nested call that breaks it is an error result with an
// invalid_structured_output diagnostic; a model call stands without its
// structured output, which the model never sees, and the tool's extension is
// reported.
func (call *toolCall) checkStructuredOutput(ctx context.Context, result ToolResult) ToolResult {
	if call.def == nil {
		return result
	}
	broken := structuredOutputError(call.def, result)
	if broken == "" {
		return result
	}
	result.StructuredOutput = nil
	if call.runtime.nested {
		result.IsError = true
		result.Diagnostics = append(result.Diagnostics, ToolDiagnostic{Severity: "error", Code: "invalid_structured_output", Message: broken})
		return result
	}
	extension := ""
	if agent, err := call.runtime.Agent(ctx); err == nil {
		extension = call.runtime.Registry().toolExtension(agent.Agent, call.def.Name)
	}
	call.runtime.reportExtension(errors.New(broken), extension)
	return result
}

// structuredOutputError is why a result's structured output breaks its
// tool's contract, or "" when it keeps it (upstream structuredOutputError).
func structuredOutputError(def *ToolDefinition, result ToolResult) string {
	if len(def.StructuredOutputSchema) == 0 {
		if len(result.StructuredOutput) == 0 {
			return ""
		}
		return fmt.Sprintf("Tool %s returned structuredOutput but declares no structuredOutputSchema", def.Name)
	}
	if len(result.StructuredOutput) == 0 {
		if result.IsError {
			return ""
		}
		return fmt.Sprintf("Tool %s returned no structuredOutput", def.Name)
	}
	path, message, ok := firstSchemaFailure(def.StructuredOutputSchema, result.StructuredOutput)
	if ok {
		return ""
	}
	return fmt.Sprintf("Tool %s returned structuredOutput that does not match its schema: %s: %s", def.Name, path, message)
}

func finishTool(ctx context.Context, r *TaskRuntime, task TaskRecord, result ToolResult, status string) error {
	return finishToolEnding(ctx, r, task, result, status, "", nil, false)
}

// finishToolEnding settles a call with its result entry. A failed outcome's
// Error is outcomeError when given, else the result content, and carries
// detail as its ErrorDetail. executed says the tool ran (a throw included)
// rather than the Harness writing the result, which decides a nested
// caller's value.
func finishToolEnding(ctx context.Context, r *TaskRuntime, task TaskRecord, result ToolResult, status, outcomeError string, detail json.RawMessage, executed bool) error {
	if !executed {
		// The tool never produced one; hooks cannot add it (upstream).
		result.StructuredOutput = nil
	}
	var input toolTaskInput
	if err := json.Unmarshal(task.Input, &input); err != nil {
		return err
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		current, err := tx.Task(task.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		var cp toolCheckpoint
		if err := json.Unmarshal(current.Checkpoint, &cp); err != nil {
			return TaskTransition{}, err
		}
		// Nested calls this call left running are aborted first.
		if err := abortNestedCalls(tx, *current); err != nil {
			return TaskTransition{}, err
		}
		// Committed output is the slot's; a version-1 record kept it in its
		// checkpoint instead.
		committed := cp.Output
		live, err := readLive(tx, task.Conversation)
		if err != nil {
			return TaskTransition{}, err
		}
		slot := live.toolSlot(task.ID)
		if slot != nil {
			if slot.Output != "" {
				committed = slot.Output
			}
			if len(slot.Details) > 0 && len(result.Details) == 0 {
				result.Details = append(json.RawMessage(nil), slot.Details...)
			}
		}
		// Omitted content is the retained Output text.
		if result.Content == "" && len(result.Diagnostics) == 0 {
			result.Content = committed
		}
		if input.Kind == "nested" {
			def, _ := r.Registry().toolByName(task, input.Call.Name)
			data, err := settleNested(tx, input, task, def, result, executed)
			if err != nil {
				return TaskTransition{}, err
			}
			outcome := &Outcome{Status: status, Result: data}
			if status == OutcomeFailed {
				outcome.Error = diagnostic(result.Content)
				if outcomeError != "" {
					outcome.Error = diagnostic(outcomeError)
				}
				if outcome.Error == "" {
					outcome.Error = "nested call failed"
				}
				outcome.ErrorDetail = detail
			}
			return TaskTransition{Status: "terminal", Outcome: outcome}, nil
		}
		// Bound the content with the tool's limits; a truncation is a warning.
		limits := OutputLimits{}
		if def, ok := r.Registry().toolByName(task, input.Call.Name); ok && def.OutputLimits != nil {
			limits = *def.OutputLimits
		}
		if bounded, lines, bytes := boundOutput(result.Content, limits); lines > 0 || bytes > 0 {
			// Images keep their places in what is kept: a kept end moves
			// them by the dropped beginning.
			shift := 0
			if limits.Retain == "tail" {
				shift = len(result.Content) - len(bounded)
			}
			if result.ImageOffsets != nil {
				offsets := make([]int, len(result.ImageOffsets))
				for i, at := range result.ImageOffsets {
					offsets[i] = min(max(at-shift, 0), len(bounded))
				}
				result.ImageOffsets = offsets
			}
			result.Content = bounded
			kept := "beginning"
			if limits.Retain == "tail" {
				kept = "end"
			}
			result.Diagnostics = append(result.Diagnostics, ToolDiagnostic{Severity: "warn", Code: "truncated",
				Message: fmt.Sprintf("Output truncated to its %s: %d lines, %d bytes dropped", kept, lines, bytes)})
		}
		details, err := json.Marshal(result)
		if err != nil {
			return TaskTransition{}, err
		}
		content := result.Content
		if len(result.Diagnostics) > 0 {
			if content != "" {
				content += "\n"
			}
			content += renderDiagnostics(result.Diagnostics)
		}
		id, err := tx.Append(task.Conversation, Entry{Kind: "pi.tool-result", Message: &Message{Role: "tool", ToolCallID: input.Call.ID, ToolName: input.Call.Name, Content: content, Images: result.Images, ImageOffsets: result.ImageOffsets, IsError: result.IsError, Usage: result.Usage, DurationMs: result.DurationMs}, Data: details})
		if err != nil {
			return TaskTransition{}, err
		}
		// The slot is done: its running output is cleared, and its entry is
		// the result; the round's end removes the slots.
		if slot != nil {
			finishSlot(slot)
			slot.Entry = id
			if err := writeLive(tx, task.Conversation, live); err != nil {
				return TaskTransition{}, err
			}
		}
		if err := recordToolUsage(tx, task.Conversation, input.Call.Name, result.Usage); err != nil {
			return TaskTransition{}, err
		}
		data, err := json.Marshal(toolTaskResult{EntryID: id, Result: result})
		if err != nil {
			return TaskTransition{}, err
		}
		outcome := &Outcome{Status: status, Result: data}
		if status == OutcomeFailed {
			outcome.Error = diagnostic(content)
			if outcomeError != "" {
				outcome.Error = diagnostic(outcomeError)
			}
			outcome.ErrorDetail = detail
		}
		return TaskTransition{Status: "terminal", Outcome: outcome}, nil
	})
}

// renderDiagnostics is upstream's <harness> block the model sees.
func renderDiagnostics(diagnostics []ToolDiagnostic) string {
	var b strings.Builder
	b.WriteString("<harness>\n")
	for i, d := range diagnostics {
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "[%s] %s", d.Severity, d.Message)
	}
	b.WriteString("\n</harness>")
	return b.String()
}

// appendToolResult appends a call's result entry and records its usage; the
// model sees its diagnostics in a <harness> block after the content.
func appendToolResult(tx *Tx, cid ConversationID, call ToolCall, result ToolResult) (EntryID, error) {
	details, err := json.Marshal(result)
	if err != nil {
		return 0, err
	}
	content := result.Content
	if len(result.Diagnostics) > 0 {
		if content != "" {
			content += "\n"
		}
		content += renderDiagnostics(result.Diagnostics)
	}
	id, err := tx.Append(cid, Entry{Kind: "pi.tool-result", Message: &Message{Role: "tool", ToolCallID: call.ID, ToolName: call.Name, Content: content, Images: result.Images, ImageOffsets: result.ImageOffsets, IsError: result.IsError, Usage: result.Usage, DurationMs: result.DurationMs}, Data: details})
	if err != nil {
		return 0, err
	}
	return id, recordToolUsage(tx, cid, call.Name, result.Usage)
}

// Default result bounds (upstream DEFAULT_MAX_BYTES and DEFAULT_MAX_LINES).
const (
	defaultMaxOutputBytes = 50 * 1024
	defaultMaxOutputLines = 2000
)

// boundOutput keeps whole lines from the head or the tail of text within
// the limits, and reports how many lines and bytes it dropped. A single line
// longer than the byte limit is cut at a rune boundary.
func boundOutput(text string, limits OutputLimits) (string, int, int) {
	maxBytes, maxLines := limits.MaxBytes, limits.MaxLines
	if maxBytes <= 0 {
		maxBytes = defaultMaxOutputBytes
	}
	if maxLines <= 0 {
		maxLines = defaultMaxOutputLines
	}
	lines := strings.SplitAfter(text, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(text) <= maxBytes && len(lines) <= maxLines {
		return text, 0, 0
	}
	tail := limits.Retain == "tail"
	var kept []string
	size := 0
	for i := 0; i < len(lines) && len(kept) < maxLines; i++ {
		line := lines[i]
		if tail {
			line = lines[len(lines)-1-i]
		}
		if size+len(line) > maxBytes {
			if len(kept) == 0 { // one oversized line: keep what fits of it
				if tail {
					line = tailBytes(line, maxBytes)
				} else {
					cut := maxBytes
					for cut > 0 && !utf8.RuneStart(line[cut]) {
						cut--
					}
					line = line[:cut]
				}
				kept = append(kept, line)
				size += len(line)
			}
			break
		}
		kept = append(kept, line)
		size += len(line)
	}
	if tail {
		for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
			kept[i], kept[j] = kept[j], kept[i]
		}
	}
	bounded := strings.Join(kept, "")
	droppedLines := len(lines) - len(kept)
	if droppedLines == 0 && len(bounded) < len(text) {
		droppedLines = 1 // part of a line
	}
	return bounded, droppedLines, len(text) - len(bounded)
}
