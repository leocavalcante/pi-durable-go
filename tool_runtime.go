package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type ToolRuntime struct {
	*TaskRuntime
	ConversationID ConversationID
	Call           ToolCall
	// nested marks a nested call's runtime: its progress goes to its nested
	// slot, unless noProgress.
	nested, noProgress bool
	executed           atomic.Bool  // Execute returned: no more nested calls
	sequence           atomic.Int64 // default nested call keys of this attempt
	output             outputThrottle
	// diagnostics are added through Diagnostic while the call runs.
	diagnosticsMu sync.Mutex
	diagnostics   []ToolDiagnostic
}

// CommitChange commits change as the call's task, which keeps running
// unchanged (upstream ToolExecutionApi.commit): documents and other records
// the call writes. It fails once the call ended.
func (r *ToolRuntime) CommitChange(ctx context.Context, change func(*Tx) error) error {
	if change == nil {
		return ErrInvalid
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		current, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		checkpoint := append(json.RawMessage(nil), current.Checkpoint...)
		if err := change(tx); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: checkpoint}, nil
	})
}

// Output adds running output to the call's retained window, the head or
// tail of its tool's output limits (upstream api.output), and publishes the
// window, sanitized, to its pi.live slot. It is presentation, not task
// progress: the checkpoint is unchanged, so output does not reset the
// task's crash count. Commits are throttled to
// Settings.Progress.OutputIntervalMs (default 100 ms), with one in flight;
// the call's result flushes what is pending, while a crash loses at most
// about one interval. With an interval of 0, each call commits. Text is
// decoded as one UTF-8 stream, so a character split across calls survives.
func (r *ToolRuntime) Output(ctx context.Context, text string) error {
	if err := r.check(); err != nil {
		return err
	}
	interval := 100 * time.Millisecond
	if ms := r.h.settings().Progress.OutputIntervalMs; ms != nil && *ms >= 0 {
		interval = time.Duration(*ms * float64(time.Millisecond))
	}
	r.output.mu.Lock()
	if !r.output.retained().push(text) {
		r.output.mu.Unlock()
		return nil
	}
	r.output.dirty = true
	if interval == 0 {
		r.output.mu.Unlock()
		return r.commitOutput(ctx)
	}
	defer r.output.mu.Unlock()
	if !r.output.armed && !r.output.stopped {
		r.output.armed = true
		r.output.disarm = r.SetTimer(r.flushOutput, interval)
	}
	return nil
}

// outputThrottle keeps a call's running output and paces its commits.
type outputThrottle struct {
	mu          sync.Mutex
	buffer      *outputBuffer    // the retained window; nil until used
	limits      *OutputLimits    // the tool's limits, defaults filled in
	dirty       bool             // output not committed yet
	diagnostics []ToolDiagnostic // reported, not yet in the slot
	armed       bool
	stopped     bool
	disarm      func()
	inFlight    sync.WaitGroup
}

// retained is the call's output window; mu is held.
func (o *outputThrottle) retained() *outputBuffer {
	if o.buffer == nil {
		limits := outputLimitsOf(nil)
		if o.limits != nil {
			limits = *o.limits
		}
		o.buffer = newOutputBuffer(limits)
	}
	return o.buffer
}

// snapshot is the retained output now; mu is not held.
func (o *outputThrottle) snapshot() boundedOutput {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.retained().snapshot()
}

// commitOutput publishes the retained window as it is when the commit runs,
// with diagnostics, to the call's slot.
func (r *ToolRuntime) commitOutput(ctx context.Context, diagnostics ...ToolDiagnostic) error {
	return r.CommitWith(ctx, progressCommit(), func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		r.output.mu.Lock()
		r.output.dirty = false
		snapshot := r.output.retained().snapshot()
		r.output.mu.Unlock()
		if err := r.updateLive(tx, func(slot *ToolSlot) {
			slot.Diagnostics = append(slot.Diagnostics, diagnostics...)
			slot.Output = snapshot.text
			if snapshot.droppedBytes > 0 {
				slot.DroppedBytes = snapshot.droppedBytes
			}
			if snapshot.droppedLines > 0 {
				slot.DroppedLines = snapshot.droppedLines
			}
		}); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
}

func (r *ToolRuntime) flushOutput() {
	r.output.mu.Lock()
	dirty := r.output.dirty
	diagnostics := r.output.diagnostics
	r.output.diagnostics = nil
	r.output.armed = false
	if !dirty && len(diagnostics) == 0 || r.output.stopped {
		r.output.mu.Unlock()
		return
	}
	r.output.inFlight.Add(1)
	r.output.mu.Unlock()
	if err := r.commitOutput(context.Background(), diagnostics...); err != nil && r.check() == nil {
		r.Report(err)
	}
	r.output.inFlight.Done()
}

// stopOutput ends the output stream and its throttling and commits what is
// still pending, so the result reads every output the tool produced.
func (r *ToolRuntime) stopOutput(ctx context.Context) error {
	r.output.mu.Lock()
	r.output.stopped = true
	disarm := r.output.disarm
	r.output.mu.Unlock()
	if disarm != nil {
		disarm()
	}
	r.output.inFlight.Wait()
	r.output.mu.Lock()
	if r.output.buffer != nil && r.output.buffer.end() {
		r.output.dirty = true
	}
	dirty := r.output.dirty
	diagnostics := r.output.diagnostics
	r.output.diagnostics = nil
	r.output.mu.Unlock()
	if !dirty && len(diagnostics) == 0 {
		return nil
	}
	return r.commitOutput(ctx, diagnostics...)
}

// tailBytes keeps at most max trailing bytes of s, starting at a rune boundary.
func tailBytes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := len(s) - max
	for cut < len(s) && !utf8.RuneStart(s[cut]) {
		cut++
	}
	return s[cut:]
}

// clearProgress removes what an interrupted attempt published, before a
// safe rerun reports from scratch, and shows the slot running again.
func (r *ToolRuntime) clearProgress(tx *Tx) error {
	return r.updateLive(tx, func(slot *ToolSlot) {
		slot.Output, slot.Details, slot.DroppedBytes, slot.DroppedLines, slot.Diagnostics = "", nil, 0, 0, nil
		slot.Status = "running"
	})
}

// Details replaces the call's published details in its pi.live slot.
func (r *ToolRuntime) Details(ctx context.Context, value json.RawMessage) error {
	if !json.Valid(value) {
		return fmt.Errorf("%w: tool details JSON", ErrInvalid)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return err
	}
	return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := r.updateLive(tx, func(slot *ToolSlot) { slot.Details = append(json.RawMessage(nil), compact.Bytes()...) }); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
}

// updateLive edits the call's slot; a slot that is missing, as for a task
// created before slots existed, is added.
func (r *ToolRuntime) updateLive(tx *Tx, update func(*ToolSlot)) error {
	if r.nested {
		if r.noProgress {
			return nil
		}
		return updateLive(tx, r.ConversationID, func(live *LiveState) error {
			for i := range live.NestedTools {
				if slot := &live.NestedTools[i]; slot.TaskID == r.ID {
					view := ToolSlot{Status: slot.Status, Output: slot.Output, Details: slot.Details,
						DroppedBytes: slot.DroppedBytes, DroppedLines: slot.DroppedLines, Diagnostics: slot.Diagnostics}
					update(&view)
					slot.Status, slot.Output, slot.Details = view.Status, view.Output, view.Details
					slot.DroppedBytes, slot.DroppedLines, slot.Diagnostics = view.DroppedBytes, view.DroppedLines, view.Diagnostics
				}
			}
			return nil
		})
	}
	return updateLive(tx, r.ConversationID, func(live *LiveState) error {
		slot := live.toolSlot(r.ID)
		if slot == nil {
			live.Tools = append(live.Tools, ToolSlot{CallID: r.Call.ID, Name: r.Call.Name, Status: "running", TaskID: r.ID})
			slot = &live.Tools[len(live.Tools)-1]
		}
		update(slot)
		return nil
	})
}

// Submit reuses this tool's owned child and deduplicated input after a restart.
// Use explicit Tx APIs when a call needs several independently named children.
func (r *ToolRuntime) Submit(ctx context.Context, content string) (ConversationID, SubmissionID, error) {
	var cid ConversationID
	var sid SubmissionID
	err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
		existing, err := tx.ConversationsByOwner(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if len(existing) > 0 {
			cid = existing[0].ID
		} else {
			cid, err = tx.CreateConversation(ConversationOptions{Owner: Owner{Task: r.ID}})
			if err != nil {
				return TaskTransition{}, err
			}
		}
		requestID := fmt.Sprintf("tool-subagent:%d", r.ID)
		for _, s := range tx.state.Submissions {
			if s.Conversation == cid && s.Request.RequestID == requestID {
				sid = s.ID
				break
			}
		}
		if sid == 0 {
			if sid, err = r.h.admitSubmission(tx, cid, SubmissionRequest{Type: "input", Content: content, RequestID: requestID}); err != nil {
				return TaskTransition{}, err
			}
		}
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
	if err == nil {
		r.h.Resume()
	}
	return cid, sid, err
}

// Env is HarnessOptions.Environment for the call's conversation.
func (r *ToolRuntime) Env() (ExecutionEnv, error) {
	return r.TaskRuntime.envOf(r.ConversationID)
}

// DeferredTools are the enabled, model-callable deferred tools this call's
// conversation does not offer yet: what a tool search loads.

// AcceptsImages reports whether the conversation's model takes images
// (ImageInputModel); true when unknown. Tools that return images say when the
// model will see a placeholder instead.
func (r *ToolRuntime) AcceptsImages(ctx context.Context) bool {
	model, ok := r.h.model().(ImageInputModel)
	if !ok {
		return true
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return true
	}
	conv := state.Conversations[r.ConversationID]
	if conv == nil {
		return true
	}
	return model.AcceptsImages(conv.Agent.Model)
}

// ImageLimits are the conversation model's image limits (ImageLimitsModel);
// zero fields when unknown, which keeps the defaults.
func (r *ToolRuntime) ImageLimits(ctx context.Context) ImageLimits {
	model, ok := r.h.model().(ImageLimitsModel)
	if !ok {
		return ImageLimits{}
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return ImageLimits{}
	}
	conv := state.Conversations[r.ConversationID]
	if conv == nil {
		return ImageLimits{}
	}
	return model.ImageLimits(conv.Agent.Model)
}

func (r *ToolRuntime) DeferredTools(ctx context.Context) ([]ToolDefinition, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return nil, err
	}
	conv := state.Conversations[r.ConversationID]
	if conv == nil {
		return nil, ErrNotFound
	}
	conv = cloneRecord(conv)
	tools := r.Registry().Tools(conv.Agent)
	offered := map[string]bool{}
	for _, tool := range offeredTools(tools, conv.Agent.ModelTools) {
		offered[tool.Name] = true
	}
	var deferred []ToolDefinition
	for _, tool := range tools {
		if tool.Deferred && callableBy(tool, "model") && !offered[tool.Name] {
			deferred = append(deferred, tool)
		}
	}
	return deferred, nil
}
