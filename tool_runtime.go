package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	// reported is what the call reports while it runs.
	reported toolReports
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
// task's crash count. Progress commits are paced as upstream's are: the
// first change after an idle period commits at once, and each commit delays
// the next by at least Settings.Progress.OutputIntervalMs (default 100 ms)
// and by its size at 100 KiB/s, with one in flight; a crash loses what was
// not committed yet, and the call's result has all of it. Text is decoded
// as one UTF-8 stream, so a character split across calls survives.
func (r *ToolRuntime) Output(ctx context.Context, text string) error {
	if err := r.check(); err != nil {
		return err
	}
	r.reported.mu.Lock()
	accepted := r.reported.retained().push(text)
	r.reported.mu.Unlock()
	if accepted {
		r.progress().mark()
	}
	return nil
}

// toolReports is what a call reports while it runs (upstream Reported): its
// retained output, diagnostics, and details, which paced progress commits
// publish to its pi.live slot.
type toolReports struct {
	mu          sync.Mutex
	buffer      *outputBuffer // the retained window; nil until used
	limits      *OutputLimits // the tool's limits, defaults filled in
	diagnostics []ToolDiagnostic
	details     json.RawMessage
	// detailsSet counts Details calls: each one changes the details, even
	// to an equal value (upstream compares copies by reference).
	detailsSet int
	// written is what the last progress commit published.
	written struct {
		text                    string
		detailsSet, diagnostics int
	}
	pacer *progressPacer
	// waiting are Details calls still waiting when the call ended; the
	// call's settling commit settles them.
	waiting []chan error
}

// retained is the call's output window; mu is held.
func (o *toolReports) retained() *outputBuffer {
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
func (o *toolReports) snapshot() boundedOutput {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.retained().snapshot()
}

// progress is the pacer of the call's progress commits.
func (r *ToolRuntime) progress() *progressPacer {
	r.reported.mu.Lock()
	defer r.reported.mu.Unlock()
	if r.reported.pacer == nil {
		interval := 100 * time.Millisecond
		if ms := r.h.settings().Progress.OutputIntervalMs; ms != nil && *ms >= 0 {
			interval = time.Duration(*ms * float64(time.Millisecond))
		}
		r.reported.pacer = &progressPacer{write: r.writeProgress, minInterval: interval, now: r.Now, setTimer: r.SetTimer,
			// Failures after the call ended or was aborted are expected; the
			// committed state stays consistent.
			onError: func(err error) {
				if r.check() == nil {
					r.Report(err)
				}
			}}
	}
	return r.reported.pacer
}

// writeProgress publishes what changed since the last progress commit to
// the call's slot, and returns about how many bytes that wrote (upstream
// publishProgress). A call without a slot to show it in writes nothing.
func (r *ToolRuntime) writeProgress() (int, error) {
	if r.nested && r.noProgress {
		return 0, nil
	}
	// Captured at once: the tool keeps reporting while the commit runs.
	r.reported.mu.Lock()
	snapshot := r.reported.retained().snapshot()
	detailsSet := r.reported.detailsSet
	details := r.reported.details
	diagnostics := len(r.reported.diagnostics)
	added := append([]ToolDiagnostic(nil), r.reported.diagnostics[r.reported.written.diagnostics:]...)
	written := r.reported.written
	r.reported.mu.Unlock()
	detailsChanged := detailsSet != written.detailsSet
	// What the commit writes: an append, a trim plus an append of what
	// follows the shared part, or the whole window.
	size := 0
	if snapshot.text != written.text {
		shared := len(written.text)
		if !strings.HasPrefix(snapshot.text, written.text) {
			shared = overlap(written.text, snapshot.text, 65536)
		}
		size += len(snapshot.text) - shared
	}
	if detailsChanged {
		size += max(len(details), len("null"))
	}
	if len(added) > 0 {
		if encoded, err := json.Marshal(added); err == nil {
			size += len(encoded)
		}
	}
	err := r.CommitWith(context.Background(), progressCommit(), func(tx *Tx) (TaskTransition, error) {
		task, err := tx.Task(r.ID)
		if err != nil {
			return TaskTransition{}, err
		}
		if err := r.updateLive(tx, func(slot *ToolSlot) {
			slot.Output = snapshot.text
			if snapshot.droppedBytes > 0 {
				slot.DroppedBytes = snapshot.droppedBytes
			}
			if snapshot.droppedLines > 0 {
				slot.DroppedLines = snapshot.droppedLines
			}
			if detailsChanged && details != nil {
				slot.Details = append(json.RawMessage(nil), details...)
			}
			slot.Diagnostics = append(slot.Diagnostics, added...)
		}); err != nil {
			return TaskTransition{}, err
		}
		return TaskTransition{Status: "running", Checkpoint: task.Checkpoint}, nil
	})
	if err != nil {
		return 0, err
	}
	r.reported.mu.Lock()
	r.reported.written.text, r.reported.written.detailsSet, r.reported.written.diagnostics = snapshot.text, detailsSet, diagnostics
	r.reported.mu.Unlock()
	return size, nil
}

// overlap is the length of the longest suffix of a, within its last scan
// bytes, that b starts with, found by probing with b's first bytes (upstream
// chord overlap); 0 when it gives up.
func overlap(a, b string, scan int) int {
	const probe, maxCandidates = 64, 8
	if a == "" || b == "" || scan == 0 {
		return 0
	}
	tail := a
	if len(a) > scan {
		tail = a[len(a)-scan:]
	}
	for _, h := range []int{min(probe, len(b)), 1} {
		head := b[:h]
		tried := 0
		for k := strings.Index(tail, head); k != -1; {
			if tried++; tried > maxCandidates {
				break
			}
			if n := len(tail) - k; n <= len(b) && tail[k:] == b[:n] {
				return n
			}
			next := strings.Index(tail[k+1:], head)
			if next == -1 {
				break
			}
			k += 1 + next
		}
		if h == 1 {
			break
		}
	}
	return 0
}

// endReports ends the call's output stream and its progress commits, which
// publish nothing more: the call's settling commit has all it reported.
// Details still waiting are kept for that commit to settle.
func (r *ToolRuntime) endReports() {
	waiting := r.progress().stop()
	r.reported.mu.Lock()
	defer r.reported.mu.Unlock()
	r.reported.retained().end()
	r.reported.waiting = append(r.reported.waiting, waiting...)
}

// settleReports settles the Details calls still waiting when the call
// ended, with the outcome of its settling commit.
func (r *ToolRuntime) settleReports(err error) {
	r.reported.mu.Lock()
	waiting := r.reported.waiting
	r.reported.waiting = nil
	r.reported.mu.Unlock()
	for _, waiter := range waiting {
		waiter <- err
	}
}

// reportedDetails are the details the call reported through Details.
func (r *ToolRuntime) reportedDetails() json.RawMessage {
	r.reported.mu.Lock()
	defer r.reported.mu.Unlock()
	return append(json.RawMessage(nil), r.reported.details...)
}

// clearProgress removes what an interrupted attempt published, before a
// safe rerun reports from scratch, and shows the slot running again.
func (r *ToolRuntime) clearProgress(tx *Tx) error {
	return r.updateLive(tx, func(slot *ToolSlot) {
		slot.Output, slot.Details, slot.DroppedBytes, slot.DroppedLines, slot.Diagnostics = "", nil, 0, 0, nil
		slot.Status = "running"
	})
}

// Details replaces the call's details (upstream api.details): they are
// published to its pi.live slot with its paced progress, and are its
// result's details when the result has none. Details returns once the
// commit that includes them did, or when ctx ends; the update stays either
// way.
func (r *ToolRuntime) Details(ctx context.Context, value json.RawMessage) error {
	if err := r.check(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !json.Valid(value) {
		return fmt.Errorf("%w: tool details JSON", ErrInvalid)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return err
	}
	r.reported.mu.Lock()
	r.reported.details = append(json.RawMessage(nil), compact.Bytes()...)
	r.reported.detailsSet++
	r.reported.mu.Unlock()
	select {
	case err := <-r.progress().markAndWait():
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
