package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Queue modes for Settings.SteeringMode and FollowUpMode.
const (
	QueueOneAtATime = "one-at-a-time"
	QueueAll        = "all"
)

// After-failed-run policies for Settings.AfterFailedRun.
const (
	AfterFailedRunKeep     = "keep"
	AfterFailedRunContinue = "continue"
	AfterFailedRunWithdraw = "withdraw"
)

// ErrIdle rejects WhenIdle "reject" input to a conversation without a run.
var ErrIdle = errors.New("durable: conversation idle")

// queueMode is a queued input's boundary mode: steer or followUp.
func queueMode(request SubmissionRequest) string {
	if request.WhenBusy == "steer" {
		return "steer"
	}
	return "followUp"
}

func isReset(request SubmissionRequest) bool {
	return request.Type == "write" && request.Entry != nil && request.Entry.Kind == "pi.reset"
}

// boundaryResult: the user items a boundary selected, in ID order, the first
// entry it placed (a run it starts begins there), and whether it placed a
// reset.
type boundaryResult struct {
	users []SubmissionID
	first EntryID
	reset bool
}

// applyBoundary places the queued items a boundary selects (Pi Durable §6):
// every write, the first or all steers, and at a final boundary the first or
// all follow-ups. A queued reset turns a post-tools boundary into a final
// one. Writes are placed first and user items after them, each in ID order,
// so user items queued before a reset run in the new context.
func (h *Harness) applyBoundary(tx *Tx, cid ConversationID, final bool) (boundaryResult, error) {
	items := queuedSubmissions(tx, cid)
	var result boundaryResult
	for _, item := range items {
		if isReset(item.Request) {
			result.reset = true
		}
	}
	final = final || result.reset
	pick := func(mode, queueMode string) []*SubmissionRecord {
		var picked []*SubmissionRecord
		for _, item := range items {
			if item.Request.Type == "input" && queueModeOf(item) == mode {
				picked = append(picked, item)
				if queueMode != QueueAll {
					break
				}
			}
		}
		return picked
	}
	users := pick("steer", h.settings().SteeringMode)
	if final {
		users = append(users, pick("followUp", h.settings().FollowUpMode)...)
	}
	sort.Slice(users, func(i, j int) bool { return users[i].ID < users[j].ID })
	for _, item := range items {
		if item.Request.Type != "write" {
			continue
		}
		if err := placeWrite(tx, cid, item); err != nil {
			return boundaryResult{}, err
		}
		if result.first == 0 && item.Status == "done" {
			result.first = item.Entry
		}
	}
	for _, item := range users {
		entry, err := tx.Append(cid, inputEntry(item.Request))
		if err != nil {
			return boundaryResult{}, err
		}
		item.Status, item.Entry = "placed", entry
		result.users = append(result.users, item.ID)
		if result.first == 0 {
			result.first = entry
		}
	}
	return result, nil
}

func queueModeOf(item *SubmissionRecord) string { return queueMode(item.Request) }

// startRun starts a run for placed inputs whose first entry is first: a new
// generation takes pi.live.run. It removes runFailure unless keepFailure, as
// a run started in the commit that ended the failed one keeps its failure.
func startRun(tx *Tx, cid ConversationID, live *LiveState, inputs []SubmissionID, first EntryID, keepFailure bool) error {
	// A run's generation continues its first input's trace (or starts at the
	// root) and links the other inputs' traces, as upstream does.
	options := TaskOptions{TraceRoot: true}
	for index, sid := range inputs {
		sub := tx.state.Submissions[sid]
		if sub == nil || len(sub.Trace) == 0 {
			continue
		}
		if index == 0 {
			options.Trace, options.TraceRoot = sub.Trace, false
		} else {
			options.Links = append(options.Links, sub.Trace)
		}
	}
	id, err := tx.CreateTask(cid, generationTaskDefinition(), json.RawMessage(`{}`), options)
	if err != nil {
		return err
	}
	live.Run = &LiveRun{TaskID: id, Inputs: append([]SubmissionID{}, inputs...), First: first}
	if !keepFailure {
		live.RunFailure = nil
	}
	conv, err := tx.Conversation(cid)
	if err != nil {
		return err
	}
	conv.Busy = id
	return nil
}

// handOver hands run control from one turn's generation to the next.
func handOver(tx *Tx, cid ConversationID, live *LiveState, from, to TaskID) error {
	if live.Run != nil && live.Run.TaskID == from {
		live.Run.TaskID = to
	}
	conv, err := tx.Conversation(cid)
	if err != nil {
		return err
	}
	if conv.Busy == from {
		conv.Busy = to
	}
	return nil
}

// runEnd says how a run ends: answered, or unanswered for a reason with a
// detail. settlement, when set, replaces the inputs' default settlement
// (Reason reason, Detail the detail text as a JSON string).
type runEnd struct {
	done       bool
	answer     EntryID
	reason     string
	detail     string
	settlement *runSettlement
}

type runSettlement struct {
	reason string
	detail string // empty: no Detail
}

// endRun ends the run task owns: it settles each input, removes run and,
// without an answer, records runFailure. It always removes generation and
// tools, whose presentation belongs to the ending run.
func endRun(tx *Tx, cid ConversationID, live *LiveState, task TaskID, end runEnd) error {
	if live.Run != nil && live.Run.TaskID == task {
		inputs := live.Run.Inputs
		if end.done {
			for _, id := range inputs {
				settleSubmission(tx, id, "done", end.answer, "", "")
			}
		} else if live.Run.AfterRun == nil {
			// A run that answered already, and runs only its after-run tasks,
			// records no failure, however it ends.
			settlement := runSettlement{reason: end.reason, detail: end.detail}
			if end.settlement != nil {
				settlement = *end.settlement
			}
			for _, id := range inputs {
				settleSubmission(tx, id, "unanswered", 0, settlement.reason, settlement.detail)
			}
			live.RunFailure = &RunFailure{TaskID: task, Inputs: append([]SubmissionID{}, inputs...), Reason: end.reason, Detail: end.detail, First: live.Run.First}
		}
		live.Run = nil
		conv, err := tx.Conversation(cid)
		if err != nil {
			return err
		}
		if conv.Busy == task {
			conv.Busy = 0
		}
	}
	live.Generation = nil
	live.Tools = nil
	return nil
}

// settleSubmission settles a live submission; settled ones are unchanged.
func settleSubmission(tx *Tx, id SubmissionID, status string, answer EntryID, reason, detail string) {
	sub := tx.state.mutSubmission(id)
	if sub == nil || sub.Status == "done" || sub.Status == "unanswered" {
		return
	}
	sub.Status, sub.Answer, sub.Reason, sub.Detail = status, answer, reason, nil
	if detail != "" {
		sub.Detail, _ = json.Marshal(detail)
	}
}

// endFailedRun ends a failed run and applies Settings.AfterFailedRun to the
// inputs queued behind it: keep them, start the next run with them, or
// withdraw them with reason run_failed.
func (h *Harness) endFailedRun(tx *Tx, cid ConversationID, live *LiveState, task TaskID, end runEnd) error {
	if err := endRun(tx, cid, live, task, end); err != nil {
		return err
	}
	switch h.settings().AfterFailedRun {
	case AfterFailedRunWithdraw:
		withdrawQueuedInputs(tx, cid, "run_failed")
	case AfterFailedRunContinue:
		result, err := h.applyBoundary(tx, cid, true)
		if err != nil {
			return err
		}
		if len(result.users) > 0 {
			return startRun(tx, cid, live, result.users, result.first, true)
		}
	}
	return nil
}

// withdrawQueuedInputs settles every queued input unanswered with reason;
// queued writes stay for later placement.
func withdrawQueuedInputs(tx *Tx, cid ConversationID, reason string) {
	for _, item := range queuedSubmissions(tx, cid) {
		if item.Request.Type == "input" {
			item.Status, item.Reason = "unanswered", reason
		}
	}
}

// admitSubmission admits a submission inside a commit (Pi Durable §6). A
// known request ID returns its submission. A display-only write is appended
// at once unless a reset is queued. A busy conversation queues anything else
// or rejects WhenBusy "reject" input and any continue with ErrBusy; an idle
// one rejects WhenIdle "reject" input with ErrIdle. An idle continue places
// queued items and starts a run with them and itself. An idle conversation
// with queued items queues the submission behind them and runs a final
// boundary. Otherwise idle input is placed and starts a run, and an idle write
// is appended.
func (h *Harness) admitSubmission(tx *Tx, cid ConversationID, request SubmissionRequest) (SubmissionID, error) {
	rec, err := tx.Conversation(cid)
	if err != nil {
		return 0, err
	}
	if len(request.Images) > 0 && request.Type != "input" {
		return 0, fmt.Errorf("%w: only an input carries images", ErrInvalid)
	}
	for _, image := range request.Images {
		if image.MimeType == "" || image.Data == "" {
			return 0, fmt.Errorf("%w: an image needs a MIME type and data", ErrInvalid)
		}
	}
	if err := validImageOffsets(request.Content, request.Images, request.ImageOffsets); err != nil {
		return 0, err
	}
	if request.RequestID != "" {
		for _, existing := range tx.state.Submissions {
			if existing.Conversation == cid && existing.Request.RequestID == request.RequestID {
				if existing.Request.Type != request.Type {
					return 0, fmt.Errorf("%w: requestId reused for another type", ErrInvalid)
				}
				return existing.ID, nil
			}
		}
	}
	create := func(status string) *SubmissionRecord {
		id := SubmissionID(tx.id())
		sub := &SubmissionRecord{ID: id, Conversation: cid, Request: request, Status: status, CreatedAt: tx.clock()}
		if tx.trace != nil {
			sub.Trace = tx.trace()
		}
		tx.state.Submissions[id] = sub
		return sub
	}
	if isDisplayOnlyWrite(request) && !queuedResetExists(tx, cid) {
		sub := create("queued")
		return sub.ID, placeWrite(tx, cid, sub)
	}
	live, err := readLive(tx, cid)
	if err != nil {
		return 0, err
	}
	busy := rec.Busy != 0 || live.Run != nil
	if busy && request.Type == "input" && request.WhenBusy == "reject" {
		return 0, ErrBusy
	}
	if !busy && request.Type == "input" && request.WhenIdle == "reject" {
		return 0, ErrIdle
	}
	if request.Type == "continue" {
		// Never queued: whether a run can continue depends on how the current
		// one ends.
		if busy {
			return 0, ErrBusy
		}
		sub := create("placed")
		result, err := h.applyBoundary(tx, cid, true)
		if err != nil {
			return 0, err
		}
		first := result.first
		if live.RunFailure != nil && live.RunFailure.First != 0 {
			first = live.RunFailure.First
		}
		if err := startRun(tx, cid, live, append(result.users, sub.ID), first, false); err != nil {
			return 0, err
		}
		return sub.ID, writeLive(tx, cid, live)
	}
	if busy || len(queuedSubmissions(tx, cid)) > 0 {
		sub := create("queued")
		if busy {
			return sub.ID, nil
		}
		result, err := h.applyBoundary(tx, cid, true)
		if err != nil {
			return 0, err
		}
		if len(result.users) > 0 {
			if err := startRun(tx, cid, live, result.users, result.first, false); err != nil {
				return 0, err
			}
			return sub.ID, writeLive(tx, cid, live)
		}
		return sub.ID, nil
	}
	sub := create("queued")
	if request.Type == "write" {
		return sub.ID, placeWrite(tx, cid, sub)
	}
	entry, err := tx.Append(cid, inputEntry(request))
	if err != nil {
		return 0, err
	}
	sub.Status, sub.Entry = "placed", entry
	if err := startRun(tx, cid, live, []SubmissionID{sub.ID}, entry, false); err != nil {
		return 0, err
	}
	return sub.ID, writeLive(tx, cid, live)
}
