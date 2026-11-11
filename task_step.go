package durable

import (
	"context"
	"encoding/json"
	"fmt"
)

func detachedTask(task *TaskRecord) (TaskRecord, error) {
	data, err := json.Marshal(task)
	if err != nil {
		return TaskRecord{}, err
	}
	var out TaskRecord
	err = json.Unmarshal(data, &out)
	return out, err
}

func (s *Scheduler) runInvocation(id TaskID, inv *taskInvocation, resolved resolvedTask, snapshot *RegistrySnapshot) {
	r := inv.runtime
	defer func() {
		// Close never writes outcomes. On an abort cancellation, release a run
		// back to pending before the fresh compensation invocation is reserved.
		if inv.ctx.Err() != nil && s.h.Err() == nil {
			_, _ = s.commitInternal(context.Background(), func(tx *Tx) error {
				t := tx.state.Tasks[id]
				if t == nil || t.Status != "running" {
					return errNoChange
				}
				r.ended.Store(true)
				t.Status = "pending"
				return nil
			})
		}
		s.finishInvocation(id, inv)
	}()
	var previousCheckpoint json.RawMessage
	var phaseErr error
	first := true
	outdated := make(map[uint64]bool)
	for {
		var record TaskRecord
		var reports []Report
		continuePhase := false
		committed, err := s.commitInternal(context.Background(), func(tx *Tx) error {
			t := tx.state.Tasks[id]
			end := func() error {
				r.ended.Store(true) // close the gate while still on the Session line
				return errNoChange
			}
			// Rules 1–3 also gate the first phase, after reservation.
			if t == nil || t.Status != "running" {
				return end()
			}
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing || s.h.Err() != nil {
				return end()
			}
			if !inv.aborting && (t.AbortRequested || belowCancellationIntent(tx.state, t)) {
				return end()
			}
			if !first {
				cause := phaseErr
				if cause == nil && inv.aborting {
					cause = fmt.Errorf("abort handler returned without a terminal outcome")
				}
				if cause == nil && jsonEqual(previousCheckpoint, t.Checkpoint) {
					cause = fmt.Errorf("task handler returned without durable progress")
				}
				if cause != nil {
					r.ended.Store(true)
					if err := s.fault(tx, t, cause); err != nil {
						return err
					}
					reports = append(reports, r.taskReport(ReportTaskFaulted, cause, "threw"))
					return nil
				}
				// Progress refreshes the phase snapshot even if the old definition
				// must continue because its replacement cannot take the task.
				snapshot = s.h.registry.Snapshot()
				next := s.h.resolve(t.Kind, snapshot)
				if next.Identity != resolved.Identity {
					if canReserve(t, next) {
						t.Status = "pending"
						r.ended.Store(true)
						return nil
					}
					if !outdated[next.Identity] {
						outdated[next.Identity] = true
						reason := outdatedReason(t, next)
						reports = append(reports, r.taskReport(ReportTaskOutdated, fmt.Errorf("task definition outdated: %s", reason), reason))
					}
				}
			}
			var err error
			record, err = detachedTask(t)
			if err != nil {
				return err
			}
			continuePhase = true
			return errNoChange // a read-only step is not a storage commit
		})
		// Fault reports require a successful durable write. Resolution reports
		// describe volatile decisions and need no write.
		if err == nil {
			for _, report := range reports {
				if report.Kind != ReportTaskFaulted || committed {
					s.h.report(report)
				}
			}
		}
		if err != nil || !continuePhase {
			return
		}
		r.snapshot.Store(snapshot)
		previousCheckpoint = append(json.RawMessage(nil), record.Checkpoint...)
		phaseErr = containPanic(func() error {
			if inv.aborting {
				if resolved.Definition.Abort == nil {
					return r.Complete(inv.ctx, Outcome{Status: OutcomeAborted})
				}
				return resolved.Definition.Abort(inv.ctx, r, record)
			}
			if resolved.Definition.Run == nil {
				return fmt.Errorf("task has no Run handler")
			}
			return resolved.Definition.Run(inv.ctx, r, record)
		})
		first = false
	}
}

func (s *Scheduler) fault(tx *Tx, t *TaskRecord, cause error) error {
	t.Outcome = &Outcome{Status: OutcomeFaulted, Error: diagnostic(cause.Error())}
	t.Memos = nil
	t.Restarts = 0
	markOwned(tx.state, t.ID, "")
	if len(ownedLive(tx.state, t.ID)) != 0 {
		t.Status = "completing"
		t.WaitOn, t.WaitPolicy, t.WakeAt = nil, "", nil
		return nil
	}
	return s.settleTerminal(tx, t)
}

func (s *Scheduler) settleTerminal(tx *Tx, t *TaskRecord) error {
	if t.Outcome != nil && (t.Outcome.Status == OutcomeFaulted || t.Outcome.Status == OutcomeOrphaned) {
		if err := s.h.settleSchedulerOutcome(tx, t); err != nil {
			return err
		}
	}
	terminalizeTask(tx, t, s.h.now())
	return nil
}

// Cleanup salvages independently decodable fields: an invalid built-in
// checkpoint caused the fault and must not turn that task fault into a Session
// failure. Structural transaction/storage errors still fail closed.
func (h *Harness) settleSchedulerOutcome(tx *Tx, task *TaskRecord) error {
	switch task.Kind {
	case "pi.generation":
		conv, err := tx.Conversation(task.Conversation)
		if err != nil {
			return err
		}
		if conv.Busy != task.ID {
			return nil
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(task.Checkpoint, &fields)
		var partial string
		_ = json.Unmarshal(fields["partial"], &partial)
		if partial != "" {
			if _, err := tx.Append(task.Conversation, Entry{Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: partial, Aborted: true}}); err != nil {
				return err
			}
		}
		var input generationInput
		_ = json.Unmarshal(task.Input, &input)
		for _, id := range input.Submissions {
			if sub := tx.state.Submissions[id]; sub != nil && sub.Status == "running" {
				sub.Status = "unanswered"
				sub.Answer = 0
				sub.Reason = task.Outcome.Reason
				sub.Detail = nil
				if task.Outcome.Status == OutcomeFaulted {
					sub.Reason = "faulted"
					sub.Detail, _ = json.Marshal(task.Outcome.Error)
				}
			}
		}
		conv.Busy = 0 // failed runs keep queued work, rather than advancing it
	case "pi.tool":
		if doc := tx.state.Documents[documentKey(liveDocument, uint64(task.Conversation))]; doc != nil {
			var live map[string]any
			if json.Unmarshal(doc.Value, &live) != nil || live == nil {
				return nil // malformed task diagnostics must not poison the Session
			}
			if tools, ok := live["tools"].(map[string]any); ok {
				delete(tools, fmt.Sprint(task.ID))
			}
			data, err := json.Marshal(live)
			if err != nil {
				return err
			}
			return tx.PutDocument(liveDocument, uint64(task.Conversation), data)
		}
	}
	return nil
}
