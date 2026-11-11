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
	return s.terminate(tx, t, Outcome{Status: OutcomeFaulted, Error: diagnostic(cause.Error())})
}

// terminate writes a scheduler outcome: it holds as completing while ordinary
// owned work is live (whose cancellation it starts), else settles terminal.
func (s *Scheduler) terminate(tx *Tx, t *TaskRecord, outcome Outcome) error {
	t.Outcome = &outcome
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
//
// Only an abort handler is faulted for the restart limit here (built-in run
// phases end themselves): the task was aborted and keeps that meaning. Its run
// ends aborted with the fault message as detail, without converting a partial
// whose conversion may be what keeps cutting the handler off, and an aborted
// compaction is no failure.
func (h *Harness) settleSchedulerOutcome(tx *Tx, task *TaskRecord) error {
	cutOff := task.Outcome.Status == OutcomeFaulted && detailReason(task.Outcome.ErrorDetail) == reasonRestartLimit
	switch task.Kind {
	case "pi.generation":
		live, err := readRunLive(tx, *task)
		if err != nil {
			return nil // malformed run state must not poison the Session
		}
		if live.Run == nil || live.Run.TaskID != task.ID {
			return nil
		}
		if !cutOff {
			if err := convertPartial(tx, *task); err != nil {
				return err
			}
		}
		end := runEnd{reason: RunFaulted, detail: task.Outcome.Error}
		switch {
		case cutOff:
			end.reason = RunAborted
		case task.Outcome.Status == OutcomeOrphaned:
			end = runEnd{reason: RunOrphaned, detail: task.Outcome.Reason, settlement: &runSettlement{reason: task.Outcome.Reason}}
		}
		// Failed runs keep queued work, rather than advancing it.
		if err := endRun(tx, task.Conversation, live, task.ID, end); err != nil {
			return err
		}
		return writeLive(tx, task.Conversation, live)
	case "pi.tool":
		live, err := readLive(tx, task.Conversation)
		if err != nil {
			return nil
		}
		if slot := live.toolSlot(task.ID); slot != nil {
			finishSlot(slot) // done without an entry; context synthesizes the result
			return writeLive(tx, task.Conversation, live)
		}
		for i := range live.NestedTools {
			if slot := &live.NestedTools[i]; slot.TaskID == task.ID {
				slot.Status, slot.Output, slot.Details = "done", "", nil
				return writeLive(tx, task.Conversation, live)
			}
		}
	case "pi.compaction":
		live, err := readLive(tx, task.Conversation)
		if err != nil {
			return nil
		}
		if cutOff {
			live.removeCompaction(task.ID)
		} else {
			message := task.Outcome.Error
			if task.Outcome.Status == OutcomeOrphaned {
				message = task.Outcome.Reason
			}
			live.failCompaction(task.ID, message)
		}
		return writeLive(tx, task.Conversation, live)
	}
	return nil
}
