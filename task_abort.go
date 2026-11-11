package durable

import "context"

// markAbort runs on the Session line. The caller enqueues staged reports after
// leaving it. A migration probe here is pure and never persists migrated values.
func (s *Scheduler) markAbort(tx *Tx, task *TaskRecord, keepRestart bool, reports *[]Report) (AbortResult, *taskInvocation, error) {
	if task.Status == "terminal" {
		return AbortTerminal, nil, nil
	}
	inv := s.active(task.ID)
	if keepRestart && task.AbortRequested && task.AbortReason == "restart" {
		markAbortTreeReason(tx.state, task.ID, "restart")
		return AbortMarked, inv, nil
	}
	if inv == nil && task.Status != "completing" && len(ownedLive(tx.state, task.ID)) == 0 {
		_, _, blocked := s.resolveForStart(task, s.h.registry.Snapshot(), reports)
		if blocked != "" {
			markAbortTree(tx.state, task.ID)
			task.Outcome = &Outcome{Status: OutcomeOrphaned, Reason: blocked}
			return AbortMarked, nil, s.settleTerminal(tx, task)
		}
	}
	markAbortTree(tx.state, task.ID)
	return AbortMarked, inv, nil
}

func (s *Scheduler) abort(ctx context.Context, id TaskID) (AbortResult, error) {
	var result AbortResult
	var inv *taskInvocation
	var reports []Report
	var decisionErr error
	_, err := s.commitInternal(ctx, func(tx *Tx) error {
		task, err := tx.Task(id)
		if err != nil {
			decisionErr = err
			return errNoChange // invalid request, not failed bookkeeping
		}
		result, inv, err = s.markAbort(tx, task, false, &reports)
		if err == nil && result == AbortTerminal {
			return errNoChange
		}
		return err
	})
	s.enqueueReports(reports)
	if err == nil {
		err = decisionErr
	}
	if err != nil {
		return "", err
	}
	// Scheduling is enabled only after the durable mark (or orphan receipt).
	if inv != nil && !inv.aborting {
		inv.cancel()
	}
	s.resume()
	s.kick()
	if inv != nil && !inv.aborting {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-s.h.session.closing:
			return "", s.h.Err()
		case <-inv.done:
		}
	}
	if err := s.h.Err(); err != nil {
		return "", err
	}
	return result, nil
}

func (s *Scheduler) wait(ctx context.Context, id TaskID) (TaskRecord, error) {
	states, stop, err := s.h.session.watchWait(ctx)
	if err != nil {
		return TaskRecord{}, err
	}
	defer stop()
	s.resume()
	for {
		select {
		case <-ctx.Done():
			return TaskRecord{}, ctx.Err()
		case <-s.h.session.closing:
			return TaskRecord{}, s.h.Err()
		case state, ok := <-states:
			if !ok {
				if ctx.Err() != nil {
					return TaskRecord{}, ctx.Err()
				}
				return TaskRecord{}, s.h.Err()
			}
			if err := s.h.Err(); err != nil {
				return TaskRecord{}, err
			}
			t := state.Tasks[id]
			if t == nil {
				return TaskRecord{}, ErrNotFound
			}
			if t.Status == "terminal" {
				return *t, nil
			}
		}
	}
}
