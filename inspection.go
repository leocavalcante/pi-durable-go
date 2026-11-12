package durable

import (
	"context"
	"fmt"
	"sort"
	"time"
)

type TaskInspection struct {
	Record TaskRecord
	State  TaskInspectionState
}
type TaskInspectionState struct {
	Kind     string
	Migrates bool
	On       []TaskID
	Until    *time.Time
	Reason   string
	Err      error
}
type HarnessInspection struct {
	Scheduling  string
	Tasks       []TaskInspection
	Submissions []SubmissionRecord
}

// Inspect captures records, invocation state, cached migration failures, and one
// registry snapshot on the Session line. It never migrates, writes, or reports.
func (h *Harness) Inspect(ctx context.Context) (HarnessInspection, error) {
	if err := h.Err(); err != nil {
		return HarnessInspection{}, err
	}
	h.session.mu.Lock()
	defer h.session.mu.Unlock()
	if err := h.Err(); err != nil {
		return HarnessInspection{}, err
	}
	if err := ctx.Err(); err != nil {
		return HarnessInspection{}, err
	}
	// The committed State is read in place; only returned records are copied.
	return h.inspectLocked(h.session.state), nil
}

// inspectLocked is Inspect's capture of a State; the caller holds the
// Session line.
func (h *Harness) inspectLocked(state *State) HarnessInspection {
	snapshot := h.registry.Snapshot()
	h.scheduler.mu.Lock()
	out := HarnessInspection{Scheduling: h.scheduler.stateLocked()}
	active := make(map[TaskID]bool, len(h.scheduler.invocations))
	for id := range h.scheduler.invocations {
		active[id] = true
	}
	failures := make(map[TaskID]migrationFailure, len(h.scheduler.migrations))
	for id, failure := range h.scheduler.migrations {
		failures[id] = failure
	}
	h.scheduler.mu.Unlock()
	if h.Err() != nil {
		out.Scheduling = "closing"
	}
	now := h.now()
	for _, id := range sortedTaskIDs(state) {
		task := state.Tasks[id]
		if task.Status == "terminal" {
			continue
		}
		item := TaskInspection{Record: *cloneRecord(task)}
		live := liveWaits(state, task)
		switch {
		case active[id]:
			item.State.Kind = "running"
		case task.Status == "completing":
			item.State.Kind = "completing"
		case len(live) > 0:
			item.State = TaskInspectionState{Kind: "waiting", On: live}
		case !task.AbortRequested && task.WakeAt != nil && now.Before(*task.WakeAt):
			until := *task.WakeAt
			item.State = TaskInspectionState{Kind: "sleeping", Until: &until}
		case task.AbortRequested && task.AbortReason != "restart":
			item.State.Kind = "pending"
		default:
			resolved := h.resolve(task.Kind, snapshot)
			decision := fit(task, resolved, failures[id])
			if decision.reason == "" && decision.migrates && resolved.Definition.Migrate == nil {
				decision.reason = "migration_failed"
				decision.err = fmt.Errorf("task %s has no migration from %d", task.Kind, task.Version)
			}
			if decision.reason != "" {
				item.State = TaskInspectionState{Kind: "blocked", Reason: decision.reason, Err: decision.err}
			} else {
				item.State = TaskInspectionState{Kind: "pending", Migrates: decision.migrates}
			}
		}
		out.Tasks = append(out.Tasks, item)
	}
	for _, sub := range state.Submissions {
		if sub.Status == "queued" || sub.Status == "placed" {
			out.Submissions = append(out.Submissions, *cloneRecord(sub))
		}
	}
	sort.Slice(out.Submissions, func(i, j int) bool { return out.Submissions[i].ID < out.Submissions[j].ID })
	return out
}
