package durable

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Harness owns one Session, its extension registry, and durable task scheduling.
type Harness struct {
	session      *Session
	registry     *Registry
	options      HarnessOptions
	scheduler    *Scheduler
	stopRegistry func()
	reports      *reportQueue
	activeMu     sync.Mutex
	active       map[string]int // pi.durable.tasks.active per kind, as reported
}

func Open(ctx context.Context, storage Storage, options HarnessOptions) (*Harness, error) {
	if storage == nil {
		return nil, fmt.Errorf("%w: storage is required", ErrInvalid)
	}
	if options.Registry == nil {
		options.Registry = NewRegistry()
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	session, err := NewSession(ctx, storage, options.Now)
	if err != nil {
		return nil, err
	}
	h := &Harness{session: session, registry: options.Registry, options: options, reports: newReportQueue(options.OnReport)}
	h.scheduler = newScheduler(h)
	h.stopRegistry = h.registry.Subscribe(h.scheduler.kick)
	session.failureHook = func(cause error) { h.report(Report{Kind: ReportStorageFailed, Err: cause}) }
	session.afterClose = h.reports.closeAdmission
	session.afterCommit = func(state *State) {
		h.scheduler.clearSettledMigrations(state)
		h.countActive(state)
		// Every adopted commit may make work runnable or change the wake.
		h.scheduler.kick()
	}
	session.beforeClose = func() {
		h.scheduler.close()
		<-h.scheduler.done
		h.scheduler.writeBackStopped()
		h.stopRegistry()
		h.countActive(nil) // a closed Harness holds no tasks
	}
	if options.ReadOnly {
		session.readOnly = true
		return h, nil // recovers nothing, schedules nothing
	}
	go h.scheduler.wakeLoop()
	if err = h.reconcile(ctx); err != nil {
		// Cleanup continues after a cancelled Open without forcing that caller
		// to join a backend that ignores cancellation.
		_ = session.closeContext(ctx)
		return nil, err
	}
	if h.metrics() != nil {
		if state, err := h.Snapshot(ctx); err == nil {
			h.countActive(state)
		}
	}
	h.scheduler.kick() // offer the first Wake after open
	return h, nil
}
func (h *Harness) Commit(ctx context.Context, change func(*Tx) error) error {
	return h.commit(ctx, change, false)
}
func (h *Harness) commit(ctx context.Context, change func(*Tx) error, internal bool) error {
	if h == nil || h.session == nil {
		return ErrClosed
	}
	sp := spanOf(ctx)
	var appended []map[string]any
	err := h.session.commit(ctx, func(tx *Tx) error {
		tx.created = func(tx *Tx, id ConversationID) error {
			if h.options.ConversationCreated != nil {
				return h.options.ConversationCreated(tx, id)
			}
			return nil
		}
		if change != nil {
			if err := change(tx); err != nil {
				return err
			}
		}
		if sp != nil {
			appended = appended[:0]
			for _, e := range tx.state.Entries {
				if e.Sequence == tx.state.Sequence+1 {
					appended = append(appended, map[string]any{"entry.id": uint64(e.ID), "entry.kind": e.Kind})
				}
			}
		}
		// Derived in the same commit: pi.inbox mirrors the queued submissions.
		if h.options.ReadOnly {
			return nil
		}
		return syncInboxes(tx)
	}, internal)
	if err == nil {
		sort.Slice(appended, func(i, j int) bool { return appended[i]["entry.id"].(uint64) < appended[j]["entry.id"].(uint64) })
		for _, attributes := range appended {
			sp.event("pi.durable.entry", attributes)
		}
	}
	return err
}
func (h *Harness) now() time.Time { return h.options.Now().UTC() }

// settings reads the Harness settings, calling SettingsFunc at each use.
func (h *Harness) settings() Settings {
	if h.options.SettingsFunc != nil {
		return h.options.SettingsFunc()
	}
	return h.options.Settings
}

func (h *Harness) Snapshot(ctx context.Context) (*State, error) { return h.session.Snapshot(ctx) }

// Resume enables task scheduling and starts a pass. A read-only Harness
// never schedules.
func (h *Harness) Resume() {
	if h == nil || h.options.ReadOnly {
		return
	}
	h.scheduler.resume()
}
func (h *Harness) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if h == nil {
		return nil
	}
	return h.session.closeContext(ctx)
}

// AbortTask marks before enabling scheduling and joins only the captured run,
// not its compensation. Caller cancellation stops the join, never the mark.
// A handler must not synchronously abort itself or a scope containing itself.
func (h *Harness) AbortTask(ctx context.Context, id TaskID) (AbortResult, error) {
	return h.scheduler.abort(ctx, id)
}
func (h *Harness) WaitTask(ctx context.Context, id TaskID) (TaskRecord, error) {
	h.Resume()
	return h.scheduler.wait(ctx, id)
}
func (h *Harness) Task(ctx context.Context, id TaskID) (TaskRecord, error) {
	s, e := h.Snapshot(ctx)
	if e != nil {
		return TaskRecord{}, e
	}
	r := s.Tasks[id]
	if r == nil {
		return TaskRecord{}, ErrNotFound
	}
	return *r, nil
}
