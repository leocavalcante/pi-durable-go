package durable

import (
	"context"
	"fmt"
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
		// Every adopted commit may make work runnable or change the wake.
		h.scheduler.kick()
	}
	session.beforeClose = func() {
		h.scheduler.close()
		<-h.scheduler.done
		h.scheduler.writeBackStopped()
		h.stopRegistry()
	}
	go h.scheduler.wakeLoop()
	if err = h.reconcile(ctx); err != nil {
		// Cleanup continues after a cancelled Open without forcing that caller
		// to join a backend that ignores cancellation.
		_ = session.closeContext(ctx)
		return nil, err
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
	return h.session.commit(ctx, func(tx *Tx) error {
		tx.created = func(tx *Tx, id ConversationID) error {
			if h.options.ConversationCreated != nil {
				return h.options.ConversationCreated(tx, id)
			}
			return nil
		}
		if change == nil {
			return nil
		}
		return change(tx)
	}, internal)
}
func (h *Harness) now() time.Time { return h.options.Now().UTC() }

func (h *Harness) Snapshot(ctx context.Context) (*State, error) { return h.session.Snapshot(ctx) }
func (h *Harness) Resume() {
	if h == nil {
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
