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
	mu           sync.Mutex
	closed       bool // admission sealed, even if Close's wait times out
	stopRegistry func()
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
	if err := options.Registry.Install(CompactionExtension()); err != nil {
		return nil, err
	}
	session, err := NewSession(ctx, storage, options.Now)
	if err != nil {
		return nil, err
	}
	h := &Harness{session: session, registry: options.Registry, options: options}
	h.scheduler = newScheduler(h)
	h.stopRegistry = h.registry.Subscribe(h.scheduler.kick)
	if err = h.reconcile(ctx); err != nil {
		h.stopRegistry()
		_ = session.Close()
		return nil, err
	}
	return h, nil
}
func (h *Harness) Commit(ctx context.Context, change func(*Tx) error) error {
	if h == nil || h.session == nil {
		return ErrClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	return h.session.Commit(ctx, func(tx *Tx) error {
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
	})
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
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	h.scheduler.close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.scheduler.done:
	}
	h.stopRegistry()
	return h.session.Close()
}
func (h *Harness) AbortTask(ctx context.Context, id TaskID) error {
	h.Resume()
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
func (h *Harness) Inspect(ctx context.Context) (HarnessInspection, error) {
	s, err := h.Snapshot(ctx)
	if err != nil {
		return HarnessInspection{}, err
	}
	out := HarnessInspection{Scheduling: h.scheduler.state()}
	for _, task := range s.Tasks {
		if task.Status == "terminal" {
			continue
		}
		d, ok := h.registry.resolve(task.Kind)
		item := TaskInspection{Record: *task}
		if !ok {
			item.Blocked = "missing_task"
		} else if task.Version > d.Version {
			item.Blocked = "host_definition_too_old"
		} else if task.Version < d.Version && d.Migrate == nil {
			item.Blocked = "missing_migration"
		} else {
			item.Ready = true
		}
		out.Tasks = append(out.Tasks, item)
	}
	sort.Slice(out.Tasks, func(i, j int) bool { return out.Tasks[i].Record.ID < out.Tasks[j].Record.ID })
	return out, nil
}
