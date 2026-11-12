package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime/debug"
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
	session.observerPanic = func(recovered any) {
		h.report(Report{Kind: ReportListenerError, Err: &panicError{value: recovered, stack: debug.Stack()}})
	}
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
		if state, err := h.read(ctx); err == nil {
			h.countActive(state)
		}
	}
	h.scheduler.kick() // offer the first Wake after open
	return h, nil
}
func (h *Harness) Commit(ctx context.Context, change func(*Tx) error) error {
	return h.commit(ctx, change, false)
}

// CommitWith is Commit with persistence options: a deferred commit of
// conversation documents is shown at once and stored within its window
// (CommitOptions). It returns once the commit is adopted: stored, or held in
// memory.
func (h *Harness) CommitWith(ctx context.Context, options CommitOptions, change func(*Tx) error) error {
	return h.commitWith(ctx, change, false, options)
}

// progressCommit is the persistence of progress commits: partial answers and
// running tool output (upstream PROGRESS_COMMIT).
func progressCommit() CommitOptions { return CommitOptions{Deferred: true} }

func (h *Harness) commit(ctx context.Context, change func(*Tx) error, internal bool) error {
	return h.commitWith(ctx, change, internal, CommitOptions{})
}

func (h *Harness) commitWith(ctx context.Context, change func(*Tx) error, internal bool, options CommitOptions) error {
	if h == nil || h.session == nil {
		return ErrClosed
	}
	within := 0.0
	if options.Deferred {
		if ms := options.WithinMs; ms != nil && (*ms < 0 || math.IsNaN(*ms)) {
			return fmt.Errorf("%w: CommitOptions.WithinMs must be a number of milliseconds, 0 or more", ErrInvalid)
		}
		within = 1000
		if ms := h.settings().Progress.PersistWithinMs; ms != nil && *ms >= 0 && !math.IsNaN(*ms) {
			within = *ms
		}
	}
	sp := spanOf(ctx)
	var appended, settled []map[string]any
	err := h.session.commitWith(ctx, func(tx *Tx) error {
		// Submissions settled by this commit: open before it, or new in it.
		var settledBefore map[SubmissionID]bool
		if sp != nil {
			settledBefore = map[SubmissionID]bool{}
			for id, sub := range tx.state.Submissions {
				if sub.Status == "done" || sub.Status == "unanswered" {
					settledBefore[id] = true
				}
			}
		}
		if propagation := h.propagation(); propagation != nil {
			tx.trace = func() json.RawMessage {
				var carrier json.RawMessage
				h.telemetryCall(func() { carrier = propagation.Inject(ctx) })
				return carrier
			}
		}
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
			settled = settled[:0]
			for id, sub := range tx.state.Submissions {
				if !settledBefore[id] && (sub.Status == "done" || sub.Status == "unanswered") {
					attributes := map[string]any{"submission.id": uint64(id), "status": sub.Status}
					if sub.Answer != 0 {
						attributes["answer.entry_id"] = uint64(sub.Answer)
					}
					settled = append(settled, attributes)
				}
			}
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
	}, internal, options, within)
	if err == nil {
		sort.Slice(appended, func(i, j int) bool { return appended[i]["entry.id"].(uint64) < appended[j]["entry.id"].(uint64) })
		for _, attributes := range appended {
			sp.event("pi.durable.entry", attributes)
		}
		sort.Slice(settled, func(i, j int) bool {
			return settled[i]["submission.id"].(uint64) < settled[j]["submission.id"].(uint64)
		})
		for _, attributes := range settled {
			sp.event("pi.durable.submission", attributes)
		}
	}
	return err
}
func (h *Harness) now() time.Time { return h.options.Now().UTC() }

// model is the Harness model, calling ModelFunc at each use.
func (h *Harness) model() Model {
	if h.options.ModelFunc != nil {
		return h.options.ModelFunc()
	}
	return h.options.Model
}

// settings reads the Harness settings, calling SettingsFunc at each use.
func (h *Harness) settings() Settings {
	if h.options.SettingsFunc != nil {
		return h.options.SettingsFunc()
	}
	return h.options.Settings
}

func (h *Harness) Snapshot(ctx context.Context) (*State, error) { return h.session.Snapshot(ctx) }

// generate sends a model request with the Harness identity.
func (h *Harness) generate(ctx context.Context, req ModelRequest, onDelta func(string) error) (ModelResponse, error) {
	return h.model().Generate(ctx, h.sendable(req), onDelta)
}

// generateEvents streams a request's events: an EventModel's own, or a
// Model's text deltas as text_delta events.
func (h *Harness) generateEvents(ctx context.Context, req ModelRequest, onEvent func(StreamEvent) error) (ModelResponse, error) {
	model := h.model()
	if events, ok := model.(EventModel); ok {
		return events.GenerateEvents(ctx, h.sendable(req), onEvent)
	}
	return model.Generate(ctx, h.sendable(req), func(text string) error {
		return onEvent(StreamEvent{Type: "text_delta", Delta: text})
	})
}

// sendable attaches what a request carries at send time: the identity and
// the stream settings.
func (h *Harness) sendable(req ModelRequest) ModelRequest {
	if req.Identity == nil {
		req.Identity = h.options.Identity
	}
	if stream := h.settings().Stream; req.Stream == nil && !stream.isZero() {
		req.Stream = &stream
	}
	return req
}

// harnessModel is the Harness model as tools see it: requests carry the
// Harness identity.
type harnessModel struct{ h *Harness }

func (m harnessModel) Generate(ctx context.Context, req ModelRequest, onDelta func(string) error) (ModelResponse, error) {
	return m.h.generate(ctx, req, onDelta)
}

// GenerateEvents streams events, as EventModel does.
func (m harnessModel) GenerateEvents(ctx context.Context, req ModelRequest, onEvent func(StreamEvent) error) (ModelResponse, error) {
	return m.h.generateEvents(ctx, req, onEvent)
}

// read returns the committed State for internal reading. It is shared with
// the Session and every other reader: never mutate it or its records. Public
// callers get Snapshot's detached copy.
func (h *Harness) read(ctx context.Context) (*State, error) { return h.session.view(ctx) }

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
	s, e := h.read(ctx)
	if e != nil {
		return TaskRecord{}, e
	}
	r := s.Tasks[id]
	if r == nil {
		return TaskRecord{}, ErrNotFound
	}
	return *cloneRecord(r), nil
}

// ObserveCommits calls observe with the changes of each commit (StateDelta),
// in commit order, after the commit is stored and before watches see it: a
// replica that applies them stays equal to storage. observe runs on the
// Session line: it must return promptly and must not call Session or
// Harness APIs. Each delta is its own detached copy. A panic is reported as
// listener_error and the observer stays attached. detach stops it.
func (h *Harness) ObserveCommits(observe func(*StateDelta)) (detach func()) {
	return h.session.observeCommits(observe)
}
