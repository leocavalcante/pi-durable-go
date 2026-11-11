package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Session serializes transactions over one Storage. State is never exposed without detaching it.
type Session struct {
	mu        sync.Mutex
	storage   Storage
	state     *State
	now       func() time.Time
	watchers  map[*stateWatcher]struct{}
	failure   atomic.Pointer[SessionFailedError]
	sealOnce  sync.Once
	closeOnce sync.Once
	closing   chan struct{}
	closeDone chan struct{}
	closeErr  error
	end       SessionEnd
	// Installed before the Harness can reconcile. failureHook only enqueues a
	// report; afterClose shuts report admission after Storage.Close settles.
	failureHook func(error)
	afterClose  func()
	// Internal Harness bookkeeping only, after durable adoption and before
	// publication. This hook must never call user code or Session APIs.
	afterCommit func(*State)
	// Installed before the Harness can schedule; called off the commit line.
	beforeClose func()
	// readOnly rejects every commit with writes with ErrReadOnly.
	readOnly bool
	// committedJSON is the adopted State's JSON while verifyCommittedState.
	committedJSON []byte
}

type stateWatcher struct {
	ch     chan *State
	done   chan struct{}
	closed bool
}

func NewSession(ctx context.Context, storage Storage, now func() time.Time) (*Session, error) {
	if storage == nil {
		return nil, fmt.Errorf("durable: nil storage")
	}
	if now == nil {
		now = time.Now
	}
	st, err := storage.Load(ctx)
	if err != nil {
		return nil, err
	}
	if st == nil {
		st = &State{Version: stateVersion, NextID: 1, Conversations: map[ConversationID]*ConversationRecord{}, Entries: map[EntryID]*Entry{}, Tasks: map[TaskID]*TaskRecord{}, Submissions: map[SubmissionID]*SubmissionRecord{}, Documents: map[string]*DocumentRecord{}}
	}
	if st.Version < 1 || st.Version > stateVersion || st.NextID == 0 {
		return nil, fmt.Errorf("%w: unsupported storage version or invalid ID allocator", ErrInvalid)
	}
	st, err = cloneState(st)
	if err != nil {
		return nil, fmt.Errorf("durable: invalid stored state: %w", err)
	}
	// Upgrade only the detached in-memory snapshot. The next successful commit
	// persists it; merely opening an older store does not write to it.
	if err := upgradeState(st); err != nil {
		return nil, fmt.Errorf("durable: cannot upgrade stored state: %w", err)
	}
	s := &Session{storage: storage, state: st, now: now, watchers: map[*stateWatcher]struct{}{}, closing: make(chan struct{}), closeDone: make(chan struct{})}
	s.recordCommitted()
	return s, nil
}

func (s *Session) Commit(ctx context.Context, fn func(*Tx) error) error {
	return s.commit(ctx, fn, false)
}

// Internal bookkeeping errors seal admission on the line, before another
// caller can publish a write. Public caller rejections retain rollback semantics.
func (s *Session) commit(ctx context.Context, fn func(*Tx) error, internal bool) (err error) {
	if err := s.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.commitLocked(ctx, fn, internal, true)
}

// writeAtClose is private shutdown bookkeeping, called after invocations join.
// It follows admitted commits on the line, bypasses the seal, and publishes
// nothing. A failed Session never writes; any real error fails a healthy one.
func (s *Session) writeAtClose(fn func(*Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure.Load() != nil {
		return nil
	}
	noChange := false
	err := s.commitLocked(context.Background(), func(tx *Tx) error {
		err := fn(tx)
		noChange = errors.Is(err, errNoChange)
		return err
	}, true, false)
	if noChange {
		return nil
	}
	return err
}

// The caller holds the line and has checked admission (or is writeAtClose).
// Only callback-returned errNoChange suppresses an internal bookkeeping write;
// an identical error from storage must still fail the Session.
func (s *Session) commitLocked(ctx context.Context, fn func(*Tx) error, internal, publish bool) (err error) {
	callbackNoChange := false
	defer func() {
		if !internal || err == nil || callbackNoChange || s.failure.Load() != nil {
			return
		}
		// Uncertain storage errors are failed below even when they wrap a
		// cancellation. Remaining cancellation errors are pre-storage or a
		// certified-unapplied write for this cancelled call.
		if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return
		}
		s.failLocked(err)
	}()
	s.verifyCommitted("before a commit")
	candidate := shallowState(s.state)
	tx := &Tx{state: candidate, active: true, now: s.now}
	func() {
		defer func() { tx.active = false }()
		if fn != nil {
			err = fn(tx)
			callbackNoChange = internal && errors.Is(err, errNoChange)
		}
	}()
	if err != nil {
		return err
	}
	s.verifyCommitted("during a commit")
	// Strict JSON round trips of the records the callback added or changed
	// validate them and detach every value the callback retained.
	changed, err := detachChanges(s.state, candidate, s.readOnly)
	if err != nil {
		return err
	}
	if s.readOnly {
		// A commit that writes nothing passes; one with writes is refused
		// before it reaches storage, and nothing is adopted.
		if changed {
			return fmt.Errorf("%w: a commit with writes refused", ErrReadOnly)
		}
		return nil
	}
	if s.state.Sequence == ^uint64(0) {
		return fmt.Errorf("%w: sequence space exhausted", ErrInvalid)
	}
	candidate.Sequence = s.state.Sequence + 1
	candidate.base = nil
	if err = ctx.Err(); err != nil {
		return err
	}
	err = s.storage.Commit(ctx, s.state.Sequence, candidate)
	if err != nil {
		var rejected *RejectedError
		if !errors.As(err, &rejected) {
			s.failLocked(err)
		}
		return err
	}
	s.state = candidate
	s.recordCommitted()
	if s.afterCommit != nil {
		s.afterCommit(candidate)
	}
	if publish {
		for w := range s.watchers {
			s.offer(w, candidate)
		}
	}
	return nil
}

func (s *Session) Snapshot(ctx context.Context) (*State, error) {
	if err := s.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.verifyCommitted("a snapshot")
	return cloneState(s.state)
}

// view returns the committed State for internal reading (see Harness.read):
// shared and immutable, never to be mutated.
func (s *Session) view(ctx context.Context) (*State, error) {
	if err := s.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.verifyCommitted("a read")
	return s.state, nil
}

// verifyCommittedState makes the Session check, in tests, that its committed
// State never changes outside an adopted commit: copy-on-write transactions
// and shared internal reads must leave it untouched (plan 0006).
var verifyCommittedState bool

// recordCommitted notes the adopted State for verifyCommitted.
func (s *Session) recordCommitted() {
	if !verifyCommittedState {
		return
	}
	data, err := json.Marshal(s.state)
	if err != nil {
		panic(fmt.Sprintf("durable: committed state is not JSON: %v", err))
	}
	s.committedJSON = data
}

// verifyCommitted panics when the committed State changed since it was
// adopted, naming the first record that differs. The caller holds s.mu.
func (s *Session) verifyCommitted(where string) {
	if !verifyCommittedState || s.committedJSON == nil {
		return
	}
	data, err := json.Marshal(s.state)
	if err != nil {
		panic(fmt.Sprintf("durable: committed state is not JSON: %v", err))
	}
	if bytes.Equal(data, s.committedJSON) {
		return
	}
	panic(fmt.Sprintf("durable: the committed state was mutated outside a commit (found %s): %s", where, firstDifference(s.committedJSON, data)))
}

// firstDifference names the first table and key whose JSON differs.
func firstDifference(before, after []byte) string {
	var a, b map[string]json.RawMessage
	_ = json.Unmarshal(before, &a)
	_ = json.Unmarshal(after, &b)
	for field, value := range b {
		if bytes.Equal(a[field], value) {
			continue
		}
		var ta, tb map[string]json.RawMessage
		if json.Unmarshal(a[field], &ta) != nil || json.Unmarshal(value, &tb) != nil {
			return field
		}
		for key, record := range tb {
			if !bytes.Equal(ta[key], record) {
				return fmt.Sprintf("%s[%s]: %s -> %s", field, key, ta[key], record)
			}
		}
		return field
	}
	return "unknown"
}

func (s *Session) Watch(ctx context.Context) (<-chan *State, func(), error) {
	if err := s.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	if err := s.Err(); err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	initial, err := cloneState(s.state)
	if err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	w := &stateWatcher{ch: make(chan *State, 1), done: make(chan struct{})}
	w.ch <- initial
	s.watchers[w] = struct{}{}
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !w.closed {
				w.closed = true
				delete(s.watchers, w)
				close(w.ch)
				close(w.done)
			}
		})
	}
	s.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-w.done:
		}
	}()
	return w.ch, cancel, nil
}

// watchWait keeps wait cancellation/sealing independent of an admitted storage
// call holding the line. The registration goroutine and the watch's own context
// cleanup finish when that call settles; cancelling a wait never joins them.
func (s *Session) watchWait(ctx context.Context) (<-chan *State, func(), error) {
	bound, cancel := context.WithCancel(ctx)
	type registration struct {
		states <-chan *State
		err    error
	}
	ready := make(chan registration, 1)
	go func() {
		states, _, err := s.Watch(bound)
		ready <- registration{states: states, err: err}
	}()
	select {
	case <-ctx.Done():
		cancel()
		return nil, nil, ctx.Err()
	case <-s.closing:
		cancel()
		return nil, nil, s.Err()
	case result := <-ready:
		if result.err != nil {
			cancel()
			return nil, nil, result.err
		}
		return result.states, cancel, nil
	}
}

func (s *Session) offer(w *stateWatcher, state *State) {
	v, err := cloneState(state)
	if err != nil {
		return
	}
	select {
	case w.ch <- v:
		return
	default:
	}
	select {
	case <-w.ch:
	default:
	}
	select {
	case w.ch <- v:
	default:
	}
}

func cloneState(s *State) (*State, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("durable: state is not strict JSON: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var out State
	if err = dec.Decode(&out); err != nil {
		return nil, err
	}
	if err = dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("durable: trailing JSON data")
	}
	if out.Conversations == nil {
		out.Conversations = map[ConversationID]*ConversationRecord{}
	}
	if out.Entries == nil {
		out.Entries = map[EntryID]*Entry{}
	}
	if out.Tasks == nil {
		out.Tasks = map[TaskID]*TaskRecord{}
	}
	if out.Submissions == nil {
		out.Submissions = map[SubmissionID]*SubmissionRecord{}
	}
	if out.Documents == nil {
		out.Documents = map[string]*DocumentRecord{}
	}
	return &out, nil
}

// stateVersion is Go's storage format version. Version 2 added outcome and
// submission diagnostics; version 3 is upstream's run model: placed
// submissions with their entry, and run control in pi.live.
const stateVersion = 3

// upgradeState upgrades a detached State to stateVersion in memory.
func upgradeState(st *State) error {
	if st.Version == 1 {
		st.Version = 2 // version 2 only added optional fields
	}
	if st.Version == 2 {
		for _, sub := range st.Submissions {
			if sub.Status == "running" {
				sub.Status = "placed"
			}
			if sub.Request.WhenBusy == "follow-up" {
				sub.Request.WhenBusy = "followUp"
			}
			if sub.Request.Type == "write" && sub.Entry == 0 && sub.Answer != 0 {
				sub.Entry, sub.Answer = sub.Answer, 0
			}
		}
		for _, doc := range st.Documents {
			if doc.Kind == liveDocument.Kind && doc.Version == 1 {
				value, err := liveFromVersion1(doc.Value)
				if err != nil {
					return err
				}
				doc.Value, doc.Version = value, liveDocument.Version
			}
		}
		// A version-2 run lived in one generation, whose input listed it.
		for id, conv := range st.Conversations {
			if conv.Busy == 0 || st.Tasks[conv.Busy] == nil {
				continue
			}
			var input generationInput
			_ = json.Unmarshal(st.Tasks[conv.Busy].Input, &input)
			key := documentKey(liveDocument, uint64(id))
			doc := st.Documents[key]
			if doc == nil {
				doc = &DocumentRecord{Kind: liveDocument.Kind, Version: liveDocument.Version, Scope: liveDocument.Scope, ScopeID: uint64(id),
					History: normalizeHistory(liveDocument.History), Fork: normalizeFork(liveDocument.Scope, liveDocument.Fork), Value: json.RawMessage(`{}`)}
				st.Documents[key] = doc
			}
			var live LiveState
			if err := json.Unmarshal(doc.Value, &live); err != nil {
				return err
			}
			if live.Run == nil {
				live.Run = &LiveRun{TaskID: conv.Busy, Inputs: append([]SubmissionID{}, input.Submissions...)}
			}
			value, err := json.Marshal(live)
			if err != nil {
				return err
			}
			doc.Value = value
		}
		st.Version = 3
	}
	return nil
}
