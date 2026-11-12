package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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
	// fullWrite makes the next commit store the whole State: the loaded one
	// was upgraded in memory.
	fullWrite bool
	// commitObservers get each published commit's changes, in order.
	commitObservers map[*commitObserver]struct{}
	// Group commit (upstream): head is the newest admitted State, which
	// commit callbacks build on; pending are admitted commits the writer has
	// not sent yet, in line order; writing says a writer goroutine runs, and
	// idle signals when it stops.
	head    *State
	pending []*pendingCommit
	writing bool
	idle    *sync.Cond
	// published signals each batch the writer published, and failure.
	published *sync.Cond
	// held are documents deferred commits changed and no store has carried
	// yet, with the time each must be stored by; heldTimer stores them then.
	held      map[string]time.Time
	heldTimer func()
	// setTimer arms the Session's timers (deadlines of held documents,
	// context expiry): HarnessOptions.SetTimer under a Harness, as upstream
	// uses setTimer; time.AfterFunc otherwise. It returns how to stop it.
	setTimer func(callback func(), delay time.Duration) (stop func())
	// stateObservers are synchronous observers (Observer) of every State
	// memory adopts.
	stateObservers map[*stateObserver]struct{}
	// cold reads the ended tasks and settled submissions a bounded Session
	// no longer keeps (plan 0008); nil in full mode.
	cold *coldRecords
	// observerPanic reports an observer that panicked.
	observerPanic func(any)
}

type stateWatcher struct {
	ch     chan *State
	done   chan struct{}
	closed bool
	// shared watchers get the committed State itself (internal readers that
	// derive detached values); the others get copies.
	shared bool
}

func NewSession(ctx context.Context, storage Storage, now func() time.Time) (*Session, error) {
	return newSession(ctx, storage, now, true)
}

// newSession opens a Session; bounded asks for bounded memory over a
// RecordStorage (plan 0008), which a read-only Session never uses: another
// process writes its storage, so records read later could be newer than the
// State it keeps.
func newSession(ctx context.Context, storage Storage, now func() time.Time, bounded bool) (*Session, error) {
	if storage == nil {
		return nil, fmt.Errorf("durable: nil storage")
	}
	if now == nil {
		now = time.Now
	}
	records, hasRecords := storage.(RecordStorage)
	bounded = bounded && hasRecords
	var st *State
	var err error
	if bounded {
		// Load what a bounded Session keeps; a store an older version wrote
		// is loaded whole and upgraded instead.
		st, bounded, err = loadResidentState(ctx, records)
		if err != nil {
			return nil, err
		}
	}
	if !bounded {
		if st, err = storage.Load(ctx); err != nil {
			return nil, err
		}
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
	upgraded := st.Version != stateVersion
	if err := upgradeState(st); err != nil {
		return nil, fmt.Errorf("durable: cannot upgrade stored state: %w", err)
	}
	s := &Session{storage: storage, state: st, head: st, now: now, watchers: map[*stateWatcher]struct{}{}, closing: make(chan struct{}), closeDone: make(chan struct{})}
	s.idle = sync.NewCond(&s.mu)
	s.published = sync.NewCond(&s.mu)
	// Bounded memory needs record reads and no whole-State write: an
	// upgraded State is written whole by its first commit, so it opens in
	// full mode once.
	if bounded && !upgraded {
		s.cold = newColdRecords(records, func(err error) { go s.fail(err) })
		s.cold.sweep = s.sweepContexts
		s.cold.timer = func(callback func(), delay time.Duration) func() { return s.timer(callback, delay) }
		st.cold = s.cold
		s.cold.evict(st)
		// Context ranges and conversation documents load as conversations
		// are used.
		s.cold.snapshot = map[ConversationID]bool{}
		s.cold.evictEntries(st, nil)
		st.loaded = s.cold.snapshot
	}
	// The upgrade changed records no delta would carry: the first commit
	// stores the whole State.
	s.fullWrite = upgraded
	s.recordCommitted()
	return s, nil
}

func (s *Session) Commit(ctx context.Context, fn func(*Tx) error) error {
	return s.commit(ctx, fn, false)
}

// Internal bookkeeping errors seal admission on the line, before another
// caller can publish a write. Storage errors fail the Session either way.
func (s *Session) commit(ctx context.Context, fn func(*Tx) error, internal bool) (err error) {
	return s.commitWith(ctx, fn, internal, CommitOptions{}, 0)
}

// commitWith is commit with persistence options; within is the default
// window of a deferred commit, in milliseconds.
func (s *Session) commitWith(ctx context.Context, fn func(*Tx) error, internal bool, options CommitOptions, within float64) (err error) {
	if err := s.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if err := s.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	if options.Deferred {
		if options.WithinMs != nil {
			within = *options.WithinMs
		}
		if within <= 0 || math.IsNaN(within) {
			options.Deferred = false // 0 stores it at once
		}
	}
	admitted, err := s.admitLocked(ctx, fn, internal, true, options, within)
	s.mu.Unlock()
	if err != nil || admitted == nil {
		return err
	}
	// The caller gets its commit's outcome once the writer stored and
	// published it, whatever happens to ctx meanwhile.
	return <-admitted.done
}

// writeAtClose is private shutdown bookkeeping, called after invocations join.
// It follows admitted commits on the line, bypasses the seal, and publishes
// nothing. A failed Session never writes; any real error fails a healthy one.
func (s *Session) writeAtClose(fn func(*Tx) error) error {
	s.mu.Lock()
	if s.failure.Load() != nil {
		s.mu.Unlock()
		return nil
	}
	noChange := false
	admitted, err := s.admitLocked(context.Background(), func(tx *Tx) error {
		err := fn(tx)
		noChange = errors.Is(err, errNoChange)
		return err
	}, true, false, CommitOptions{}, 0)
	s.mu.Unlock()
	if noChange {
		return nil
	}
	if err != nil || admitted == nil {
		return err
	}
	return <-admitted.done
}

// pendingCommit is a commit admitted to the writer: stored in its batch,
// then published in line order (upstream group commit).
type pendingCommit struct {
	state   *State
	delta   *StateDelta
	publish bool
	done    chan error
}

// admitLocked runs a commit's callback on the newest admitted State and
// admits its changes to the writer; nil when there is nothing to store. The
// caller holds the line and has checked admission (or is writeAtClose). Only
// callback-returned errNoChange suppresses an internal bookkeeping write.
func (s *Session) admitLocked(ctx context.Context, fn func(*Tx) error, internal, publish bool, options CommitOptions, within float64) (admitted *pendingCommit, err error) {
	callbackNoChange := false
	defer func() {
		if !internal || err == nil || callbackNoChange || s.failure.Load() != nil {
			return
		}
		// A cancellation the callback returned for its own cancelled context
		// does not fail the Session; storage errors fail it in the writer.
		if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return
		}
		s.failLocked(err)
	}()
	s.verifyCommitted("before a commit")
	base := s.head
	candidate := shallowState(base)
	tx := &Tx{state: candidate, active: true, now: s.now}
	func() {
		defer func() { tx.active = false }()
		if fn != nil {
			err = fn(tx)
			callbackNoChange = internal && errors.Is(err, errNoChange)
		}
	}()
	if err != nil {
		return nil, err
	}
	s.verifyCommitted("during a commit")
	// Strict JSON round trips of the records the callback added or changed
	// validate them and detach every value the callback retained.
	changed, delta, err := detachChanges(base, candidate, s.readOnly)
	if err != nil {
		return nil, err
	}
	if s.readOnly {
		// A commit that writes nothing passes; one with writes is refused
		// before it reaches storage, and nothing is adopted.
		if changed {
			return nil, fmt.Errorf("%w: a commit with writes refused", ErrReadOnly)
		}
		return nil, nil
	}
	// A commit without writes never reaches storage (upstream); a State
	// upgraded in memory at open is a write the next commit stores.
	if !changed && !s.fullWrite {
		return nil, nil
	}
	if options.Deferred && !s.fullWrite && holdable(base, candidate, delta) {
		return s.holdLocked(candidate, delta, publish, within), nil
	}
	if base.Sequence == ^uint64(0) {
		return nil, fmt.Errorf("%w: sequence space exhausted", ErrInvalid)
	}
	candidate.Sequence = base.Sequence + 1
	candidate.base = nil
	delta.Sequence = candidate.Sequence
	// Documents deferred commits changed are stored in front of this one.
	s.foldHeldLocked(candidate, delta)
	return s.admitStoredLocked(candidate, delta, publish), nil
}

// admitStoredLocked queues a commit for the writer.
func (s *Session) admitStoredLocked(candidate *State, delta *StateDelta, publish bool) *pendingCommit {
	s.head = candidate
	admitted := &pendingCommit{state: candidate, delta: delta, publish: publish, done: make(chan error, 1)}
	s.pending = append(s.pending, admitted)
	if !s.writing {
		s.writing = true
		go s.writeLoop()
	}
	return admitted
}

// holdable reports whether a deferred commit can be held in memory: it
// changes only documents that exist, are conversation-scoped with latest
// history and the initial fork, keep their version, and are not removed.
// Records it touched without changing them are left out first.
func holdable(base, candidate *State, delta *StateDelta) bool {
	trimUnchanged(base.Conversations, candidate.Conversations, delta.Conversations)
	trimUnchanged(base.Entries, candidate.Entries, delta.Entries)
	trimUnchanged(base.Tasks, candidate.Tasks, delta.Tasks)
	trimUnchanged(base.Submissions, candidate.Submissions, delta.Submissions)
	if len(delta.Conversations)+len(delta.Entries)+len(delta.Tasks)+len(delta.Submissions) > 0 ||
		len(delta.Removed.Conversations)+len(delta.Removed.Entries)+len(delta.Removed.Tasks)+len(delta.Removed.Submissions)+len(delta.Removed.Documents) > 0 ||
		base.Version != candidate.Version || base.NextID != candidate.NextID || base.Root != candidate.Root || len(delta.Documents) == 0 {
		return false
	}
	for key, doc := range delta.Documents {
		before := base.Documents[key]
		if before == nil || doc.Scope != "conversation" || doc.History != "latest" || doc.Fork != "initial" || doc.Version != before.Version {
			return false
		}
	}
	return true
}

// trimUnchanged drops from a delta the records a commit touched without
// changing them, and gives the candidate the committed ones back.
func trimUnchanged[K comparable, V any](base, candidate, touched map[K]*V) {
	for key, record := range touched {
		committed := base[key]
		if committed == nil {
			continue
		}
		before, err1 := json.Marshal(committed)
		after, err2 := json.Marshal(record)
		if err1 == nil && err2 == nil && bytes.Equal(before, after) {
			candidate[key] = committed
			delete(touched, key)
		}
	}
}

// holdLocked adopts a deferred commit in memory: shown at once, or after the
// commits admitted before it are published; its documents are stored later.
func (s *Session) holdLocked(candidate *State, delta *StateDelta, publish bool, within float64) *pendingCommit {
	candidate.Sequence = s.head.Sequence // memory only: no sequence of its own
	candidate.base = nil
	s.head = candidate
	if s.held == nil {
		s.held = map[string]time.Time{}
	}
	deadline := time.Time{} // zero: no deadline (math.Inf(1))
	if !math.IsInf(within, 1) {
		deadline = s.now().Add(time.Duration(within * float64(time.Millisecond)))
	}
	for key := range delta.Documents {
		if current, ok := s.held[key]; !ok || !deadline.IsZero() && (current.IsZero() || deadline.Before(current)) {
			s.held[key] = deadline
		}
	}
	s.armHeldLocked()
	item := &pendingCommit{state: candidate, publish: publish, done: make(chan error, 1)}
	if s.writing {
		// Published when the commits before it are.
		s.pending = append(s.pending, item)
		return item
	}
	s.publishMemoryLocked(item)
	item.done <- nil
	return item
}

// publishMemoryLocked shows a held commit: adopted, without a store.
func (s *Session) publishMemoryLocked(item *pendingCommit) {
	if s.cold != nil {
		s.cold.evict(item.state)
	}
	s.state = item.state
	s.recordCommitted(false)
	if s.afterCommit != nil {
		s.afterCommit(item.state)
	}
	s.adoptedLocked(item.state)
	if item.publish {
		for w := range s.watchers {
			s.offer(w, item.state)
		}
	}
}

// foldHeldLocked adds the held documents' current values to a commit being
// stored, which stores them in front of its own changes.
func (s *Session) foldHeldLocked(candidate *State, delta *StateDelta) {
	if len(s.held) == 0 {
		return
	}
	for key := range s.held {
		if doc := candidate.Documents[key]; doc != nil {
			if _, own := delta.Documents[key]; !own {
				if delta.Documents == nil {
					delta.Documents = map[string]*DocumentRecord{}
				}
				delta.Documents[key] = doc
			}
		}
	}
	s.held = nil
	if s.heldTimer != nil {
		s.heldTimer()
		s.heldTimer = nil
	}
}

// armHeldLocked arms the store of held documents at the earliest deadline.
func (s *Session) armHeldLocked() {
	if s.heldTimer != nil {
		s.heldTimer()
		s.heldTimer = nil
	}
	var earliest time.Time
	for _, deadline := range s.held {
		if !deadline.IsZero() && (earliest.IsZero() || deadline.Before(earliest)) {
			earliest = deadline
		}
	}
	if earliest.IsZero() {
		return
	}
	s.heldTimer = s.timer(func() { s.storeHeld(false) }, earliest.Sub(s.now()))
}

// storeHeld stores the held documents as a commit of their own: at a
// deadline, or at close (waiting for the store).
func (s *Session) storeHeld(wait bool) {
	s.mu.Lock()
	if len(s.held) == 0 || s.failure.Load() != nil {
		s.mu.Unlock()
		return
	}
	candidate := shallowState(s.head)
	candidate.base = nil
	candidate.Sequence = s.head.Sequence + 1
	delta := &StateDelta{Version: candidate.Version, Sequence: candidate.Sequence, NextID: candidate.NextID, Root: candidate.Root}
	s.foldHeldLocked(candidate, delta)
	admitted := s.admitStoredLocked(candidate, delta, false)
	s.mu.Unlock()
	if wait {
		<-admitted.done
	}
}

// writeLoop is the Session's writer (upstream group commit): it stores every
// commit admitted while no store call is in flight in one call, off the
// line, and publishes them one by one in line order. Storage failures are
// final: the commits of the failed call get its error, later ones the
// Session's failure.
func (s *Session) writeLoop() {
	for {
		// The context retention may be host code: read it off the line.
		retention := time.Duration(0)
		if s.cold != nil {
			retention = s.cold.contextRetention()
		}
		s.mu.Lock()
		batch := s.pending
		s.pending = nil
		if len(batch) == 0 || s.failure.Load() != nil {
			for _, item := range batch {
				item.done <- s.Err()
			}
			s.writing = false
			s.idle.Broadcast()
			s.mu.Unlock()
			return
		}
		full := s.fullWrite
		var stores []*pendingCommit
		for _, item := range batch {
			if item.delta != nil {
				stores = append(stores, item)
			}
		}
		s.mu.Unlock()
		stored, err := s.store(stores, full)
		s.mu.Lock()
		failed := s.failure.Load() != nil
		position := 0 // stores published so far
		lastStore := len(stores) - 1
		failure := func() error {
			// Storage failures are final (upstream): the first error a commit
			// returns, a certified rejection included, fails the Session
			// before any commit of the call learns of it.
			if err != nil && !failed {
				s.failLocked(err)
				failed = true
			}
			if err != nil {
				return err
			}
			return s.Err()
		}
		for _, item := range batch {
			if item.delta == nil {
				// A held commit is published when the commits before it are.
				switch {
				case position > stored:
					item.done <- failure() // a store before it failed
				case failed:
					item.done <- s.Err()
				default:
					s.publishMemoryLocked(item)
					item.done <- nil
				}
				continue
			}
			if position >= stored {
				item.done <- failure()
				position++
				continue
			}
			position++
			if failed {
				item.done <- s.Err()
				continue
			}
			if s.cold != nil {
				s.cold.evict(item.state)
				// A read error already failed the Session; keeping every
				// entry is safe meanwhile.
				s.cold.forgetDocuments(item.delta)
				s.cold.forgetConversations(item.delta)
				_ = s.cold.keepEntries(item.state, s.laterStates(batch, item), item.delta, s.now(), retention, s.held)
				s.cold.evictConversations(item.state)
			}
			s.state = item.state
			s.fullWrite = false
			s.recordCommitted(position-1 == lastStore)
			if s.afterCommit != nil {
				s.afterCommit(item.state)
			}
			// Observers follow storage, including the unpublished close
			// write-back.
			s.notifyCommitObservers(item.delta, item.state)
			s.adoptedLocked(item.state)
			if item.publish {
				for w := range s.watchers {
					s.offer(w, item.state)
				}
			}
			item.done <- nil
		}
		if err != nil {
			failure()
		}
		s.published.Broadcast()
		s.mu.Unlock()
	}
}

// store sends one batch, in order, each commit checked against the one
// before it: deltas in one call where the storage takes several
// (BatchDeltaStorage), one call per commit otherwise, and whole States where
// it takes no deltas or an upgraded State must be written whole. It returns
// how many commits it stored before an error.
func (s *Session) store(batch []*pendingCommit, full bool) (int, error) {
	ctx := context.Background() // admitted commits are stored whatever their callers do
	deltas, isDelta := s.storage.(DeltaStorage)
	start := 0
	for start < len(batch) && (!isDelta || full && start == 0) {
		item := batch[start]
		if err := s.storage.Commit(ctx, item.state.Sequence-1, item.state); err != nil {
			return start, err
		}
		start++
	}
	rest := batch[start:]
	if len(rest) == 0 {
		return start, nil
	}
	if many, ok := s.storage.(BatchDeltaStorage); ok && len(rest) > 1 {
		list := make([]*StateDelta, len(rest))
		for i, item := range rest {
			list[i] = item.delta
		}
		stored, err := many.CommitDeltas(ctx, rest[0].state.Sequence-1, list)
		return start + stored, err
	}
	for i, item := range rest {
		if err := deltas.CommitDelta(ctx, item.state.Sequence-1, item.delta); err != nil {
			return start + i, err
		}
	}
	return len(batch), nil
}

// laterStates are the States admitted after item: the rest of its batch,
// then the pending commits, in line order. The caller holds s.mu.
func (s *Session) laterStates(batch []*pendingCommit, item *pendingCommit) []laterState {
	var later []laterState
	after := false
	for _, other := range batch {
		if after {
			later = append(later, laterState{state: other.state, delta: other.delta})
		}
		after = after || other == item
	}
	for _, other := range s.pending {
		later = append(later, laterState{state: other.state, delta: other.delta})
	}
	return later
}

// sweepContexts unloads the context ranges of conversations idle for the
// context retention while nothing is published: a copy of the State without
// them replaces it (readers keep theirs). With commits underway, their
// publication does it.
func (s *Session) sweepContexts() {
	if s.cold == nil {
		return
	}
	retention := s.cold.contextRetention()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure.Load() != nil || s.writing || s.Err() != nil {
		return
	}
	next := shallowState(s.state)
	next.base = nil
	before := len(next.Entries)
	documents, conversations := len(next.Documents), len(next.Conversations)
	_ = s.cold.keepEntries(next, nil, nil, s.now(), retention, s.held)
	s.cold.evictConversations(next)
	if len(next.Entries) != before || len(next.Documents) != documents || len(next.Conversations) != conversations {
		s.state, s.head = next, next
		s.recordCommitted(false)
	}
}

// timer arms a Session timer through setTimer, or time.AfterFunc.
func (s *Session) timer(callback func(), delay time.Duration) func() {
	if s.setTimer != nil {
		if stop := s.setTimer(callback, delay); stop != nil {
			return stop
		}
		return func() {}
	}
	t := time.AfterFunc(delay, callback)
	return func() { t.Stop() }
}

// drainWriter waits until the writer stored and published every admitted
// commit, or failed them.
func (s *Session) drainWriter() {
	s.mu.Lock()
	for s.writing {
		s.idle.Wait()
	}
	s.mu.Unlock()
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
	for {
		state, ahead, err := s.wholeStateLocked()
		if err != nil || !ahead {
			return state, err
		}
		// Storage holds commits not published yet: wait for them.
		s.published.Wait()
		if err := s.Err(); err != nil {
			return nil, err
		}
	}
}

// wholeStateLocked is a detached copy of the whole committed State. A
// bounded Session adds the records it no longer keeps from storage, which
// reads every stored record; ahead reports storage holding commits not
// published yet, whose records could be newer, and no State is returned
// then. The caller holds s.mu.
func (s *Session) wholeStateLocked() (*State, bool, error) {
	out, err := cloneState(s.state)
	if err != nil || s.cold == nil {
		return out, false, err
	}
	stored, err := s.storage.Load(context.Background())
	if err != nil || stored == nil {
		return out, false, err
	}
	if stored.Sequence > out.Sequence {
		// Records storage holds may be newer than the published ones.
		return nil, true, nil
	}
	for id, task := range stored.Tasks {
		if out.Tasks[id] == nil && task.Status == "terminal" {
			out.Tasks[id] = task
		}
	}
	for id, sub := range stored.Submissions {
		if out.Submissions[id] == nil && !residentSubmission(sub) {
			out.Submissions[id] = sub
		}
	}
	for id, e := range stored.Entries {
		if out.Entries[id] == nil {
			out.Entries[id] = e
		}
	}
	for key, doc := range stored.Documents {
		if out.Documents[key] == nil {
			out.Documents[key] = doc
		}
	}
	for id, conversation := range stored.Conversations {
		if out.Conversations[id] == nil {
			out.Conversations[id] = conversation
		}
	}
	return out, false, nil
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
func (s *Session) recordCommitted(storeCaughtUp ...bool) {
	if !verifyCommittedState {
		return
	}
	data, err := json.Marshal(s.state)
	if err != nil {
		panic(fmt.Sprintf("durable: committed state is not JSON: %v", err))
	}
	s.committedJSON = data
	// A test storage that can show what it holds must hold the same State,
	// once an upgraded State has been written and the batch that stored this
	// commit published its last one.
	caughtUp := len(storeCaughtUp) == 0 || storeCaughtUp[0]
	if stored, ok := s.storage.(interface{ storedStateJSON() []byte }); ok && !s.fullWrite && caughtUp {
		held := stored.storedStateJSON()
		if held != nil && s.cold != nil {
			held = s.residentJSON(held)
		}
		if held != nil && !bytes.Equal(held, data) {
			panic(fmt.Sprintf("durable: storage diverged from the committed state: %s", firstDifference(data, held)))
		}
	}
}

// residentJSON is a stored State's JSON with only the tasks and submissions
// a bounded Session keeps; every other one must be ended or settled.
func (s *Session) residentJSON(storedJSON []byte) []byte {
	var stored State
	if err := json.Unmarshal(storedJSON, &stored); err != nil {
		panic(fmt.Sprintf("durable: stored state is not JSON: %v", err))
	}
	for id, task := range stored.Tasks {
		if s.state.Tasks[id] == nil {
			if task.Status != "terminal" {
				panic(fmt.Sprintf("durable: live task %d left a bounded Session's state", id))
			}
			delete(stored.Tasks, id)
		}
	}
	for id, sub := range stored.Submissions {
		if s.state.Submissions[id] == nil {
			if residentSubmission(sub) {
				panic(fmt.Sprintf("durable: open submission %d left a bounded Session's state", id))
			}
			delete(stored.Submissions, id)
		}
	}
	for id := range stored.Entries {
		if s.state.Entries[id] == nil {
			delete(stored.Entries, id)
		}
	}
	for key := range stored.Documents {
		if s.state.Documents[key] == nil {
			delete(stored.Documents, key)
		}
	}
	for id := range stored.Conversations {
		if s.state.Conversations[id] == nil {
			delete(stored.Conversations, id)
		}
	}
	data, err := json.Marshal(stored)
	if err != nil {
		panic(fmt.Sprintf("durable: stored state is not JSON: %v", err))
	}
	return data
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
	return s.watch(ctx, false)
}

// watchShared is Watch for internal readers: it delivers the committed State
// itself, which they must not mutate, instead of a copy per commit.
func (s *Session) watchShared(ctx context.Context) (<-chan *State, func(), error) {
	return s.watch(ctx, true)
}

func (s *Session) watch(ctx context.Context, shared bool) (<-chan *State, func(), error) {
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
	initial := s.state
	if !shared {
		for {
			whole, ahead, err := s.wholeStateLocked()
			if err != nil {
				s.mu.Unlock()
				return nil, nil, err
			}
			if !ahead {
				initial = whole
				break
			}
			s.published.Wait()
			if err := s.Err(); err != nil {
				s.mu.Unlock()
				return nil, nil, err
			}
		}
	}
	w := &stateWatcher{ch: make(chan *State, 1), done: make(chan struct{}), shared: shared}
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
		states, _, err := s.watchShared(bound)
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
	v := state
	if !w.shared {
		var err error
		if s.cold != nil && state == s.state {
			var ahead bool
			// Storage ahead of this publication: a later one offers the
			// newest State (watches are coalescing).
			if v, ahead, err = s.wholeStateLocked(); ahead {
				return
			}
		} else {
			v, err = cloneState(state)
		}
		if err != nil {
			return
		}
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

type commitObserver struct{ observe func(*StateDelta) }

// observeCommits attaches observe to every published commit (see
// Harness.ObserveCommits); detach stops it.
func (s *Session) observeCommits(observe func(*StateDelta)) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commitObservers == nil {
		s.commitObservers = map[*commitObserver]struct{}{}
	}
	o := &commitObserver{observe: observe}
	s.commitObservers[o] = struct{}{}
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.commitObservers, o)
	}
}

// notifyCommitObservers gives each observer a detached copy of the commit's
// changes. The caller holds s.mu.
func (s *Session) notifyCommitObservers(delta *StateDelta, committed *State) {
	if len(s.commitObservers) == 0 {
		return
	}
	delta.Sequence = committed.Sequence
	for o := range s.commitObservers {
		detached, err := delta.Clone()
		if err != nil {
			return
		}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil && s.observerPanic != nil {
					s.observerPanic(recovered)
				}
			}()
			o.observe(detached)
		}()
	}
}
