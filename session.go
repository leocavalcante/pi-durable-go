package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// Session serializes transactions over one Storage. State is never exposed without detaching it.
type Session struct {
	mu        sync.Mutex
	storage   Storage
	state     *State
	now       func() time.Time
	closed    bool
	poisoned  bool
	watchers  map[*stateWatcher]struct{}
	closeDone chan struct{}
	closeErr  error
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
		st = &State{Version: 1, NextID: 1, Conversations: map[ConversationID]*ConversationRecord{}, Entries: map[EntryID]*Entry{}, Tasks: map[TaskID]*TaskRecord{}, Submissions: map[SubmissionID]*SubmissionRecord{}, Documents: map[string]*DocumentRecord{}}
	}
	if st.Version != 1 || st.NextID == 0 {
		return nil, fmt.Errorf("%w: unsupported storage version or invalid ID allocator", ErrInvalid)
	}
	st, err = cloneState(st)
	if err != nil {
		return nil, fmt.Errorf("durable: invalid stored state: %w", err)
	}
	return &Session{storage: storage, state: st, now: now, watchers: map[*stateWatcher]struct{}{}}, nil
}

func (s *Session) Commit(ctx context.Context, fn func(*Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.poisoned {
		return ErrPoisoned
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	candidate, err := cloneState(s.state)
	if err != nil {
		return err
	}
	tx := &Tx{state: candidate, active: true, now: s.now}
	func() {
		defer func() { tx.active = false }()
		if fn != nil {
			err = fn(tx)
		}
	}()
	if err != nil {
		return err
	}
	if s.state.Sequence == ^uint64(0) {
		return fmt.Errorf("%w: sequence space exhausted", ErrInvalid)
	}
	candidate.Sequence = s.state.Sequence + 1
	// Marshal/unmarshal both enforces strict JSON representability and detaches all values
	// retained by a callback from the committed snapshot.
	candidate, err = cloneState(candidate)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	err = s.storage.Commit(ctx, s.state.Sequence, candidate)
	if err != nil {
		var rejected *RejectedError
		if !errors.As(err, &rejected) {
			s.poisoned = true
		}
		return err
	}
	s.state = candidate
	for w := range s.watchers {
		s.offer(w, candidate)
	}
	return nil
}

func (s *Session) Snapshot(ctx context.Context) (*State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrClosed
	}
	if s.poisoned {
		return nil, ErrPoisoned
	}
	return cloneState(s.state)
}

func (s *Session) Watch(ctx context.Context) (<-chan *State, func(), error) {
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return nil, nil, err
	}
	if s.closed {
		s.mu.Unlock()
		return nil, nil, ErrClosed
	}
	if s.poisoned {
		s.mu.Unlock()
		return nil, nil, ErrPoisoned
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

func (s *Session) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	s.closeDone = make(chan struct{})
	for w := range s.watchers {
		if !w.closed {
			w.closed = true
			close(w.ch)
			close(w.done)
		}
		delete(s.watchers, w)
	}
	s.mu.Unlock()
	err := s.storage.Close()
	s.mu.Lock()
	s.closeErr = err
	close(s.closeDone)
	s.mu.Unlock()
	return err
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
