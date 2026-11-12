package durable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
)

// WatchEnd says why an observer ended (upstream Chord WatchEnd).
type WatchEnd struct {
	// Kind is ended (the observed scope's owner ended: a task document's
	// task), failed (Err is the error that failed the Session, or ErrClosed
	// once it closed), or listener_error (Err is the observer's own panic).
	Kind string
	Err  error
}

// Observer is a synchronous observer of committed state, for adapters
// (upstream Observer): Advance gets each next value, in order, as memory
// adopts a commit that changes it, a stored commit once it is stored and a
// deferred one before it is. It runs on the Session line, so it must only
// record the value: quickly, without blocking or calling the Harness. A panic
// is reported as listener_error, detaches the observer, and ends it with
// listener_error. End is called once, and never after detach. Values are the
// observer's own.
type Observer[T any] struct {
	Advance func(T)
	End     func(WatchEnd)
}

// stateObserver is an observer attached to the Session: advance runs at
// every adoption on the line and says whether the observed scope ended.
type stateObserver struct {
	advance func(*State) (ended bool)
	end     func(WatchEnd)
	cleanup func()
	once    sync.Once
}

// finish ends the observer once: cleanup, then its End (a panic in End is
// contained). The caller holds the Session line.
func (o *stateObserver) finish(s *Session, end *WatchEnd) {
	o.once.Do(func() {
		if o.cleanup != nil {
			o.cleanup()
		}
		if end == nil || o.end == nil {
			return
		}
		defer func() {
			if recovered := recover(); recovered != nil && s.observerPanic != nil {
				s.observerPanic(recovered)
			}
		}()
		o.end(*end)
	})
}

// observe attaches an observer built from the committed State, on the line.
func (s *Session) observe(attach func(*State) (*stateObserver, error)) (func(), error) {
	if err := s.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.Err(); err != nil {
		return nil, err
	}
	o, err := attach(s.state)
	if err != nil {
		return nil, err
	}
	if s.stateObservers == nil {
		s.stateObservers = map[*stateObserver]struct{}{}
	}
	s.stateObservers[o] = struct{}{}
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, attached := s.stateObservers[o]; attached {
			delete(s.stateObservers, o)
			o.finish(s, nil)
		}
	}, nil
}

// adoptedLocked advances every attached observer with a State memory adopted.
func (s *Session) adoptedLocked(state *State) {
	for o := range s.stateObservers {
		s.advanceObserverLocked(o, state)
	}
}

func (s *Session) advanceObserverLocked(o *stateObserver, state *State) {
	var ended bool
	var panicked any
	func() {
		defer func() { panicked = recover() }()
		ended = o.advance(state)
	}()
	switch {
	case panicked != nil:
		delete(s.stateObservers, o)
		if s.observerPanic != nil {
			s.observerPanic(panicked)
		}
		o.finish(s, &WatchEnd{Kind: "listener_error", Err: fmt.Errorf("observer panicked: %v", panicked)})
	case ended:
		delete(s.stateObservers, o)
		o.finish(s, &WatchEnd{Kind: "ended"})
	}
}

// endObserversLocked ends every attached observer.
func (s *Session) endObserversLocked(end WatchEnd) {
	for o := range s.stateObservers {
		delete(s.stateObservers, o)
		o.finish(s, &end)
	}
}

// ObserveTaskGraph attaches a synchronous Observer to the task graph
// (upstream observeTaskGraph): it returns the graph at attachment, which
// later values follow, and detach. A blocked reason that a registry change
// changes advances it outside any commit.
func (h *Harness) ObserveTaskGraph(observer Observer[TaskGraph]) (TaskGraph, func(), error) {
	var initial TaskGraph
	var last []byte
	var o *stateObserver
	detach, err := h.session.observe(func(state *State) (*stateObserver, error) {
		initial = taskGraphOf(state, h.inspectLocked(state))
		last, _ = json.Marshal(initial)
		o = &stateObserver{end: observer.End, advance: func(state *State) bool {
			graph := taskGraphOf(state, h.inspectLocked(state))
			data, _ := json.Marshal(graph)
			if !bytes.Equal(data, last) {
				last = data
				if observer.Advance != nil {
					observer.Advance(graph)
				}
			}
			return false
		}}
		session := h.session
		o.cleanup = h.registry.Subscribe(func() {
			session.mu.Lock()
			defer session.mu.Unlock()
			if _, attached := session.stateObservers[o]; attached {
				session.advanceObserverLocked(o, session.state)
			}
		})
		return o, nil
	})
	if err != nil {
		return TaskGraph{}, nil, err
	}
	return initial, detach, nil
}

// Observe attaches a synchronous Observer to the conversation's view, as
// WatchWindow windows it (upstream observeConversation): it returns the view
// at attachment, which later values follow, and detach.
func (c *Conversation) Observe(window ConversationWindow, observer Observer[ConversationView]) (ConversationView, func(), error) {
	window, err := window.resolved()
	if err != nil {
		return ConversationView{}, nil, err
	}
	var initial ConversationView
	var view *ConversationView
	var last []byte
	detach, err := c.h.session.observe(func(state *State) (*stateObserver, error) {
		first, err := c.advanceView(state, nil, window)
		if err != nil {
			return nil, err
		}
		view, initial = first, first.detached()
		last, _ = json.Marshal(first)
		return &stateObserver{end: observer.End, advance: func(state *State) bool {
			next, err := c.advanceView(state, view, window)
			if err != nil {
				return false
			}
			view = next
			data, _ := json.Marshal(next)
			if !bytes.Equal(data, last) {
				last = data
				if observer.Advance != nil {
					observer.Advance(next.detached())
				}
			}
			return false
		}}, nil
	})
	if err != nil {
		return ConversationView{}, nil, err
	}
	return initial, detach, nil
}

// ObserveDocument attaches a synchronous Observer to one document's value
// (upstream observeDoc): it returns the value at attachment (nil when
// absent), which later values follow, nil once retired, and detach. The
// observer of a task document ends ended once its task is terminal, after the
// value that commit left.
func ObserveDocument(h *Harness, def DocumentDefinition, id uint64, observer Observer[json.RawMessage]) (json.RawMessage, func(), error) {
	return observeDocument(h, def, id, "", observer)
}

// ObserveFamilyDocument is ObserveDocument for a family member.
func ObserveFamilyDocument(h *Harness, def DocumentDefinition, id uint64, member string, observer Observer[json.RawMessage]) (json.RawMessage, func(), error) {
	return observeDocument(h, def, id, member, observer)
}

func observeDocument(h *Harness, def DocumentDefinition, id uint64, member string, observer Observer[json.RawMessage]) (json.RawMessage, func(), error) {
	if err := validateDocument(def); err != nil {
		return nil, nil, err
	}
	key, err := keyOf(def, id, member)
	if err != nil {
		return nil, nil, err
	}
	valueOf := func(state *State) json.RawMessage {
		if doc := state.Documents[key]; doc != nil {
			return doc.Value
		}
		return nil
	}
	var initial json.RawMessage
	var last json.RawMessage
	detach, err := h.session.observe(func(state *State) (*stateObserver, error) {
		last = valueOf(state)
		initial = cloneRaw(last)
		return &stateObserver{end: observer.End, advance: func(state *State) bool {
			value := valueOf(state)
			if !bytes.Equal(value, last) || (value == nil) != (last == nil) {
				last = value
				if observer.Advance != nil {
					observer.Advance(cloneRaw(value))
				}
			}
			if def.Scope == "task" {
				task := state.Tasks[TaskID(id)]
				return task == nil || task.Status == "terminal"
			}
			return false
		}}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return initial, detach, nil
}
