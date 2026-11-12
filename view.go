package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ConversationView is a window over a conversation's fork-aware history
// (upstream spec 9.3) with its built-in documents.
type ConversationView struct {
	Conversation ConversationRecord
	// Entries is the window, in entry ID order.
	Entries []Entry
	// Before is the ID of the entry just before the window; 0 when the
	// window starts the transcript.
	Before EntryID
	// Docs holds the conversation's built-in (pi.*) documents by kind.
	Docs map[string]json.RawMessage
}

// ConversationWindow says which entries a view holds: it starts with the
// newest Min entries (default 50), takes appends, and once it would hold
// more than Max (default 2*Min) drops its oldest down to Min. With From, the
// first value starts at From instead when at most Max entries are at or
// after it; a From past the newest entry starts empty.
type ConversationWindow struct {
	From EntryID
	Min  int
	Max  int
}

// WatchWindow watches the conversation through a window: it delivers the first
// value at once and a next one after each commit that changes the window
// or the conversation's documents, coalesced to the latest. stop ends it.
func (c *Conversation) WatchWindow(ctx context.Context, window ConversationWindow) (<-chan ConversationView, func(), error) {
	window, err := window.resolved()
	if err != nil {
		return nil, nil, err
	}
	states, stopWatch, err := c.h.session.watchShared(ctx)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan ConversationView, 1)
	go func() {
		defer close(out)
		defer stopWatch()
		var view *ConversationView
		var last []byte
		for {
			state, err := c.h.read(ctx)
			if err != nil {
				return
			}
			next, err := c.advanceView(state, view, window)
			if err != nil {
				return
			}
			view = next
			data, _ := json.Marshal(next)
			if string(data) != string(last) {
				last = data
				// The watcher keeps its own view for the next window; each
				// value sent is a detached copy.
				sent := next.detached()
				select {
				case out <- sent:
				default:
					select {
					case <-out:
					default:
					}
					select {
					case out <- sent:
					case <-ctx.Done():
						return
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case _, ok := <-states:
				if !ok {
					return
				}
			}
		}
	}()
	return out, func() { cancel(); stopWatch() }, nil
}

// resolved fills a window's defaults and checks its bounds.
func (w ConversationWindow) resolved() (ConversationWindow, error) {
	if w.Min == 0 {
		w.Min = 50
	}
	if w.Max == 0 {
		w.Max = 2 * w.Min
	}
	if w.Min < 1 || w.Max < w.Min {
		return w, fmt.Errorf("%w: window bounds", ErrInvalid)
	}
	return w, nil
}

// advanceView computes the window's next value from a snapshot.
func (c *Conversation) advanceView(state *State, previous *ConversationView, window ConversationWindow) (*ConversationView, error) {
	record := state.Conversations[c.id]
	if record == nil {
		return nil, ErrNotFound
	}
	history, err := entriesFromState(state, c.id)
	if err != nil {
		return nil, err
	}
	var entries []Entry
	switch {
	case previous == nil:
		start := len(history) - window.Min
		if window.From != 0 {
			at := len(history)
			for i, e := range history {
				if e.ID >= window.From {
					at = i
					break
				}
			}
			if len(history)-at <= window.Max {
				start = at
			}
		}
		if start < 0 {
			start = 0
		}
		entries = append(entries, history[start:]...)
	default:
		entries = append(entries, previous.Entries...)
		newest := previous.Before
		if len(entries) > 0 {
			newest = entries[len(entries)-1].ID
		}
		for _, e := range history {
			if e.ID > newest {
				entries = append(entries, e)
			}
		}
		if len(entries) > window.Max {
			entries = append([]Entry(nil), entries[len(entries)-window.Min:]...)
		}
	}
	view := &ConversationView{Conversation: *cloneRecord(record), Entries: entries, Docs: map[string]json.RawMessage{}}
	if len(entries) > 0 {
		for i, e := range history {
			if e.ID == entries[0].ID && i > 0 {
				view.Before = history[i-1].ID
				break
			}
		}
	} else if len(history) > 0 {
		view.Before = history[len(history)-1].ID
	} else if previous != nil {
		view.Before = previous.Before
	}
	for _, doc := range state.Documents {
		if doc.Scope == "conversation" && ConversationID(doc.ScopeID) == c.id && strings.HasPrefix(doc.Kind, "pi.") {
			view.Docs[doc.Kind] = append(json.RawMessage(nil), doc.Value...)
		}
	}
	return view, nil
}

// detached copies a view, so its receiver cannot change the watcher's.
func (v *ConversationView) detached() ConversationView {
	out := ConversationView{Conversation: *cloneRecord(&v.Conversation), Before: v.Before, Docs: make(map[string]json.RawMessage, len(v.Docs))}
	out.Entries = make([]Entry, len(v.Entries))
	for i, e := range v.Entries {
		out.Entries[i] = e.detached()
	}
	for kind, value := range v.Docs {
		out.Docs[kind] = cloneRaw(value)
	}
	return out
}
