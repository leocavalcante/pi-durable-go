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
	unpin := c.h.session.pinConversation(c.id)
	go func() {
		defer close(out)
		defer stopWatch()
		defer unpin()
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
	record, err := state.conversation(c.id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, ErrNotFound
	}
	// Windows read only the newest entries, walking back from the head.
	var entries []Entry
	var before EntryID
	switch {
	case previous == nil:
		// Enough for the window, and one more: the entry before it.
		limit := window.Min + 1
		if window.From != 0 {
			limit = window.Max + 2
		}
		var newest []Entry
		if err := walkBack(state, c.id, func(e *Entry) (bool, error) {
			newest = append(newest, e.detached())
			return len(newest) < limit, nil
		}); err != nil {
			return nil, err
		}
		take := min(window.Min, len(newest))
		if window.From != 0 {
			// The entries from From on, when at most Max of them follow it.
			fromOn := 0
			for fromOn < len(newest) && newest[fromOn].ID >= window.From {
				fromOn++
			}
			if fromOn <= window.Max {
				take = fromOn
			}
		}
		if take < len(newest) {
			before = newest[take].ID
		}
		entries = newest[:take]
		reverseEntries(entries)
	default:
		entries = append(entries, previous.Entries...)
		newest := previous.Before
		if len(entries) > 0 {
			newest = entries[len(entries)-1].ID
		}
		var added []Entry
		stop := EntryID(0)
		if err := walkBack(state, c.id, func(e *Entry) (bool, error) {
			if e.ID <= newest {
				stop = e.ID
				return false, nil
			}
			added = append(added, e.detached())
			return true, nil
		}); err != nil {
			return nil, err
		}
		reverseEntries(added)
		before = previous.Before
		if len(entries) == 0 && len(added) > 0 {
			before = stop
		}
		entries = append(entries, added...)
		if len(entries) > window.Max {
			before = entries[len(entries)-window.Min-1].ID
			entries = append([]Entry(nil), entries[len(entries)-window.Min:]...)
		}
	}
	view := &ConversationView{Conversation: *cloneRecord(record), Entries: entries, Before: before, Docs: map[string]json.RawMessage{}}
	docs, err := state.conversationDocuments(c.id)
	if err != nil {
		return nil, err
	}
	for _, doc := range docs {
		if strings.HasPrefix(doc.Kind, "pi.") {
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
