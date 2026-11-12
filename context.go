package durable

import (
	"context"
	"fmt"
)

// Model context derivation (upstream harness/context.ts, spec §2.1): the
// newest head marker (a reset or a compaction summary) starts the range;
// entry edits omit or replace earlier entries' messages; tool results follow
// their calls; and the baseline system message leads.

// contextView is the active transcript as model context sees it: the newest
// head marker, then the entries it keeps and the ones after it, each with
// the messages it contributes after edits, and the derived messages.
type contextView struct {
	head          *Entry
	entries       []Entry
	contributions [][]Message
	messages      []Message
}

// deriveContextView derives the view; with at, as of that entry, as the
// context a fork at it would start with.
func deriveContextView(state *State, cid ConversationID, at EntryID) (contextView, error) {
	entries, err := entriesFromState(state, cid)
	if err != nil {
		return contextView{}, err
	}
	if at != 0 {
		for i, e := range entries {
			if e.ID > at {
				entries = entries[:i]
				break
			}
		}
	}
	return deriveContext(entries)
}

// isHeadMarker reports whether an entry starts a new context range.
func isHeadMarker(e Entry) bool { return e.Kind == "pi.reset" || e.Kind == "pi.compaction" }

// isSystemEntry reports whether an entry is a positional prompt and tool
// change (pi.system with a system message). Earlier versions journaled
// pi.system entries without a message; they contribute nothing.
func isSystemEntry(e Entry) bool {
	return e.Kind == "pi.system" && e.Message != nil && e.Message.Role == "system"
}

// carriesSystemMessage reports whether an entry's own message, or the
// replacement messages of its edits, include a system message.
func carriesSystemMessage(e Entry) bool {
	if e.Message != nil && e.Message.Role == "system" {
		return true
	}
	for _, edit := range e.Edits {
		if edit.Action == "replace" {
			for _, m := range edit.Messages {
				if m.Role == "system" {
					return true
				}
			}
		}
	}
	return false
}

// deriveContext derives the view from a conversation's history, oldest
// first.
func deriveContext(history []Entry) (contextView, error) {
	var head *Entry
	for i := len(history) - 1; i >= 0; i-- {
		if isHeadMarker(history[i]) {
			h := history[i]
			head = &h
			break
		}
	}
	// The range runs from the head marker's start through the tail: a reset
	// starts at itself, a summary at the first entry it keeps.
	inRange := func(Entry) bool { return true }
	if head != nil {
		switch head.Kind {
		case "pi.reset":
			start := head.ID
			inRange = func(e Entry) bool { return e.ID >= start }
		case "pi.compaction":
			_, cut, err := compactionSummary(*head)
			if err != nil {
				return contextView{}, err
			}
			found := false
			for _, e := range history {
				if e.ID == cut {
					found = true
					break
				}
			}
			if !found {
				return contextView{}, fmt.Errorf("%w: compaction cut", ErrInvalid)
			}
			inRange = func(e Entry) bool { return e.ID > cut }
		}
	}
	var scanned []Entry
	for _, e := range history {
		if inRange(e) {
			scanned = append(scanned, e)
		}
	}
	// Edits of every entry in the range count, including older head markers;
	// the newest edit of a target wins.
	edits := map[EntryID]ContextEdit{}
	for _, e := range scanned {
		for _, edit := range e.Edits {
			edits[edit.Target] = edit
		}
	}
	view := contextView{head: head}
	if head != nil {
		view.entries = append(view.entries, *head)
	}
	for _, e := range scanned {
		if !isHeadMarker(e) {
			view.entries = append(view.entries, e)
		}
	}
	var flat []Message
	// The first system message's entry, and whether it is the entry's own
	// message: an edit's replacement is never the baseline's message.
	firstSystem, firstOwn := EntryID(0), false
	for _, e := range view.entries {
		edit, edited := edits[e.ID]
		contributed, err := contribute(e, edit, edited)
		if err != nil {
			return contextView{}, err
		}
		view.contributions = append(view.contributions, contributed)
		for _, m := range contributed {
			if m.Role == "system" && firstSystem == 0 {
				firstSystem, firstOwn = e.ID, !edited
			}
		}
		flat = append(flat, contributed...)
	}
	baseline, ok := findBaseline(head, view.entries)
	view.messages = leadWithSystem(orderToolResults(flat), ok && firstOwn && baseline == firstSystem)
	return view, nil
}

// contribute is an entry's model messages after its edit, without the
// assistant attempts that never reach context.
func contribute(e Entry, edit ContextEdit, edited bool) ([]Message, error) {
	if edited && edit.Action == "omit" {
		return nil, nil
	}
	var messages []Message
	switch {
	case edited && edit.Action == "replace":
		for _, m := range edit.Messages {
			messages = append(messages, m.detached())
		}
	case e.Kind == "pi.compaction":
		summary, _, err := compactionSummary(e)
		if err != nil {
			return nil, err
		}
		messages = []Message{summary}
	case e.Kind == "pi.reset":
		// A plain reset contributes nothing; a handoff carries its text.
		if e.Message != nil && e.Message.Content != "" {
			messages = []Message{*e.Message}
		}
	case e.Message != nil:
		messages = []Message{*e.Message}
	}
	kept := messages[:0]
	for _, m := range messages {
		if contributesContext(&m) {
			kept = append(kept, m)
		}
	}
	return kept, nil
}

// findBaseline finds the entry whose system message is the baseline: the
// first pi.system entry after the head marker, or of the transcript without
// one when no earlier entry carries a system message. ok is false when there
// is none.
func findBaseline(head *Entry, entries []Entry) (EntryID, bool) {
	for _, e := range entries {
		if head != nil && e.ID <= head.ID {
			continue
		}
		if isSystemEntry(e) {
			return e.ID, true
		}
		if head == nil && carriesSystemMessage(e) {
			return 0, false
		}
	}
	return 0, false
}

// leadWithSystem moves the first system message to the front when only user
// messages precede it, or when it is the baseline's: a run's input is
// committed before generation writes the prompt, and providers treat only a
// leading system message as the initial prompt and tool set. Other system
// messages stay where they were committed.
func leadWithSystem(messages []Message, baseline bool) []Message {
	index := -1
	for i, m := range messages {
		if m.Role == "system" {
			index = i
			break
		}
	}
	if index <= 0 {
		return messages
	}
	if !baseline {
		for _, m := range messages[:index] {
			if m.Role != "user" {
				return messages
			}
		}
	}
	out := make([]Message, 0, len(messages))
	out = append(out, messages[index])
	out = append(out, messages[:index]...)
	return append(out, messages[index+1:]...)
}

// ContextView is a conversation's committed model context (upstream
// ContextView): the newest head marker (a reset or a compaction summary),
// the entries it keeps and the ones after it, the messages each contributes
// after edits, and the derived messages.
type ContextView struct {
	Head          *Entry
	Entries       []Entry
	Contributions [][]Message
	Messages      []Message
}

func (v contextView) public() ContextView {
	out := ContextView{Entries: v.entries, Contributions: v.contributions, Messages: v.messages}
	if v.head != nil {
		head := v.head.detached()
		out.Head = &head
	}
	return out
}

// ContextView reads the conversation's model context; with at, as of that
// entry of its history, as a fork at it would start with.
func (c *Conversation) ContextView(ctx context.Context, at EntryID) (ContextView, error) {
	state, err := c.h.read(ctx)
	if err != nil {
		return ContextView{}, err
	}
	view, err := deriveContextView(state, c.id, at)
	if err != nil {
		return ContextView{}, err
	}
	return view.public(), nil
}

// Context reads a conversation's model context (upstream runtime.context);
// with at, as of that entry.
func (r *TaskRuntime) Context(ctx context.Context, cid ConversationID, at EntryID) (ContextView, error) {
	if err := r.check(); err != nil {
		return ContextView{}, err
	}
	state, err := r.h.read(ctx)
	if err != nil {
		return ContextView{}, err
	}
	view, err := deriveContextView(state, cid, at)
	if err != nil {
		return ContextView{}, err
	}
	return view.public(), nil
}
