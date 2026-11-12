package durable

import (
	"context"
	"fmt"
)

// Bounded entries (plan 0008 M3). A bounded Session keeps each
// conversation's context range in memory: the entries of its lineage from the
// bound of its newest head marker on (a reset itself, a compaction's cut), or
// its whole lineage without one, plus what forks reaching into it need. Older
// entries leave the State as the commit that moved the range is published.
// Every read still finds them: State.entry and walkBack fall back to storage,
// so residency only decides what costs a storage read.

const coldEntryCapacity = 1024

// entry is an entry record: the State's own, or one read through the cold
// cache. Never mutate the result. Nil when none exists.
func (s *State) entry(id EntryID) (*Entry, error) {
	if e := s.Entries[id]; e != nil || s.cold == nil {
		return e, nil
	}
	return s.cold.entry(id)
}

func (c *coldRecords) entry(id EntryID) (*Entry, error) {
	c.mu.Lock()
	if e, ok := c.entries.get(id); ok {
		c.mu.Unlock()
		return e, nil
	}
	c.mu.Unlock()
	e, err := c.storage.Entry(context.Background(), id)
	if err != nil {
		err = fmt.Errorf("durable: reading entry %d: %w", id, err)
		c.fail(err)
		return nil, err
	}
	if e != nil {
		c.mu.Lock()
		c.entries.put(id, e)
		c.mu.Unlock()
	}
	return e, nil
}

// walkBack visits a conversation's lineage from its head backward, newest
// first, until visit returns false. Entries the State no longer keeps come
// from storage a page at a time. Never mutate a visited entry.
func walkBack(st *State, cid ConversationID, visit func(*Entry) (bool, error)) error {
	c := st.Conversations[cid]
	if c == nil {
		return ErrNotFound
	}
	seen := map[EntryID]bool{}
	for id := c.Head; id != 0; {
		if seen[id] {
			return fmt.Errorf("%w: entry cycle", ErrInvalid)
		}
		seen[id] = true
		e := st.Entries[id]
		if e == nil && st.cold != nil {
			// The rest of the lineage is older than what the State keeps.
			return walkStored(st, cid, id, visit)
		}
		if e == nil {
			return fmt.Errorf("%w: missing entry %d", ErrInvalid, id)
		}
		more, err := visit(e)
		if err != nil || !more {
			return err
		}
		id = e.Parent
	}
	return nil
}

// walkStored continues walkBack in storage from entry from down.
func walkStored(st *State, cid ConversationID, from EntryID, visit func(*Entry) (bool, error)) error {
	query := EntryQuery{MaxEntryID: from, Order: "descending"}
	var cursor *EntryCursor
	for {
		page, err := st.cold.storage.ScanEntries(context.Background(), cid, query, 256, cursor)
		if err != nil {
			err = fmt.Errorf("durable: reading entries of conversation %d: %w", cid, err)
			st.cold.fail(err)
			return err
		}
		for i := range page.Items {
			more, err := visit(&page.Items[i])
			if err != nil || !more {
				return err
			}
		}
		if page.Next == nil {
			return nil
		}
		cursor = page.Next
	}
}

// entriesFromState is a conversation's whole lineage, oldest first, detached.
func entriesFromState(s *State, cid ConversationID) ([]Entry, error) {
	var entries []Entry
	if err := walkBack(s, cid, func(e *Entry) (bool, error) {
		entries = append(entries, e.detached())
		return true, nil
	}); err != nil {
		return nil, err
	}
	reverseEntries(entries)
	return entries, nil
}

// rangeEntries is a conversation's context range, oldest first, detached:
// its lineage from the bound of its newest head marker on, or all of it. It
// derives the same context as the whole lineage, reading only the range.
func rangeEntries(s *State, cid ConversationID) ([]Entry, error) {
	var entries []Entry
	bound := EntryID(0)
	found := false
	if err := walkBack(s, cid, func(e *Entry) (bool, error) {
		if found && e.ID < bound {
			return false, nil
		}
		entries = append(entries, e.detached())
		if !found && isHeadMarker(*e) {
			found, bound = true, markerBound(*e)
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	reverseEntries(entries)
	return entries, nil
}

// markerBound is the oldest entry a head marker's range keeps: a reset
// itself, a compaction's cut (which context derivation looks up). Zero keeps
// the whole lineage, for a marker it cannot read.
func markerBound(e Entry) EntryID {
	switch e.Kind {
	case "pi.reset":
		return e.ID
	case "pi.compaction":
		if _, cut, err := compactionSummary(e); err == nil {
			return cut
		}
	}
	return 0
}

// rangeStart is where a conversation's context range starts: the bound of
// the newest head marker in its lineage, or 0.
func rangeStart(s *State, cid ConversationID) (EntryID, error) {
	start := EntryID(0)
	err := walkBack(s, cid, func(e *Entry) (bool, error) {
		if isHeadMarker(*e) {
			start = markerBound(*e)
			return false, nil
		}
		return true, nil
	})
	return start, err
}

func reverseEntries(entries []Entry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}

// keepEntries recomputes the range starts a published State changed (head
// markers appended, conversations created) and evicts each conversation's
// entries below the smallest start among itself and the conversations whose
// lineage passes through it. The caller holds the line; the State is not
// shared yet.
func (c *coldRecords) keepEntries(st *State, delta *StateDelta, all bool) error {
	changed := all
	if all {
		c.starts = map[ConversationID]EntryID{}
		for id := range st.Conversations {
			start, err := rangeStart(st, id)
			if err != nil {
				return err
			}
			c.starts[id] = start
		}
	}
	if delta != nil {
		for id := range delta.Conversations {
			if _, known := c.starts[id]; !known && st.Conversations[id] != nil {
				start, err := rangeStart(st, id)
				if err != nil {
					return err
				}
				c.starts[id], changed = start, true
			}
		}
		for _, e := range delta.Entries {
			if isHeadMarker(*e) {
				c.starts[e.Conversation], changed = markerBound(*e), true
			}
		}
	}
	if !changed {
		return nil
	}
	keep := map[ConversationID]EntryID{}
	for id := range st.Conversations {
		start := c.starts[id]
		seen := map[ConversationID]bool{}
		for at := id; at != 0 && !seen[at]; {
			seen[at] = true
			if current, ok := keep[at]; !ok || start < current {
				keep[at] = start
			}
			parent := st.Conversations[at]
			if parent == nil {
				break
			}
			at = parent.Parent
		}
	}
	for id, e := range st.Entries {
		if from, ok := keep[e.Conversation]; ok && e.ID < from {
			delete(st.Entries, id)
		}
	}
	return nil
}

// loadResidentState loads what a bounded Session keeps at open (plan 0008):
// the header, live tasks, and open submissions (LoadResident), every
// conversation, the documents of the session, of each conversation, and of
// each live task (an ended task's documents are retired with it), and each
// conversation's context range. ok is false for a store an older version
// wrote, which must be loaded whole to be upgraded; nil for an empty store.
func loadResidentState(ctx context.Context, records RecordStorage) (st *State, ok bool, err error) {
	st, err = records.LoadResident(ctx)
	if err != nil || st == nil {
		return nil, err == nil, err
	}
	if st.Version != stateVersion {
		return nil, false, nil
	}
	// One logical load, as Storage.Load is: once begun, it completes, and
	// Open's own checks see a cancellation.
	ctx = context.WithoutCancel(ctx)
	ensureTables(st)
	var cursor *ScanCursor
	for {
		page, err := records.ScanConversations(ctx, ConversationQuery{}, 512, cursor)
		if err != nil {
			return nil, false, err
		}
		for i := range page.Items {
			conversation := page.Items[i]
			st.Conversations[conversation.ID] = &conversation
		}
		if page.Next == nil {
			break
		}
		cursor = page.Next
	}
	scopes := []DocumentQuery{{Scope: "session"}}
	for id := range st.Conversations {
		scopes = append(scopes, DocumentQuery{Scope: "conversation", ScopeID: uint64(id)})
	}
	for id := range st.Tasks {
		scopes = append(scopes, DocumentQuery{Scope: "task", ScopeID: uint64(id)})
	}
	for _, scope := range scopes {
		var cursor *ScanCursor
		for {
			page, err := records.ScanDocuments(ctx, scope, 512, cursor)
			if err != nil {
				return nil, false, err
			}
			for i := range page.Items {
				doc := page.Items[i]
				key := documentKey(DocumentDefinition{Kind: doc.Kind, Scope: doc.Scope}, doc.ScopeID)
				if doc.Member != "" {
					key += "/" + doc.Member
				}
				st.Documents[key] = &doc
			}
			if page.Next == nil {
				break
			}
			cursor = page.Next
		}
	}
	// Each conversation's context range, read from storage.
	view := &State{Conversations: st.Conversations, cold: &coldRecords{storage: records, fail: func(error) {}}}
	for id := range st.Conversations {
		entries, err := rangeEntries(view, id)
		if err != nil {
			return nil, false, err
		}
		for i := range entries {
			entry := entries[i]
			st.Entries[entry.ID] = &entry
		}
	}
	return st, true, nil
}
