package durable

import (
	"context"
	"fmt"
	"time"
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
	c, err := st.conversation(cid)
	if err != nil {
		return err
	}
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

// keepEntries keeps the context ranges of the conversations a bounded
// Session has loaded, as upstream's scheduler keeps context ranges: a
// conversation a published commit touches is loaded (its range read in, into
// head too when that is a later State), its range start follows the head
// markers appended to it, and once it has been idle (not busy) for the
// context retention it is unloaded. Each conversation keeps its own entries
// from the smallest start among the loaded conversations whose lineage
// passes through it; the others leave st. Reads of what is not kept fall
// back to storage. The caller holds the line; st is not shared yet.
// laterState is a State admitted after the one being published, with the
// delta that made it (nil for a held commit).
type laterState struct {
	state *State
	delta *StateDelta
}

func (c *coldRecords) keepEntries(st *State, later []laterState, delta *StateDelta, now time.Time, retention, documentRetention time.Duration, held map[string]time.Time) error {
	changed := false
	if delta != nil {
		touched := map[ConversationID]bool{}
		for id := range delta.Conversations {
			touched[id] = true
		}
		markers := map[ConversationID]EntryID{}
		for _, e := range delta.Entries {
			touched[e.Conversation] = true
			if isHeadMarker(*e) && e.ID >= markers[e.Conversation] {
				markers[e.Conversation] = e.ID
			}
		}
		for id := range touched {
			if st.Conversations[id] == nil {
				continue
			}
			if _, loaded := c.loaded[id]; !loaded {
				if err := c.load(st, later, id, now); err != nil {
					return err
				}
				changed = true
			} else if marker, ok := markers[id]; ok {
				c.starts[id] = markerBound(*st.Entries[marker])
				changed = true
			}
			c.loaded[id] = now
		}
		// Documents a commit wrote are in use.
		c.mu.Lock()
		for key := range delta.Documents {
			c.documentTouched[key] = now
		}
		for _, key := range delta.Removed.Documents {
			delete(c.documentTouched, key)
		}
		c.mu.Unlock()
	}
	for id, touched := range c.loaded {
		conversation := st.Conversations[id]
		if conversation == nil || conversation.Busy == 0 && now.Sub(touched) >= retention {
			delete(c.loaded, id)
			delete(c.starts, id)
			changed = true
		}
	}
	if changed {
		c.evictEntries(st, held)
	}
	idle := c.evictIdleDocuments(st, now, documentRetention, held)
	if changed || idle || c.snapshot == nil {
		// A loaded conversation one of whose documents left is not
		// complete: readers merge storage's documents for it.
		c.snapshot = map[ConversationID]bool{}
		c.mu.Lock()
		for id := range c.loaded {
			if !c.incomplete[id] {
				c.snapshot[id] = true
			}
		}
		c.mu.Unlock()
	}
	st.loaded = c.snapshot
	c.scheduleExpiry(st, now, retention, documentRetention)
	return nil
}

// load reads a conversation's context range and documents into st and
// every later State admitted meanwhile, so each of them holds all of a
// loaded conversation's documents (State.loaded promises it); a document a
// later commit retired stays out of that State and those after it.
func (c *coldRecords) load(st *State, later []laterState, id ConversationID, now time.Time) error {
	entries, err := rangeEntries(st, id)
	if err != nil {
		return err
	}
	docs, err := st.conversationDocuments(id)
	if err != nil {
		return err
	}
	insert := func(target *State, removed map[string]bool) {
		for _, doc := range docs {
			key := keyOfRecord(doc)
			if target.Documents[key] == nil && !removed[key] {
				target.Documents[key] = doc
			}
		}
		for i := range entries {
			entry := entries[i]
			if target.Entries[entry.ID] == nil {
				target.Entries[entry.ID] = &entry
			}
		}
	}
	insert(st, nil)
	c.mu.Lock()
	for _, doc := range docs {
		c.documentTouched[keyOfRecord(doc)] = now
	}
	delete(c.incomplete, id)
	c.mu.Unlock()
	removed := map[string]bool{}
	for _, next := range later {
		if next.state == st {
			continue
		}
		if next.delta != nil {
			for _, key := range next.delta.Removed.Documents {
				removed[key] = true
			}
		}
		insert(next.state, removed)
	}
	start := EntryID(0)
	for i := len(entries) - 1; i >= 0; i-- {
		if isHeadMarker(entries[i]) {
			start = markerBound(entries[i])
			break
		}
	}
	c.starts[id] = start
	return nil
}

// evictEntries removes from st the entries no loaded conversation keeps,
// and the documents of conversations not loaded, except held ones (their
// changes are not stored yet).
func (c *coldRecords) evictEntries(st *State, held map[string]time.Time) {
	c.mu.Lock()
	for key, doc := range st.Documents {
		if doc.Scope == "conversation" && c.loaded[ConversationID(doc.ScopeID)].IsZero() {
			if _, pinned := held[key]; !pinned {
				delete(st.Documents, key)
				delete(c.documentTouched, key)
			}
		}
	}
	for id := range c.incomplete {
		if c.loaded[id].IsZero() {
			delete(c.incomplete, id)
		}
	}
	c.mu.Unlock()
	keep := map[ConversationID]EntryID{}
	for id := range c.loaded {
		start := c.starts[id]
		seen := map[ConversationID]bool{}
		for at := id; at != 0 && !seen[at]; {
			seen[at] = true
			if current, ok := keep[at]; !ok || start < current {
				keep[at] = start
			}
			conversation, _ := st.conversation(at)
			if conversation == nil {
				break
			}
			at = conversation.Parent
		}
	}
	for id, e := range st.Entries {
		if from, ok := keep[e.Conversation]; !ok || e.ID < from {
			delete(st.Entries, id)
		}
	}
}

// scheduleExpiry arms the sweep that unloads the next idle conversation, for
// a Session that publishes nothing meanwhile.
func (c *coldRecords) scheduleExpiry(st *State, now time.Time, retention, documentRetention time.Duration) {
	var next time.Time
	consider := func(at time.Time) {
		if next.IsZero() || at.Before(next) {
			next = at
		}
	}
	if retention < maxRetention {
		for id, touched := range c.loaded {
			if conversation := st.Conversations[id]; conversation != nil && conversation.Busy == 0 {
				consider(touched.Add(retention))
			}
		}
	}
	if documentRetention < maxRetention {
		c.mu.Lock()
		for _, touched := range c.documentTouched {
			consider(touched.Add(documentRetention))
		}
		c.mu.Unlock()
	}
	if c.expiry != nil {
		c.expiry()
		c.expiry = nil
	}
	if !next.IsZero() && c.sweep != nil && c.timer != nil {
		c.expiry = c.timer(c.sweep, next.Sub(now))
	}
}

// maxRetention stands for an unlimited retention (math.Inf(1)).
const maxRetention = time.Duration(1<<63 - 1)

// loadResidentState loads what a bounded Session keeps at open (plan 0008):
// the header, live tasks, and open submissions (LoadResident), the
// conversations in use, and the documents of the session and of each live
// task (an ended task's documents are retired with it). Other conversations,
// context ranges, and conversation documents load as they are used. ok is false for a store an older version
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
	// The conversations in use: the root, those of live tasks and open
	// submissions, and those live tasks own. Others load as they are used.
	want := map[ConversationID]bool{}
	if st.Root != 0 {
		want[st.Root] = true
	}
	for id, task := range st.Tasks {
		want[task.Conversation] = true
		var cursor *ScanCursor
		for {
			page, err := records.ScanConversations(ctx, ConversationQuery{OwnerTask: id}, 256, cursor)
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
	}
	for _, sub := range st.Submissions {
		want[sub.Conversation] = true
	}
	for id := range want {
		if st.Conversations[id] != nil {
			continue
		}
		conversation, err := records.Conversation(ctx, id)
		if err != nil {
			return nil, false, err
		}
		if conversation != nil {
			st.Conversations[id] = conversation
		}
	}
	// Conversation documents load with their conversations.
	scopes := []DocumentQuery{{Scope: "session"}}
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
	return st, true, nil
}
