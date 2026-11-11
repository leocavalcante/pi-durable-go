package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

// TranscriptItem is one item of a client transcript: an entry; a gap, whose
// entries strictly between After and UpTo are missing (render "load more"
// and call FillGap); or "earlier", always first, when the transcript
// continues above (call LoadEarlier).
type TranscriptItem struct {
	Kind  string  `json:"kind"`
	Entry *Entry  `json:"entry,omitempty"`
	After EntryID `json:"after,omitempty"`
	UpTo  EntryID `json:"upTo,omitempty"`
}

// TranscriptView is a conversation view as a renderer sees it: the client's
// whole transcript as Items instead of the window.
type TranscriptView struct {
	Conversation ConversationRecord
	Docs         map[string]json.RawMessage
	Items        []TranscriptItem
}

// EntryLoader pages a conversation's entries, as ScanEntries does without a
// cursor.
type EntryLoader func(ctx context.Context, query EntryQuery, limit int) (EntryPage, error)

// ConversationTranscript is what a client has of one conversation (upstream
// spec 9.3): every entry it got from a view or fetched, and the gaps
// between. Merge every value of a WatchWindow; a window whose Before lies
// past the newest entry held does not continue the transcript, and the
// entries in between become a gap. Entries never change once committed, so
// the transcript only grows. Calls are serialized; it is safe for
// concurrent use.
type ConversationTranscript struct {
	mu       sync.Mutex
	load     EntryLoader
	pageSize int
	entries  []Entry
	ids      map[EntryID]bool
	gaps     []TranscriptItem
	started  bool
	low      int64 // exclusive lower bound of the known range; minInf at the start
	tail     int64 // inclusive upper bound: newest entry, or a later empty window's Before
	last     *ConversationView
}

const minInf = math.MinInt64

// NewConversationTranscript starts an empty transcript loading pages of
// pageSize entries (default 50) through load.
func NewConversationTranscript(load EntryLoader, pageSize int) (*ConversationTranscript, error) {
	if pageSize == 0 {
		pageSize = 50
	}
	if pageSize < 1 || load == nil {
		return nil, fmt.Errorf("%w: page size must be at least 1 and a loader is required", ErrInvalid)
	}
	return &ConversationTranscript{load: load, pageSize: pageSize, ids: map[EntryID]bool{}, low: minInf, tail: minInf}, nil
}

// Transcript starts a client transcript of this conversation, loading
// earlier pages and gaps through ScanEntries.
func (c *Conversation) Transcript(pageSize int) (*ConversationTranscript, error) {
	return NewConversationTranscript(func(ctx context.Context, query EntryQuery, limit int) (EntryPage, error) {
		return c.ScanEntries(ctx, query, limit, nil)
	}, pageSize)
}

// Newest is the ID the transcript reaches: its newest entry, or the Before
// of a later empty window; false before the first merge and when empty.
func (t *ConversationTranscript) Newest() (EntryID, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.newest()
}

func (t *ConversationTranscript) newest() (EntryID, bool) {
	if t.tail == minInf {
		return 0, false
	}
	return EntryID(t.tail), true
}

// Resume is the window that continues this transcript after a dropped
// watch: From right after Newest, or the default window when empty; false
// before the first merge.
func (t *ConversationTranscript) Resume() (ConversationWindow, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.started {
		return ConversationWindow{}, false
	}
	if newest, ok := t.newest(); ok {
		return ConversationWindow{From: newest + 1}, true
	}
	return ConversationWindow{}, true
}

// Merge takes one value of a view: its window's entries join the transcript.
func (t *ConversationTranscript) Merge(view ConversationView) TranscriptView {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.last = &view
	before := int64(minInf)
	if view.Before != 0 {
		before = int64(view.Before)
	}
	hi := before
	if n := len(view.Entries); n > 0 {
		hi = int64(view.Entries[n-1].ID)
	}
	t.cover(before, hi, view.Entries)
	return t.result()
}

// LoadEarlier loads the page before the oldest entry held; with nothing
// earlier it returns the current view.
func (t *ConversationTranscript) LoadEarlier(ctx context.Context) (TranscriptView, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.last == nil {
		return TranscriptView{}, errors.New("durable: ConversationTranscript: merge a view first")
	}
	if t.low == minInf {
		return t.result(), nil
	}
	low := t.low
	page, err := t.load(ctx, EntryQuery{MaxEntryID: EntryID(low), Order: "descending"}, t.pageSize)
	if err != nil {
		return TranscriptView{}, err
	}
	items := append([]Entry(nil), page.Items...)
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
	if page.Next == nil || len(items) == 0 {
		t.cover(minInf, low, items)
	} else {
		t.cover(int64(items[0].ID)-1, low, items)
	}
	return t.result(), nil
}

// FillGap loads the next page of gap, a gap item of Items, shrinking it from
// its start or removing it once nothing is missing. A gap that has shrunk
// since loads the gap it became; one no longer there returns the current
// view.
func (t *ConversationTranscript) FillGap(ctx context.Context, gap TranscriptItem) (TranscriptView, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.last == nil {
		return TranscriptView{}, errors.New("durable: ConversationTranscript: merge a view first")
	}
	var current *TranscriptItem
	for i := range t.gaps {
		if t.gaps[i].After == gap.After && t.gaps[i].UpTo == gap.UpTo {
			current = &t.gaps[i]
			break
		}
	}
	if current == nil {
		for i := range t.gaps {
			if t.gaps[i].After < gap.UpTo && gap.After < t.gaps[i].UpTo {
				current = &t.gaps[i]
				break
			}
		}
	}
	if current == nil {
		return t.result(), nil
	}
	after, upTo := current.After, current.UpTo
	if upTo <= after+1 {
		t.cover(int64(after), int64(upTo)-1, nil)
		return t.result(), nil
	}
	page, err := t.load(ctx, EntryQuery{MinEntryID: after + 1, MaxEntryID: upTo - 1, Order: "ascending"}, t.pageSize)
	if err != nil {
		return TranscriptView{}, err
	}
	if page.Next == nil || len(page.Items) == 0 {
		t.cover(int64(after), int64(upTo)-1, page.Items)
	} else {
		t.cover(int64(after), int64(page.Items[len(page.Items)-1].ID), page.Items)
	}
	return t.result(), nil
}

// cover records that every entry with an ID in (lo, hi] is known, added
// being those entries in ID order.
func (t *ConversationTranscript) cover(lo, hi int64, added []Entry) {
	if lo != minInf && lo <= 0 {
		lo = minInf // no entry has an ID at or below 0
	}
	held := len(t.entries)
	for _, e := range added {
		if !t.ids[e.ID] {
			t.ids[e.ID] = true
			t.entries = append(t.entries, e)
		}
	}
	sort.SliceStable(t.entries, func(i, j int) bool { return t.entries[i].ID < t.entries[j].ID })
	switch {
	case !t.started || (held == 0 && len(t.gaps) == 0 && lo >= t.tail):
		t.started = true
		t.low = lo
		if hi > t.tail {
			t.tail = hi
		}
	case lo > t.tail:
		upTo := lo + 1
		if len(added) > 0 {
			upTo = int64(added[0].ID)
		}
		t.gaps = append(t.gaps, TranscriptItem{Kind: "gap", After: EntryID(t.tail), UpTo: EntryID(upTo)})
		t.tail = hi
	case hi != minInf && hi < t.low:
		upTo := t.low + 1
		for _, e := range t.entries {
			if int64(e.ID) > hi && !containsEntry(added, e.ID) {
				upTo = int64(e.ID)
				break
			}
		}
		t.gaps = append([]TranscriptItem{{Kind: "gap", After: EntryID(hi), UpTo: EntryID(upTo)}}, t.gaps...)
		t.low = lo
	default:
		t.gaps = t.subtract(lo, hi)
		if lo < t.low {
			t.low = lo
		}
		if hi > t.tail {
			t.tail = hi
		}
	}
}

func containsEntry(entries []Entry, id EntryID) bool {
	for _, e := range entries {
		if e.ID == id {
			return true
		}
	}
	return false
}

// subtract returns the gaps without (lo, hi].
func (t *ConversationTranscript) subtract(lo, hi int64) []TranscriptItem {
	if hi <= lo {
		return t.gaps
	}
	var gaps []TranscriptItem
	for _, gap := range t.gaps {
		after, upTo := int64(gap.After), int64(gap.UpTo)
		if hi <= after || lo+1 >= upTo {
			gaps = append(gaps, gap)
			continue
		}
		if lo > after {
			firstKnown := lo + 1
			for _, e := range t.entries {
				if int64(e.ID) > lo {
					firstKnown = int64(e.ID)
					break
				}
			}
			gaps = append(gaps, TranscriptItem{Kind: "gap", After: gap.After, UpTo: EntryID(min(upTo, firstKnown))})
		}
		if hi+1 < upTo {
			gaps = append(gaps, TranscriptItem{Kind: "gap", After: EntryID(hi), UpTo: gap.UpTo})
		}
	}
	return gaps
}

// result builds the items: "earlier" until the start is known, then the
// entries with each gap before the entry after it.
func (t *ConversationTranscript) result() TranscriptView {
	var items []TranscriptItem
	if t.low != minInf {
		items = append(items, TranscriptItem{Kind: "earlier"})
	}
	next := 0
	for i := range t.entries {
		for next < len(t.gaps) && t.gaps[next].After < t.entries[i].ID {
			items = append(items, t.gaps[next])
			next++
		}
		entry := t.entries[i]
		items = append(items, TranscriptItem{Kind: "entry", Entry: &entry})
	}
	items = append(items, t.gaps[next:]...)
	view := TranscriptView{Items: items}
	if t.last != nil {
		view.Conversation, view.Docs = t.last.Conversation, t.last.Docs
	}
	return view
}
