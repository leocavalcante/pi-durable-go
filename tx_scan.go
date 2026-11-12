package durable

import (
	"context"
	"fmt"
	"sort"
)

// ScanCursor continues a scan of records ordered by ID (After) or, for
// documents, by storage key (AfterKey); it carries the scan's order.
type ScanCursor struct {
	After    uint64
	AfterKey string
	Order    string
}

// ScanPage is one page of a scan; Next is nil after the last page. Its
// records are detached copies.
type ScanPage[T any] struct {
	Items []T
	Next  *ScanCursor
}

// ConversationQuery filters a scan of conversations (upstream
// ConversationQuery): those owned by OwnerTask, or by a task of
// OwnerConversation (0: any); Order is ascending (default) or descending.
type ConversationQuery struct {
	OwnerConversation ConversationID
	OwnerTask         TaskID
	Order             string
}

// TaskQuery filters a scan of task records (upstream TaskQuery); zero
// fields match every task. Order is ascending (default) or descending.
type TaskQuery struct {
	Conversation   ConversationID
	Kind           string
	Status         string
	AbortRequested *bool
	Background     *bool
	Order          string
}

// ScanConversations pages through conversations by ID inside a commit
// (upstream tx.scanConversations).
func (tx *Tx) ScanConversations(query ConversationQuery, limit int, cursor *ScanCursor) (ScanPage[ConversationRecord], error) {
	if err := tx.check(); err != nil {
		return ScanPage[ConversationRecord]{}, err
	}
	return scanRecords(tx.state.Conversations, query.Order, limit, cursor, conversationFilter(tx.state, query))
}

// ScanTasks pages through task records matching every filter, by ID, inside
// a commit (upstream tx.scanTasks).
func (tx *Tx) ScanTasks(query TaskQuery, limit int, cursor *ScanCursor) (ScanPage[TaskRecord], error) {
	if err := tx.check(); err != nil {
		return ScanPage[TaskRecord]{}, err
	}
	if tx.state.cold == nil {
		return scanRecords(tx.state.Tasks, query.Order, limit, cursor, taskFilter(query))
	}
	// A bounded Session keeps live tasks only: merge the ended ones from
	// storage, where the State's own record of a task decides.
	return scanMerged(tx.state.Tasks, query.Order, limit, cursor, taskFilter(query), func(c *ScanCursor, n int) (ScanPage[TaskRecord], error) {
		return tx.state.cold.storage.ScanTasks(context.Background(), query, n, c)
	}, func(t *TaskRecord) uint64 { return uint64(t.ID) })
}

// ScanEntries pages through a conversation's fork-aware history inside a
// commit, as Conversation.ScanEntries does (upstream tx.scanEntries).
func (tx *Tx) ScanEntries(cid ConversationID, query EntryQuery, limit int, cursor *EntryCursor) (EntryPage, error) {
	if err := tx.check(); err != nil {
		return EntryPage{}, err
	}
	if err := checkEntryScan(query, limit, cursor); err != nil {
		return EntryPage{}, err
	}
	entries, err := entriesFromState(tx.state, cid)
	if err != nil {
		return EntryPage{}, err
	}
	page := scanEntries(entries, query, limit, cursor)
	for i := range page.Items {
		page.Items[i] = page.Items[i].detached()
	}
	return page, nil
}

// LatestEntryID is the newest entry visible from a conversation; 0 when it
// has none (upstream tx.latestEntryId).
func (tx *Tx) LatestEntryID(cid ConversationID) (EntryID, error) {
	if err := tx.check(); err != nil {
		return 0, err
	}
	c := tx.state.Conversations[cid]
	if c == nil {
		return 0, ErrNotFound
	}
	return c.Head, nil
}

// SubmissionByRequest is the submission a conversation admitted with a
// request ID, detached; ErrNotFound when there is none (upstream
// tx.submissionByRequest).
func (tx *Tx) SubmissionByRequest(cid ConversationID, requestID string) (SubmissionRecord, error) {
	if err := tx.check(); err != nil {
		return SubmissionRecord{}, err
	}
	if requestID == "" {
		return SubmissionRecord{}, fmt.Errorf("%w: a request ID is required", ErrInvalid)
	}
	sub, err := tx.state.submissionByRequest(cid, requestID)
	if err != nil {
		return SubmissionRecord{}, err
	}
	if sub == nil {
		return SubmissionRecord{}, ErrNotFound
	}
	return *cloneRecord(sub), nil
}

// scanRecords pages through a table's records matching keep, by ID.
func scanRecords[K ~uint64, V any](table map[K]*V, order string, limit int, cursor *ScanCursor, keep func(*V) bool) (ScanPage[V], error) {
	if limit <= 0 {
		return ScanPage[V]{}, fmt.Errorf("%w: limit must be positive", ErrInvalid)
	}
	if cursor != nil {
		order = cursor.Order
	}
	if order == "" {
		order = "ascending"
	}
	if order != "ascending" && order != "descending" {
		return ScanPage[V]{}, fmt.Errorf("%w: order", ErrInvalid)
	}
	ids := make([]K, 0, len(table))
	for id, record := range table {
		if cursor != nil && (order == "ascending" && uint64(id) <= cursor.After || order == "descending" && uint64(id) >= cursor.After) {
			continue
		}
		if keep(record) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		if order == "descending" {
			return ids[i] > ids[j]
		}
		return ids[i] < ids[j]
	})
	var page ScanPage[V]
	for i, id := range ids {
		if i == limit {
			page.Next = &ScanCursor{After: uint64(ids[i-1]), Order: order}
			break
		}
		page.Items = append(page.Items, *cloneRecord(table[id]))
	}
	return page, nil
}

// scanMerged pages through a table's records matching keep: the State's own,
// merged by ID with stored ones it does not keep (stored, which pages through
// storage with the same filter).
func scanMerged[K ~uint64, V any](table map[K]*V, order string, limit int, cursor *ScanCursor, keep func(*V) bool,
	stored func(*ScanCursor, int) (ScanPage[V], error), idOf func(*V) uint64) (ScanPage[V], error) {
	if limit <= 0 {
		return ScanPage[V]{}, fmt.Errorf("%w: limit must be positive", ErrInvalid)
	}
	if cursor != nil {
		order = cursor.Order
	}
	if order == "" {
		order = "ascending"
	}
	if order != "ascending" && order != "descending" {
		return ScanPage[V]{}, fmt.Errorf("%w: order", ErrInvalid)
	}
	before := func(a, b uint64) bool {
		if order == "descending" {
			return a > b
		}
		return a < b
	}
	// The State's own matches, past the cursor, in order.
	var own []uint64
	for id, record := range table {
		if cursor != nil && !before(cursor.After, uint64(id)) {
			continue
		}
		if keep(record) {
			own = append(own, uint64(id))
		}
	}
	sort.Slice(own, func(i, j int) bool { return before(own[i], own[j]) })
	// Stored records the State does not keep, a page at a time.
	var pending []V
	storedCursor := cursor // the stored scan's query carries the order
	exhausted := false
	next := func() (*V, error) {
		for len(pending) == 0 && !exhausted {
			page, err := stored(storedCursor, limit+1)
			if err != nil {
				return nil, err
			}
			for _, record := range page.Items {
				if _, ok := table[K(idOf(&record))]; !ok {
					pending = append(pending, record)
				}
			}
			if page.Next == nil {
				exhausted = true
			}
			storedCursor = page.Next
		}
		if len(pending) == 0 {
			return nil, nil
		}
		return &pending[0], nil
	}
	var page ScanPage[V]
	var last uint64
	for {
		storedHead, err := next()
		if err != nil {
			return ScanPage[V]{}, err
		}
		var take *V
		switch {
		case len(own) > 0 && (storedHead == nil || before(own[0], idOf(storedHead))):
			take = cloneRecord(table[K(own[0])])
			own = own[1:]
		case storedHead != nil:
			record := *storedHead
			take = &record
			pending = pending[1:]
		default:
			return page, nil
		}
		if len(page.Items) == limit {
			page.Next = &ScanCursor{After: last, Order: order}
			return page, nil
		}
		page.Items = append(page.Items, *take)
		last = idOf(take)
	}
}
