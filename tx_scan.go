package durable

import (
	"fmt"
	"sort"
)

// ScanCursor continues a scan of records ordered by ID; it carries the
// scan's order.
type ScanCursor struct {
	After uint64
	Order string
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
	return scanRecords(tx.state.Conversations, query.Order, limit, cursor, func(c *ConversationRecord) bool {
		if query.OwnerTask != 0 && c.Owner.Task != query.OwnerTask {
			return false
		}
		if query.OwnerConversation != 0 {
			owner := tx.state.Tasks[c.Owner.Task]
			return c.Owner.Task != 0 && owner != nil && owner.Conversation == query.OwnerConversation
		}
		return true
	})
}

// ScanTasks pages through task records matching every filter, by ID, inside
// a commit (upstream tx.scanTasks).
func (tx *Tx) ScanTasks(query TaskQuery, limit int, cursor *ScanCursor) (ScanPage[TaskRecord], error) {
	if err := tx.check(); err != nil {
		return ScanPage[TaskRecord]{}, err
	}
	return scanRecords(tx.state.Tasks, query.Order, limit, cursor, func(t *TaskRecord) bool {
		return (query.Conversation == 0 || t.Conversation == query.Conversation) &&
			(query.Kind == "" || t.Kind == query.Kind) &&
			(query.Status == "" || t.Status == query.Status) &&
			(query.AbortRequested == nil || t.AbortRequested == *query.AbortRequested) &&
			(query.Background == nil || t.Background == *query.Background)
	})
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
	for _, sub := range tx.state.Submissions {
		if sub.Conversation == cid && sub.Request.RequestID == requestID {
			return *cloneRecord(sub), nil
		}
	}
	return SubmissionRecord{}, ErrNotFound
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
