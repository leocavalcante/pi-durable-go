package durable

import (
	"context"
	"fmt"
	"sort"
)

// RecordStorage is a DeltaStorage that reads stored records one at a time
// (upstream's record-level Storage), so a Session can keep only the records it
// needs in memory (plan 0008). LoadResident loads the header, the tasks that
// are not terminal, and the submissions that are queued or placed; every other
// record is read on demand. Lookups return nil without an error for a record
// that is not stored; results are detached copies; scans page by ID.
type RecordStorage interface {
	DeltaStorage
	LoadResident(ctx context.Context) (*State, error)
	Conversation(ctx context.Context, id ConversationID) (*ConversationRecord, error)
	ScanConversations(ctx context.Context, query ConversationQuery, limit int, cursor *ScanCursor) (ScanPage[ConversationRecord], error)
	Entry(ctx context.Context, id EntryID) (*Entry, error)
	// ScanEntries pages through the entries visible from a conversation: its
	// own, then its fork parent's up to the fork point, and so on.
	ScanEntries(ctx context.Context, cid ConversationID, query EntryQuery, limit int, cursor *EntryCursor) (EntryPage, error)
	Task(ctx context.Context, id TaskID) (*TaskRecord, error)
	ScanTasks(ctx context.Context, query TaskQuery, limit int, cursor *ScanCursor) (ScanPage[TaskRecord], error)
	Submission(ctx context.Context, id SubmissionID) (*SubmissionRecord, error)
	SubmissionByRequest(ctx context.Context, cid ConversationID, requestID string) (*SubmissionRecord, error)
	ScanSubmissions(ctx context.Context, query SubmissionQuery, limit int, cursor *ScanCursor) (ScanPage[SubmissionRecord], error)
	Document(ctx context.Context, key string) (*DocumentRecord, error)
	ScanDocuments(ctx context.Context, query DocumentQuery, limit int, cursor *ScanCursor) (ScanPage[DocumentRecord], error)
}

// SubmissionQuery filters a scan of submissions (upstream SubmissionQuery);
// zero fields match every submission. Order is ascending (default) or
// descending.
type SubmissionQuery struct {
	Conversation ConversationID
	Status       string
	Order        string
}

// DocumentQuery filters a scan of documents (upstream DocumentQuery): those
// of one scope (session, conversation, or task, with its ID), of one kind
// when Kind is set. Documents are scanned by storage key, ascending.
type DocumentQuery struct {
	Scope   string
	ScopeID uint64
	Kind    string
}

// residentTask and residentSubmission say which records a bounded Session
// keeps in memory: a task that is not terminal, a submission queued or
// placed.
func residentTask(t *TaskRecord) bool { return t.Status != "terminal" }
func residentSubmission(s *SubmissionRecord) bool {
	return s.Status == "queued" || s.Status == "placed"
}

// StateRecords serves RecordStorage reads from a State a storage keeps whole
// in memory, as MemoryStorage and JSONL storage do. The caller serializes
// access to the State; results are detached copies.
type StateRecords struct{ State *State }

// Resident is the State's header with its live tasks and open submissions.
func (r StateRecords) Resident() (*State, error) {
	if r.State == nil {
		return nil, nil
	}
	out := &State{Version: r.State.Version, Sequence: r.State.Sequence, NextID: r.State.NextID, Root: r.State.Root}
	ensureTables(out)
	for id, task := range r.State.Tasks {
		if residentTask(task) {
			clone, err := strictClone(task)
			if err != nil {
				return nil, err
			}
			out.Tasks[id] = clone
		}
	}
	for id, sub := range r.State.Submissions {
		if residentSubmission(sub) {
			clone, err := strictClone(sub)
			if err != nil {
				return nil, err
			}
			out.Submissions[id] = clone
		}
	}
	return out, nil
}

func (r StateRecords) Conversation(id ConversationID) *ConversationRecord {
	if r.State == nil || r.State.Conversations[id] == nil {
		return nil
	}
	return cloneRecord(r.State.Conversations[id])
}

func (r StateRecords) ScanConversations(query ConversationQuery, limit int, cursor *ScanCursor) (ScanPage[ConversationRecord], error) {
	if r.State == nil {
		return scanRecords(map[ConversationID]*ConversationRecord{}, query.Order, limit, cursor, func(*ConversationRecord) bool { return true })
	}
	return scanRecords(r.State.Conversations, query.Order, limit, cursor, conversationFilter(r.State, query))
}

func (r StateRecords) Entry(id EntryID) *Entry {
	if r.State == nil || r.State.Entries[id] == nil {
		return nil
	}
	e := r.State.Entries[id].detached()
	return &e
}

func (r StateRecords) ScanEntries(cid ConversationID, query EntryQuery, limit int, cursor *EntryCursor) (EntryPage, error) {
	if err := checkEntryScan(query, limit, cursor); err != nil {
		return EntryPage{}, err
	}
	if r.State == nil {
		return EntryPage{}, ErrNotFound
	}
	entries, err := entriesFromState(r.State, cid)
	if err != nil {
		return EntryPage{}, err
	}
	return scanEntries(entries, query, limit, cursor), nil
}

func (r StateRecords) Task(id TaskID) *TaskRecord {
	if r.State == nil || r.State.Tasks[id] == nil {
		return nil
	}
	return cloneRecord(r.State.Tasks[id])
}

func (r StateRecords) ScanTasks(query TaskQuery, limit int, cursor *ScanCursor) (ScanPage[TaskRecord], error) {
	tasks := map[TaskID]*TaskRecord{}
	if r.State != nil {
		tasks = r.State.Tasks
	}
	return scanRecords(tasks, query.Order, limit, cursor, taskFilter(query))
}

func (r StateRecords) Submission(id SubmissionID) *SubmissionRecord {
	if r.State == nil || r.State.Submissions[id] == nil {
		return nil
	}
	return cloneRecord(r.State.Submissions[id])
}

func (r StateRecords) SubmissionByRequest(cid ConversationID, requestID string) *SubmissionRecord {
	if r.State == nil || requestID == "" {
		return nil
	}
	for _, sub := range r.State.Submissions {
		if sub.Conversation == cid && sub.Request.RequestID == requestID {
			return cloneRecord(sub)
		}
	}
	return nil
}

func (r StateRecords) ScanSubmissions(query SubmissionQuery, limit int, cursor *ScanCursor) (ScanPage[SubmissionRecord], error) {
	subs := map[SubmissionID]*SubmissionRecord{}
	if r.State != nil {
		subs = r.State.Submissions
	}
	return scanRecords(subs, query.Order, limit, cursor, submissionFilter(query))
}

func (r StateRecords) Document(key string) *DocumentRecord {
	if r.State == nil || r.State.Documents[key] == nil {
		return nil
	}
	return cloneRecord(r.State.Documents[key])
}

func (r StateRecords) ScanDocuments(query DocumentQuery, limit int, cursor *ScanCursor) (ScanPage[DocumentRecord], error) {
	if err := checkDocumentScan(query, limit, cursor); err != nil {
		return ScanPage[DocumentRecord]{}, err
	}
	var keys []string
	if r.State != nil {
		for key, doc := range r.State.Documents {
			if DocumentMatches(doc, query) && (cursor == nil || key > cursor.AfterKey) {
				keys = append(keys, key)
			}
		}
	}
	sort.Strings(keys)
	var page ScanPage[DocumentRecord]
	for i, key := range keys {
		if i == limit {
			page.Next = &ScanCursor{AfterKey: keys[i-1]}
			break
		}
		page.Items = append(page.Items, *cloneRecord(r.State.Documents[key]))
	}
	return page, nil
}

// DocumentMatches reports whether a document is in a query's scope and kind.
func DocumentMatches(doc *DocumentRecord, query DocumentQuery) bool {
	scopeID := query.ScopeID
	if query.Scope == "session" {
		scopeID = 0
	}
	return doc.Scope == query.Scope && doc.ScopeID == scopeID && (query.Kind == "" || doc.Kind == query.Kind)
}

func checkDocumentScan(query DocumentQuery, limit int, cursor *ScanCursor) error {
	if limit <= 0 {
		return fmt.Errorf("%w: limit must be positive", ErrInvalid)
	}
	switch query.Scope {
	case "session", "conversation", "task":
	default:
		return fmt.Errorf("%w: document scope %q", ErrInvalid, query.Scope)
	}
	if cursor != nil && cursor.Order != "" && cursor.Order != "ascending" {
		return fmt.Errorf("%w: documents scan ascending", ErrInvalid)
	}
	return nil
}

// conversationFilter matches a ConversationQuery against a State's records.
func conversationFilter(st *State, query ConversationQuery) func(*ConversationRecord) bool {
	return func(c *ConversationRecord) bool {
		if query.OwnerTask != 0 && c.Owner.Task != query.OwnerTask {
			return false
		}
		if query.OwnerConversation != 0 {
			if c.Owner.Task == 0 {
				return false
			}
			owner, _ := st.task(c.Owner.Task) // an ended owner too
			return owner != nil && owner.Conversation == query.OwnerConversation
		}
		return true
	}
}

func taskFilter(query TaskQuery) func(*TaskRecord) bool {
	return func(t *TaskRecord) bool {
		return (query.Conversation == 0 || t.Conversation == query.Conversation) &&
			(query.Kind == "" || t.Kind == query.Kind) &&
			(query.Status == "" || t.Status == query.Status) &&
			(query.AbortRequested == nil || t.AbortRequested == *query.AbortRequested) &&
			(query.Background == nil || t.Background == *query.Background)
	}
}

func submissionFilter(query SubmissionQuery) func(*SubmissionRecord) bool {
	return func(s *SubmissionRecord) bool {
		return (query.Conversation == 0 || s.Conversation == query.Conversation) &&
			(query.Status == "" || s.Status == query.Status)
	}
}
