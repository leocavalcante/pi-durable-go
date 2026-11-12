package durable

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

// Bounded Session memory (plan 0008 M2). Over a RecordStorage the Session
// keeps live work in its State: tasks that are not terminal, the ended tasks
// a live task waits on, and submissions queued or placed. Every other task
// and submission leaves the State as the commit that settled it is
// published (never through a delta: storage keeps it) and is read by ID
// through coldRecords, an LRU cache in front of storage, as upstream's
// CachedStorage does.

const (
	coldTaskCapacity         = 256
	coldSubmissionCapacity   = 256
	coldDocumentCapacity     = 256
	coldConversationCapacity = 4096
	// defaultContextRetention is upstream's settings.contextRetentionMs.
	defaultContextRetention = 10 * time.Minute
)

// coldRecords reads records a bounded Session no longer keeps. Cached records
// are immutable (a terminal task or settled submission never changes) and
// shared: never mutate one.
type coldRecords struct {
	storage     RecordStorage
	mu          sync.Mutex
	tasks       *lru[TaskID, *TaskRecord]
	submissions *lru[SubmissionID, *SubmissionRecord]
	entries     *lru[EntryID, *Entry]
	documents   *lru[string, *DocumentRecord]
	// conversations holds conversation records a bounded Session evicted or
	// read; dropped when a commit changes one (forgetConversations).
	conversations *lru[ConversationID, *ConversationRecord]
	// starts are where each loaded conversation's context range starts, and
	// loaded when a published commit last touched it (entries.go).
	starts map[ConversationID]EntryID
	loaded map[ConversationID]time.Time
	// snapshot is loaded as published States carry it: immutable once set.
	snapshot map[ConversationID]bool
	// contextRetention is how long an idle conversation keeps its range;
	// sweep unloads expired ones while nothing is published, at expiry.
	contextRetention func() time.Duration
	sweep            func()
	expiry           *time.Timer
	// fail fails the Session on a storage read error, which upstream treats
	// as fatal like any storage error.
	fail func(error)
}

func newColdRecords(storage RecordStorage, fail func(error)) *coldRecords {
	return &coldRecords{
		storage:          storage,
		tasks:            newLRU[TaskID, *TaskRecord](coldTaskCapacity),
		submissions:      newLRU[SubmissionID, *SubmissionRecord](coldSubmissionCapacity),
		entries:          newLRU[EntryID, *Entry](coldEntryCapacity),
		documents:        newLRU[string, *DocumentRecord](coldDocumentCapacity),
		conversations:    newLRU[ConversationID, *ConversationRecord](coldConversationCapacity),
		starts:           map[ConversationID]EntryID{},
		loaded:           map[ConversationID]time.Time{},
		contextRetention: func() time.Duration { return defaultContextRetention },
		fail:             fail,
	}
}

func (c *coldRecords) task(id TaskID) (*TaskRecord, error) {
	c.mu.Lock()
	if t, ok := c.tasks.get(id); ok {
		c.mu.Unlock()
		return t, nil
	}
	c.mu.Unlock()
	t, err := c.storage.Task(context.Background(), id)
	if err != nil {
		err = fmt.Errorf("durable: reading task %d: %w", id, err)
		c.fail(err)
		return nil, err
	}
	if t != nil && t.Status == "terminal" {
		c.mu.Lock()
		c.tasks.put(id, t)
		c.mu.Unlock()
	}
	return t, nil
}

func (c *coldRecords) submission(id SubmissionID) (*SubmissionRecord, error) {
	c.mu.Lock()
	if s, ok := c.submissions.get(id); ok {
		c.mu.Unlock()
		return s, nil
	}
	c.mu.Unlock()
	s, err := c.storage.Submission(context.Background(), id)
	if err != nil {
		err = fmt.Errorf("durable: reading submission %d: %w", id, err)
		c.fail(err)
		return nil, err
	}
	if s != nil && !residentSubmission(s) {
		c.mu.Lock()
		c.submissions.put(id, s)
		c.mu.Unlock()
	}
	return s, nil
}

func (c *coldRecords) submissionByRequest(cid ConversationID, requestID string) (*SubmissionRecord, error) {
	s, err := c.storage.SubmissionByRequest(context.Background(), cid, requestID)
	if err != nil {
		err = fmt.Errorf("durable: reading a submission by request: %w", err)
		c.fail(err)
		return nil, err
	}
	return s, nil
}

// evict moves out of a State about to be published the records a bounded
// Session does not keep: terminal tasks no live task waits on, and settled
// submissions. The caller holds the line, and the State is not shared yet.
func (c *coldRecords) evict(st *State) {
	awaited := map[TaskID]bool{}
	for _, t := range st.Tasks {
		if t.Status != "terminal" {
			for _, id := range t.WaitOn {
				awaited[id] = true
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, t := range st.Tasks {
		if t.Status == "terminal" && !awaited[id] {
			delete(st.Tasks, id)
			c.tasks.put(id, t)
		}
	}
	for id, s := range st.Submissions {
		if !residentSubmission(s) {
			delete(st.Submissions, id)
			c.submissions.put(id, s)
		}
	}
}

// task is a task record: the State's own, or an ended one read through the
// cold cache. Never mutate the result. Nil when none exists.
func (s *State) task(id TaskID) (*TaskRecord, error) {
	if t := s.Tasks[id]; t != nil || s.cold == nil {
		return t, nil
	}
	return s.cold.task(id)
}

// submission is a submission record: the State's own, or a settled one read
// through the cold cache. Never mutate the result. Nil when none exists.
func (s *State) submission(id SubmissionID) (*SubmissionRecord, error) {
	if sub := s.Submissions[id]; sub != nil || s.cold == nil {
		return sub, nil
	}
	return s.cold.submission(id)
}

// submissionByRequest finds the submission a conversation admitted with a
// request ID: a resident one, or a settled one in storage.
func (s *State) submissionByRequest(cid ConversationID, requestID string) (*SubmissionRecord, error) {
	for _, sub := range s.Submissions {
		if sub.Conversation == cid && sub.Request.RequestID == requestID {
			return sub, nil
		}
	}
	if s.cold == nil || requestID == "" {
		return nil, nil
	}
	sub, err := s.cold.submissionByRequest(cid, requestID)
	if err != nil || sub == nil {
		return nil, err
	}
	// A record the State keeps is newer than storage's.
	if resident := s.Submissions[sub.ID]; resident != nil {
		return resident, nil
	}
	return sub, nil
}

// lru is a small least-recently-used map.
type lru[K comparable, V any] struct {
	capacity int
	order    *list.List
	items    map[K]*list.Element
}

type lruItem[K comparable, V any] struct {
	key   K
	value V
}

func newLRU[K comparable, V any](capacity int) *lru[K, V] {
	return &lru[K, V]{capacity: capacity, order: list.New(), items: map[K]*list.Element{}}
}

func (l *lru[K, V]) get(key K) (V, bool) {
	if element, ok := l.items[key]; ok {
		l.order.MoveToFront(element)
		return element.Value.(lruItem[K, V]).value, true
	}
	var zero V
	return zero, false
}

func (l *lru[K, V]) put(key K, value V) {
	if element, ok := l.items[key]; ok {
		element.Value = lruItem[K, V]{key: key, value: value}
		l.order.MoveToFront(element)
		return
	}
	l.items[key] = l.order.PushFront(lruItem[K, V]{key: key, value: value})
	for l.order.Len() > l.capacity {
		oldest := l.order.Back()
		l.order.Remove(oldest)
		delete(l.items, oldest.Value.(lruItem[K, V]).key)
	}
}

func (l *lru[K, V]) len() int { return l.order.Len() }

// document reads a document a bounded Session does not keep; the cache
// holds it until a commit writes or retires it (forgetDocuments).
func (c *coldRecords) document(key string) (*DocumentRecord, error) {
	c.mu.Lock()
	if d, ok := c.documents.get(key); ok {
		c.mu.Unlock()
		return d, nil
	}
	c.mu.Unlock()
	d, err := c.storage.Document(context.Background(), key)
	if err != nil {
		err = fmt.Errorf("durable: reading document %s: %w", key, err)
		c.fail(err)
		return nil, err
	}
	if d != nil {
		c.mu.Lock()
		c.documents.put(key, d)
		c.mu.Unlock()
	}
	return d, nil
}

// forgetDocuments drops cached documents a published commit changed.
func (c *coldRecords) forgetDocuments(delta *StateDelta) {
	if len(delta.Documents) == 0 && len(delta.Removed.Documents) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range delta.Documents {
		c.documents.remove(key)
	}
	for _, key := range delta.Removed.Documents {
		c.documents.remove(key)
	}
}

// document is a document record: the State's own, or one a bounded Session
// does not keep, read through the cold cache. Never mutate the result. Nil
// when none exists.
func (s *State) document(key string) (*DocumentRecord, error) {
	if d := s.Documents[key]; d != nil || s.cold == nil || s.removedDocuments[key] {
		return d, nil
	}
	return s.cold.document(key)
}

// conversationDocuments are a conversation's documents: the State's own
// when it keeps them all (a loaded conversation), else merged with storage's,
// the State's own record of a document deciding. Never mutate the results.
func (s *State) conversationDocuments(cid ConversationID) ([]*DocumentRecord, error) {
	query := DocumentQuery{Scope: "conversation", ScopeID: uint64(cid)}
	own := map[string]*DocumentRecord{}
	for key, doc := range s.Documents {
		if DocumentMatches(doc, query) {
			own[key] = doc
		}
	}
	if s.cold != nil && !s.loaded[cid] {
		var cursor *ScanCursor
		for {
			page, err := s.cold.storage.ScanDocuments(context.Background(), query, 256, cursor)
			if err != nil {
				err = fmt.Errorf("durable: reading documents of conversation %d: %w", cid, err)
				s.cold.fail(err)
				return nil, err
			}
			for i := range page.Items {
				doc := page.Items[i]
				key := keyOfRecord(&doc)
				if _, kept := own[key]; !kept && !s.removedDocuments[key] {
					own[key] = &doc
				}
			}
			if page.Next == nil {
				break
			}
			cursor = page.Next
		}
	}
	out := make([]*DocumentRecord, 0, len(own))
	for _, doc := range own {
		out = append(out, doc)
	}
	return out, nil
}

// keyOfRecord is a document record's storage key.
func keyOfRecord(doc *DocumentRecord) string {
	key := documentKey(DocumentDefinition{Kind: doc.Kind, Scope: doc.Scope}, doc.ScopeID)
	if doc.Member != "" {
		key += "/" + doc.Member
	}
	return key
}

func (l *lru[K, V]) remove(key K) {
	if element, ok := l.items[key]; ok {
		l.order.Remove(element)
		delete(l.items, key)
	}
}

// conversation reads a conversation record a bounded Session does not keep.
func (c *coldRecords) conversation(id ConversationID) (*ConversationRecord, error) {
	c.mu.Lock()
	if conversation, ok := c.conversations.get(id); ok {
		c.mu.Unlock()
		return conversation, nil
	}
	c.mu.Unlock()
	conversation, err := c.storage.Conversation(context.Background(), id)
	if err != nil {
		err = fmt.Errorf("durable: reading conversation %d: %w", id, err)
		c.fail(err)
		return nil, err
	}
	if conversation != nil {
		c.mu.Lock()
		c.conversations.put(id, conversation)
		c.mu.Unlock()
	}
	return conversation, nil
}

// forgetConversations drops cached conversation records a published commit
// changed.
func (c *coldRecords) forgetConversations(delta *StateDelta) {
	if len(delta.Conversations) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range delta.Conversations {
		c.conversations.remove(id)
	}
}

// evictConversations removes from a State about to be published the
// conversation records a bounded Session does not keep: it keeps the root,
// loaded conversations, busy ones, those of live tasks and open submissions,
// and those live tasks own. The others go to the cache.
func (c *coldRecords) evictConversations(st *State) {
	keep := map[ConversationID]bool{st.Root: true}
	for id := range c.loaded {
		keep[id] = true
	}
	live := map[TaskID]bool{}
	for id, task := range st.Tasks {
		if task.Status != "terminal" {
			live[id] = true
			keep[task.Conversation] = true
		}
	}
	for _, sub := range st.Submissions {
		keep[sub.Conversation] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, conversation := range st.Conversations {
		if keep[id] || conversation.Busy != 0 || live[conversation.Owner.Task] {
			continue
		}
		delete(st.Conversations, id)
		c.conversations.put(id, conversation)
	}
}

// conversation is a conversation record: the State's own, or one a bounded
// Session does not keep. Never mutate the result. Nil when none exists.
func (s *State) conversation(id ConversationID) (*ConversationRecord, error) {
	if c := s.Conversations[id]; c != nil || s.cold == nil || id == 0 {
		return c, nil
	}
	return s.cold.conversation(id)
}
