package durable

import (
	"container/list"
	"context"
	"fmt"
	"strings"
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
	// defaultContextRetention and defaultDocumentRetention are upstream's
	// settings.contextRetentionMs and settings.documentRetentionMs.
	defaultContextRetention  = 10 * time.Minute
	defaultDocumentRetention = 10 * time.Minute
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
	// invalidations counts the forgets of mutable cached records: a read
	// caches its result only if none happened while it read storage.
	invalidations uint64
	// overlayDocuments and overlayConversations are the values records had
	// before the commits stored and not yet published (overlayFrom).
	overlayDocuments     map[string]*DocumentRecord
	overlayConversations map[ConversationID]*ConversationRecord
	// starts are where each loaded conversation's context range starts, and
	// loaded when a published commit last touched it (entries.go).
	starts map[ConversationID]EntryID
	loaded map[ConversationID]time.Time
	// snapshot is loaded as published States carry it: immutable once set.
	snapshot map[ConversationID]bool
	// documentTouched is when each kept document was last written or
	// loaded; pins and pinnedConversations keep observed documents (upstream
	// keeps observed documents); incomplete marks loaded conversations one
	// of whose documents left after the document retention.
	documentTouched     map[string]time.Time
	pins                map[string]int
	pinnedConversations map[ConversationID]int
	incomplete          map[ConversationID]bool
	// contextRetention is how long an idle conversation keeps its range;
	// sweep unloads expired ones while nothing is published, at expiry.
	contextRetention  func() time.Duration
	documentRetention func() time.Duration
	sweep             func()
	expiry            func()
	timer             func(callback func(), delay time.Duration) (stop func())
	// fail fails the Session on a storage read error, which upstream treats
	// as fatal like any storage error.
	fail func(error)
}

func newColdRecords(storage RecordStorage, fail func(error)) *coldRecords {
	return &coldRecords{
		storage:             storage,
		tasks:               newLRU[TaskID, *TaskRecord](coldTaskCapacity),
		submissions:         newLRU[SubmissionID, *SubmissionRecord](coldSubmissionCapacity),
		entries:             newLRU[EntryID, *Entry](coldEntryCapacity),
		documents:           newLRU[string, *DocumentRecord](coldDocumentCapacity),
		conversations:       newLRU[ConversationID, *ConversationRecord](coldConversationCapacity),
		starts:              map[ConversationID]EntryID{},
		loaded:              map[ConversationID]time.Time{},
		documentTouched:     map[string]time.Time{},
		pins:                map[string]int{},
		pinnedConversations: map[ConversationID]int{},
		incomplete:          map[ConversationID]bool{},
		contextRetention:    func() time.Duration { return defaultContextRetention },
		documentRetention:   func() time.Duration { return defaultDocumentRetention },
		fail:                fail,
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
	// Revisions are read from storage only (as-of reads and forks).
	for key := range st.Revisions {
		delete(st.Revisions, key)
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
	if d, overlaid := c.overlayDocuments[key]; overlaid {
		c.mu.Unlock()
		return d, nil
	}
	if d, ok := c.documents.get(key); ok {
		c.mu.Unlock()
		return d, nil
	}
	invalidations := c.invalidations
	c.mu.Unlock()
	d, err := c.storage.Document(context.Background(), key)
	if err != nil {
		err = fmt.Errorf("durable: reading document %s: %w", key, err)
		c.fail(err)
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// A group stored while this read ran: what readers see until it
	// publishes is its pre-image.
	if overlaid, ok := c.overlayDocuments[key]; ok {
		return overlaid, nil
	}
	if d != nil && c.invalidations == invalidations {
		c.documents.put(key, d)
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
	c.invalidations++
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
	s.noteRead(key)
	if d := s.Documents[key]; d != nil || s.cold == nil || s.removedDocuments[key] {
		return d, nil
	}
	return s.cold.document(key)
}

// noteRead records a document a transaction reads (a candidate State has a
// base); published States record nothing.
func (s *State) noteRead(key string) {
	if s.base == nil {
		return
	}
	if s.readDocuments == nil {
		s.readDocuments = map[string]bool{}
	}
	s.readDocuments[key] = true
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
		// Documents that commits stored but not yet published changed read
		// as they were before them.
		prefix := documentScopePrefix(query.Scope, query.ScopeID)
		s.cold.mu.Lock()
		for key, record := range s.cold.overlayDocuments {
			if _, kept := s.Documents[key]; kept || !strings.HasPrefix(key, prefix) {
				continue
			}
			if record == nil {
				delete(own, key)
			} else {
				own[key] = record
			}
		}
		s.cold.mu.Unlock()
	}
	out := make([]*DocumentRecord, 0, len(own))
	for key, doc := range own {
		s.noteRead(key)
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
	if conversation, overlaid := c.overlayConversations[id]; overlaid {
		c.mu.Unlock()
		return conversation, nil
	}
	if conversation, ok := c.conversations.get(id); ok {
		c.mu.Unlock()
		return conversation, nil
	}
	invalidations := c.invalidations
	c.mu.Unlock()
	conversation, err := c.storage.Conversation(context.Background(), id)
	if err != nil {
		err = fmt.Errorf("durable: reading conversation %d: %w", id, err)
		c.fail(err)
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if overlaid, ok := c.overlayConversations[id]; ok {
		return overlaid, nil
	}
	if conversation != nil && c.invalidations == invalidations {
		c.conversations.put(id, conversation)
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
	c.invalidations++
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

// preImages are the values records had before a commit, for the records it
// changed that a bounded Session did not keep (nil: absent).
type preImages struct {
	documents     map[string]*DocumentRecord
	conversations map[ConversationID]*ConversationRecord
}

// preImagesOf collects a commit's pre-images: the stored records its
// transaction loaded to change, and absent ones for those it created that
// its base did not hold.
func preImagesOf(base, candidate *State, delta *StateDelta) *preImages {
	pre := &preImages{}
	note := func(key string, record *DocumentRecord) {
		if pre.documents == nil {
			pre.documents = map[string]*DocumentRecord{}
		}
		pre.documents[key] = record
	}
	documents := func(key string) {
		if record, loaded := candidate.preDocuments[key]; loaded {
			note(key, record)
		} else if base.Documents[key] == nil && base.cold != nil {
			note(key, nil)
		}
	}
	for key := range delta.Documents {
		documents(key)
	}
	for _, key := range delta.Removed.Documents {
		documents(key)
	}
	for id := range delta.Conversations {
		record, loaded := candidate.preConversations[id]
		if !loaded && (base.Conversations[id] != nil || base.cold == nil) {
			continue
		}
		// A conversation the base lacks and no transaction loaded is new:
		// absent until it publishes.
		if pre.conversations == nil {
			pre.conversations = map[ConversationID]*ConversationRecord{}
		}
		pre.conversations[id] = record
	}
	candidate.preDocuments, candidate.preConversations = nil, nil
	if pre.documents == nil && pre.conversations == nil {
		return nil
	}
	return pre
}

// overlayFrom sets the values reads find for records the given commits
// change while they are stored and not published: the earliest commit's
// pre-image of each. Nil clears it.
func (c *coldRecords) overlayFrom(items []*pendingCommit) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.overlayDocuments, c.overlayConversations = nil, nil
	for _, item := range items {
		if item.pre == nil {
			continue
		}
		for key, record := range item.pre.documents {
			if c.overlayDocuments == nil {
				c.overlayDocuments = map[string]*DocumentRecord{}
			}
			if _, set := c.overlayDocuments[key]; !set {
				c.overlayDocuments[key] = record
			}
		}
		for id, record := range item.pre.conversations {
			if c.overlayConversations == nil {
				c.overlayConversations = map[ConversationID]*ConversationRecord{}
			}
			if _, set := c.overlayConversations[id]; !set {
				c.overlayConversations[id] = record
			}
		}
	}
}

// documentScopePrefix is the storage key prefix of a scope's documents.
func documentScopePrefix(scope string, id uint64) string {
	if scope == "session" {
		id = 0
	}
	return fmt.Sprintf("%s:%d:", scope, id)
}

// pinDocument keeps a document an observer follows; the returned func
// unpins it.
func (c *coldRecords) pinDocument(key string) func() {
	c.mu.Lock()
	c.pins[key]++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			if c.pins[key]--; c.pins[key] <= 0 {
				delete(c.pins, key)
			}
			c.mu.Unlock()
		})
	}
}

// pinConversation keeps the documents of a conversation a view follows.
func (c *coldRecords) pinConversation(id ConversationID) func() {
	c.mu.Lock()
	c.pinnedConversations[id]++
	c.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			if c.pinnedConversations[id]--; c.pinnedConversations[id] <= 0 {
				delete(c.pinnedConversations, id)
			}
			c.mu.Unlock()
		})
	}
}

// evictIdleDocuments drops from st the documents unused (unwritten and not
// loaded) for the document retention, except held and observed ones; it
// reports whether any left. The caller holds the Session line.
func (c *coldRecords) evictIdleDocuments(st *State, now time.Time, retention time.Duration, held map[string]time.Time) bool {
	if retention >= maxRetention {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	evicted := false
	for key, doc := range st.Documents {
		touched, known := c.documentTouched[key]
		if !known {
			c.documentTouched[key] = now
			continue
		}
		if now.Sub(touched) < retention {
			continue
		}
		if _, pinned := held[key]; pinned || c.pins[key] > 0 {
			continue
		}
		if doc.Scope == "conversation" && c.pinnedConversations[ConversationID(doc.ScopeID)] > 0 {
			continue
		}
		delete(st.Documents, key)
		delete(c.documentTouched, key)
		if doc.Scope == "conversation" {
			c.incomplete[ConversationID(doc.ScopeID)] = true
		}
		evicted = true
	}
	return evicted
}
