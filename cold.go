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
	coldTaskCapacity       = 256
	coldSubmissionCapacity = 256
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
	// starts are where each loaded conversation's context range starts, and
	// loaded when a published commit last touched it (entries.go).
	starts map[ConversationID]EntryID
	loaded map[ConversationID]time.Time
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
	return &coldRecords{storage: storage, tasks: newLRU[TaskID, *TaskRecord](coldTaskCapacity),
		submissions: newLRU[SubmissionID, *SubmissionRecord](coldSubmissionCapacity),
		entries:     newLRU[EntryID, *Entry](coldEntryCapacity), starts: map[ConversationID]EntryID{}, loaded: map[ConversationID]time.Time{},
		contextRetention: func() time.Duration { return defaultContextRetention }, fail: fail}
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
