package durable

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStorage is a detached, revision-checked in-memory backend.
type MemoryStorage struct {
	mu     sync.Mutex
	state  *State
	closed bool
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }
func (m *MemoryStorage) Load(ctx context.Context) (*State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.closed {
		return nil, ErrClosed
	}
	if m.state == nil {
		return nil, nil
	}
	return cloneState(m.state)
}
func (m *MemoryStorage) Commit(ctx context.Context, expected uint64, next *State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return &RejectedError{Err: err}
	}
	if m.closed {
		return &RejectedError{Err: ErrClosed}
	}
	actual := uint64(0)
	if m.state != nil {
		actual = m.state.Sequence
	}
	if actual != expected {
		return &RejectedError{Err: fmt.Errorf("%w: expected sequence %d, have %d", ErrConflict, expected, actual)}
	}
	if next == nil || expected == ^uint64(0) || next.Sequence != expected+1 {
		return &RejectedError{Err: fmt.Errorf("%w: next sequence must increase", ErrInvalid)}
	}
	copy, err := cloneState(next)
	if err != nil {
		return &RejectedError{Err: err}
	}
	m.state = copy
	return nil
}
func (m *MemoryStorage) Close() error { m.mu.Lock(); defer m.mu.Unlock(); m.closed = true; return nil }

// CommitDelta applies one commit's changes to the stored copy (DeltaStorage).
func (m *MemoryStorage) CommitDelta(ctx context.Context, expected uint64, delta *StateDelta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return &RejectedError{Err: err}
	}
	if m.closed {
		return &RejectedError{Err: ErrClosed}
	}
	actual := uint64(0)
	if m.state != nil {
		actual = m.state.Sequence
	}
	if actual != expected {
		return &RejectedError{Err: fmt.Errorf("%w: expected sequence %d, have %d", ErrConflict, expected, actual)}
	}
	if delta == nil || expected == ^uint64(0) || delta.Sequence != expected+1 {
		return &RejectedError{Err: fmt.Errorf("%w: next sequence must increase", ErrInvalid)}
	}
	detached, err := delta.Clone()
	if err != nil {
		return &RejectedError{Err: err}
	}
	if m.state == nil {
		m.state = &State{}
	}
	return detached.ApplyTo(m.state)
}

// CommitDeltas stores several commits' changes in order (BatchDeltaStorage).
func (m *MemoryStorage) CommitDeltas(ctx context.Context, expected uint64, deltas []*StateDelta) (int, error) {
	for i, delta := range deltas {
		if err := m.CommitDelta(ctx, expected+uint64(i), delta); err != nil {
			return i, err
		}
	}
	return len(deltas), nil
}

// MemoryStorage reads records from its copy (RecordStorage).
func (m *MemoryStorage) records(ctx context.Context) (StateRecords, func(), error) {
	m.mu.Lock()
	if err := ctx.Err(); err != nil {
		m.mu.Unlock()
		return StateRecords{}, nil, err
	}
	if m.closed {
		m.mu.Unlock()
		return StateRecords{}, nil, ErrClosed
	}
	return StateRecords{State: m.state}, m.mu.Unlock, nil
}

func (m *MemoryStorage) LoadResident(ctx context.Context) (*State, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Resident()
}

func (m *MemoryStorage) Conversation(ctx context.Context, id ConversationID) (*ConversationRecord, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Conversation(id), nil
}

func (m *MemoryStorage) ScanConversations(ctx context.Context, query ConversationQuery, limit int, cursor *ScanCursor) (ScanPage[ConversationRecord], error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return ScanPage[ConversationRecord]{}, err
	}
	defer unlock()
	return r.ScanConversations(query, limit, cursor)
}

func (m *MemoryStorage) Entry(ctx context.Context, id EntryID) (*Entry, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Entry(id), nil
}

func (m *MemoryStorage) ScanEntries(ctx context.Context, cid ConversationID, query EntryQuery, limit int, cursor *EntryCursor) (EntryPage, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return EntryPage{}, err
	}
	defer unlock()
	return r.ScanEntries(cid, query, limit, cursor)
}

func (m *MemoryStorage) Task(ctx context.Context, id TaskID) (*TaskRecord, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Task(id), nil
}

func (m *MemoryStorage) ScanTasks(ctx context.Context, query TaskQuery, limit int, cursor *ScanCursor) (ScanPage[TaskRecord], error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return ScanPage[TaskRecord]{}, err
	}
	defer unlock()
	return r.ScanTasks(query, limit, cursor)
}

func (m *MemoryStorage) Submission(ctx context.Context, id SubmissionID) (*SubmissionRecord, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Submission(id), nil
}

func (m *MemoryStorage) SubmissionByRequest(ctx context.Context, cid ConversationID, requestID string) (*SubmissionRecord, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.SubmissionByRequest(cid, requestID), nil
}

func (m *MemoryStorage) ScanSubmissions(ctx context.Context, query SubmissionQuery, limit int, cursor *ScanCursor) (ScanPage[SubmissionRecord], error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return ScanPage[SubmissionRecord]{}, err
	}
	defer unlock()
	return r.ScanSubmissions(query, limit, cursor)
}

func (m *MemoryStorage) Document(ctx context.Context, key string) (*DocumentRecord, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Document(key), nil
}

func (m *MemoryStorage) ScanDocuments(ctx context.Context, query DocumentQuery, limit int, cursor *ScanCursor) (ScanPage[DocumentRecord], error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return ScanPage[DocumentRecord]{}, err
	}
	defer unlock()
	return r.ScanDocuments(query, limit, cursor)
}

func (m *MemoryStorage) DocumentRevision(ctx context.Context, docKey string, atOrBefore uint64) (*DocumentRevision, error) {
	r, unlock, err := m.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.DocumentRevision(docKey, atOrBefore), nil
}

var _ RecordStorage = (*MemoryStorage)(nil)
