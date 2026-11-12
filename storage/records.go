package storage

import (
	"context"

	durable "github.com/leocavalcante/pi-durable-go"
)

// JSONL storage keeps its whole State in memory, as upstream's JSONL storage
// keeps a MemoryStorage, and serves record reads from it (RecordStorage).

func (s *jsonlStorage) records(ctx context.Context) (durable.StateRecords, func(), error) {
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return durable.StateRecords{}, nil, err
	}
	if s.closed {
		s.mu.Unlock()
		return durable.StateRecords{}, nil, durable.ErrClosed
	}
	return durable.StateRecords{State: s.state}, s.mu.Unlock, nil
}

func (s *jsonlStorage) LoadResident(ctx context.Context) (*durable.State, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Resident()
}

func (s *jsonlStorage) Conversation(ctx context.Context, id durable.ConversationID) (*durable.ConversationRecord, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Conversation(id), nil
}

func (s *jsonlStorage) ScanConversations(ctx context.Context, query durable.ConversationQuery, limit int, cursor *durable.ScanCursor) (durable.ScanPage[durable.ConversationRecord], error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return durable.ScanPage[durable.ConversationRecord]{}, err
	}
	defer unlock()
	return r.ScanConversations(query, limit, cursor)
}

func (s *jsonlStorage) Entry(ctx context.Context, id durable.EntryID) (*durable.Entry, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Entry(id), nil
}

func (s *jsonlStorage) ScanEntries(ctx context.Context, cid durable.ConversationID, query durable.EntryQuery, limit int, cursor *durable.EntryCursor) (durable.EntryPage, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return durable.EntryPage{}, err
	}
	defer unlock()
	return r.ScanEntries(cid, query, limit, cursor)
}

func (s *jsonlStorage) Task(ctx context.Context, id durable.TaskID) (*durable.TaskRecord, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Task(id), nil
}

func (s *jsonlStorage) ScanTasks(ctx context.Context, query durable.TaskQuery, limit int, cursor *durable.ScanCursor) (durable.ScanPage[durable.TaskRecord], error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return durable.ScanPage[durable.TaskRecord]{}, err
	}
	defer unlock()
	return r.ScanTasks(query, limit, cursor)
}

func (s *jsonlStorage) Submission(ctx context.Context, id durable.SubmissionID) (*durable.SubmissionRecord, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Submission(id), nil
}

func (s *jsonlStorage) SubmissionByRequest(ctx context.Context, cid durable.ConversationID, requestID string) (*durable.SubmissionRecord, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.SubmissionByRequest(cid, requestID), nil
}

func (s *jsonlStorage) ScanSubmissions(ctx context.Context, query durable.SubmissionQuery, limit int, cursor *durable.ScanCursor) (durable.ScanPage[durable.SubmissionRecord], error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return durable.ScanPage[durable.SubmissionRecord]{}, err
	}
	defer unlock()
	return r.ScanSubmissions(query, limit, cursor)
}

func (s *jsonlStorage) Document(ctx context.Context, key string) (*durable.DocumentRecord, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.Document(key), nil
}

func (s *jsonlStorage) ScanDocuments(ctx context.Context, query durable.DocumentQuery, limit int, cursor *durable.ScanCursor) (durable.ScanPage[durable.DocumentRecord], error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return durable.ScanPage[durable.DocumentRecord]{}, err
	}
	defer unlock()
	return r.ScanDocuments(query, limit, cursor)
}

func (s *jsonlStorage) DocumentRevision(ctx context.Context, docKey string, atOrBefore uint64) (*durable.DocumentRevision, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.DocumentRevision(docKey, atOrBefore), nil
}

func (s *jsonlStorage) DocumentRevisions(ctx context.Context, prefix string, atOrBefore uint64) (map[string]*durable.DocumentRevision, error) {
	r, unlock, err := s.records(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return r.DocumentRevisions(prefix, atOrBefore), nil
}

var _ durable.RecordStorage = (*jsonlStorage)(nil)
