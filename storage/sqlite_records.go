package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	durable "github.com/leocavalcante/pi-durable-go"
)

// SQLite serves record reads from durable_records with JSON expressions over
// the stored values, indexed for the lookups a bounded Session makes
// (RecordStorage). A database still holding a legacy single-row snapshot is
// read through that State until its first write converts it.

// recordIndexes are created by writable opens.
var recordIndexes = []string{
	"CREATE INDEX IF NOT EXISTS durable_entries_by_conversation ON durable_records(json_extract(CAST(value AS TEXT),'$.conversation'), CAST(key AS INTEGER)) WHERE kind='entry'",
	"CREATE INDEX IF NOT EXISTS durable_tasks_by_status ON durable_records(json_extract(CAST(value AS TEXT),'$.status')) WHERE kind='task'",
	"CREATE INDEX IF NOT EXISTS durable_tasks_by_conversation ON durable_records(json_extract(CAST(value AS TEXT),'$.conversation')) WHERE kind='task'",
	"CREATE INDEX IF NOT EXISTS durable_submissions_by_status ON durable_records(json_extract(CAST(value AS TEXT),'$.status')) WHERE kind='submission'",
	"CREATE INDEX IF NOT EXISTS durable_submissions_by_request ON durable_records(json_extract(CAST(value AS TEXT),'$.conversation'), json_extract(CAST(value AS TEXT),'$.request.requestId')) WHERE kind='submission'",
	"CREATE INDEX IF NOT EXISTS durable_conversations_by_owner ON durable_records(json_extract(CAST(value AS TEXT),'$.owner.task')) WHERE kind='conversation'",
}

// field is a JSON field of a stored record value.
func field(path string) string { return "json_extract(CAST(value AS TEXT),'$." + path + "')" }

// read runs a record read on the record tables, or on the State of a store
// that has none yet (empty, or a legacy snapshot).
func (s *sqliteStorage) read(ctx context.Context, tables func(querier) error, whole func(durable.StateRecords) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return durable.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.db == nil {
		return whole(durable.StateRecords{})
	}
	if !s.recordTables {
		var headers int
		err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='durable_header'").Scan(&headers)
		if err == nil && headers > 0 {
			err = s.db.QueryRowContext(ctx, "SELECT count(*) FROM durable_header").Scan(&headers)
		}
		if err != nil {
			return err
		}
		if headers == 0 {
			st, err := load(ctx, s.db)
			if err != nil {
				return err
			}
			return whole(durable.StateRecords{State: st})
		}
		s.recordTables = true
	}
	return tables(s.db)
}

func decodeRecord[T any](value []byte) (*T, error) {
	var v T
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, fmt.Errorf("storage: invalid SQLite record: %w", err)
	}
	return &v, nil
}

// lookup reads one record by kind and key; nil when none is stored.
func lookup[T any](ctx context.Context, q querier, kind, key string) (*T, error) {
	var value []byte
	err := q.QueryRowContext(ctx, "SELECT value FROM durable_records WHERE kind=? AND key=?", kind, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeRecord[T](value)
}

// scanByID pages through records of a kind matching where, by numeric key.
func scanByID[T any](ctx context.Context, q querier, kind, where string, args []any, order string, limit int, cursor *durable.ScanCursor) (durable.ScanPage[T], error) {
	if limit <= 0 {
		return durable.ScanPage[T]{}, fmt.Errorf("%w: limit must be positive", durable.ErrInvalid)
	}
	if cursor != nil {
		order = cursor.Order
	}
	if order == "" {
		order = "ascending"
	}
	direction := "ASC"
	switch order {
	case "ascending":
	case "descending":
		direction = "DESC"
	default:
		return durable.ScanPage[T]{}, fmt.Errorf("%w: order", durable.ErrInvalid)
	}
	query := "SELECT key, value FROM durable_records WHERE kind=?" + where
	args = append([]any{kind}, args...)
	if cursor != nil {
		if order == "ascending" {
			query += " AND CAST(key AS INTEGER) > ?"
		} else {
			query += " AND CAST(key AS INTEGER) < ?"
		}
		args = append(args, cursor.After)
	}
	query += " ORDER BY CAST(key AS INTEGER) " + direction + " LIMIT ?"
	args = append(args, limit+1)
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return durable.ScanPage[T]{}, err
	}
	defer rows.Close()
	var page durable.ScanPage[T]
	var last uint64
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return durable.ScanPage[T]{}, err
		}
		if len(page.Items) == limit {
			page.Next = &durable.ScanCursor{After: last, Order: order}
			break
		}
		record, err := decodeRecord[T](value)
		if err != nil {
			return durable.ScanPage[T]{}, err
		}
		page.Items = append(page.Items, *record)
		last, _ = strconv.ParseUint(key, 10, 64)
	}
	return page, rows.Err()
}

func (s *sqliteStorage) LoadResident(ctx context.Context) (*durable.State, error) {
	var out *durable.State
	err := s.read(ctx, func(q querier) error {
		var data []byte
		if err := q.QueryRowContext(ctx, "SELECT header FROM durable_header WHERE singleton=1").Scan(&data); err != nil {
			return err
		}
		var h header
		if err := json.Unmarshal(data, &h); err != nil {
			return fmt.Errorf("storage: invalid SQLite header: %w", err)
		}
		out = &durable.State{Version: h.Version, Sequence: h.Sequence, NextID: h.NextID, Root: h.Root,
			Conversations: map[durable.ConversationID]*durable.ConversationRecord{}, Entries: map[durable.EntryID]*durable.Entry{},
			Tasks: map[durable.TaskID]*durable.TaskRecord{}, Submissions: map[durable.SubmissionID]*durable.SubmissionRecord{},
			Documents: map[string]*durable.DocumentRecord{}}
		rows, err := q.QueryContext(ctx, "SELECT kind, key, value FROM durable_records WHERE kind='task' AND "+field("status")+" != 'terminal' UNION ALL "+
			"SELECT kind, key, value FROM durable_records WHERE kind='submission' AND "+field("status")+" IN ('queued','placed')")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var kind, key string
			var value []byte
			if err := rows.Scan(&kind, &key, &value); err != nil {
				return err
			}
			if err := loadRecord(out, kind, key, value); err != nil {
				return fmt.Errorf("storage: invalid SQLite %s record %s: %w", kind, key, err)
			}
		}
		return rows.Err()
	}, func(r durable.StateRecords) error {
		var err error
		out, err = r.Resident()
		return err
	})
	return out, err
}

func (s *sqliteStorage) Conversation(ctx context.Context, id durable.ConversationID) (*durable.ConversationRecord, error) {
	var out *durable.ConversationRecord
	err := s.read(ctx, func(q querier) (err error) {
		out, err = lookup[durable.ConversationRecord](ctx, q, kindConversation, uintKey(id))
		return err
	}, func(r durable.StateRecords) error { out = r.Conversation(id); return nil })
	return out, err
}

func (s *sqliteStorage) ScanConversations(ctx context.Context, query durable.ConversationQuery, limit int, cursor *durable.ScanCursor) (durable.ScanPage[durable.ConversationRecord], error) {
	var out durable.ScanPage[durable.ConversationRecord]
	err := s.read(ctx, func(q querier) (err error) {
		where, args := "", []any{}
		if query.OwnerTask != 0 {
			where += " AND " + field("owner.task") + " = ?"
			args = append(args, uint64(query.OwnerTask))
		}
		if query.OwnerConversation != 0 {
			where += " AND " + field("owner.task") + " IN (SELECT CAST(key AS INTEGER) FROM durable_records WHERE kind='task' AND " + field("conversation") + " = ?)"
			args = append(args, uint64(query.OwnerConversation))
		}
		out, err = scanByID[durable.ConversationRecord](ctx, q, kindConversation, where, args, query.Order, limit, cursor)
		return err
	}, func(r durable.StateRecords) (err error) {
		out, err = r.ScanConversations(query, limit, cursor)
		return err
	})
	return out, err
}

func (s *sqliteStorage) Entry(ctx context.Context, id durable.EntryID) (*durable.Entry, error) {
	var out *durable.Entry
	err := s.read(ctx, func(q querier) (err error) {
		out, err = lookup[durable.Entry](ctx, q, kindEntry, uintKey(id))
		return err
	}, func(r durable.StateRecords) error { out = r.Entry(id); return nil })
	return out, err
}

// ScanEntries reads a conversation's lineage in segments: its own entries,
// then each fork ancestor's up to the smallest fork point so far. Segments
// are ordered by ID: an ancestor's entries precede the fork's own.
func (s *sqliteStorage) ScanEntries(ctx context.Context, cid durable.ConversationID, query durable.EntryQuery, limit int, cursor *durable.EntryCursor) (durable.EntryPage, error) {
	var out durable.EntryPage
	err := s.read(ctx, func(q querier) error {
		if limit <= 0 {
			return fmt.Errorf("%w: limit must be positive", durable.ErrInvalid)
		}
		order := query.Order
		if cursor != nil {
			order = cursor.Order
		}
		if order == "" {
			order = "descending"
		}
		if order != "ascending" && order != "descending" {
			return fmt.Errorf("%w: order", durable.ErrInvalid)
		}
		type segment struct {
			conversation durable.ConversationID
			bound        uint64 // 0: unbounded
		}
		var segments []segment
		var bound uint64
		seen := map[durable.ConversationID]bool{}
		for at := cid; at != 0; {
			if seen[at] {
				return fmt.Errorf("%w: conversation cycle", durable.ErrInvalid)
			}
			seen[at] = true
			c, err := lookup[durable.ConversationRecord](ctx, q, kindConversation, uintKey(at))
			if err != nil {
				return err
			}
			if c == nil {
				if at == cid {
					return durable.ErrNotFound
				}
				return fmt.Errorf("%w: missing conversation %d", durable.ErrInvalid, at)
			}
			segments = append(segments, segment{conversation: at, bound: bound})
			if c.Parent != 0 && (bound == 0 || uint64(c.ForkAt) < bound) {
				bound = uint64(c.ForkAt)
			}
			at = c.Parent
		}
		if order == "ascending" {
			for i, j := 0, len(segments)-1; i < j; i, j = i+1, j-1 {
				segments[i], segments[j] = segments[j], segments[i]
			}
		}
		direction := "DESC"
		if order == "ascending" {
			direction = "ASC"
		}
		for _, seg := range segments {
			sqlQuery := "SELECT value FROM durable_records WHERE kind='entry' AND " + field("conversation") + " = ?"
			args := []any{uint64(seg.conversation)}
			if seg.bound != 0 {
				sqlQuery += " AND CAST(key AS INTEGER) <= ?"
				args = append(args, seg.bound)
			}
			if query.MinEntryID != 0 {
				sqlQuery += " AND CAST(key AS INTEGER) >= ?"
				args = append(args, uint64(query.MinEntryID))
			}
			if query.MaxEntryID != 0 {
				sqlQuery += " AND CAST(key AS INTEGER) <= ?"
				args = append(args, uint64(query.MaxEntryID))
			}
			if cursor != nil {
				if order == "ascending" {
					sqlQuery += " AND CAST(key AS INTEGER) > ?"
				} else {
					sqlQuery += " AND CAST(key AS INTEGER) < ?"
				}
				args = append(args, uint64(cursor.After))
			}
			sqlQuery += " ORDER BY CAST(key AS INTEGER) " + direction + " LIMIT ?"
			args = append(args, limit+1-len(out.Items))
			rows, err := q.QueryContext(ctx, sqlQuery, args...)
			if err != nil {
				return err
			}
			for rows.Next() {
				var value []byte
				if err := rows.Scan(&value); err != nil {
					rows.Close()
					return err
				}
				entry, err := decodeRecord[durable.Entry](value)
				if err != nil {
					rows.Close()
					return err
				}
				if len(out.Items) == limit {
					out.Next = &durable.EntryCursor{After: out.Items[limit-1].ID, Order: order}
					break
				}
				out.Items = append(out.Items, *entry)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			if out.Next != nil {
				break
			}
		}
		return nil
	}, func(r durable.StateRecords) (err error) {
		out, err = r.ScanEntries(cid, query, limit, cursor)
		return err
	})
	return out, err
}

func (s *sqliteStorage) Task(ctx context.Context, id durable.TaskID) (*durable.TaskRecord, error) {
	var out *durable.TaskRecord
	err := s.read(ctx, func(q querier) (err error) {
		out, err = lookup[durable.TaskRecord](ctx, q, kindTask, uintKey(id))
		return err
	}, func(r durable.StateRecords) error { out = r.Task(id); return nil })
	return out, err
}

func (s *sqliteStorage) ScanTasks(ctx context.Context, query durable.TaskQuery, limit int, cursor *durable.ScanCursor) (durable.ScanPage[durable.TaskRecord], error) {
	var out durable.ScanPage[durable.TaskRecord]
	err := s.read(ctx, func(q querier) (err error) {
		where, args := "", []any{}
		if query.Conversation != 0 {
			where += " AND " + field("conversation") + " = ?"
			args = append(args, uint64(query.Conversation))
		}
		if query.Kind != "" {
			where += " AND " + field("kind") + " = ?"
			args = append(args, query.Kind)
		}
		if query.Status != "" {
			where += " AND " + field("status") + " = ?"
			args = append(args, query.Status)
		}
		if query.AbortRequested != nil {
			where += " AND IFNULL(" + field("abortRequested") + ", 0) = ?"
			args = append(args, boolInt(*query.AbortRequested))
		}
		if query.Background != nil {
			where += " AND IFNULL(" + field("background") + ", 0) = ?"
			args = append(args, boolInt(*query.Background))
		}
		out, err = scanByID[durable.TaskRecord](ctx, q, kindTask, where, args, query.Order, limit, cursor)
		return err
	}, func(r durable.StateRecords) (err error) { out, err = r.ScanTasks(query, limit, cursor); return err })
	return out, err
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (s *sqliteStorage) Submission(ctx context.Context, id durable.SubmissionID) (*durable.SubmissionRecord, error) {
	var out *durable.SubmissionRecord
	err := s.read(ctx, func(q querier) (err error) {
		out, err = lookup[durable.SubmissionRecord](ctx, q, kindSubmission, uintKey(id))
		return err
	}, func(r durable.StateRecords) error { out = r.Submission(id); return nil })
	return out, err
}

func (s *sqliteStorage) SubmissionByRequest(ctx context.Context, cid durable.ConversationID, requestID string) (*durable.SubmissionRecord, error) {
	var out *durable.SubmissionRecord
	err := s.read(ctx, func(q querier) error {
		if requestID == "" {
			return nil
		}
		var value []byte
		err := q.QueryRowContext(ctx, "SELECT value FROM durable_records WHERE kind='submission' AND "+field("conversation")+" = ? AND "+field("request.requestId")+" = ? LIMIT 1",
			uint64(cid), requestID).Scan(&value)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		out, err = decodeRecord[durable.SubmissionRecord](value)
		return err
	}, func(r durable.StateRecords) error { out = r.SubmissionByRequest(cid, requestID); return nil })
	return out, err
}

func (s *sqliteStorage) ScanSubmissions(ctx context.Context, query durable.SubmissionQuery, limit int, cursor *durable.ScanCursor) (durable.ScanPage[durable.SubmissionRecord], error) {
	var out durable.ScanPage[durable.SubmissionRecord]
	err := s.read(ctx, func(q querier) (err error) {
		where, args := "", []any{}
		if query.Conversation != 0 {
			where += " AND " + field("conversation") + " = ?"
			args = append(args, uint64(query.Conversation))
		}
		if query.Status != "" {
			where += " AND " + field("status") + " = ?"
			args = append(args, query.Status)
		}
		out, err = scanByID[durable.SubmissionRecord](ctx, q, kindSubmission, where, args, query.Order, limit, cursor)
		return err
	}, func(r durable.StateRecords) (err error) {
		out, err = r.ScanSubmissions(query, limit, cursor)
		return err
	})
	return out, err
}

func (s *sqliteStorage) Document(ctx context.Context, key string) (*durable.DocumentRecord, error) {
	var out *durable.DocumentRecord
	err := s.read(ctx, func(q querier) (err error) {
		out, err = lookup[durable.DocumentRecord](ctx, q, kindDocument, key)
		return err
	}, func(r durable.StateRecords) error { out = r.Document(key); return nil })
	return out, err
}

// ScanDocuments reads a scope's documents by key range: their keys start
// with "scope:id:".
func (s *sqliteStorage) ScanDocuments(ctx context.Context, query durable.DocumentQuery, limit int, cursor *durable.ScanCursor) (durable.ScanPage[durable.DocumentRecord], error) {
	var out durable.ScanPage[durable.DocumentRecord]
	err := s.read(ctx, func(q querier) error {
		if limit <= 0 {
			return fmt.Errorf("%w: limit must be positive", durable.ErrInvalid)
		}
		switch query.Scope {
		case "session", "conversation", "task":
		default:
			return fmt.Errorf("%w: document scope %q", durable.ErrInvalid, query.Scope)
		}
		id := query.ScopeID
		if query.Scope == "session" {
			id = 0
		}
		prefix := fmt.Sprintf("%s:%d:", query.Scope, id)
		upper := prefix[:len(prefix)-1] + ";"
		sqlQuery := "SELECT key, value FROM durable_records WHERE kind='document' AND key >= ? AND key < ?"
		args := []any{prefix, upper}
		if query.Kind != "" {
			sqlQuery += " AND " + field("kind") + " = ?"
			args = append(args, query.Kind)
		}
		if cursor != nil {
			sqlQuery += " AND key > ?"
			args = append(args, cursor.AfterKey)
		}
		sqlQuery += " ORDER BY key LIMIT ?"
		args = append(args, limit+1)
		rows, err := q.QueryContext(ctx, sqlQuery, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		var last string
		for rows.Next() {
			var key string
			var value []byte
			if err := rows.Scan(&key, &value); err != nil {
				return err
			}
			if len(out.Items) == limit {
				out.Next = &durable.ScanCursor{AfterKey: last}
				break
			}
			doc, err := decodeRecord[durable.DocumentRecord](value)
			if err != nil {
				return err
			}
			out.Items = append(out.Items, *doc)
			last = key
		}
		return rows.Err()
	}, func(r durable.StateRecords) (err error) { out, err = r.ScanDocuments(query, limit, cursor); return err })
	return out, err
}

// DocumentRevision reads the newest revision at or before a sequence with
// one key-range query: revision keys sort by sequence within a document.
func (s *sqliteStorage) DocumentRevision(ctx context.Context, docKey string, atOrBefore uint64) (*durable.DocumentRevision, error) {
	var out *durable.DocumentRevision
	err := s.read(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, "SELECT key, value FROM durable_records WHERE kind='revision' AND key >= ? AND key <= ? ORDER BY key DESC",
			docKey+"@", durable.RevisionKey(docKey, atOrBefore))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			var value []byte
			if err := rows.Scan(&key, &value); err != nil {
				return err
			}
			// A member key may hold "@": only this document's own keys.
			if strings.Contains(key[len(docKey)+1:], "@") {
				continue
			}
			out, err = decodeRecord[durable.DocumentRevision](value)
			return err
		}
		return rows.Err()
	}, func(r durable.StateRecords) error { out = r.DocumentRevision(docKey, atOrBefore); return nil })
	return out, err
}

// DocumentRevisions reads the revisions under a key prefix in key order,
// keeping each document's newest at or before the sequence.
func (s *sqliteStorage) DocumentRevisions(ctx context.Context, prefix string, atOrBefore uint64) (map[string]*durable.DocumentRevision, error) {
	out := map[string]*durable.DocumentRevision{}
	err := s.read(ctx, func(q querier) error {
		rows, err := q.QueryContext(ctx, "SELECT key, value FROM durable_records WHERE kind='revision' AND key >= ? AND key < ? ORDER BY key", prefix, prefix+"\xff")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			var value []byte
			if err := rows.Scan(&key, &value); err != nil {
				return err
			}
			at := strings.LastIndex(key, "@")
			if at < 0 {
				continue
			}
			revision, err := decodeRecord[durable.DocumentRevision](value)
			if err != nil {
				return err
			}
			if revision.Sequence > atOrBefore {
				continue
			}
			if best := out[key[:at]]; best == nil || revision.Sequence > best.Sequence {
				out[key[:at]] = revision
			}
		}
		return rows.Err()
	}, func(r durable.StateRecords) error { out = r.DocumentRevisions(prefix, atOrBefore); return nil })
	return out, err
}

var _ durable.RecordStorage = (*sqliteStorage)(nil)
