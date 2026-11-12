package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	durable "github.com/leocavalcante/pi-durable-go"
	_ "modernc.org/sqlite"
)

// OpenSQLite opens a single-owner SQLite store that keeps one row per record
// (durable_records) and a header row (durable_header), so a commit writes
// only the records it changed (durable.DeltaStorage). A database written by
// earlier versions, which held the whole State in one durable_state row, is
// read as is and converted inside its first write's transaction.
func OpenSQLite(ctx context.Context, file string) (durable.Storage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if file == "" || strings.HasPrefix(file, "file:") || strings.Contains(file, "?") {
		return nil, fmt.Errorf("storage: SQLite requires a filesystem path or :memory:, not a URI/DSN")
	}
	if file != ":memory:" {
		f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", file)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"CREATE TABLE IF NOT EXISTS durable_header (singleton INTEGER PRIMARY KEY CHECK(singleton=1), sequence INTEGER NOT NULL, header BLOB NOT NULL)",
		"CREATE TABLE IF NOT EXISTS durable_records (kind TEXT NOT NULL, key TEXT NOT NULL, value BLOB NOT NULL, PRIMARY KEY(kind, key)) WITHOUT ROWID",
	} {
		if _, err = db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &sqliteStorage{db: db}, nil
}

type sqliteStorage struct {
	mu     sync.Mutex
	db     *sql.DB
	closed bool
	// readOnly storage reads what another process writes and writes nothing.
	readOnly bool
}

// OpenSQLiteReadOnly opens a database as a viewer of one another process
// writes, as storage.OpenJSONLReadOnly does for JSONL: it creates and writes
// nothing (SQLite's mode=ro), each Load reads the current committed State,
// and commits are rejected with durable.ErrReadOnly. A missing file is an
// empty store.
func OpenSQLiteReadOnly(ctx context.Context, file string) (durable.Storage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if file == "" || file == ":memory:" || strings.HasPrefix(file, "file:") || strings.Contains(file, "?") {
		return nil, fmt.Errorf("storage: read-only SQLite requires a filesystem path")
	}
	if _, err := os.Stat(file); errors.Is(err, os.ErrNotExist) {
		return &sqliteStorage{readOnly: true}, nil
	} else if err != nil {
		return nil, err
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return nil, err
	}
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &sqliteStorage{db: db, readOnly: true}, nil
}

// header is the State without its tables.
type header struct {
	Version  int                    `json:"version"`
	Sequence uint64                 `json:"sequence"`
	NextID   uint64                 `json:"nextId"`
	Root     durable.ConversationID `json:"root"`
}

// Record kinds of durable_records.
const (
	kindConversation = "conversation"
	kindEntry        = "entry"
	kindTask         = "task"
	kindSubmission   = "submission"
	kindDocument     = "document"
)

type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *sqliteStorage) Load(ctx context.Context) (*durable.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, durable.ErrClosed
	}
	if s.db == nil { // a read-only open of a missing file
		return nil, nil
	}
	return load(ctx, s.db)
}

// load reads the State from the record tables, or from a legacy single-row
// snapshot when no header was written yet; nil for an empty store.
func load(ctx context.Context, q querier) (*durable.State, error) {
	// A read-only open of an earlier database has no record tables.
	var tables int
	if err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='durable_header'").Scan(&tables); err != nil {
		return nil, err
	}
	if tables == 0 {
		return loadLegacy(ctx, q)
	}
	var data []byte
	err := q.QueryRowContext(ctx, "SELECT header FROM durable_header WHERE singleton=1").Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return loadLegacy(ctx, q)
	}
	if err != nil {
		return nil, err
	}
	var h header
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, fmt.Errorf("storage: invalid SQLite header: %w", err)
	}
	st := &durable.State{Version: h.Version, Sequence: h.Sequence, NextID: h.NextID, Root: h.Root,
		Conversations: map[durable.ConversationID]*durable.ConversationRecord{}, Entries: map[durable.EntryID]*durable.Entry{},
		Tasks: map[durable.TaskID]*durable.TaskRecord{}, Submissions: map[durable.SubmissionID]*durable.SubmissionRecord{},
		Documents: map[string]*durable.DocumentRecord{}}
	rows, err := q.QueryContext(ctx, "SELECT kind, key, value FROM durable_records")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, key string
		var value []byte
		if err := rows.Scan(&kind, &key, &value); err != nil {
			return nil, err
		}
		if err := loadRecord(st, kind, key, value); err != nil {
			return nil, fmt.Errorf("storage: invalid SQLite %s record %s: %w", kind, key, err)
		}
	}
	return st, rows.Err()
}

func loadRecord(st *durable.State, kind, key string, value []byte) error {
	id, _ := strconv.ParseUint(key, 10, 64)
	switch kind {
	case kindConversation:
		var v durable.ConversationRecord
		if err := json.Unmarshal(value, &v); err != nil {
			return err
		}
		st.Conversations[durable.ConversationID(id)] = &v
	case kindEntry:
		var v durable.Entry
		if err := json.Unmarshal(value, &v); err != nil {
			return err
		}
		st.Entries[durable.EntryID(id)] = &v
	case kindTask:
		var v durable.TaskRecord
		if err := json.Unmarshal(value, &v); err != nil {
			return err
		}
		st.Tasks[durable.TaskID(id)] = &v
	case kindSubmission:
		var v durable.SubmissionRecord
		if err := json.Unmarshal(value, &v); err != nil {
			return err
		}
		st.Submissions[durable.SubmissionID(id)] = &v
	case kindDocument:
		var v durable.DocumentRecord
		if err := json.Unmarshal(value, &v); err != nil {
			return err
		}
		st.Documents[key] = &v
	default:
		return fmt.Errorf("unknown record kind")
	}
	return nil
}

// loadLegacy reads the single-row snapshot of earlier versions, if any.
func loadLegacy(ctx context.Context, q querier) (*durable.State, error) {
	var exists int
	if err := q.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='durable_state'").Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, nil
	}
	var data []byte
	err := q.QueryRowContext(ctx, "SELECT snapshot FROM durable_state WHERE singleton=1").Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var v durable.State
	if err = json.Unmarshal(data, &v); err != nil {
		return nil, fmt.Errorf("storage: invalid SQLite snapshot: %w", err)
	}
	return &v, nil
}

// storedSequence is the sequence the store is at, converting a legacy
// snapshot into records first, inside the write's transaction.
func storedSequence(ctx context.Context, tx *sql.Tx) (uint64, error) {
	var sequence uint64
	err := tx.QueryRowContext(ctx, "SELECT sequence FROM durable_header WHERE singleton=1").Scan(&sequence)
	if err == nil {
		return sequence, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	legacy, err := loadLegacy(ctx, tx)
	if err != nil || legacy == nil {
		return 0, err
	}
	if err := writeAll(ctx, tx, legacy); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "DROP TABLE durable_state"); err != nil {
		return 0, err
	}
	return legacy.Sequence, nil
}

func writeHeader(ctx context.Context, tx *sql.Tx, h header) error {
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO durable_header(singleton, sequence, header) VALUES(1, ?, ?) ON CONFLICT(singleton) DO UPDATE SET sequence=excluded.sequence, header=excluded.header", h.Sequence, data)
	return err
}

func putRecord(ctx context.Context, tx *sql.Tx, kind, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO durable_records(kind, key, value) VALUES(?, ?, ?) ON CONFLICT(kind, key) DO UPDATE SET value=excluded.value", kind, key, data)
	return err
}

func deleteRecord(ctx context.Context, tx *sql.Tx, kind, key string) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM durable_records WHERE kind=? AND key=?", kind, key)
	return err
}

func uintKey[K ~uint64](id K) string { return strconv.FormatUint(uint64(id), 10) }

// writeAll replaces every record and the header with st.
func writeAll(ctx context.Context, tx *sql.Tx, st *durable.State) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM durable_records"); err != nil {
		return err
	}
	for id, v := range st.Conversations {
		if err := putRecord(ctx, tx, kindConversation, uintKey(id), v); err != nil {
			return err
		}
	}
	for id, v := range st.Entries {
		if err := putRecord(ctx, tx, kindEntry, uintKey(id), v); err != nil {
			return err
		}
	}
	for id, v := range st.Tasks {
		if err := putRecord(ctx, tx, kindTask, uintKey(id), v); err != nil {
			return err
		}
	}
	for id, v := range st.Submissions {
		if err := putRecord(ctx, tx, kindSubmission, uintKey(id), v); err != nil {
			return err
		}
	}
	for key, v := range st.Documents {
		if err := putRecord(ctx, tx, kindDocument, key, v); err != nil {
			return err
		}
	}
	return writeHeader(ctx, tx, header{Version: st.Version, Sequence: st.Sequence, NextID: st.NextID, Root: st.Root})
}

// write runs one store transaction after the sequence check. Every error
// before COMMIT rolls the transaction back, so nothing was applied and it is
// a RejectedError; only a failed COMMIT is uncertain.
func (s *sqliteStorage) write(ctx context.Context, expected, next uint64, apply func(*sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readOnly {
		return &durable.RejectedError{Err: durable.ErrReadOnly}
	}
	if s.closed {
		return &durable.RejectedError{Err: durable.ErrClosed}
	}
	if err := ctx.Err(); err != nil {
		return &durable.RejectedError{Err: err}
	}
	if expected == ^uint64(0) || next != expected+1 {
		return &durable.RejectedError{Err: durable.ErrInvalid}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return &durable.RejectedError{Err: err}
	}
	defer tx.Rollback()
	old, err := storedSequence(ctx, tx)
	if err != nil {
		return &durable.RejectedError{Err: err}
	}
	if old != expected {
		return &durable.RejectedError{Err: fmt.Errorf("%w: expected sequence %d, have %d", durable.ErrConflict, expected, old)}
	}
	if err := apply(tx); err != nil {
		return &durable.RejectedError{Err: err}
	}
	return tx.Commit()
}

func (s *sqliteStorage) Commit(ctx context.Context, expected uint64, next *durable.State) error {
	if next == nil {
		return &durable.RejectedError{Err: durable.ErrInvalid}
	}
	return s.write(ctx, expected, next.Sequence, func(tx *sql.Tx) error { return writeAll(ctx, tx, next) })
}

// CommitDelta writes the changed records, removes the removed ones, and
// updates the header in one transaction.
func (s *sqliteStorage) CommitDelta(ctx context.Context, expected uint64, delta *durable.StateDelta) error {
	if delta == nil {
		return &durable.RejectedError{Err: durable.ErrInvalid}
	}
	return s.write(ctx, expected, delta.Sequence, func(tx *sql.Tx) error {
		for id, v := range delta.Conversations {
			if err := putRecord(ctx, tx, kindConversation, uintKey(id), v); err != nil {
				return err
			}
		}
		for id, v := range delta.Entries {
			if err := putRecord(ctx, tx, kindEntry, uintKey(id), v); err != nil {
				return err
			}
		}
		for id, v := range delta.Tasks {
			if err := putRecord(ctx, tx, kindTask, uintKey(id), v); err != nil {
				return err
			}
		}
		for id, v := range delta.Submissions {
			if err := putRecord(ctx, tx, kindSubmission, uintKey(id), v); err != nil {
				return err
			}
		}
		for key, v := range delta.Documents {
			if err := putRecord(ctx, tx, kindDocument, key, v); err != nil {
				return err
			}
		}
		for _, id := range delta.Removed.Conversations {
			if err := deleteRecord(ctx, tx, kindConversation, uintKey(id)); err != nil {
				return err
			}
		}
		for _, id := range delta.Removed.Entries {
			if err := deleteRecord(ctx, tx, kindEntry, uintKey(id)); err != nil {
				return err
			}
		}
		for _, id := range delta.Removed.Tasks {
			if err := deleteRecord(ctx, tx, kindTask, uintKey(id)); err != nil {
				return err
			}
		}
		for _, id := range delta.Removed.Submissions {
			if err := deleteRecord(ctx, tx, kindSubmission, uintKey(id)); err != nil {
				return err
			}
		}
		for _, key := range delta.Removed.Documents {
			if err := deleteRecord(ctx, tx, kindDocument, key); err != nil {
				return err
			}
		}
		return writeHeader(ctx, tx, header{Version: delta.Version, Sequence: delta.Sequence, NextID: delta.NextID, Root: delta.Root})
	})
}

func (s *sqliteStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
