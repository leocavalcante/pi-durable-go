package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	durable "github.com/leocavalcante/pi-durable-go"
	_ "modernc.org/sqlite"
)

// OpenSQLite opens an MVP whole-snapshot SQLite database using WAL and FULL synchronization.
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
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "CREATE TABLE IF NOT EXISTS durable_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), sequence INTEGER NOT NULL, snapshot BLOB NOT NULL)"} {
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
}

func (s *sqliteStorage) Load(ctx context.Context) (*durable.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, durable.ErrClosed
	}
	var b []byte
	err := s.db.QueryRowContext(ctx, "SELECT snapshot FROM durable_state WHERE singleton=1").Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var v durable.State
	if err = json.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("storage: invalid SQLite snapshot: %w", err)
	}
	return &v, nil
}
func (s *sqliteStorage) Commit(ctx context.Context, expected uint64, next *durable.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &durable.RejectedError{Err: durable.ErrClosed}
	}
	if err := ctx.Err(); err != nil {
		return &durable.RejectedError{Err: err}
	}
	if next == nil || expected == ^uint64(0) || next.Sequence != expected+1 {
		return &durable.RejectedError{Err: durable.ErrInvalid}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return &durable.RejectedError{Err: err}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var old uint64
	err = tx.QueryRowContext(ctx, "SELECT sequence FROM durable_state WHERE singleton=1").Scan(&old)
	if errors.Is(err, sql.ErrNoRows) {
		old = 0
	} else if err != nil {
		return err
	}
	if old != expected {
		return &durable.RejectedError{Err: fmt.Errorf("%w: expected sequence %d, have %d", durable.ErrConflict, expected, old)}
	}
	if old == 0 {
		_, err = tx.ExecContext(ctx, "INSERT INTO durable_state(singleton,sequence,snapshot) VALUES(1,?,?)", next.Sequence, data)
	} else {
		var res sql.Result
		res, err = tx.ExecContext(ctx, "UPDATE durable_state SET sequence=?,snapshot=? WHERE singleton=1 AND sequence=?", next.Sequence, data, expected)
		if err == nil {
			var n int64
			n, err = res.RowsAffected()
			if err == nil && n != 1 {
				return &durable.RejectedError{Err: durable.ErrConflict}
			}
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *sqliteStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}
