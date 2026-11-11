// Package storage provides durable whole-snapshot storage backends.
// The JSONL and SQLite formats here are MVP formats and are not upstream wire compatible.
package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	durable "github.com/leocavalcante/pi-durable-go"
)

// OpenJSONL opens a single-owner append-only snapshot log. Only an incomplete final line is recovered.
// Advisory locking is currently unsupported on Windows.
func OpenJSONL(ctx context.Context, directory string) (durable.Storage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(directory, "snapshots.jsonl"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	unlock, err := lockFile(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	s := &jsonlStorage{file: f, unlock: unlock, path: filepath.Join(directory, "snapshots.jsonl")}
	if err = s.recover(); err != nil {
		_ = s.Close()
		return nil, err
	}
	// A compaction that crashed before its rename leaves only a stale copy.
	_ = os.Remove(s.path + ".compact")
	return s, nil
}

type jsonlStorage struct {
	mu     sync.Mutex
	file   *os.File
	unlock func() error
	state  *durable.State
	closed bool
	path   string
	size   int64 // bytes of complete snapshots in the file
}

// The log is compacted to its latest snapshot once it holds more than
// compactFactor snapshots' worth of bytes and at least compactMinBytes.
const (
	compactFactor   = 8
	compactMinBytes = 1 << 20
)

func (s *jsonlStorage) Load(ctx context.Context) (*durable.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, durable.ErrClosed
	}
	if s.state == nil {
		return nil, nil
	}
	b, e := json.Marshal(s.state)
	if e != nil {
		return nil, e
	}
	var v durable.State
	e = json.Unmarshal(b, &v)
	return &v, e
}
func (s *jsonlStorage) Commit(ctx context.Context, expected uint64, next *durable.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &durable.RejectedError{Err: durable.ErrClosed}
	}
	if err := ctx.Err(); err != nil {
		return &durable.RejectedError{Err: err}
	}
	actual := uint64(0)
	if s.state != nil {
		actual = s.state.Sequence
	}
	if actual != expected {
		return &durable.RejectedError{Err: fmt.Errorf("%w: expected sequence %d, have %d", durable.ErrConflict, expected, actual)}
	}
	if next == nil || expected == ^uint64(0) || next.Sequence != expected+1 {
		return &durable.RejectedError{Err: durable.ErrInvalid}
	}
	b, err := json.Marshal(next)
	if err != nil {
		return &durable.RejectedError{Err: err}
	}
	b = append(b, '\n')
	if _, err = s.file.Write(b); err != nil {
		return err
	}
	if err = s.file.Sync(); err != nil {
		return err
	}
	s.size += int64(len(b))
	cp, err := copyState(next)
	if err != nil {
		return err
	}
	s.state = cp
	if s.size >= compactMinBytes && s.size > compactFactor*int64(len(b)) {
		// Best effort: the commit is durable either way.
		_ = s.compact(b)
	}
	return nil
}

// compact replaces the log with its latest snapshot line. The copy is
// locked before it is renamed over the log, so the single-owner lock never
// lapses; a crash leaves either the old log or the new one, and both end
// with the latest snapshot. Not on Windows, where the open log cannot be
// replaced.
func (s *jsonlStorage) compact(latest []byte) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	tmp := s.path + ".compact"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	unlock, err := lockFile(f)
	if err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	abandon := func(err error) error {
		_ = unlock()
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if _, err := f.Write(latest); err != nil {
		return abandon(err)
	}
	if err := f.Sync(); err != nil {
		return abandon(err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return abandon(err)
	}
	if dir, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	old, oldUnlock := s.file, s.unlock
	s.file, s.unlock, s.size = f, unlock, int64(len(latest))
	_ = oldUnlock()
	return old.Close()
}
func (s *jsonlStorage) recover() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(s.file)
	var good int64
	var previous uint64
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				break
			}
			var v durable.State
			if e := json.Unmarshal(line, &v); e != nil {
				return fmt.Errorf("storage: corrupt complete JSONL snapshot: %w", e)
			}
			if v.Sequence == 0 || v.Sequence <= previous {
				return fmt.Errorf("storage: non-increasing JSONL sequence %d", v.Sequence)
			}
			previous = v.Sequence
			cp, e := copyState(&v)
			if e != nil {
				return e
			}
			s.state = cp
			good += int64(len(line))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	s.size = good
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() != good {
		if err = s.file.Truncate(good); err != nil {
			return err
		}
		if err = s.file.Sync(); err != nil {
			return err
		}
	}
	_, err = s.file.Seek(0, io.SeekEnd)
	return err
}
func (s *jsonlStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	if e := s.unlock(); e != nil {
		errs = append(errs, e)
	}
	if e := s.file.Close(); e != nil {
		errs = append(errs, e)
	}
	return errors.Join(errs...)
}
func copyState(v *durable.State) (*durable.State, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return nil, e
	}
	var cp durable.State
	e = json.Unmarshal(b, &cp)
	return &cp, e
}
