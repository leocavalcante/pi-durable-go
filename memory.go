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
