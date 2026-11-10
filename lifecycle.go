package durable

import "context"

// SessionFailedError preserves the first uncertain storage failure. It matches
// ErrPoisoned with errors.Is and unwraps to the original cause. The triggering
// operation returns the original error; later operations and pending waits
// return this wrapper. The message intentionally does not disclose the cause.
type SessionFailedError struct{ cause error }

func (e *SessionFailedError) Error() string        { return ErrPoisoned.Error() }
func (e *SessionFailedError) Unwrap() error        { return e.cause }
func (e *SessionFailedError) Is(target error) bool { return target == ErrPoisoned }

// SessionEnd is available after shutdown, including backend closure. Reason is
// "closed" for a clean close or "failed" with the first storage failure in Err.
// Errors may contain private backend details; do not publish them unreviewed.
type SessionEnd struct {
	Reason string
	Err    error
}

// Err reports sealed admission immediately, without waiting for active task
// code or storage calls to return. A fatal failure takes priority over a close.
func (s *Session) Err() error {
	if failure := s.failure.Load(); failure != nil {
		return failure
	}
	select {
	case <-s.closing:
		return ErrClosed
	default:
		return nil
	}
}

// Done closes once shutdown is complete, including Storage.Close and, for a
// Harness Session, all active task invocations. It does not initiate shutdown.
func (s *Session) Done() <-chan struct{} { return s.closeDone }

// WaitClosed waits for the terminal result without initiating shutdown.
// Cancelling the wait does not cancel cleanup or reopen admission.
func (s *Session) WaitClosed(ctx context.Context) (SessionEnd, error) {
	select {
	case <-ctx.Done():
		return SessionEnd{}, ctx.Err()
	case <-s.closeDone:
		return s.end, nil
	}
}

// Close seals admission, ends watches, joins admitted work, and closes storage.
// Repeated calls join the same shutdown and return the backend close error.
func (s *Session) Close() error { return s.closeContext(context.Background()) }

func (s *Session) closeContext(ctx context.Context) error {
	s.startClose()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeDone:
		return s.closeErr
	}
}

func (s *Session) startClose() {
	s.closeOnce.Do(func() {
		// Sealing never waits on the mutation mutex. A caller can time out a
		// Close wait even if admitted storage or task code ignores cancellation.
		close(s.closing)
		go s.finishClose()
	})
}

// Called under the commit mutex. Never publish the uncertain candidate, and
// end current watches before cancelling task invocations during shutdown.
func (s *Session) failLocked(cause error) {
	s.failure.CompareAndSwap(nil, &SessionFailedError{cause: cause})
	s.closeWatchersLocked()
	s.startClose()
}

func (s *Session) closeWatchersLocked() {
	for w := range s.watchers {
		if !w.closed {
			w.closed = true
			close(w.ch)
			close(w.done)
		}
		delete(s.watchers, w)
	}
}

func (s *Session) finishClose() {
	// Every storage commit runs under this mutex. Wait for an admitted one to
	// settle before touching the backend; newly queued calls see ErrClosed.
	s.mu.Lock()
	s.closeWatchersLocked()
	s.mu.Unlock()
	if s.beforeClose != nil {
		s.beforeClose()
	}
	err := s.storage.Close()
	if err != nil {
		s.failure.CompareAndSwap(nil, &SessionFailedError{cause: err})
	}
	s.closeErr = err
	s.end = SessionEnd{Reason: "closed"}
	if failure := s.failure.Load(); failure != nil {
		s.end = SessionEnd{Reason: "failed", Err: failure.cause}
	}
	// Fields above are immutable after this publication, for every waiter.
	close(s.closeDone)
}

// Err reports the Harness Session's immediate admission/failure status.
func (h *Harness) Err() error {
	if h == nil || h.session == nil {
		return ErrClosed
	}
	return h.session.Err()
}

// Done and WaitClosed observe automatic failure shutdown as well as Close.
func (h *Harness) Done() <-chan struct{} { return h.session.Done() }
func (h *Harness) WaitClosed(ctx context.Context) (SessionEnd, error) {
	return h.session.WaitClosed(ctx)
}
