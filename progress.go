package durable

import (
	"sync"
	"time"
)

// progressBytesPerSecond is the pause each progress commit buys per byte it
// wrote (upstream chord PROGRESS_BYTES_PER_SECOND).
const progressBytesPerSecond = 100 * 1024

// progressPacer paces commits of a changing value by their size (upstream
// chord Progress): the first change after an idle period commits at once;
// each commit then delays the next by at least minInterval and by its
// written size at 100 KiB/s. At most one commit is in flight; changes made
// meanwhile coalesce into the next one. It paces on the Harness clock.
type progressPacer struct {
	// write commits the current value and returns the bytes it wrote.
	write       func() (int, error)
	onError     func(error)
	minInterval time.Duration
	now         func() time.Time
	setTimer    func(func(), time.Duration) func()

	mu       sync.Mutex
	waiters  []chan error
	disarm   func()        // the armed timer; nil when none
	inFlight chan struct{} // closed when the write in flight ends; nil when none
	nextAt   time.Time
	dirty    bool
	stopped  bool
}

// mark schedules a commit.
func (p *progressPacer) mark() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dirty = true
	p.schedule()
}

// markAndWait schedules a commit; the channel receives the outcome of the
// commit that includes this change.
func (p *progressPacer) markAndWait() <-chan error {
	waiter := make(chan error, 1)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.waiters = append(p.waiters, waiter)
	p.dirty = true
	p.schedule()
	return waiter
}

// stop stops committing and waits for the commit in flight; it returns the
// waiters the final commit must settle.
func (p *progressPacer) stop() []chan error {
	p.mu.Lock()
	p.stopped = true
	if p.disarm != nil {
		p.disarm()
		p.disarm = nil
	}
	inFlight := p.inFlight
	p.mu.Unlock()
	if inFlight != nil {
		<-inFlight
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	waiters := p.waiters
	p.waiters = nil
	return waiters
}

// schedule commits now or arms the timer for when the pause ends; mu is
// held.
func (p *progressPacer) schedule() {
	if p.stopped || p.disarm != nil || p.inFlight != nil {
		return
	}
	if wait := p.nextAt.Sub(p.now()); wait > 0 {
		p.disarm = p.setTimer(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.disarm = nil
			// Checks the clock again: a timer that fires early waits for the rest.
			p.schedule()
		}, wait)
		return
	}
	p.flush()
}

// flush starts a commit of what changed; mu is held.
func (p *progressPacer) flush() {
	if p.stopped || !p.dirty {
		return
	}
	p.dirty = false
	waiters := p.waiters
	p.waiters = nil
	started := p.now()
	done := make(chan struct{})
	p.inFlight = done
	go func() {
		written, err := p.write()
		pause := p.minInterval
		if err == nil {
			pause = max(pause, time.Duration(float64(written)*float64(time.Second)/progressBytesPerSecond))
		}
		for _, waiter := range waiters {
			waiter <- err
		}
		if err != nil && p.onError != nil {
			p.onError(err)
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.nextAt = started.Add(pause)
		p.inFlight = nil
		close(done)
		if p.dirty {
			p.schedule()
		}
	}()
}
