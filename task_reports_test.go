package durable

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReportQueueFIFOAsyncDrainAndMetadataCopy(t *testing.T) {
	started := make(chan Report, 1)
	received := make(chan Report, 2)
	release := make(chan struct{})
	q := newReportQueue(func(report Report) {
		if report.Kind == ReportTaskFaulted {
			started <- report
			<-release
		}
		received <- report
	})
	firstTask := &ReportTask{ID: 7, Kind: "test.task", Conversation: 3}
	q.enqueue(Report{Kind: ReportTaskFaulted, Task: firstTask})
	firstTask.ID = 999

	select {
	case got := <-started:
		if got.Task == nil || got.Task.ID != 7 {
			t.Fatalf("queued task metadata was retained by reference: %+v", got.Task)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("report worker did not start")
	}

	second := Report{Kind: ReportTaskError, Reason: "second"}
	enqueued := make(chan struct{})
	go func() {
		q.enqueue(second)
		close(enqueued)
	}()
	select {
	case <-enqueued:
	case <-time.After(time.Second):
		t.Fatal("producer blocked behind a slow report handler")
	}

	closed := make(chan struct{})
	go func() {
		q.closeAdmission()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("closing report admission waited for delivery")
	}
	close(release)

	var got []ReportKind
	for range 2 {
		select {
		case report := <-received:
			got = append(got, report.Kind)
		case <-time.After(3 * time.Second):
			t.Fatal("queued reports were not drained")
		}
	}
	if want := []ReportKind{ReportTaskFaulted, ReportTaskError}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery order=%v, want %v", got, want)
	}
	select {
	case <-q.workerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("report worker did not exit after draining")
	}
}

func TestReportQueueContainsPanicsAndNilHandlerStartsNoWorker(t *testing.T) {
	var calls atomic.Int32
	delivered := make(chan ReportKind, 1)
	q := newReportQueue(func(report Report) {
		if calls.Add(1) == 1 {
			panic("report handler panic")
		}
		delivered <- report.Kind
	})
	q.enqueue(Report{Kind: ReportTaskError, Reason: "panic"})
	q.enqueue(Report{Kind: ReportTaskOutdated, Reason: "continues"})
	q.closeAdmission()
	select {
	case kind := <-delivered:
		if kind != ReportTaskOutdated {
			t.Fatalf("delivered report after panic=%q", kind)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not continue after handler panic")
	}
	select {
	case <-q.workerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("report worker did not exit")
	}

	nilQueue := newReportQueue(nil)
	if nilQueue.workerDone != nil || nilQueue.ready != nil {
		t.Fatal("nil handler allocated a waiting worker")
	}
	nilQueue.enqueue(Report{Kind: ReportStorageFailed})
	nilQueue.closeAdmission()
}

func TestFailureReportHookIsOnceAfterSealAndReentrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	bookkeepingErr := errors.New("scheduler bookkeeping failed")
	closeErr := errors.New("secondary backend close error")
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), closeErr: closeErr}
	s, err := NewSession(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}

	handlerDone := make(chan error, 1)
	q := newReportQueue(func(report Report) {
		if report.Kind != ReportStorageFailed || report.Err != bookkeepingErr {
			handlerDone <- errors.New("report did not preserve the first failure")
			return
		}
		if !errors.Is(s.Err(), bookkeepingErr) {
			handlerDone <- errors.New("failure callback ran before Session sealing")
			return
		}
		if _, snapshotErr := s.Snapshot(context.Background()); !errors.Is(snapshotErr, bookkeepingErr) {
			handlerDone <- errors.New("reentrant snapshot did not see Session failure")
			return
		}
		if got := s.Close(); got != closeErr {
			handlerDone <- errors.New("reentrant Close did not return the backend close error")
			return
		}
		end, waitErr := s.WaitClosed(context.Background())
		if waitErr != nil || end.Kind != "failed" || end.Err != bookkeepingErr {
			handlerDone <- errors.New("reentrant WaitClosed did not observe the first failure")
			return
		}
		handlerDone <- nil
	})
	s.failureHook = func(cause error) { q.enqueue(Report{Kind: ReportStorageFailed, Err: cause}) }
	s.afterClose = q.closeAdmission

	// Repeating fail and a later Storage.Close error must not replace or repeat
	// the first cause's one report.
	s.fail(bookkeepingErr)
	s.fail(errors.New("later bookkeeping failure"))
	end, err := s.WaitClosed(ctx)
	if err != nil || end.Kind != "failed" || end.Err != bookkeepingErr {
		t.Fatalf("Session end=%+v error=%v", end, err)
	}
	select {
	case callbackErr := <-handlerDone:
		if callbackErr != nil {
			t.Fatal(callbackErr)
		}
	case <-ctx.Done():
		t.Fatal("reentrant report handler deadlocked shutdown")
	}
	select {
	case <-q.workerDone:
	case <-ctx.Done():
		t.Fatal("report queue did not drain")
	}
	if got := store.closes.Load(); got != 1 {
		t.Fatalf("Storage.Close called %d times", got)
	}
}

func TestHarnessReportHandlerCanReenterCloseAndWaitClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cause := errors.New("scheduler bookkeeping failure")
	var h *Harness
	reportResult := make(chan error, 1)
	releaseHandler := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseHandler) })

	var err error
	h, err = Open(ctx, NewMemoryStorage(), HarnessOptions{OnReport: func(report Report) {
		if report.Kind != ReportStorageFailed || report.Err != cause || !errors.Is(h.Err(), cause) {
			reportResult <- errors.New("unexpected or prematurely delivered failure report")
			return
		}
		if _, snapshotErr := h.Snapshot(context.Background()); !errors.Is(snapshotErr, cause) {
			reportResult <- errors.New("reentrant snapshot did not observe failure")
			return
		}
		if err := h.Close(context.Background()); err != nil {
			reportResult <- errors.New("reentrant Close returned an error")
			return
		}
		end, waitErr := h.WaitClosed(context.Background())
		if waitErr != nil || end.Kind != "failed" || end.Err != cause {
			reportResult <- errors.New("reentrant WaitClosed did not observe failure")
			return
		}
		reportResult <- nil
		<-releaseHandler
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	h.session.fail(cause)
	select {
	case reportErr := <-reportResult:
		if reportErr != nil {
			t.Fatal(reportErr)
		}
	case <-ctx.Done():
		t.Fatal("OnReport reentrant shutdown deadlocked")
	}
	if _, err := h.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.reports.workerDone:
		t.Fatal("Session shutdown joined a still-running OnReport callback")
	default:
	}
	releaseOnce.Do(func() { close(releaseHandler) })
	select {
	case <-h.reports.workerDone:
	case <-ctx.Done():
		t.Fatal("OnReport worker did not resume and drain")
	}
}

func TestSessionFailureReportIncludesBackendCloseError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cause := errors.New("backend close failure")
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), closeErr: cause}
	s, err := NewSession(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan Report, 2)
	q := newReportQueue(func(report Report) { received <- report })
	s.failureHook = func(err error) { q.enqueue(Report{Kind: ReportStorageFailed, Err: err}) }
	s.afterClose = q.closeAdmission
	if err := s.Close(); err != cause {
		t.Fatalf("Close error=%v, want %v", err, cause)
	}
	end, err := s.WaitClosed(ctx)
	if err != nil || end.Kind != "failed" || end.Err != cause {
		t.Fatalf("end=%+v error=%v", end, err)
	}
	select {
	case report := <-received:
		if report.Kind != ReportStorageFailed || report.Err != cause {
			t.Fatalf("report=%+v", report)
		}
	case <-ctx.Done():
		t.Fatal("backend close failure was not reported")
	}
	select {
	case <-q.workerDone:
	case <-ctx.Done():
		t.Fatal("report worker did not finish")
	}
	select {
	case report := <-received:
		t.Fatalf("duplicate failure report: %+v", report)
	default:
	}
}

func TestSessionFailureReportForUncertainWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cause := errors.New("uncertain write")
	store := &lifecycleStorage{MemoryStorage: NewMemoryStorage(), writeErr: cause}
	store.fail.Store(true)
	s, err := NewSession(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan Report, 2)
	q := newReportQueue(func(report Report) { received <- report })
	s.failureHook = func(err error) { q.enqueue(Report{Kind: ReportStorageFailed, Err: err}) }
	s.afterClose = q.closeAdmission
	if err := s.Commit(ctx, touchStorage); err != cause {
		t.Fatalf("Commit error=%v", err)
	}
	if _, err := s.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case report := <-received:
		if report.Kind != ReportStorageFailed || report.Err != cause {
			t.Fatalf("report=%+v", report)
		}
	case <-ctx.Done():
		t.Fatal("uncertain write failure was not reported")
	}
	select {
	case <-q.workerDone:
	case <-ctx.Done():
		t.Fatal("report worker did not finish")
	}
	select {
	case report := <-received:
		t.Fatalf("duplicate failure report: %+v", report)
	default:
	}
}
