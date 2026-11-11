package durable

import (
	"sync"
)

type ReportKind string

const (
	ReportStorageFailed   ReportKind = "storage_failed"
	ReportTaskFaulted     ReportKind = "task_faulted"
	ReportMigrationFailed ReportKind = "migration_failed"
	ReportTaskOutdated    ReportKind = "task_outdated"
	ReportTaskError       ReportKind = "task_error"
	// ReportTaskInterrupted: a task committed a failed outcome whose ErrorDetail
	// reason is restart_limit or interrupted, so crashes ended it.
	ReportTaskInterrupted ReportKind = "task_interrupted"
	// ReportHostError: a host callback (Report.Callback) failed, such as OnWake.
	ReportHostError ReportKind = "host_error"
	// ReportExtensionError: extension code a task called failed, such as a
	// prompt section or a hook; the task continued without it.
	ReportExtensionError ReportKind = "extension_error"
)

type ReportTask struct {
	ID           TaskID
	Kind         string
	Conversation ConversationID
}

type Report struct {
	Kind      ReportKind
	Err       error
	Task      *ReportTask
	Extension string
	Reason    string
	// Restarts is the task's restart count before the commit that ended it
	// (task_interrupted only).
	Restarts int
	// Callback names the failed host callback (host_error only): "onWake",
	// "setTimer", "env", or "now".
	Callback string
}

// reportQueue delivers best-effort diagnostics serially without coupling
// producers or Session shutdown to a possibly slow report handler.
type reportQueue struct {
	mu         sync.Mutex
	ready      *sync.Cond
	handler    func(Report)
	queue      []Report
	head       int
	closed     bool
	workerDone chan struct{}
}

func newReportQueue(handler func(Report)) *reportQueue {
	q := &reportQueue{handler: handler}
	if handler == nil {
		return q
	}
	q.ready = sync.NewCond(&q.mu)
	q.workerDone = make(chan struct{})
	go q.run()
	return q
}

// enqueue copies mutable task metadata and never waits for handler delivery.
func (q *reportQueue) enqueue(report Report) {
	if q == nil || q.handler == nil {
		return
	}
	if report.Task != nil {
		task := *report.Task
		report.Task = &task
	}
	q.mu.Lock()
	if !q.closed {
		q.queue = append(q.queue, report)
		q.ready.Signal()
	}
	q.mu.Unlock()
}

// closeAdmission prevents new reports but does not wait for queued deliveries.
// The worker drains all accepted reports before exiting; a blocked callback may
// therefore outlive Session shutdown by design.
func (q *reportQueue) closeAdmission() {
	if q == nil || q.handler == nil {
		return
	}
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.ready.Broadcast()
	}
	q.mu.Unlock()
}

func (q *reportQueue) run() {
	defer close(q.workerDone)
	for {
		q.mu.Lock()
		for q.head == len(q.queue) && !q.closed {
			q.ready.Wait()
		}
		if q.head == len(q.queue) {
			q.queue = nil
			q.head = 0
			q.mu.Unlock()
			return
		}
		report := q.queue[q.head]
		q.queue[q.head] = Report{}
		q.head++
		// Periodically release consumed backing-array entries without making
		// queue movement part of callback execution.
		if q.head >= 64 && q.head*2 >= len(q.queue) {
			q.queue = append([]Report(nil), q.queue[q.head:]...)
			q.head = 0
		}
		q.mu.Unlock()

		func() {
			defer func() { _ = recover() }()
			q.handler(report)
		}()
	}
}

// report is the Harness' internal nonblocking report admission point. Reports
// are diagnostics, not durable events; a missing or closed queue drops them.
func (h *Harness) report(report Report) {
	if h == nil || h.reports == nil {
		return
	}
	h.reports.enqueue(report)
}
