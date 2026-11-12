package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errSimulatedCrash = errors.New("test: simulated host crash")

// crashStorage simulates killing the host over a shared backend. After the
// switch, every Commit returns an uncertain error (never a RejectedError) and
// writes nothing, so the failed Session leaves running records exactly as a
// killed process would: a failed Session writes nothing at close. Close
// releases only this wrapper; the backend stays open for the next host.
type crashStorage struct {
	Storage
	crashed atomic.Bool
	// crashOn, when set, kills the host instead of applying a matching commit.
	crashOn func(*State) bool
}

func (s *crashStorage) Commit(ctx context.Context, sequence uint64, next *State) error {
	if s.crashed.Load() || s.crashOn != nil && s.crashOn(next) {
		s.crashed.Store(true)
		return errSimulatedCrash
	}
	return s.Storage.Commit(ctx, sequence, next)
}

func (s *crashStorage) Close() error { return nil }

// crashRecordStorage is crashStorage over a backend with record reads: it
// stores deltas, rebuilding the next whole State for crashOn.
type crashRecordStorage struct {
	*crashStorage
	records RecordStorage
}

func (s *crashRecordStorage) CommitDelta(ctx context.Context, expected uint64, delta *StateDelta) error {
	if s.crashed.Load() {
		return errSimulatedCrash
	}
	if s.crashOn != nil {
		next, err := s.records.Load(ctx)
		if err != nil {
			return err
		}
		if next == nil {
			next = &State{}
		}
		detached, err := delta.Clone()
		if err != nil {
			return err
		}
		if err := detached.ApplyTo(next); err != nil {
			return err
		}
		if s.crashOn(next) {
			s.crashed.Store(true)
			return errSimulatedCrash
		}
	}
	return s.records.CommitDelta(ctx, expected, delta)
}

func (s *crashRecordStorage) CommitDeltas(ctx context.Context, expected uint64, deltas []*StateDelta) (int, error) {
	return commitEach(ctx, s, expected, deltas)
}

func (s *crashRecordStorage) LoadResident(ctx context.Context) (*State, error) {
	return s.records.LoadResident(ctx)
}
func (s *crashRecordStorage) Conversation(ctx context.Context, id ConversationID) (*ConversationRecord, error) {
	return s.records.Conversation(ctx, id)
}
func (s *crashRecordStorage) ScanConversations(ctx context.Context, query ConversationQuery, limit int, cursor *ScanCursor) (ScanPage[ConversationRecord], error) {
	return s.records.ScanConversations(ctx, query, limit, cursor)
}
func (s *crashRecordStorage) Entry(ctx context.Context, id EntryID) (*Entry, error) {
	return s.records.Entry(ctx, id)
}
func (s *crashRecordStorage) ScanEntries(ctx context.Context, cid ConversationID, query EntryQuery, limit int, cursor *EntryCursor) (EntryPage, error) {
	return s.records.ScanEntries(ctx, cid, query, limit, cursor)
}
func (s *crashRecordStorage) Task(ctx context.Context, id TaskID) (*TaskRecord, error) {
	return s.records.Task(ctx, id)
}
func (s *crashRecordStorage) ScanTasks(ctx context.Context, query TaskQuery, limit int, cursor *ScanCursor) (ScanPage[TaskRecord], error) {
	return s.records.ScanTasks(ctx, query, limit, cursor)
}
func (s *crashRecordStorage) Submission(ctx context.Context, id SubmissionID) (*SubmissionRecord, error) {
	return s.records.Submission(ctx, id)
}
func (s *crashRecordStorage) SubmissionByRequest(ctx context.Context, cid ConversationID, requestID string) (*SubmissionRecord, error) {
	return s.records.SubmissionByRequest(ctx, cid, requestID)
}
func (s *crashRecordStorage) ScanSubmissions(ctx context.Context, query SubmissionQuery, limit int, cursor *ScanCursor) (ScanPage[SubmissionRecord], error) {
	return s.records.ScanSubmissions(ctx, query, limit, cursor)
}
func (s *crashRecordStorage) Document(ctx context.Context, key string) (*DocumentRecord, error) {
	return s.records.Document(ctx, key)
}
func (s *crashRecordStorage) ScanDocuments(ctx context.Context, query DocumentQuery, limit int, cursor *ScanCursor) (ScanPage[DocumentRecord], error) {
	return s.records.ScanDocuments(ctx, query, limit, cursor)
}
func (s *crashRecordStorage) DocumentRevision(ctx context.Context, docKey string, atOrBefore uint64) (*DocumentRevision, error) {
	return s.records.DocumentRevision(ctx, docKey, atOrBefore)
}

// crashHosts opens successive Harnesses over one backend, each of which can crash.
type crashHosts struct {
	t       *testing.T
	backend Storage
	options HarnessOptions
	store   *crashStorage
}

func newCrashHosts(t *testing.T, backend Storage, options HarnessOptions) *crashHosts {
	if backend == nil {
		backend = NewMemoryStorage()
	}
	// As upstream's restart tests do: let deferred progress commits reach
	// storage before a crash.
	if options.Settings.Progress.PersistWithinMs == nil {
		options.Settings.Progress.PersistWithinMs = ptr(0.0)
	}
	return &crashHosts{t: t, backend: backend, options: options}
}

// open starts the next host over the shared backend; crashOn applies to it only.
func (c *crashHosts) open(crashOn func(*State) bool) *Harness {
	c.t.Helper()
	return c.openWith(c.options, crashOn)
}

func (c *crashHosts) openWith(options HarnessOptions, crashOn func(*State) bool) *Harness {
	c.t.Helper()
	c.store = &crashStorage{Storage: c.backend, crashOn: crashOn}
	var storage Storage = c.store
	if records, ok := c.backend.(RecordStorage); ok {
		// A backend with record reads gives a bounded Session (plan 0008).
		storage = &crashRecordStorage{crashStorage: c.store, records: records}
	}
	h, err := Open(context.Background(), storage, options)
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	})
	return h
}

// crash kills h now and waits until its task goroutines have stopped.
func (c *crashHosts) crash(h *Harness) {
	c.t.Helper()
	c.store.crashed.Store(true)
	if err := h.Commit(context.Background(), touchStorage); err == nil {
		c.t.Fatal("commit succeeded on a crashed host")
	}
	c.died(h)
}

// died waits for a host that a crash point killed.
func (c *crashHosts) died(h *Harness) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	end, err := h.WaitClosed(ctx)
	if err != nil {
		c.t.Fatalf("crashed host did not stop: %v", err)
	}
	if end.Kind != "failed" || !errors.Is(end.Err, errSimulatedCrash) {
		c.t.Fatalf("host end=%+v, want a simulated crash", end)
	}
}

// reportLog collects reports from every host of a test.
type reportLog struct {
	mu      sync.Mutex
	reports []Report
}

func (l *reportLog) add(report Report) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reports = append(l.reports, report)
}

func (l *reportLog) of(kind ReportKind) []Report {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Report
	for _, report := range l.reports {
		if report.Kind == kind {
			out = append(out, report)
		}
	}
	return out
}

// drained closes h and waits until its report worker delivered everything.
func drained(t *testing.T, h *Harness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.Close(ctx)
	if h.reports.workerDone == nil {
		return
	}
	select {
	case <-h.reports.workerDone:
	case <-ctx.Done():
		t.Fatal("report worker did not drain")
	}
}

func installTasks(t *testing.T, registry *Registry, name string, definitions ...TaskDefinition) {
	t.Helper()
	if err := registry.Install(Extension{Name: name, Tasks: definitions}); err != nil {
		t.Fatal(err)
	}
}

// seedTask creates a task and sets scheduler-owned fields directly, standing in
// for crashes a test does not replay one by one.
func seedTask(t *testing.T, h *Harness, cid ConversationID, definition TaskDefinition, opts TaskOptions, seed func(*TaskRecord)) TaskID {
	t.Helper()
	var id TaskID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		if id, err = tx.CreateTask(cid, definition, nil, opts); err != nil {
			return err
		}
		if seed != nil {
			seed(tx.state.mutTask(id))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func assertRestartLimitFault(t *testing.T, record TaskRecord, message string) {
	t.Helper()
	if record.Status != "terminal" || record.Outcome == nil || record.Outcome.Status != OutcomeFaulted ||
		record.Outcome.Error != message || !jsonEqual(record.Outcome.ErrorDetail, restartLimitDetail) || record.Restarts != 0 {
		t.Fatalf("record=%+v outcome=%+v, want faulted %q with restart_limit", record, record.Outcome, message)
	}
}

func TestCrashLoopRestartLimitNormalization(t *testing.T) {
	for _, tc := range []struct {
		setting, want float64
	}{
		{0, 5}, {-1, 5}, {0.5, 5}, {nan(), 5}, {1, 1}, {2.7, 2}, {3, 3}, {inf(), inf()},
	} {
		if got := normalizeRestartLimit(tc.setting); got != tc.want {
			t.Errorf("normalizeRestartLimit(%v)=%v, want %v", tc.setting, got, tc.want)
		}
	}
	if cutOffTooOften(0, 1) || !cutOffTooOften(1, 1) || cutOffTooOften(4, 5) || !cutOffTooOften(6, 5) || cutOffTooOften(1<<30, inf()) {
		t.Error("cutOffTooOften")
	}
	if !salvageable(5, 5) || salvageable(6, 5) || !salvageable(0, 5) {
		t.Error("salvageable")
	}
	if cutOffCount(1) != "once" || cutOffCount(3) != "3 times in a row" {
		t.Error("cutOffCount")
	}
	for _, tc := range []struct {
		outcome *Outcome
		want    string
	}{
		{&Outcome{Status: OutcomeFailed, Error: "x", ErrorDetail: json.RawMessage(`{"reason":"interrupted"}`)}, reasonInterrupted},
		{&Outcome{Status: OutcomeFailed, Error: "x", ErrorDetail: json.RawMessage(` {"reason" : "restart_limit", "n": 1} `)}, reasonRestartLimit},
		{&Outcome{Status: OutcomeFailed, Error: "x", ErrorDetail: json.RawMessage(`{"reason":"other"}`)}, ""},
		{&Outcome{Status: OutcomeFailed, Error: "x", ErrorDetail: json.RawMessage(`"interrupted"`)}, ""},
		{&Outcome{Status: OutcomeFailed, Error: "x", ErrorDetail: json.RawMessage(`["interrupted"]`)}, ""},
		{&Outcome{Status: OutcomeFailed, Error: "x", ErrorDetail: json.RawMessage(`{"reason":["interrupted"]}`)}, ""},
		{&Outcome{Status: OutcomeFailed, Error: "x"}, ""},
		{&Outcome{Status: OutcomeFaulted, Error: "x", ErrorDetail: restartLimitDetail}, ""},
		{&Outcome{Status: OutcomeAborted, ErrorDetail: interruptedDetail}, ""},
		{nil, ""},
	} {
		if got := interruptionOf(tc.outcome); got != tc.want {
			t.Errorf("interruptionOf(%+v)=%q, want %q", tc.outcome, got, tc.want)
		}
	}
}

func nan() float64 { var zero float64; return zero / zero }
func inf() float64 { var zero float64; return 1 / zero }

func TestCrashLoopExtensionTaskFaultsAtLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := &reportLog{}
	registry := NewRegistry()
	started := make(chan int, 8)
	var runs atomic.Int32
	definition := TaskDefinition{Kind: "test.crash-loop", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		runs.Add(1)
		started <- task.Restarts
		<-ctx.Done()
		return ctx.Err()
	}}
	installTasks(t, registry, "crashy", definition)
	hosts := newCrashHosts(t, nil, HarnessOptions{Registry: registry, OnReport: log.add})
	h := hosts.open(nil)
	id := createTaskForTest(t, h, definition)
	for crash := 0; crash < defaultRestartLimit; crash++ {
		h.Resume()
		select {
		case restarts := <-started:
			if restarts != crash {
				t.Fatalf("run %d saw restarts=%d", crash+1, restarts)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		hosts.crash(h)
		h = hosts.open(nil)
		if task, err := h.Task(ctx, id); err != nil || task.Status != "pending" || task.Restarts != crash+1 {
			t.Fatalf("after crash %d: task=%+v error=%v", crash+1, task, err)
		}
	}
	record, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	message := "Task " + itoa(id) + " (test.crash-loop) was cut off 5 times without progress"
	assertRestartLimitFault(t, record, message)
	if runs.Load() != defaultRestartLimit {
		t.Fatalf("Run called %d times, want %d", runs.Load(), defaultRestartLimit)
	}
	drained(t, h)
	faults := log.of(ReportTaskFaulted)
	if len(faults) != 1 || faults[0].Reason != reasonRestartLimit || faults[0].Extension != "crashy" || faults[0].Err == nil ||
		faults[0].Err.Error() != message || faults[0].Task == nil || faults[0].Task.ID != id || faults[0].Task.Kind != "test.crash-loop" {
		t.Fatalf("task_faulted reports=%+v", faults)
	}
	if got := log.of(ReportTaskInterrupted); len(got) != 0 {
		t.Fatalf("scheduler fault also reported task_interrupted: %+v", got)
	}
}

func itoa(id TaskID) string {
	data, _ := json.Marshal(id)
	return string(data)
}

func TestCrashLoopHeldFaultCascadesToOwnedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	registry := NewRegistry()
	var parentRuns, childRuns, childAborts atomic.Int32
	release := make(chan struct{})
	parent := TaskDefinition{Kind: "test.cut-parent", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		parentRuns.Add(1)
		return nil
	}}
	child := TaskDefinition{Kind: "test.cut-child", Version: 1,
		Run: func(context.Context, *TaskRuntime, TaskRecord) error { childRuns.Add(1); return nil },
		Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
			childAborts.Add(1)
			<-release // the parent holds while its owned work cleans up
			return r.Complete(ctx, Outcome{Status: OutcomeAborted})
		}}
	installTasks(t, registry, "cut", parent, child)
	h := taskTestHarness(t, HarnessOptions{Registry: registry})
	root := testRoot(t, h)
	parentID := seedTask(t, h, root.ID(), parent, TaskOptions{}, func(task *TaskRecord) { task.Restarts = 5 })
	childID := seedTask(t, h, root.ID(), child, TaskOptions{Owner: Owner{Task: parentID}}, nil)
	h.Resume()
	deadline := time.Now().Add(5 * time.Second)
	for {
		task, err := h.Task(ctx, parentID)
		if err != nil {
			t.Fatal(err)
		}
		if task.Status == "completing" {
			if task.Outcome == nil || task.Outcome.Status != OutcomeFaulted || !jsonEqual(task.Outcome.ErrorDetail, restartLimitDetail) || task.Restarts != 0 {
				t.Fatalf("held fault=%+v", task)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("parent never held its fault: %+v", task)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	childRecord, err := h.WaitTask(ctx, childID)
	if err != nil || childRecord.Outcome == nil || childRecord.Outcome.Status != OutcomeAborted {
		t.Fatalf("child=%+v error=%v", childRecord, err)
	}
	record, err := h.WaitTask(ctx, parentID)
	if err != nil {
		t.Fatal(err)
	}
	assertRestartLimitFault(t, record, "Task "+itoa(parentID)+" (test.cut-parent) was cut off 5 times without progress")
	if parentRuns.Load() != 0 || childRuns.Load() != 0 || childAborts.Load() != 1 {
		t.Fatalf("parent runs=%d child runs=%d aborts=%d", parentRuns.Load(), childRuns.Load(), childAborts.Load())
	}
}

func TestCrashLoopLimitPrecedesResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log := &reportLog{}
	h := taskTestHarness(t, HarnessOptions{OnReport: log.add})
	root := testRoot(t, h)
	missing := TaskDefinition{Kind: "test.missing-definition", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		t.Error("a task without an installed definition ran")
		return nil
	}}
	seed := func(restarts int, abort bool, reason string) TaskID {
		return seedTask(t, h, root.ID(), missing, TaskOptions{}, func(task *TaskRecord) {
			task.Restarts, task.AbortRequested, task.AbortReason = restarts, abort, reason
		})
	}
	blocked := seed(5, false, "")
	abortBlocked := seed(5, true, "")
	abandoned := seed(5, true, "restart")
	orphan := seed(4, true, "")
	waiting := seed(4, false, "")
	h.Resume()
	kind := " (test.missing-definition) "
	for id, message := range map[TaskID]string{
		blocked:      "Task " + itoa(blocked) + kind + "was cut off 5 times without progress",
		abortBlocked: "Abort handler of task " + itoa(abortBlocked) + kind + "was cut off 5 times",
		abandoned:    "Abort handler of task " + itoa(abandoned) + kind + "was cut off 5 times",
	} {
		record, err := h.WaitTask(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		assertRestartLimitFault(t, record, message)
	}
	record, err := h.WaitTask(ctx, orphan)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeOrphaned || record.Outcome.Reason != "missing_task" {
		t.Fatalf("below the limit an abort-marked blocked task orphans: %+v error=%v", record, err)
	}
	inspection, err := h.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, task := range inspection.Tasks {
		if task.Record.ID == waiting {
			found = true
			if task.Record.Status != "pending" || task.State.Kind != "blocked" || task.State.Reason != "missing_task" || task.Record.Restarts != 4 {
				t.Fatalf("below the limit a blocked task stays blocked: %+v", task)
			}
		}
	}
	if !found {
		t.Fatal("blocked task missing from inspection")
	}
	drained(t, h)
	faults := log.of(ReportTaskFaulted)
	if len(faults) != 3 {
		t.Fatalf("task_faulted reports=%+v", faults)
	}
	for _, report := range faults {
		if report.Reason != reasonRestartLimit || report.Extension != "" || report.Task == nil || report.Task.Kind != missing.Kind {
			t.Fatalf("report=%+v", report)
		}
	}
}

func TestCrashLoopAbortHandlerFaultsAtLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := &reportLog{}
	registry := NewRegistry()
	var aborts atomic.Int32
	definition := TaskDefinition{Kind: "test.abort-crash", Version: 1,
		Run: func(ctx context.Context, _ *TaskRuntime, _ TaskRecord) error { <-ctx.Done(); return ctx.Err() },
		Abort: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
			aborts.Add(1)
			return r.Complete(ctx, Outcome{Status: OutcomeAborted})
		}}
	installTasks(t, registry, "abortive", definition)
	hosts := newCrashHosts(t, nil, HarnessOptions{Registry: registry, OnReport: log.add, Settings: Settings{RestartLimit: 2}})
	// Every host dies while committing the abort handler's outcome.
	dies := func(next *State) bool {
		for _, task := range next.Tasks {
			if task.Kind == definition.Kind && task.Status == "terminal" {
				return true
			}
		}
		return false
	}
	h := hosts.open(dies)
	id := createTaskForTest(t, h, definition)
	h.Resume()
	if _, err := h.AbortTask(ctx, id); err != nil && !errors.Is(err, ErrPoisoned) && !errors.Is(err, errSimulatedCrash) {
		t.Fatal(err)
	}
	hosts.died(h)
	for crash := 1; ; crash++ {
		if crash < 2 {
			h = hosts.open(dies)
		} else {
			h = hosts.open(nil) // the last host survives to record the fault
		}
		if task, err := h.Task(ctx, id); err != nil || task.Restarts != crash || !task.AbortRequested {
			t.Fatalf("after crash %d: task=%+v error=%v", crash, task, err)
		}
		if crash == 2 {
			break
		}
		h.Resume()
		hosts.died(h)
	}
	h.Resume()
	record, err := h.WaitTask(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	assertRestartLimitFault(t, record, "Abort handler of task "+itoa(id)+" (test.abort-crash) was cut off 2 times")
	if aborts.Load() != 2 {
		t.Fatalf("abort handler ran %d times, want 2", aborts.Load())
	}
	drained(t, h)
	if faults := log.of(ReportTaskFaulted); len(faults) != 1 || faults[0].Extension != "abortive" || faults[0].Reason != reasonRestartLimit {
		t.Fatalf("task_faulted reports=%+v", faults)
	}
}

func TestCrashLoopLimitSettings(t *testing.T) {
	for _, tc := range []struct {
		name    string
		setting float64
		runs    []int // restart counts that still run
		faults  []int // restart counts that fault
	}{
		{"default", 0, []int{4}, []int{5, 9}},
		{"negative", -3, []int{4}, []int{5}},
		{"nan", nan(), []int{4}, []int{5}},
		{"fraction", 2.7, []int{1}, []int{2}},
		{"one", 1, nil, []int{1}},
		{"unlimited", inf(), []int{1 << 20}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			registry := NewRegistry()
			definition := TaskDefinition{Kind: "test.limit-setting", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
				return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
			}}
			installTasks(t, registry, "limits", definition)
			h := taskTestHarness(t, HarnessOptions{Registry: registry, Settings: Settings{RestartLimit: tc.setting}})
			root := testRoot(t, h)
			check := func(restarts int, want string) {
				id := seedTask(t, h, root.ID(), definition, TaskOptions{}, func(task *TaskRecord) { task.Restarts = restarts })
				record, err := h.WaitTask(ctx, id)
				if err != nil || record.Outcome == nil || record.Outcome.Status != want {
					t.Fatalf("restarts=%d: record=%+v error=%v, want %s", restarts, record, err, want)
				}
			}
			for _, restarts := range tc.runs {
				check(restarts, OutcomeCompleted)
			}
			for _, restarts := range tc.faults {
				check(restarts, OutcomeFaulted)
			}
		})
	}
}

func TestCrashLoopReopenWithHigherLimitRunsTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	registry := NewRegistry()
	definition := TaskDefinition{Kind: "test.higher-limit", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	installTasks(t, registry, "higher", definition)
	backend := NewMemoryStorage()
	hosts := newCrashHosts(t, backend, HarnessOptions{Registry: registry, Settings: Settings{RestartLimit: 2}})
	h := hosts.open(nil)
	id := seedTask(t, h, testRoot(t, h).ID(), definition, TaskOptions{}, func(task *TaskRecord) { task.Restarts = 2 })
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	h = hosts.openWith(HarnessOptions{Registry: registry, Settings: Settings{RestartLimit: 3}}, nil)
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome == nil || record.Outcome.Status != OutcomeCompleted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
}

// blockingStreamModel commits one delta, then waits for cancellation.
type blockingStreamModel struct {
	delta    string
	streamed chan struct{}
	once     sync.Once
}

func (m *blockingStreamModel) Generate(ctx context.Context, _ ModelRequest, onDelta func(string) error) (ModelResponse, error) {
	if err := onDelta(m.delta); err != nil {
		return ModelResponse{}, err
	}
	m.once.Do(func() { close(m.streamed) })
	<-ctx.Done()
	return ModelResponse{}, ctx.Err()
}

func TestCrashLoopAbortedGenerationEndsAbortedWhenCutOff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	log := &reportLog{}
	model := &blockingStreamModel{delta: "half", streamed: make(chan struct{})}
	hosts := newCrashHosts(t, nil, HarnessOptions{Model: model, OnReport: log.add, Settings: Settings{RestartLimit: 2}})
	// Every host dies while committing the generation abort handler's outcome.
	dies := func(next *State) bool {
		for _, task := range next.Tasks {
			if task.Kind == "pi.generation" && task.AbortRequested && task.Status == "terminal" {
				return true
			}
		}
		return false
	}
	h := hosts.open(dies)
	root := testRoot(t, h)
	first, err := root.Submit(ctx, SubmissionRequest{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-model.streamed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	queued, err := root.Submit(ctx, SubmissionRequest{Content: "later"})
	if err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	taskID := state.Conversations[root.ID()].Busy
	waitPartial(t, h, root.ID(), "half")
	if _, err := h.AbortTask(ctx, taskID); err != nil && !errors.Is(err, ErrPoisoned) && !errors.Is(err, errSimulatedCrash) {
		t.Fatal(err)
	}
	hosts.died(h)
	for crash := 1; ; crash++ {
		if crash < 2 {
			h = hosts.open(dies)
		} else {
			h = hosts.open(nil) // the last host survives to record the fault
		}
		if task, err := h.Task(ctx, taskID); err != nil || task.Restarts != crash || !task.AbortRequested || task.Status != "pending" {
			t.Fatalf("after crash %d: task=%+v error=%v", crash, task, err)
		}
		if crash == 2 {
			break
		}
		h.Resume()
		hosts.died(h)
	}
	h.Resume()
	record, err := h.WaitTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	message := "Abort handler of task " + itoa(taskID) + " (pi.generation) was cut off 2 times"
	assertRestartLimitFault(t, record, message)
	answer, err := (&Submission{h: h, id: first.ID()}).Record(ctx)
	detail, _ := json.Marshal(message)
	if err != nil || answer.Status != "unanswered" || answer.Reason != "aborted" || string(answer.Detail) != string(detail) {
		t.Fatalf("submission=%+v error=%v", answer, err)
	}
	if later, err := (&Submission{h: h, id: queued.ID()}).Record(ctx); err != nil || later.Status != "queued" {
		t.Fatalf("queued input=%+v error=%v", later, err)
	}
	state, err = h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Conversations[root.ID()].Busy != 0 {
		t.Fatal("cut-off generation kept the conversation busy")
	}
	entries, err := entriesFromState(state, root.ID())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Kind == "pi.assistant" {
			t.Fatalf("cut-off abort converted its partial: %+v", entry.Message)
		}
	}
	drained(t, h)
	faults := log.of(ReportTaskFaulted)
	if len(faults) != 1 || faults[0].Reason != reasonRestartLimit || faults[0].Extension != "" || faults[0].Task.Kind != "pi.generation" {
		t.Fatalf("task_faulted reports=%+v", faults)
	}
}

func TestCrashLoopTaskInterruptedReports(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome Outcome
		reason  string
		hold    bool
	}{
		{name: "interrupted", outcome: Outcome{Status: OutcomeFailed, Error: "host stopped", ErrorDetail: interruptedDetail}, reason: reasonInterrupted},
		{name: "restart-limit", outcome: Outcome{Status: OutcomeFailed, Error: "gave up", ErrorDetail: json.RawMessage(`{"reason":"restart_limit","extra":true}`)}, reason: reasonRestartLimit},
		{name: "held", outcome: Outcome{Status: OutcomeFailed, Error: "held", ErrorDetail: interruptedDetail}, reason: reasonInterrupted, hold: true},
		{name: "other-reason", outcome: Outcome{Status: OutcomeFailed, Error: "x", ErrorDetail: json.RawMessage(`{"reason":"model_error"}`)}},
		{name: "no-detail", outcome: Outcome{Status: OutcomeFailed, Error: "x"}},
		{name: "string-detail", outcome: Outcome{Status: OutcomeFailed, Error: "x", ErrorDetail: json.RawMessage(`"interrupted"`)}},
		{name: "completed", outcome: Outcome{Status: OutcomeCompleted}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			log := &reportLog{}
			registry := NewRegistry()
			release := make(chan struct{})
			child := TaskDefinition{Kind: "test.interrupted-child", Version: 1,
				Run: func(ctx context.Context, _ *TaskRuntime, _ TaskRecord) error { <-ctx.Done(); return ctx.Err() },
				Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
					<-release
					return r.Complete(ctx, Outcome{Status: OutcomeAborted})
				}}
			definition := TaskDefinition{Kind: "test.interrupted", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
				return r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
					if tc.hold {
						if _, err := tx.CreateTask(task.Conversation, child, nil, TaskOptions{Owner: Owner{Task: task.ID}}); err != nil {
							return TaskTransition{}, err
						}
					}
					outcome := tc.outcome
					return TaskTransition{Status: "terminal", Outcome: &outcome}, nil
				})
			}}
			installTasks(t, registry, "interrupting", definition, child)
			h := taskTestHarness(t, HarnessOptions{Registry: registry, OnReport: log.add})
			id := seedTask(t, h, testRoot(t, h).ID(), definition, TaskOptions{}, func(task *TaskRecord) { task.Restarts = 3 })
			h.Resume()
			if tc.hold {
				deadline := time.Now().Add(5 * time.Second)
				for {
					task, err := h.Task(ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if task.Status == "completing" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("never held: %+v", task)
					}
					time.Sleep(time.Millisecond)
				}
				// The report is due at commit, before the hold settles.
				deadline = time.Now().Add(5 * time.Second)
				for len(log.of(ReportTaskInterrupted)) == 0 {
					if time.Now().After(deadline) {
						t.Fatal("held outcome not reported at commit")
					}
					time.Sleep(time.Millisecond)
				}
			}
			close(release)
			record, err := h.WaitTask(ctx, id)
			if err != nil || record.Outcome == nil || record.Outcome.Status != tc.outcome.Status || record.Restarts != 0 {
				t.Fatalf("record=%+v error=%v", record, err)
			}
			drained(t, h)
			reports := log.of(ReportTaskInterrupted)
			if tc.reason == "" {
				if len(reports) != 0 {
					t.Fatalf("unexpected task_interrupted: %+v", reports)
				}
				return
			}
			if len(reports) != 1 {
				t.Fatalf("task_interrupted reports=%+v", reports)
			}
			report := reports[0]
			if report.Reason != tc.reason || report.Restarts != 3 || report.Extension != "interrupting" || report.Err == nil ||
				report.Err.Error() != tc.outcome.Error || report.Task == nil || report.Task.ID != id || report.Task.Kind != definition.Kind {
				t.Fatalf("report=%+v", report)
			}
		})
	}
}

// faultWriteStorage fails the commit that would record a restart-limit fault.
type faultWriteStorage struct {
	*MemoryStorage
	err error
}

func (s *faultWriteStorage) Commit(ctx context.Context, sequence uint64, next *State) error {
	for _, task := range next.Tasks {
		if task.Outcome != nil && task.Outcome.Status == OutcomeFaulted && detailReason(task.Outcome.ErrorDetail) == reasonRestartLimit {
			return s.err
		}
	}
	return s.MemoryStorage.Commit(ctx, sequence, next)
}

// Close keeps the backend readable for the test's assertions.
func (s *faultWriteStorage) Close() error { return nil }

func TestCrashLoopFaultWriteFailureFailsSession(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"certified-rejection", &RejectedError{Err: ErrConflict}},
		{"uncertain", errors.New("disk vanished")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			log := &reportLog{}
			registry := NewRegistry()
			definition := TaskDefinition{Kind: "test.fault-write", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
				t.Error("cut-off task ran")
				return nil
			}}
			installTasks(t, registry, "fault-write", definition)
			store := &faultWriteStorage{MemoryStorage: NewMemoryStorage(), err: tc.err}
			h, err := Open(ctx, store, HarnessOptions{Registry: registry, OnReport: log.add})
			if err != nil {
				t.Fatal(err)
			}
			id := seedTask(t, h, testRoot(t, h).ID(), definition, TaskOptions{}, func(task *TaskRecord) { task.Restarts = 5 })
			h.Resume()
			end, err := h.WaitClosed(ctx)
			if err != nil || end.Kind != "failed" || !errors.Is(end.Err, tc.err) {
				t.Fatalf("end=%+v error=%v", end, err)
			}
			if h.reports.workerDone != nil {
				<-h.reports.workerDone
			}
			if faults := log.of(ReportTaskFaulted); len(faults) != 0 {
				t.Fatalf("unwritten fault was reported: %+v", faults)
			}
			if failed := log.of(ReportStorageFailed); len(failed) != 1 {
				t.Fatalf("storage_failed reports=%+v", failed)
			}
			state, err := store.MemoryStorage.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if task := state.Tasks[id]; task == nil || task.Status != "pending" || task.Restarts != 5 {
				t.Fatalf("stored task=%+v", task)
			}
		})
	}
}
