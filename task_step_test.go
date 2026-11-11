package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func taskTestHarness(t *testing.T, options HarnessOptions) *Harness {
	t.Helper()
	h, err := Open(context.Background(), NewMemoryStorage(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return h
}

// Upstream v1.5.2 creates one runtime before the phase loop (#run/#runtime).
// Only an ending step closes it; continuing progress is not a phase revocation.
func TestTaskStepContinuingInvocationRetainsRuntime(t *testing.T) {
	h := testHarness(t)
	var captured *TaskRuntime
	var phases int
	definition := TaskDefinition{Kind: "test.runtime-lifetime", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
		phases++
		if phases == 1 {
			captured = r
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
				return TaskTransition{Status: "running", Checkpoint: json.RawMessage(`1`)}, nil
			})
		}
		if captured != r {
			t.Error("continuing phase replaced its invocation runtime")
		}
		if _, err := captured.Memo(ctx, "still-live", json.RawMessage(`true`)); err != nil {
			return err
		}
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
	}}
	if err := h.registry.Install(Extension{Name: "runtime-lifetime", Tasks: []TaskDefinition{definition}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, definition)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome.Status != OutcomeCompleted || phases != 2 {
		t.Fatalf("phases=%d record=%+v error=%v", phases, record, err)
	}
}

func TestTaskStepCheckpointProgress(t *testing.T) {
	for _, tc := range []struct {
		name, initial, next, side string
		progress                  bool
	}{
		{"progress", `{"n":0}`, `{"n":1}`, "", true},
		{"same", `{"n":0}`, `{"n":0}`, "", false},
		{"object-order", `{"a":1,"b":2}`, `{"b":2.0,"a":1e0}`, "", false},
		{"large-integer", `9007199254740992`, `9007199254740993`, "", true},
		{"number-spelling", `1`, `1.0`, "", false},
		{"memo-only", `{}`, `{}`, "memo", false},
		{"argument-mutation", `{"n":0}`, `{"n":0}`, "argument", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHarness(t)
			var runs atomic.Int32
			var invocation *taskInvocation
			def := TaskDefinition{Kind: "test.step", Version: 1,
				Initial: func(json.RawMessage) (json.RawMessage, error) { return json.RawMessage(tc.initial), nil },
				Run: func(ctx context.Context, r *TaskRuntime, record TaskRecord) error {
					if runs.Add(1) == 2 {
						if invocation != r.invocation {
							t.Error("progress reserved a new invocation")
						}
						return r.Complete(ctx, Outcome{Status: OutcomeCompleted})
					}
					invocation = r.invocation
					if tc.side == "argument" {
						record.Checkpoint[len(record.Checkpoint)-2] = '9'
						return nil
					}
					if tc.side == "memo" {
						_, err := r.Memo(ctx, "memo", json.RawMessage(`true`))
						return err
					}
					return r.Commit(ctx, func(*Tx) (TaskTransition, error) {
						return TaskTransition{Status: "running", Checkpoint: json.RawMessage(tc.next)}, nil
					})
				},
			}
			if err := h.registry.Install(Extension{Name: "steps", Tasks: []TaskDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			id := createTaskForTest(t, h, def)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			record, err := h.WaitTask(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			want, count := OutcomeFaulted, int32(1)
			if tc.progress {
				want, count = OutcomeCompleted, 2
			}
			if record.Outcome.Status != want || runs.Load() != count {
				t.Fatalf("outcome=%+v runs=%d", record.Outcome, runs.Load())
			}
		})
	}
}

func TestTaskFaultReportsErrorsAndPanics(t *testing.T) {
	for _, mode := range []string{"error", "empty", "panic"} {
		t.Run(mode, func(t *testing.T) {
			reports := make(chan Report, 4)
			h := taskTestHarness(t, HarnessOptions{OnReport: func(report Report) { reports <- report }})
			cause := errors.New(strings.Repeat("界", 3000))
			def := TaskDefinition{Kind: "test.fault", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
				r.Report(errors.New("diagnostic"))
				if mode == "panic" {
					panic(strings.Repeat("界", 3000))
				}
				if mode == "empty" {
					return errors.New("")
				}
				return cause
			}}
			if err := h.registry.Install(Extension{Name: "installer", Tasks: []TaskDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			id := createTaskForTest(t, h, def)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			record, err := h.WaitTask(ctx, id)
			if err != nil || record.Outcome.Status != OutcomeFaulted {
				t.Fatalf("record=%+v error=%v", record, err)
			}
			message := record.Outcome.Error
			if message == "" || len(message) > 4096 || !utf8.ValidString(message) || strings.Contains(message, "goroutine") {
				t.Fatalf("bad persisted message: %q", message)
			}
			for _, kind := range []ReportKind{ReportTaskError, ReportTaskFaulted} {
				select {
				case report := <-reports:
					if report.Kind != kind || report.Task.ID != id || report.Extension != "installer" {
						t.Fatalf("report=%+v", report)
					}
					if kind == ReportTaskFaulted {
						if report.Reason != "threw" {
							t.Fatal(report)
						}
						if mode == "error" && !errors.Is(report.Err, cause) {
							t.Fatal("cause not retained")
						}
						if mode == "panic" {
							var p *panicError
							if !errors.As(report.Err, &p) || len(p.Stack()) == 0 {
								t.Fatal("panic stack missing")
							}
						}
					}
				case <-ctx.Done():
					t.Fatal("missing report")
				}
			}
		})
	}
}

func TestTaskStepKeepsCommittedOutcomeAfterError(t *testing.T) {
	for _, aborting := range []bool{false, true} {
		t.Run(map[bool]string{false: "run", true: "abort"}[aborting], func(t *testing.T) {
			h := testHarness(t)
			handler := func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
				if err := r.Complete(ctx, Outcome{Status: OutcomeCompleted}); err != nil {
					return err
				}
				return errors.New("too late")
			}
			def := TaskDefinition{Kind: "test.keep", Version: 1, Run: handler, Abort: handler}
			if err := h.registry.Install(Extension{Name: "keep", Tasks: []TaskDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			id := createTaskForTest(t, h, def)
			if aborting {
				if _, err := h.AbortTask(context.Background(), id); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			record, err := h.WaitTask(ctx, id)
			if err != nil || record.Outcome.Status != OutcomeCompleted {
				t.Fatalf("record=%+v error=%v", record, err)
			}
		})
	}
}

func TestTaskFaultAbortHandlers(t *testing.T) {
	for _, mode := range []string{"empty", "error", "panic", "wait", "pending"} {
		t.Run(mode, func(t *testing.T) {
			h := testHarness(t)
			def := TaskDefinition{Kind: "test.abort-fault", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { t.Error("Run started"); return nil },
				Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
					switch mode {
					case "error":
						return errors.New("abort error")
					case "panic":
						panic("abort panic")
					case "wait", "pending":
						status := "waiting"
						if mode == "pending" {
							status = "pending"
						}
						return r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: status}, nil })
					}
					return nil
				},
			}
			if err := h.registry.Install(Extension{Name: "abort-fault", Tasks: []TaskDefinition{def}}); err != nil {
				t.Fatal(err)
			}
			id := createTaskForTest(t, h, def)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := h.AbortTask(ctx, id); err != nil {
				t.Fatal(err)
			}
			record, err := h.WaitTask(ctx, id)
			if err != nil || record.Outcome.Status != OutcomeFaulted {
				t.Fatalf("record=%+v error=%v", record, err)
			}
		})
	}
}

func TestTaskFaultAbortMarkWins(t *testing.T) {
	h := testHarness(t)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	def := TaskDefinition{Kind: "test.race-fault", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error {
		close(started)
		<-release
		return errors.New("racing error")
	}}
	if err := h.registry.Install(Extension{Name: "race", Tasks: []TaskDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, def)
	h.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := h.Commit(ctx, func(tx *Tx) error { markAbortTree(tx.state, id); return nil }); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	record, err := h.WaitTask(ctx, id)
	if err != nil || record.Outcome.Status != OutcomeAborted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
}

func TestTaskFaultHeldUntilOwnedCleanup(t *testing.T) {
	h := testHarness(t)
	childAbort, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	child := TaskDefinition{Kind: "test.fault-child", Version: 1,
		Run: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
			return r.Commit(ctx, func(*Tx) (TaskTransition, error) { return TaskTransition{Status: "waiting"}, nil })
		},
		Abort: func(ctx context.Context, r *TaskRuntime, _ TaskRecord) error {
			close(childAbort)
			<-release
			return r.Complete(ctx, Outcome{Status: OutcomeAborted})
		},
	}
	parent := TaskDefinition{Kind: "test.fault-parent", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, record TaskRecord) error {
		if err := r.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
			_, err := tx.CreateTask(record.Conversation, child, nil, TaskOptions{Owner: Owner{Task: r.ID}})
			task, _ := tx.Task(r.ID)
			task.Memos = map[string]json.RawMessage{"drop": json.RawMessage(`true`)}
			return TaskTransition{Status: "running"}, err
		}); err != nil {
			return err
		}
		return errors.New("owner bug")
	}}
	if err := h.registry.Install(Extension{Name: "held-fault", Tasks: []TaskDefinition{parent, child}}); err != nil {
		t.Fatal(err)
	}
	id := createTaskForTest(t, h, parent)
	h.Resume()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	select {
	case <-childAbort:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	record, err := h.Task(ctx, id)
	if err != nil || record.Status != "completing" || record.Outcome.Status != OutcomeFaulted || len(record.Memos) != 0 {
		t.Fatalf("held=%+v error=%v", record, err)
	}
	once.Do(func() { close(release) })
	record, err = h.WaitTask(ctx, id)
	if err != nil || record.Outcome.Status != OutcomeFaulted {
		t.Fatalf("record=%+v error=%v", record, err)
	}
}
