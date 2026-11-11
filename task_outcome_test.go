package durable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func createTaskForTest(t *testing.T, h *Harness, definition TaskDefinition) TaskID {
	t.Helper()
	c := testRoot(t, h)
	var id TaskID
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		var err error
		id, err = tx.CreateTask(c.id, definition, nil, TaskOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestTaskOutcomeValidation(t *testing.T) {
	cases := []struct {
		name  string
		out   Outcome
		valid bool
	}{
		{"completed", Outcome{Status: OutcomeCompleted}, true},
		{"failed", Outcome{Status: OutcomeFailed, Error: "declined", ErrorDetail: json.RawMessage(`{"why":1}`)}, true},
		{"aborted", Outcome{Status: OutcomeAborted, Reason: "restart", Result: json.RawMessage(`42`)}, true},
		{"failed-empty", Outcome{Status: OutcomeFailed}, false},
		{"completed-error", Outcome{Status: OutcomeCompleted, Error: "bad"}, false},
		{"completed-detail", Outcome{Status: OutcomeCompleted, ErrorDetail: json.RawMessage(`null`)}, false},
		{"completed-reason", Outcome{Status: OutcomeCompleted, Reason: "bad"}, false},
		{"aborted-error", Outcome{Status: OutcomeAborted, Error: "bad"}, false},
		{"aborted-detail", Outcome{Status: OutcomeAborted, ErrorDetail: json.RawMessage(`{}`)}, false},
		{"failed-reason", Outcome{Status: OutcomeFailed, Error: "bad", Reason: "bad"}, false},
		{"invalid-json", Outcome{Status: OutcomeFailed, Error: "bad", ErrorDetail: json.RawMessage(`{`)}, false},
		{"faulted", Outcome{Status: OutcomeFaulted, Error: "scheduler only"}, false},
		{"orphaned", Outcome{Status: OutcomeOrphaned, Reason: "missing_task"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTaskOutcome(tc.out)
			if tc.valid && err != nil || !tc.valid && !errors.Is(err, ErrInvalid) {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}

func TestTaskOutcomeHoldsDropMemosAndKeepCheckpoint(t *testing.T) {
	for _, status := range []string{OutcomeCompleted, OutcomeFailed, OutcomeAborted} {
		t.Run(status, func(t *testing.T) {
			h := testHarness(t)
			def := TaskDefinition{Kind: "test.hold", Version: 1, Run: func(context.Context, *TaskRuntime, TaskRecord) error { return nil }}
			id := createTaskForTest(t, h, def)
			err := h.Commit(context.Background(), func(tx *Tx) error {
				task := tx.state.mutTask(id)
				task.Checkpoint = json.RawMessage(`{"retained":true}`)
				task.Memos = map[string]json.RawMessage{"key": json.RawMessage(`true`)}
				if _, err := tx.CreateTask(task.Conversation, def, nil, TaskOptions{Owner: Owner{Task: id}}); err != nil {
					return err
				}
				out := Outcome{Status: status}
				if status == OutcomeFailed {
					out.Error = "declined"
				}
				return applyTransition(tx, task, TaskTransition{Status: "terminal", Outcome: &out}, h.now())
			})
			if err != nil {
				t.Fatal(err)
			}
			task, err := h.Task(context.Background(), id)
			if err != nil || task.Status != "completing" || len(task.Memos) != 0 || string(task.Checkpoint) != `{"retained":true}` {
				t.Fatalf("hold=%+v, error=%v", task, err)
			}
		})
	}
}

func TestTaskDiagnosticBounds(t *testing.T) {
	for _, message := range []string{"", strings.Repeat("界", 3000), strings.Repeat("x", 10000), "bad\xffutf8"} {
		got := diagnostic(message)
		if got == "" || len(got) > maxDiagnosticBytes || !utf8.ValidString(got) {
			t.Fatalf("invalid diagnostic: len=%d, valid=%v", len(got), utf8.ValidString(got))
		}
	}
}

func TestTaskJSONEquality(t *testing.T) {
	for _, tc := range []struct {
		a, b  string
		equal bool
	}{
		{`{"a":1,"b":[true,null]}`, `{"b":[true,null],"a":1.0}`, true},
		{`1e0`, `1`, true}, {`-0`, `0.00e99999999`, true},
		{`1.23e+100000000`, `123e99999998`, true},
		{`9007199254740992`, `9007199254740993`, false},
		{`1e-100000000`, `1e-100000001`, false},
		{`[1,2]`, `[2,1]`, false}, {`{"a":null}`, `{"b":null}`, false},
		{`"1"`, `1`, false}, {`null`, `{}`, false},
	} {
		if got := jsonEqual(json.RawMessage(tc.a), json.RawMessage(tc.b)); got != tc.equal {
			t.Errorf("jsonEqual(%s, %s)=%v", tc.a, tc.b, got)
		}
	}
}
