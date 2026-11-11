package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

func TestStateVersionTwoOutcomeFieldsRoundTrip(t *testing.T) {
	for _, backend := range []string{"sqlite", "jsonl"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			path := t.TempDir()
			open := func() durable.Storage {
				t.Helper()
				var st durable.Storage
				var err error
				if backend == "sqlite" {
					st, err = OpenSQLite(ctx, filepath.Join(path, "state.sqlite"))
				} else {
					st, err = OpenJSONL(ctx, path)
				}
				if err != nil {
					t.Fatal(err)
				}
				return st
			}
			st := open()
			state := testState(1)
			state.Version, state.NextID = 2, 10
			state.Tasks[1] = &durable.TaskRecord{ID: 1, Kind: "test.failed", Version: 1, Status: "terminal", Input: json.RawMessage(`null`), Checkpoint: json.RawMessage(`null`), Outcome: &durable.Outcome{Status: durable.OutcomeFaulted, Error: "fault", ErrorDetail: json.RawMessage(`{"code":7}`)}}
			state.Tasks[2] = &durable.TaskRecord{ID: 2, Kind: "test.orphaned", Version: 1, Status: "terminal", Input: json.RawMessage(`null`), Checkpoint: json.RawMessage(`null`), Outcome: &durable.Outcome{Status: durable.OutcomeOrphaned, Reason: "missing_task"}}
			state.Tasks[3] = &durable.TaskRecord{ID: 3, Kind: "test.aborted", Version: 1, Status: "terminal", Input: json.RawMessage(`null`), Checkpoint: json.RawMessage(`null`), Outcome: &durable.Outcome{Status: durable.OutcomeAborted, Reason: "restart", Result: json.RawMessage(`{"cleaned":true}`)}}
			state.Submissions[4] = &durable.SubmissionRecord{ID: 4, Status: "unanswered", Reason: "faulted", Detail: json.RawMessage(`"fault detail"`)}
			if err := st.Commit(ctx, 0, state); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			st = open()
			h, err := durable.Open(ctx, st, durable.HarnessOptions{})
			if err != nil {
				st.Close()
				t.Fatal(err)
			}
			defer h.Close(ctx)
			got, err := h.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got.Version != 3 || !reflect.DeepEqual(got.Tasks, state.Tasks) || !reflect.DeepEqual(got.Submissions, state.Submissions) {
				t.Fatalf("round trip lost fields: %+v", got)
			}
		})
	}
}
