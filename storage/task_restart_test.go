package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

func TestRestartCounterRoundTripAndLegacyOpen(t *testing.T) {
	for _, backend := range []string{"sqlite", "jsonl"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			open := func() durable.Storage {
				t.Helper()
				var st durable.Storage
				var err error
				if backend == "sqlite" {
					st, err = OpenSQLite(ctx, filepath.Join(dir, "state.sqlite"))
				} else {
					st, err = OpenJSONL(ctx, dir)
				}
				if err != nil {
					t.Fatal(err)
				}
				return st
			}

			st := open()
			state := testState(1)
			state.Version, state.NextID = 2, 2
			state.Tasks[1] = &durable.TaskRecord{ID: 1, Kind: "test.restart-storage", Version: 1,
				Status: "pending", Input: json.RawMessage(`null`), Checkpoint: json.RawMessage(`{}`), Restarts: 7}
			if err := st.Commit(ctx, 0, state); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}

			st = open()
			loaded, err := st.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Version != 2 || loaded.Tasks[1].Restarts != 7 {
				t.Fatalf("round trip version=%d task=%+v", loaded.Version, loaded.Tasks[1])
			}

			legacy := testState(2)
			legacy.Version, legacy.NextID = 2, 2
			legacy.Tasks[1] = &durable.TaskRecord{ID: 1, Kind: "test.restart-storage", Version: 1,
				Status: "running", Input: json.RawMessage(`null`), Checkpoint: json.RawMessage(`{}`)}
			data, err := json.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte(`"restarts"`)) {
				t.Fatal("zero restart count was not omitted from the legacy record")
			}
			if err := st.Commit(ctx, loaded.Sequence, legacy); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}

			h, err := durable.Open(ctx, open(), durable.HarnessOptions{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := h.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			got, err := h.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got.Version != 4 || got.Tasks[1].Status != "pending" || got.Tasks[1].Restarts != 1 {
				t.Fatalf("legacy open version=%d task=%+v", got.Version, got.Tasks[1])
			}
		})
	}
}
