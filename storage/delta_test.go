package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

// runHistory drives a Harness over st: conversations, entries, a fork that
// removes its creation baseline, tasks, and documents, then closes it and
// returns the committed State.
func runHistory(t *testing.T, st durable.Storage) *durable.State {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h, err := durable.Open(ctx, st, durable.HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := h.Root(ctx, durable.ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	doc := durable.DocumentDefinition{Kind: "app.counter", Version: 1, Scope: "conversation"}
	var at durable.EntryID
	for i := 0; i < 20; i++ {
		if err := h.Commit(ctx, func(tx *durable.Tx) error {
			id, err := tx.Append(root.ID(), durable.Entry{Kind: "app.note", Data: json.RawMessage(`{"n":1}`)})
			at = id
			if err != nil {
				return err
			}
			return tx.PutDocument(doc, uint64(root.ID()), json.RawMessage(`{"count":`+itoa(i)+`}`))
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := root.Fork(ctx, at, durable.ConversationOptions{}); err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return state
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

func sameState(t *testing.T, want, got *durable.State) {
	t.Helper()
	a, _ := json.Marshal(want)
	b, _ := json.Marshal(got)
	if string(a) != string(b) {
		t.Fatalf("reopened state differs:\nwant %s\ngot  %s", a, b)
	}
}

func TestDeltaBackendsReopenToTheCommittedState(t *testing.T) {
	ctx := context.Background()
	for _, backend := range []string{"sqlite", "jsonl", "memory"} {
		t.Run(backend, func(t *testing.T) {
			dir := t.TempDir()
			memory := durable.NewMemoryStorage()
			open := func() durable.Storage {
				switch backend {
				case "sqlite":
					st, err := OpenSQLite(ctx, filepath.Join(dir, "state.sqlite"))
					if err != nil {
						t.Fatal(err)
					}
					return st
				case "jsonl":
					st, err := OpenJSONL(ctx, dir)
					if err != nil {
						t.Fatal(err)
					}
					return st
				}
				return memory
			}
			st := open()
			if _, ok := st.(durable.DeltaStorage); !ok {
				t.Fatalf("%s is not a DeltaStorage", backend)
			}
			want := runHistory(t, st)
			if backend == "memory" {
				memory = durable.NewMemoryStorage()
				return // a closed MemoryStorage cannot be reopened
			}
			reopened := open()
			defer reopened.Close()
			got, err := reopened.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			sameState(t, want, got)
		})
	}
}

func TestSQLiteConvertsALegacySnapshotOnItsFirstWrite(t *testing.T) {
	ctx := context.Background()
	file := filepath.Join(t.TempDir(), "legacy.sqlite")
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	legacy := testState(1)
	legacy.Version, legacy.NextID = 3, 5
	legacy.Documents["session:0:app.flag"] = &durable.DocumentRecord{Kind: "app.flag", Version: 1, Scope: "session", History: "latest", Value: json.RawMessage(`true`)}
	data, _ := json.Marshal(legacy)
	for _, q := range []string{
		"CREATE TABLE durable_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), sequence INTEGER NOT NULL, snapshot BLOB NOT NULL)",
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("INSERT INTO durable_state(singleton, sequence, snapshot) VALUES(1, ?, ?)", legacy.Sequence, data); err != nil {
		t.Fatal(err)
	}
	db.Close()

	st, err := OpenSQLite(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := st.Load(ctx)
	if err != nil || loaded == nil || loaded.Sequence != legacy.Sequence {
		t.Fatalf("legacy load=%+v error=%v", loaded, err)
	}
	h, err := durable.Open(ctx, st, durable.HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(ctx, func(tx *durable.Tx) error {
		return tx.PutDocument(durable.DocumentDefinition{Kind: "app.flag", Version: 1, Scope: "session"}, 0, json.RawMessage(`false`))
	}); err != nil {
		t.Fatal(err)
	}
	want, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLite(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sameState(t, want, got)
	check, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Close()
	var tables int
	if err := check.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='durable_state'").Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("legacy table kept: %d %v", tables, err)
	}
}

func TestJSONLDeltasCompactAndRecover(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := OpenJSONL(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(ctx, st, durable.HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := h.Root(ctx, durable.ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(string(make([]byte, 4096)))
	for i := 0; i < 600; i++ {
		if err := h.Commit(ctx, func(tx *durable.Tx) error {
			_, err := tx.Append(root.ID(), durable.Entry{Kind: "app.note", Data: data})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	want, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "snapshots.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	stateSize, _ := json.Marshal(want)
	// At most the last whole State plus deltas weighing as much (or 1 MiB).
	if limit := 2*int64(len(stateSize)) + compactMinBytes; info.Size() > limit {
		t.Fatalf("log is %d bytes for a %d-byte State", info.Size(), len(stateSize))
	}
	// A torn final delta line is dropped.
	f, err := os.OpenFile(filepath.Join(dir, "snapshots.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"delta":{"version":3,"sequence":`); err != nil {
		t.Fatal(err)
	}
	f.Close()
	reopened, err := OpenJSONL(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sameState(t, want, got)
}

func BenchmarkSQLiteCommitWithHistory(b *testing.B) {
	ctx := context.Background()
	st, err := OpenSQLite(ctx, filepath.Join(b.TempDir(), "bench.sqlite"))
	if err != nil {
		b.Fatal(err)
	}
	h, err := durable.Open(ctx, st, durable.HarnessOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer h.Close(ctx)
	root, err := h.Root(ctx, durable.ConversationOptions{})
	if err != nil {
		b.Fatal(err)
	}
	data, _ := json.Marshal(string(make([]byte, 1024)))
	for done := 0; done < 5000; done += 500 {
		if err := h.Commit(ctx, func(tx *durable.Tx) error {
			for i := 0; i < 500; i++ {
				if _, err := tx.Append(root.ID(), durable.Entry{Kind: "app.note", Data: data}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := h.Commit(ctx, func(tx *durable.Tx) error {
			_, err := tx.Append(root.ID(), durable.Entry{Kind: "app.note", Data: json.RawMessage(`1`)})
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}
