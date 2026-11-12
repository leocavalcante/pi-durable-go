package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/storagetest"
)

func TestSQLiteRecordReadsOfALegacySnapshot(t *testing.T) {
	ctx := context.Background()
	file := filepath.Join(t.TempDir(), "legacy.sqlite")
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	legacy := storagetest.State()
	data, _ := json.Marshal(legacy)
	if _, err := db.Exec("CREATE TABLE durable_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), sequence INTEGER NOT NULL, snapshot BLOB NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO durable_state(singleton, sequence, snapshot) VALUES(1, ?, ?)", legacy.Sequence, data); err != nil {
		t.Fatal(err)
	}
	db.Close()
	for name, open := range map[string]func(context.Context, string) (durable.Storage, error){"writable": OpenSQLite, "read-only": OpenSQLiteReadOnly} {
		store, err := open(ctx, file)
		if err != nil {
			t.Fatal(err)
		}
		records := store.(durable.RecordStorage)
		// Not converted yet: reads come from the snapshot.
		task, err := records.Task(ctx, 42)
		if err != nil || task == nil || task.Status != "terminal" {
			t.Fatalf("%s: task=%+v error=%v", name, task, err)
		}
		resident, err := records.LoadResident(ctx)
		if err != nil || resident == nil || len(resident.Tasks) != 2 || len(resident.Submissions) != 2 || resident.NextID != 60 {
			t.Fatalf("%s: resident=%+v error=%v", name, resident, err)
		}
		entries, err := records.ScanEntries(ctx, 3, durable.EntryQuery{Order: "ascending"}, 10, nil)
		if err != nil || len(entries.Items) != 3 || entries.Items[0].ID != 10 || entries.Items[2].ID != 30 {
			t.Fatalf("%s: entries=%+v error=%v", name, entries, err)
		}
		store.Close()
	}
}
