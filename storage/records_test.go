package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

// recordState is a State with forks, ownership, live and ended work, and
// documents of every scope, for record read conformance.
func recordState() *durable.State {
	at := time.Unix(1, 0).UTC()
	entry := func(id, parent durable.EntryID, cid durable.ConversationID) *durable.Entry {
		return &durable.Entry{ID: id, Conversation: cid, Parent: parent, Kind: "app.note", Data: json.RawMessage(fmt.Sprintf(`{"n":%d}`, id)), Sequence: 1, CreatedAt: at}
	}
	doc := func(scope string, id uint64, kind, member string) *durable.DocumentRecord {
		return &durable.DocumentRecord{Kind: kind, Version: 1, Scope: scope, ScopeID: id, History: "latest", Fork: "initial", Member: member, Value: json.RawMessage(`{}`)}
	}
	return &durable.State{Version: 4, Sequence: 1, NextID: 60, Root: 1,
		Conversations: map[durable.ConversationID]*durable.ConversationRecord{
			1:  {ID: 1, Head: 12, CreatedAt: at},
			2:  {ID: 2, Parent: 1, ForkAt: 11, Head: 21, CreatedAt: at},
			3:  {ID: 3, Parent: 2, ForkAt: 11, Head: 30, CreatedAt: at},
			4:  {ID: 4, Owner: durable.Owner{Task: 40}, Head: 41, CreatedAt: at},
			13: {ID: 13, Head: 14, CreatedAt: at},
		},
		Entries: map[durable.EntryID]*durable.Entry{
			10: entry(10, 0, 1), 11: entry(11, 10, 1), 12: entry(12, 11, 1),
			20: entry(20, 11, 2), 21: entry(21, 20, 2),
			30: entry(30, 11, 3),
			41: entry(41, 0, 4),
			14: entry(14, 0, 13),
		},
		Tasks: map[durable.TaskID]*durable.TaskRecord{
			40: {ID: 40, Conversation: 1, Kind: "app.work", Version: 1, Status: "running", Input: json.RawMessage(`{}`), Checkpoint: json.RawMessage(`{}`)},
			42: {ID: 42, Conversation: 1, Kind: "app.work", Version: 1, Status: "terminal", Background: true, Input: json.RawMessage(`{}`), Checkpoint: json.RawMessage(`{}`), Outcome: &durable.Outcome{Status: durable.OutcomeCompleted}},
			43: {ID: 43, Conversation: 2, Kind: "app.other", Version: 1, Status: "pending", AbortRequested: true, Input: json.RawMessage(`{}`), Checkpoint: json.RawMessage(`{}`)},
		},
		Submissions: map[durable.SubmissionID]*durable.SubmissionRecord{
			50: {ID: 50, Conversation: 1, Status: "queued", Request: durable.SubmissionRequest{Type: "input", Content: "a", RequestID: "a"}},
			51: {ID: 51, Conversation: 1, Status: "done", Request: durable.SubmissionRequest{Type: "input", Content: "b", RequestID: "b"}, Entry: 12},
			52: {ID: 52, Conversation: 2, Status: "placed", Request: durable.SubmissionRequest{Type: "input", Content: "c"}, Entry: 21},
		},
		Documents: map[string]*durable.DocumentRecord{
			"session:0:app.s":          doc("session", 0, "app.s", ""),
			"conversation:1:app.c":     doc("conversation", 1, "app.c", ""),
			"conversation:1:app.fam/x": doc("conversation", 1, "app.fam", "x"),
			"conversation:1:app.fam/y": doc("conversation", 1, "app.fam", "y"),
			"conversation:13:app.c":    doc("conversation", 13, "app.c", ""),
			"task:40:app.t":            doc("task", 40, "app.t", ""),
		},
	}
}

func asJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// pages collects every page of a scan, two records at a time.
func pages[T any](t *testing.T, scan func(cursor *durable.ScanCursor) (durable.ScanPage[T], error)) []T {
	t.Helper()
	var all []T
	var cursor *durable.ScanCursor
	for i := 0; i < 50; i++ {
		page, err := scan(cursor)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Items...)
		if page.Next == nil {
			return all
		}
		cursor = page.Next
	}
	t.Fatal("a scan never ended")
	return nil
}

func entryPages(t *testing.T, scan func(cursor *durable.EntryCursor) (durable.EntryPage, error)) []durable.EntryID {
	t.Helper()
	var ids []durable.EntryID
	var cursor *durable.EntryCursor
	for i := 0; i < 50; i++ {
		page, err := scan(cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Items {
			ids = append(ids, e.ID)
		}
		if page.Next == nil {
			return ids
		}
		cursor = page.Next
	}
	t.Fatal("an entry scan never ended")
	return nil
}

func TestRecordStoragesReadAlike(t *testing.T) {
	ctx := context.Background()
	jsonl, err := OpenJSONL(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sqlite, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "records.db"))
	if err != nil {
		t.Fatal(err)
	}
	reference := durable.StateRecords{State: recordState()}
	for name, store := range map[string]durable.Storage{"memory": durable.NewMemoryStorage(), "jsonl": jsonl, "sqlite": sqlite} {
		t.Run(name, func(t *testing.T) {
			defer store.Close()
			if err := store.Commit(ctx, 0, recordState()); err != nil {
				t.Fatal(err)
			}
			records, ok := store.(durable.RecordStorage)
			if !ok {
				t.Fatal("not a RecordStorage")
			}
			want, _ := reference.Resident()
			got, err := records.LoadResident(ctx)
			if err != nil || asJSON(t, got) != asJSON(t, want) {
				t.Fatalf("resident=%s error=%v\nwant %s", asJSON(t, got), err, asJSON(t, want))
			}
			for _, id := range []durable.ConversationID{1, 2, 3, 4, 13, 99} {
				if got, err := records.Conversation(ctx, id); err != nil || asJSON(t, got) != asJSON(t, reference.Conversation(id)) {
					t.Fatalf("conversation %d=%s error=%v", id, asJSON(t, got), err)
				}
			}
			for _, query := range []durable.ConversationQuery{{}, {Order: "descending"}, {OwnerTask: 40}, {OwnerConversation: 1}, {OwnerConversation: 2}} {
				got := pages(t, func(c *durable.ScanCursor) (durable.ScanPage[durable.ConversationRecord], error) {
					return records.ScanConversations(ctx, query, 2, c)
				})
				want := pages(t, func(c *durable.ScanCursor) (durable.ScanPage[durable.ConversationRecord], error) {
					return reference.ScanConversations(query, 2, c)
				})
				if asJSON(t, got) != asJSON(t, want) {
					t.Fatalf("conversations %+v=%s\nwant %s", query, asJSON(t, got), asJSON(t, want))
				}
			}
			for _, id := range []durable.EntryID{10, 21, 41, 98} {
				if got, err := records.Entry(ctx, id); err != nil || asJSON(t, got) != asJSON(t, reference.Entry(id)) {
					t.Fatalf("entry %d=%s error=%v", id, asJSON(t, got), err)
				}
			}
			for _, cid := range []durable.ConversationID{1, 2, 3, 4} {
				for _, query := range []durable.EntryQuery{{}, {Order: "ascending"}, {MinEntryID: 11, MaxEntryID: 20}, {MinEntryID: 11, Order: "ascending"}} {
					got := entryPages(t, func(c *durable.EntryCursor) (durable.EntryPage, error) {
						return records.ScanEntries(ctx, cid, query, 2, c)
					})
					want := entryPages(t, func(c *durable.EntryCursor) (durable.EntryPage, error) {
						return reference.ScanEntries(cid, query, 2, c)
					})
					if asJSON(t, got) != asJSON(t, want) {
						t.Fatalf("entries of %d %+v=%v, want %v", cid, query, got, want)
					}
				}
			}
			if _, err := records.ScanEntries(ctx, 99, durable.EntryQuery{}, 2, nil); err == nil {
				t.Fatal("an unknown conversation scanned")
			}
			yes, no := true, false
			for _, id := range []durable.TaskID{40, 42, 43, 97} {
				if got, err := records.Task(ctx, id); err != nil || asJSON(t, got) != asJSON(t, reference.Task(id)) {
					t.Fatalf("task %d=%s error=%v", id, asJSON(t, got), err)
				}
			}
			for _, query := range []durable.TaskQuery{{}, {Order: "descending"}, {Conversation: 1}, {Kind: "app.other"}, {Status: "terminal"}, {AbortRequested: &yes}, {Background: &no}} {
				got := pages(t, func(c *durable.ScanCursor) (durable.ScanPage[durable.TaskRecord], error) {
					return records.ScanTasks(ctx, query, 2, c)
				})
				want := pages(t, func(c *durable.ScanCursor) (durable.ScanPage[durable.TaskRecord], error) {
					return reference.ScanTasks(query, 2, c)
				})
				if asJSON(t, got) != asJSON(t, want) {
					t.Fatalf("tasks %+v=%s\nwant %s", query, asJSON(t, got), asJSON(t, want))
				}
			}
			for _, id := range []durable.SubmissionID{50, 51, 52, 96} {
				if got, err := records.Submission(ctx, id); err != nil || asJSON(t, got) != asJSON(t, reference.Submission(id)) {
					t.Fatalf("submission %d=%s error=%v", id, asJSON(t, got), err)
				}
			}
			for _, request := range []struct {
				cid durable.ConversationID
				id  string
			}{{1, "a"}, {1, "b"}, {2, "a"}, {1, ""}} {
				got, err := records.SubmissionByRequest(ctx, request.cid, request.id)
				if err != nil || asJSON(t, got) != asJSON(t, reference.SubmissionByRequest(request.cid, request.id)) {
					t.Fatalf("submission by %v=%s error=%v", request, asJSON(t, got), err)
				}
			}
			for _, query := range []durable.SubmissionQuery{{}, {Order: "descending"}, {Conversation: 1}, {Status: "placed"}} {
				got := pages(t, func(c *durable.ScanCursor) (durable.ScanPage[durable.SubmissionRecord], error) {
					return records.ScanSubmissions(ctx, query, 2, c)
				})
				want := pages(t, func(c *durable.ScanCursor) (durable.ScanPage[durable.SubmissionRecord], error) {
					return reference.ScanSubmissions(query, 2, c)
				})
				if asJSON(t, got) != asJSON(t, want) {
					t.Fatalf("submissions %+v=%s\nwant %s", query, asJSON(t, got), asJSON(t, want))
				}
			}
			for _, key := range []string{"session:0:app.s", "conversation:1:app.fam/x", "task:40:app.t", "conversation:2:none"} {
				if got, err := records.Document(ctx, key); err != nil || asJSON(t, got) != asJSON(t, reference.Document(key)) {
					t.Fatalf("document %s=%s error=%v", key, asJSON(t, got), err)
				}
			}
			for _, query := range []durable.DocumentQuery{{Scope: "session"}, {Scope: "conversation", ScopeID: 1}, {Scope: "conversation", ScopeID: 1, Kind: "app.fam"}, {Scope: "conversation", ScopeID: 13}, {Scope: "task", ScopeID: 40}, {Scope: "task", ScopeID: 42}} {
				got := pages(t, func(c *durable.ScanCursor) (durable.ScanPage[durable.DocumentRecord], error) {
					return records.ScanDocuments(ctx, query, 2, c)
				})
				want := pages(t, func(c *durable.ScanCursor) (durable.ScanPage[durable.DocumentRecord], error) {
					return reference.ScanDocuments(query, 2, c)
				})
				if asJSON(t, got) != asJSON(t, want) {
					t.Fatalf("documents %+v=%s\nwant %s", query, asJSON(t, got), asJSON(t, want))
				}
			}
		})
	}
}

func TestSQLiteRecordReadsOfALegacySnapshot(t *testing.T) {
	ctx := context.Background()
	file := filepath.Join(t.TempDir(), "legacy.sqlite")
	db, err := sql.Open("sqlite", file)
	if err != nil {
		t.Fatal(err)
	}
	legacy := recordState()
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

func TestRevisionReadsIgnoreMembersThatLookLikeRevisions(t *testing.T) {
	ctx := context.Background()
	sqlite, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "revisions.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sqlite.Close()
	st := recordState()
	key := "conversation:1:app.fam/a"
	tricky := "conversation:1:app.fam/a@00000000000000000001"
	st.Revisions = map[string]*durable.DocumentRevision{
		durable.RevisionKey(key, 3):    {Version: 1, Sequence: 3, Value: json.RawMessage(`{"own":3}`)},
		durable.RevisionKey(tricky, 9): {Version: 1, Sequence: 9, Value: json.RawMessage(`{"other":9}`)},
	}
	if err := sqlite.Commit(ctx, 0, st); err != nil {
		t.Fatal(err)
	}
	for name, store := range map[string]durable.RecordStorage{"sqlite": sqlite.(durable.RecordStorage), "memory": nil} {
		var got *durable.DocumentRevision
		if store == nil {
			got = durable.StateRecords{State: st}.DocumentRevision(key, 50)
		} else if got, err = store.DocumentRevision(ctx, key, 50); err != nil {
			t.Fatal(err)
		}
		if got == nil || string(got.Value) != `{"own":3}` {
			t.Fatalf("%s: revision=%+v", name, got)
		}
		if store != nil {
			if none, err := store.DocumentRevision(ctx, key, 2); err != nil || none != nil {
				t.Fatalf("%s: before the first revision=%+v error=%v", name, none, err)
			}
		}
	}
}
