// Package storagetest checks a durable.Storage against the contracts a
// Session relies on, as upstream's @earendil-works/pi-durable/testing storage
// conformance does for its storage interface. Run it from a backend's tests:
//
//	func TestConformance(t *testing.T) {
//		storagetest.Run(t, func(t *testing.T) durable.Storage {
//			return openMyStorage(t) // fresh and empty
//		})
//	}
//
// It checks the Storage contract, and DeltaStorage, BatchDeltaStorage, and
// RecordStorage when the storage implements them. open must return a fresh,
// empty storage for each case; the suite closes it.
package storagetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

// Run runs every case as a subtest.
func Run(t *testing.T, open func(t *testing.T) durable.Storage) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, st durable.Storage)
	}{
		{"loads nothing when empty", loadsNothingWhenEmpty},
		{"commits in sequence and refuses the rest", commitsInSequence},
		{"detaches what it stores and loads", detaches},
		{"applies deltas", appliesDeltas},
		{"stores batches in order", storesBatches},
		{"reads records as the State has them", readsRecords},
		{"reads revisions of members named like revisions", readsTrickyRevisions},
		{"refuses operations after close", refusesAfterClose},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := open(t)
			defer st.Close()
			c.run(t, st)
		})
	}
}

// State is a State with forks, ownership, live and ended work, documents of
// every scope, and document revisions: what the suite stores and reads back.
func State() *durable.State {
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
		Revisions: map[string]*durable.DocumentRevision{
			durable.RevisionKey("conversation:1:app.c", 2):     {Version: 1, Sequence: 2, Value: json.RawMessage(`{"r":2}`), Fork: "asOf"},
			durable.RevisionKey("conversation:1:app.c", 7):     {Version: 1, Sequence: 7, Value: json.RawMessage(`{"r":7}`), Fork: "asOf"},
			durable.RevisionKey("conversation:1:app.gone", 3):  {Version: 1, Sequence: 3, Value: json.RawMessage(`{"r":3}`), Fork: "asOf"},
			durable.RevisionKey("conversation:1:app.gone", 5):  {Version: 1, Sequence: 5, Retired: true, Fork: "asOf"},
			durable.RevisionKey("conversation:13:app.c", 4):    {Version: 1, Sequence: 4, Value: json.RawMessage(`{"r":4}`)},
			durable.RevisionKey("conversation:1:app.fam/x", 6): {Version: 1, Sequence: 6, Value: json.RawMessage(`{"x":6}`)},
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

// loaded is the stored State, or a failure.
func loaded(t *testing.T, st durable.Storage) *durable.State {
	t.Helper()
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	return state
}

// sameState fails unless the stored State is want, compared as JSON.
func sameState(t *testing.T, st durable.Storage, want *durable.State) {
	t.Helper()
	if got := asJSON(t, loaded(t, st)); got != asJSON(t, want) {
		t.Fatalf("stored State=%s\nwant %s", got, asJSON(t, want))
	}
}

// rejected fails unless err is a RejectedError matching target.
func rejected(t *testing.T, err, target error, what string) {
	t.Helper()
	var refusal *durable.RejectedError
	if !errors.As(err, &refusal) || !errors.Is(err, target) {
		t.Fatalf("%s: error=%v, want a RejectedError for %v", what, err, target)
	}
}

func loadsNothingWhenEmpty(t *testing.T, st durable.Storage) {
	if state := loaded(t, st); state != nil {
		t.Fatalf("an empty storage loaded %s", asJSON(t, state))
	}
}

func commitsInSequence(t *testing.T, st durable.Storage) {
	ctx := context.Background()
	first := State()
	if err := st.Commit(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	sameState(t, st, State())
	stale := State()
	stale.NextID = 99
	rejected(t, st.Commit(ctx, 0, stale), durable.ErrConflict, "a commit from a stale sequence")
	skipping := State()
	skipping.Sequence = 3
	rejected(t, st.Commit(ctx, 1, skipping), durable.ErrInvalid, "a commit that skips a sequence")
	var refusal *durable.RejectedError
	if err := st.Commit(ctx, 1, nil); !errors.As(err, &refusal) {
		t.Fatalf("a nil State: error=%v, want a RejectedError", err)
	}
	// Refused commits change nothing.
	sameState(t, st, State())
	second := State()
	second.Sequence = 2
	second.NextID = 61
	if err := st.Commit(ctx, 1, second); err != nil {
		t.Fatal(err)
	}
	want := State()
	want.Sequence, want.NextID = 2, 61
	sameState(t, st, want)
}

func detaches(t *testing.T, st durable.Storage) {
	ctx := context.Background()
	state := State()
	if err := st.Commit(ctx, 0, state); err != nil {
		t.Fatal(err)
	}
	// What the caller keeps is its own.
	state.Conversations[1].Head = 99
	state.Entries[10].Data = json.RawMessage(`{"changed":true}`)
	state.Documents["session:0:app.s"].Value = json.RawMessage(`{"changed":true}`)
	delete(state.Tasks, 40)
	sameState(t, st, State())
	// So is what a load returns.
	got := loaded(t, st)
	got.Conversations[1].Head = 98
	got.Entries[10].Data[0] = '['
	delete(got.Documents, "task:40:app.t")
	sameState(t, st, State())
}

func appliesDeltas(t *testing.T, st durable.Storage) {
	deltas, ok := st.(durable.DeltaStorage)
	if !ok {
		t.Skip("not a DeltaStorage")
	}
	ctx := context.Background()
	if err := st.Commit(ctx, 0, State()); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(2, 0).UTC()
	delta := &durable.StateDelta{Version: 4, Sequence: 2, NextID: 62, Root: 1,
		Conversations: map[durable.ConversationID]*durable.ConversationRecord{1: {ID: 1, Head: 60, CreatedAt: time.Unix(1, 0).UTC()}},
		Entries:       map[durable.EntryID]*durable.Entry{60: {ID: 60, Conversation: 1, Parent: 12, Kind: "app.note", Data: json.RawMessage(`{"n":60}`), Sequence: 2, CreatedAt: at}},
		Tasks:         map[durable.TaskID]*durable.TaskRecord{43: {ID: 43, Conversation: 2, Kind: "app.other", Version: 1, Status: "running", Input: json.RawMessage(`{}`), Checkpoint: json.RawMessage(`{"step":1}`)}},
		Submissions:   map[durable.SubmissionID]*durable.SubmissionRecord{61: {ID: 61, Conversation: 1, Status: "queued", Request: durable.SubmissionRequest{Type: "input", Content: "d", RequestID: "d"}}},
		Documents:     map[string]*durable.DocumentRecord{"conversation:1:app.c": {Kind: "app.c", Version: 1, Scope: "conversation", ScopeID: 1, History: "latest", Fork: "initial", Value: json.RawMessage(`{"v":2}`)}},
		Revisions:     map[string]*durable.DocumentRevision{durable.RevisionKey("conversation:1:app.c", 8): {Version: 1, Sequence: 8, Value: json.RawMessage(`{"v":2}`)}},
		Removed: durable.RemovedKeys{
			Entries:     []durable.EntryID{14},
			Tasks:       []durable.TaskID{42},
			Submissions: []durable.SubmissionID{51},
			Documents:   []string{"conversation:1:app.fam/y"},
		},
	}
	want := State()
	kept, err := delta.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if err := kept.ApplyTo(want); err != nil {
		t.Fatal(err)
	}
	if err := deltas.CommitDelta(ctx, 1, delta); err != nil {
		t.Fatal(err)
	}
	sameState(t, st, want)
	// The same contract as Commit: refused deltas change nothing.
	rejected(t, deltas.CommitDelta(ctx, 1, &durable.StateDelta{Version: 4, Sequence: 2, NextID: 70}), durable.ErrConflict, "a delta from a stale sequence")
	rejected(t, deltas.CommitDelta(ctx, 2, &durable.StateDelta{Version: 4, Sequence: 4, NextID: 70}), durable.ErrInvalid, "a delta that skips a sequence")
	sameState(t, st, want)
}

func storesBatches(t *testing.T, st durable.Storage) {
	batches, ok := st.(durable.BatchDeltaStorage)
	if !ok {
		t.Skip("not a BatchDeltaStorage")
	}
	ctx := context.Background()
	if err := st.Commit(ctx, 0, State()); err != nil {
		t.Fatal(err)
	}
	note := func(sequence uint64, id durable.EntryID) *durable.StateDelta {
		return &durable.StateDelta{Version: 4, Sequence: sequence, NextID: uint64(id) + 1, Root: 1,
			Entries: map[durable.EntryID]*durable.Entry{id: {ID: id, Conversation: 13, Parent: 14, Kind: "app.note", Data: json.RawMessage(`{}`), Sequence: sequence, CreatedAt: time.Unix(3, 0).UTC()}},
		}
	}
	want := State()
	group := []*durable.StateDelta{note(2, 60), note(3, 61), note(4, 62)}
	for _, delta := range group {
		kept, err := delta.Clone()
		if err != nil {
			t.Fatal(err)
		}
		if err := kept.ApplyTo(want); err != nil {
			t.Fatal(err)
		}
	}
	n, err := batches.CommitDeltas(ctx, 1, group)
	if err != nil || n != len(group) {
		t.Fatalf("a batch stored %d of %d: %v", n, len(group), err)
	}
	sameState(t, st, want)
	// A batch whose second commit skips a sequence: what it reports stored
	// stands, and nothing after it is stored.
	refused := []*durable.StateDelta{note(5, 63), note(7, 64)}
	n, err = batches.CommitDeltas(ctx, 4, refused)
	if err == nil || n < 0 || n > 1 {
		t.Fatalf("a batch with a skipped sequence stored %d, error=%v", n, err)
	}
	if n == 1 {
		kept, cloneErr := refused[0].Clone()
		if cloneErr != nil {
			t.Fatal(cloneErr)
		}
		if applyErr := kept.ApplyTo(want); applyErr != nil {
			t.Fatal(applyErr)
		}
	}
	sameState(t, st, want)
}

func readsRecords(t *testing.T, st durable.Storage) {
	records, ok := st.(durable.RecordStorage)
	if !ok {
		t.Skip("not a RecordStorage")
	}
	ctx := context.Background()
	if err := st.Commit(ctx, 0, State()); err != nil {
		t.Fatal(err)
	}
	reference := durable.StateRecords{State: State()}
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
	for _, at := range []uint64{1, 3, 5, 9} {
		for _, key := range []string{"conversation:1:app.c", "conversation:1:app.gone", "conversation:13:app.c", "conversation:1:app.fam/x"} {
			got, err := records.DocumentRevision(ctx, key, at)
			if err != nil || asJSON(t, got) != asJSON(t, reference.DocumentRevision(key, at)) {
				t.Fatalf("revision of %s at %d=%s error=%v", key, at, asJSON(t, got), err)
			}
		}
		for _, prefix := range []string{"conversation:1:", "conversation:13:", "task:40:"} {
			got, err := records.DocumentRevisions(ctx, prefix, at)
			if err != nil || asJSON(t, got) != asJSON(t, reference.DocumentRevisions(prefix, at)) {
				t.Fatalf("revisions under %s at %d=%s error=%v", prefix, at, asJSON(t, got), err)
			}
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
}

func readsTrickyRevisions(t *testing.T, st durable.Storage) {
	records, ok := st.(durable.RecordStorage)
	if !ok {
		t.Skip("not a RecordStorage")
	}
	ctx := context.Background()
	state := State()
	// Members may contain "@": a member's revisions are not another's.
	key := "conversation:1:app.fam/a"
	tricky := "conversation:1:app.fam/a@00000000000000000001"
	state.Revisions = map[string]*durable.DocumentRevision{
		durable.RevisionKey(key, 3):    {Version: 1, Sequence: 3, Value: json.RawMessage(`{"own":3}`)},
		durable.RevisionKey(tricky, 9): {Version: 1, Sequence: 9, Value: json.RawMessage(`{"other":9}`)},
	}
	if err := st.Commit(ctx, 0, state); err != nil {
		t.Fatal(err)
	}
	reference := durable.StateRecords{State: state}
	for _, at := range []uint64{2, 3, 50} {
		got, err := records.DocumentRevision(ctx, key, at)
		if err != nil || asJSON(t, got) != asJSON(t, reference.DocumentRevision(key, at)) {
			t.Fatalf("revision of %s at %d=%s error=%v, want %s", key, at, asJSON(t, got), err, asJSON(t, reference.DocumentRevision(key, at)))
		}
	}
	got, err := records.DocumentRevisions(ctx, "conversation:1:", 50)
	if err != nil || asJSON(t, got) != asJSON(t, reference.DocumentRevisions("conversation:1:", 50)) {
		t.Fatalf("revisions=%s error=%v", asJSON(t, got), err)
	}
}

func refusesAfterClose(t *testing.T, st durable.Storage) {
	ctx := context.Background()
	if err := st.Commit(ctx, 0, State()); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Load(ctx); !errors.Is(err, durable.ErrClosed) {
		t.Fatalf("Load after close: error=%v, want ErrClosed", err)
	}
	next := State()
	next.Sequence = 2
	rejected(t, st.Commit(ctx, 1, next), durable.ErrClosed, "a commit after close")
	if deltas, ok := st.(durable.DeltaStorage); ok {
		rejected(t, deltas.CommitDelta(ctx, 1, &durable.StateDelta{Version: 4, Sequence: 2, NextID: 60}), durable.ErrClosed, "a delta after close")
	}
	if err := st.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
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
