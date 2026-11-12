package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func residentCounts(h *Harness) (tasks, submissions, coldTasks int) {
	h.session.mu.Lock()
	defer h.session.mu.Unlock()
	if h.session.cold != nil {
		h.session.cold.mu.Lock()
		coldTasks = h.session.cold.tasks.len()
		h.session.cold.mu.Unlock()
	}
	return len(h.session.state.Tasks), len(h.session.state.Submissions), coldTasks
}

func TestBoundedSessionKeepsLiveTasksOnly(t *testing.T) {
	// Hundreds of commits: the test-only whole-State check would dominate.
	verify := verifyCommittedState
	verifyCommittedState = false
	t.Cleanup(func() { verifyCommittedState = verify })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	registry := NewRegistry()
	definition := TaskDefinition{Kind: "test.done", Version: 1, Run: func(ctx context.Context, r *TaskRuntime, task TaskRecord) error {
		return r.Complete(ctx, Outcome{Status: OutcomeCompleted, Result: task.Input})
	}}
	installTasks(t, registry, "done", definition)
	h := taskTestHarness(t, HarnessOptions{Registry: registry})
	root := testRoot(t, h)
	const count = 300
	var ids []TaskID
	if err := h.Commit(ctx, func(tx *Tx) error {
		for i := 0; i < count; i++ {
			id, err := tx.CreateTask(root.ID(), definition, json.RawMessage(fmt.Sprintf("%d", i)), TaskOptions{})
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	h.Resume()
	for _, id := range ids {
		if record, err := h.WaitTask(ctx, id); err != nil || record.Status != "terminal" {
			t.Fatalf("task %d=%+v error=%v", id, record, err)
		}
	}
	// Ended tasks left memory: the State keeps none, the cache at most 256.
	tasks, _, cold := residentCounts(h)
	if tasks != 0 || cold > coldTaskCapacity {
		t.Fatalf("resident tasks=%d cached=%d", tasks, cold)
	}
	// Every read by ID still finds them, the oldest from storage.
	first, err := h.Task(ctx, ids[0])
	if err != nil || first.Outcome == nil || string(first.Outcome.Result) != "0" {
		t.Fatalf("first task=%+v error=%v", first, err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil || len(state.Tasks) != count {
		t.Fatalf("snapshot has %d tasks, error=%v", len(state.Tasks), err)
	}
	found := 0
	if err := h.Commit(ctx, func(tx *Tx) error {
		var cursor *ScanCursor
		for {
			page, err := tx.ScanTasks(TaskQuery{Kind: "test.done", Status: "terminal"}, 64, cursor)
			if err != nil {
				return err
			}
			found += len(page.Items)
			if page.Next == nil {
				return nil
			}
			cursor = page.Next
		}
	}); err != nil {
		t.Fatal(err)
	}
	if found != count {
		t.Fatalf("scanned %d ended tasks, want %d", found, count)
	}
}

func TestBoundedSessionKeepsOpenSubmissionsOnly(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root := testRoot(t, h)
	write := func(requestID string) SubmissionID {
		t.Helper()
		sub, err := root.Submit(ctx, SubmissionRequest{Type: "write", RequestID: requestID, Entry: &Entry{Kind: "app.note", Data: json.RawMessage(`{}`)}})
		if err != nil {
			t.Fatal(err)
		}
		return sub.ID()
	}
	first := write("note-1")
	write("note-2")
	if _, submissions, _ := residentCounts(h); submissions != 0 {
		t.Fatalf("%d settled submissions kept", submissions)
	}
	// A settled submission is still found by its request ID and by ID.
	if again := write("note-1"); again != first {
		t.Fatalf("a repeated request admitted %d, want %d", again, first)
	}
	sub, err := h.Submission(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if record, err := sub.Record(ctx); err != nil || record.Status != "done" {
		t.Fatalf("record=%+v error=%v", record, err)
	}
}

func TestReadOnlySessionsKeepEverything(t *testing.T) {
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	if h.session.cold == nil {
		t.Fatal("a Harness over memory storage is not bounded")
	}
	viewer, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer viewer.Close(context.Background())
	if viewer.session.cold != nil {
		t.Fatal("a read-only Harness is bounded")
	}
}

func residentEntries(h *Harness, cid ConversationID) int {
	h.session.mu.Lock()
	defer h.session.mu.Unlock()
	n := 0
	for _, e := range h.session.state.Entries {
		if e.Conversation == cid {
			n++
		}
	}
	return n
}

func TestBoundedSessionKeepsContextRangesOnly(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root := testRoot(t, h)
	old := appendNotes(t, h, root, 40)
	if _, err := root.Reset(ctx, ""); err != nil {
		t.Fatal(err)
	}
	recent := appendNotes(t, h, root, 3)
	// The reset moved the context range: the State keeps the reset and what
	// follows.
	if n := residentEntries(h, root.ID()); n > 5 {
		t.Fatalf("%d entries kept after a reset", n)
	}
	// Whole-history reads still find everything, from storage.
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 40+1+3 || entries[1].ID > old[0] {
		t.Fatalf("history has %d entries", len(entries))
	}
	page, err := root.ScanEntries(ctx, EntryQuery{Order: "ascending", MaxEntryID: old[9]}, 100, nil)
	if err != nil || len(page.Items) == 0 || page.Items[len(page.Items)-1].ID != old[9] {
		t.Fatalf("scan=%+v error=%v", page, err)
	}
	// A window larger than the range reads the older entries.
	view, stop, err := root.WatchWindow(ctx, ConversationWindow{Min: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	select {
	case first := <-view:
		if len(first.Entries) != 10 || first.Entries[9].ID != recent[2] || first.Before == 0 {
			t.Fatalf("window=%d entries, before %d", len(first.Entries), first.Before)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no window")
	}
	// The current context starts at the reset; an earlier one reads storage.
	current, err := root.ContextView(ctx, 0)
	if err != nil || current.Head == nil {
		t.Fatalf("context=%+v error=%v", current, err)
	}
	if _, err := root.ContextView(ctx, old[20]); err != nil {
		t.Fatal(err)
	}
	// A fork at an entry the State no longer keeps reads its lineage.
	fork, err := root.Fork(ctx, old[10], ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	forked, err := fork.Entries(ctx)
	if err != nil || len(forked) < 11 {
		t.Fatalf("fork history=%d error=%v", len(forked), err)
	}
	if _, err := fork.Context(ctx); err != nil {
		t.Fatal(err)
	}
}

// reopenable is memory storage that survives its Harness's close.
type reopenable struct{ *MemoryStorage }

func (reopenable) Close() error { return nil }

func TestBoundedOpenLoadsOnlyWhatItKeeps(t *testing.T) {
	ctx := context.Background()
	store := reopenable{NewMemoryStorage()}
	h, err := Open(ctx, store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root := testRoot(t, h)
	old := appendNotes(t, h, root, 20)
	if _, err := root.Reset(ctx, ""); err != nil {
		t.Fatal(err)
	}
	appendNotes(t, h, root, 2)
	write, err := root.Submit(ctx, SubmissionRequest{Type: "write", Entry: &Entry{Kind: "app.note", Data: json.RawMessage(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(ctx)
	if n := residentEntries(reopened, root.ID()); n > 5 {
		t.Fatalf("reopening loaded %d entries", n)
	}
	if _, submissions, _ := residentCounts(reopened); submissions != 0 {
		t.Fatalf("reopening loaded %d settled submissions", submissions)
	}
	again, err := reopened.Root(ctx, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := again.Entries(ctx)
	if err != nil || len(entries) < 20 || entries[1].ID != old[0] {
		t.Fatalf("history after reopening=%d error=%v", len(entries), err)
	}
	if sub, err := reopened.Submission(ctx, write.ID()); err != nil {
		t.Fatal(err)
	} else if record, err := sub.Record(ctx); err != nil || record.Status != "done" {
		t.Fatalf("write=%+v error=%v", record, err)
	}
}

func TestIdleConversationsDropTheirContextRanges(t *testing.T) {
	ctx := context.Background()
	h, root := runHarness(t, echoModel{}, Settings{ContextRetentionMs: ptr(0.0)})
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "one"}))
	// Idle with no retention: the range leaves memory, by the next
	// publication or the sweep.
	deadline := time.Now().Add(5 * time.Second)
	for residentEntries(h, root.ID()) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d entries kept by an idle conversation", residentEntries(h, root.ID()))
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The next run reads its context back and answers with all of it.
	record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "two"}))
	if record.Status != "done" {
		t.Fatalf("second run=%+v", record)
	}
	messages, err := root.Context(ctx)
	if err != nil {
		t.Fatal(err)
	}
	users := 0
	for _, m := range messages {
		if m.Role == "user" {
			users++
		}
	}
	if users != 2 {
		t.Fatalf("context has %d user messages, want 2: %+v", users, messages)
	}
}

func TestBusyAndRetainedConversationsKeepTheirRanges(t *testing.T) {
	h, root := runHarness(t, echoModel{}, Settings{})
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "one"}))
	if residentEntries(h, root.ID()) == 0 {
		t.Fatal("a conversation dropped its range within the retention")
	}
}

func residentDocuments(h *Harness, cid ConversationID) int {
	h.session.mu.Lock()
	defer h.session.mu.Unlock()
	n := 0
	for _, doc := range h.session.state.Documents {
		if doc.Scope == "conversation" && ConversationID(doc.ScopeID) == cid {
			n++
		}
	}
	return n
}

// idleDocuments opens a Harness without context retention over memory
// storage, with a conversation holding two documents, and waits until they
// left memory.
func idleDocuments(t *testing.T, store Storage) (*Harness, *Conversation, DocumentDefinition, DocumentDefinition) {
	t.Helper()
	h, err := Open(context.Background(), store, HarnessOptions{Settings: Settings{ContextRetentionMs: ptr(0.0)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	root := testRoot(t, h)
	notes := DocumentDefinition{Kind: "test.notes", Version: 1, Scope: "conversation", History: "rewindable", Fork: "current"}
	gone := DocumentDefinition{Kind: "test.gone", Version: 1, Scope: "conversation"}
	if err := h.Commit(context.Background(), func(tx *Tx) error {
		if err := tx.PutDocument(notes, uint64(root.ID()), json.RawMessage(`{"n":1}`)); err != nil {
			return err
		}
		return tx.PutDocument(gone, uint64(root.ID()), json.RawMessage(`{}`))
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for residentDocuments(h, root.ID()) > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d documents kept by an idle conversation", residentDocuments(h, root.ID()))
		}
		time.Sleep(5 * time.Millisecond)
	}
	return h, root, notes, gone
}

func TestIdleConversationsDropTheirDocuments(t *testing.T) {
	ctx := context.Background()
	h, root, notes, gone := idleDocuments(t, NewMemoryStorage())
	type doc struct{ N int }
	if got, err := ReadDocument[doc](ctx, h, notes, uint64(root.ID())); err != nil || got.N != 1 {
		t.Fatalf("read of an unloaded document=%+v error=%v", got, err)
	}
	// Updating it keeps its stored history; retiring the other removes it.
	if err := h.Commit(ctx, func(tx *Tx) error {
		if err := tx.PutDocument(notes, uint64(root.ID()), json.RawMessage(`{"n":2}`)); err != nil {
			return err
		}
		return tx.RetireDocument(gone, uint64(root.ID()))
	}); err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	record := state.Documents[documentKey(notes, uint64(root.ID()))]
	revisions := 0
	for key := range state.Revisions {
		if strings.HasPrefix(key, documentKey(notes, uint64(root.ID()))+"@") {
			revisions++
		}
	}
	if record == nil || revisions != 2 || string(record.Value) != `{"n":2}` {
		t.Fatalf("notes after an update=%+v with %d revisions", record, revisions)
	}
	if state.Documents[documentKey(gone, uint64(root.ID()))] != nil {
		t.Fatal("a retired document survived")
	}
	// Views and forks see the stored documents.
	view, err := root.View(ctx)
	if err != nil || string(view.Documents["test.notes"]) != `{"n":2}` {
		t.Fatalf("view documents=%v error=%v", view.Documents, err)
	}
	entries, _ := root.Entries(ctx)
	fork, err := root.Fork(ctx, entries[len(entries)-1].ID, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ReadDocument[doc](ctx, h, notes, uint64(fork.ID())); err != nil || got.N != 2 {
		t.Fatalf("forked notes=%+v error=%v", got, err)
	}
}

func TestHeldDocumentsStayUntilStored(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStorage()
	h, root, notes, _ := idleDocuments(t, store)
	// Load the conversation, hold a change, and let it idle again.
	if err := h.Commit(ctx, func(tx *Tx) error { return tx.PutDocument(notes, uint64(root.ID()), json.RawMessage(`{"n":5}`)) }); err != nil {
		t.Fatal(err)
	}
	if err := h.CommitWith(ctx, deferredWithin(math.Inf(1)), func(tx *Tx) error {
		return tx.PutDocument(notes, uint64(root.ID()), json.RawMessage(`{"n":6}`))
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // sweeps run meanwhile
	type doc struct{ N int }
	if got, err := ReadDocument[doc](ctx, h, notes, uint64(root.ID())); err != nil || got.N != 6 {
		t.Fatalf("held value=%+v error=%v", got, err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if value := string(store.state.Documents[documentKey(notes, uint64(root.ID()))].Value); value != `{"n":6}` {
		t.Fatalf("stored at close=%s", value)
	}
}

func TestIdleConversationRecordsLeaveMemory(t *testing.T) {
	ctx := context.Background()
	h, root := runHarness(t, echoModel{}, Settings{ContextRetentionMs: ptr(0.0)})
	var created []ConversationID
	for i := 0; i < 5; i++ {
		if err := h.Commit(ctx, func(tx *Tx) error {
			id, err := tx.CreateConversation(ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}, Cwd: fmt.Sprintf("/w%d", i)}})
			created = append(created, id)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	resident := func() int {
		h.session.mu.Lock()
		defer h.session.mu.Unlock()
		return len(h.session.state.Conversations)
	}
	deadline := time.Now().Add(5 * time.Second)
	for resident() > 1 {
		if time.Now().After(deadline) {
			t.Fatalf("%d conversation records kept", resident())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Every read still finds an evicted conversation; using it loads it.
	c, err := h.Conversation(ctx, created[2])
	if err != nil {
		t.Fatal(err)
	}
	if agent, err := c.Agent(ctx); err != nil || agent.Cwd != "/w2" {
		t.Fatalf("agent=%+v error=%v", agent, err)
	}
	if record := waitSettled(t, h, submit(t, c, SubmissionRequest{Content: "hi"})); record.Status != "done" {
		t.Fatalf("run on an evicted conversation=%+v", record)
	}
	found := 0
	if err := h.Commit(ctx, func(tx *Tx) error {
		page, err := tx.ScanConversations(ConversationQuery{}, 100, nil)
		found = len(page.Items)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if found != 6 {
		t.Fatalf("scanned %d conversations, want 6", found)
	}
	if _, err := h.Usage(ctx); err != nil {
		t.Fatal(err)
	}
	_ = root
}

// aheadStorage applies an armed commit, then holds its call open: storage is
// ahead of the published State meanwhile.
type aheadStorage struct {
	*MemoryStorage
	armed   atomic.Bool
	stored  chan struct{}
	release chan struct{}
}

func (s *aheadStorage) CommitDelta(ctx context.Context, expected uint64, delta *StateDelta) error {
	err := s.MemoryStorage.CommitDelta(ctx, expected, delta)
	if s.armed.CompareAndSwap(true, false) {
		close(s.stored)
		<-s.release
	}
	return err
}

func (s *aheadStorage) CommitDeltas(ctx context.Context, expected uint64, deltas []*StateDelta) (int, error) {
	return commitEach(ctx, s, expected, deltas)
}

func TestSnapshotWaitsForStoredCommitsToPublish(t *testing.T) {
	ctx := context.Background()
	store := &aheadStorage{MemoryStorage: NewMemoryStorage(), stored: make(chan struct{}), release: make(chan struct{})}
	h, err := Open(ctx, store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	store.armed.Store(true)
	committed := make(chan error, 1)
	go func() { committed <- h.Commit(ctx, touchStorage) }()
	<-store.stored
	snapshots := make(chan *State, 1)
	go func() {
		state, err := h.Snapshot(ctx)
		if err != nil {
			t.Error(err)
		}
		snapshots <- state
	}()
	select {
	case <-snapshots:
		t.Fatal("a snapshot read storage ahead of the published State")
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	if state := <-snapshots; state == nil || state.Documents[documentKey(touchDocument, 0)] == nil {
		t.Fatalf("snapshot after publication=%+v", state)
	}
}

func TestReadsDoNotSeeStoredUnpublishedValues(t *testing.T) {
	ctx := context.Background()
	store := &aheadStorage{MemoryStorage: NewMemoryStorage(), stored: make(chan struct{}), release: make(chan struct{})}
	h, root, notes, _ := idleDocuments(t, store)
	type doc struct{ N int }
	fresh := DocumentDefinition{Kind: "test.fresh", Version: 1, Scope: "conversation"}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(store.release) }) }
	defer release() // a failing check must not leave the commit hanging
	store.armed.Store(true)
	committed := make(chan error, 1)
	go func() {
		committed <- h.Commit(ctx, func(tx *Tx) error {
			if err := tx.PutDocument(fresh, uint64(root.ID()), json.RawMessage(`{"n":5}`)); err != nil {
				return err
			}
			return tx.PutDocument(notes, uint64(root.ID()), json.RawMessage(`{"n":9}`))
		})
	}()
	<-store.stored
	// Stored, not published: reads find the published values, the created
	// document absent.
	if got, err := ReadDocument[doc](ctx, h, notes, uint64(root.ID())); err != nil || got.N != 1 {
		t.Fatalf("read while the commit is stored, not published=%+v error=%v", got, err)
	}
	if got, err := ReadDocument[doc](ctx, h, fresh, uint64(root.ID())); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a document an unpublished commit created=%+v error=%v", got, err)
	}
	release()
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	if got, err := ReadDocument[doc](ctx, h, notes, uint64(root.ID())); err != nil || got.N != 9 {
		t.Fatalf("read after publication=%+v error=%v", got, err)
	}
}

func TestDocumentRetentionDropsIdleDocuments(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{Settings: Settings{DocumentRetentionMs: ptr(0.0)}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	root := testRoot(t, h)
	cid := uint64(root.ID())
	kept := DocumentDefinition{Kind: "test.kept", Version: 1, Scope: "conversation"}
	dropped := DocumentDefinition{Kind: "test.dropped", Version: 1, Scope: "conversation"}
	_, detach, err := ObserveDocument(h, kept, cid, Observer[json.RawMessage]{})
	if err != nil {
		t.Fatal(err)
	}
	defer detach()
	if err := h.Commit(ctx, func(tx *Tx) error {
		if err := tx.PutDocument(kept, cid, json.RawMessage(`{"n":1}`)); err != nil {
			return err
		}
		return tx.PutDocument(dropped, cid, json.RawMessage(`{"n":2}`))
	}); err != nil {
		t.Fatal(err)
	}
	// Another commit lets the next publication drop the idle document.
	if err := h.Commit(ctx, touchStorage); err != nil {
		t.Fatal(err)
	}
	resident := func(def DocumentDefinition) bool {
		h.session.mu.Lock()
		defer h.session.mu.Unlock()
		return h.session.state.Documents[documentKey(def, cid)] != nil
	}
	if resident(dropped) || !resident(kept) {
		t.Fatalf("resident: dropped=%v kept (observed)=%v", resident(dropped), resident(kept))
	}
	type doc struct{ N int }
	if got, err := ReadDocument[doc](ctx, h, dropped, cid); err != nil || got.N != 2 {
		t.Fatalf("a dropped document reads %+v error=%v", got, err)
	}
	view, err := root.View(ctx)
	if err != nil || string(view.Documents["test.dropped"]) != `{"n":2}` || string(view.Documents["test.kept"]) != `{"n":1}` {
		t.Fatalf("view documents=%v error=%v", view.Documents, err)
	}
}
