package durable

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

// holeyEntries returns count entries whose IDs skip every third ID.
func holeyEntries(count int) []Entry {
	var entries []Entry
	for id := 1; len(entries) < count; id++ {
		if id%3 != 0 {
			entries = append(entries, Entry{ID: EntryID(id), Kind: "note"})
		}
	}
	return entries
}

type countingLoader struct {
	entries []Entry
	calls   int
}

func (l *countingLoader) load(_ context.Context, query EntryQuery, limit int) (EntryPage, error) {
	l.calls++
	var matching []Entry
	for _, e := range l.entries {
		if (query.MinEntryID == 0 || e.ID >= query.MinEntryID) && (query.MaxEntryID == 0 || e.ID <= query.MaxEntryID) {
			matching = append(matching, e)
		}
	}
	if query.Order != "ascending" {
		for i, j := 0, len(matching)-1; i < j; i, j = i+1, j-1 {
			matching[i], matching[j] = matching[j], matching[i]
		}
	}
	page := EntryPage{Items: matching}
	if len(matching) > limit {
		page.Items = matching[:limit]
		page.Next = &EntryCursor{After: matching[limit-1].ID, Order: query.Order}
	}
	return page, nil
}

func windowOf(all []Entry, first, last int) ConversationView {
	view := ConversationView{Entries: append([]Entry(nil), all[first:last+1]...)}
	if first > 0 {
		view.Before = all[first-1].ID
	}
	return view
}

func shape(view TranscriptView) []string {
	var out []string
	for _, item := range view.Items {
		switch item.Kind {
		case "entry":
			out = append(out, fmt.Sprint(item.Entry.ID))
		case "gap":
			out = append(out, fmt.Sprintf("gap %d..%d", item.After, item.UpTo))
		default:
			out = append(out, item.Kind)
		}
	}
	return out
}

func idsOf(entries []Entry) []string {
	var out []string
	for _, e := range entries {
		out = append(out, fmt.Sprint(e.ID))
	}
	return out
}

func gapIn(view TranscriptView) TranscriptItem {
	for _, item := range view.Items {
		if item.Kind == "gap" {
			return item
		}
	}
	return TranscriptItem{}
}

func newTranscript(t *testing.T, loader *countingLoader, pageSize int) *ConversationTranscript {
	t.Helper()
	transcript, err := NewConversationTranscript(loader.load, pageSize)
	if err != nil {
		t.Fatal(err)
	}
	return transcript
}

func expectShape(t *testing.T, got TranscriptView, want ...[]string) {
	t.Helper()
	var flat []string
	for _, part := range want {
		flat = append(flat, part...)
	}
	if !reflect.DeepEqual(shape(got), flat) {
		t.Fatalf("items=%v\nwant  %v", shape(got), flat)
	}
}

func TestTranscriptFirstWindowAndAppends(t *testing.T) {
	all := holeyEntries(30)
	later := newTranscript(t, &countingLoader{entries: all}, 0)
	expectShape(t, later.Merge(windowOf(all, 15, 19)), []string{"earlier"}, idsOf(all[15:20]))
	if newest, _ := later.Newest(); newest != all[19].ID {
		t.Fatalf("newest=%d", newest)
	}
	whole := newTranscript(t, &countingLoader{entries: all}, 0)
	expectShape(t, whole.Merge(windowOf(all, 0, 19)), idsOf(all[:20]))

	helper := newTranscript(t, &countingLoader{entries: all}, 0)
	helper.Merge(windowOf(all, 10, 19))
	expectShape(t, helper.Merge(windowOf(all, 10, 21)), []string{"earlier"}, idsOf(all[10:22]))
	// The window dropped its oldest; the transcript keeps them.
	expectShape(t, helper.Merge(windowOf(all, 18, 21)), []string{"earlier"}, idsOf(all[10:22]))
}

func TestTranscriptGapsFillPageByPage(t *testing.T) {
	ctx := context.Background()
	all := holeyEntries(200)
	loader := &countingLoader{entries: all}
	helper := newTranscript(t, loader, 50)
	helper.Merge(windowOf(all, 150, 159))
	stalled := helper.Merge(windowOf(all, 190, 199))
	gap := gapIn(stalled)
	if gap.After != all[159].ID || gap.UpTo != all[190].ID {
		t.Fatalf("gap=%+v", gap)
	}
	expectShape(t, stalled, []string{"earlier"}, idsOf(all[150:160]), []string{fmt.Sprintf("gap %d..%d", all[159].ID, all[190].ID)}, idsOf(all[190:]))
	filled, err := helper.FillGap(ctx, gap)
	if err != nil {
		t.Fatal(err)
	}
	expectShape(t, filled, []string{"earlier"}, idsOf(all[150:]))
	if loader.calls != 1 {
		t.Fatalf("calls=%d", loader.calls)
	}

	small := newTranscript(t, &countingLoader{entries: all}, 10)
	small.Merge(windowOf(all, 100, 109))
	view := small.Merge(windowOf(all, 190, 199))
	if view, err = small.FillGap(ctx, gapIn(view)); err != nil {
		t.Fatal(err)
	}
	if gap := gapIn(view); gap.After != all[119].ID || gap.UpTo != all[190].ID {
		t.Fatalf("shrunk gap=%+v", gap)
	}
	for gapIn(view).Kind == "gap" {
		if view, err = small.FillGap(ctx, gapIn(view)); err != nil {
			t.Fatal(err)
		}
	}
	expectShape(t, view, []string{"earlier"}, idsOf(all[100:]))
}

func TestTranscriptLoadsEarlierUntilTheStart(t *testing.T) {
	ctx := context.Background()
	all := holeyEntries(45)
	loader := &countingLoader{entries: all}
	helper := newTranscript(t, loader, 20)
	helper.Merge(windowOf(all, 40, 44))
	view, err := helper.LoadEarlier(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expectShape(t, view, []string{"earlier"}, idsOf(all[20:]))
	if view, err = helper.LoadEarlier(ctx); err != nil {
		t.Fatal(err)
	}
	expectShape(t, view, idsOf(all))
	calls := loader.calls
	if _, err := helper.LoadEarlier(ctx); err != nil || loader.calls != calls {
		t.Fatalf("loaded past the start: calls=%d error=%v", loader.calls, err)
	}
}

func TestTranscriptResume(t *testing.T) {
	all := holeyEntries(20)
	helper := newTranscript(t, &countingLoader{entries: all}, 0)
	if _, ok := helper.Resume(); ok {
		t.Fatal("resume before a merge")
	}
	if _, err := helper.LoadEarlier(context.Background()); err == nil {
		t.Fatal("loadEarlier before a merge")
	}
	empty := newTranscript(t, &countingLoader{}, 0)
	empty.Merge(ConversationView{})
	if window, ok := empty.Resume(); !ok || window != (ConversationWindow{}) {
		t.Fatalf("empty resume=%+v ok=%v", window, ok)
	}
	helper.Merge(windowOf(all, 10, 15))
	if window, _ := helper.Resume(); window.From != all[15].ID+1 {
		t.Fatalf("resume=%+v", window)
	}
	// An empty window right after the newest adds nothing; one past it opens a gap.
	before := shape(helper.Merge(ConversationView{Before: all[15].ID}))
	if !reflect.DeepEqual(before, append([]string{"earlier"}, idsOf(all[10:16])...)) {
		t.Fatalf("items=%v", before)
	}
	view := helper.Merge(windowOf(all, 18, 19))
	expectShape(t, view, []string{"earlier"}, idsOf(all[10:16]), []string{fmt.Sprintf("gap %d..%d", all[15].ID, all[18].ID)}, idsOf(all[18:20]))
}

func TestTranscriptOverAConversation(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	c := testRoot(t, h)
	ids := appendNotes(t, h, c, 6)
	transcript, err := c.Transcript(2)
	if err != nil {
		t.Fatal(err)
	}
	views, stop, err := c.WatchWindow(ctx, ConversationWindow{Min: 2, Max: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	view := transcript.Merge(<-views)
	for view.Items[0].Kind == "earlier" {
		if view, err = transcript.LoadEarlier(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var got []EntryID
	for _, item := range view.Items {
		if item.Entry.Kind == "app.note" {
			got = append(got, item.Entry.ID)
		}
	}
	if !reflect.DeepEqual(got, ids) {
		t.Fatalf("transcript=%v want %v", got, ids)
	}
}
