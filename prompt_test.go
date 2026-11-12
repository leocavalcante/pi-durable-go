package durable

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func texts(pairs ...string) orderedText {
	var o orderedText
	for i := 0; i+1 < len(pairs); i += 2 {
		o.set(pairs[i], pairs[i+1])
	}
	return o
}

func str(s string) *string { return &s }

func tool(name, description string) ToolSpec {
	return ToolSpec{Name: name, Description: description, Parameters: json.RawMessage(`{"type":"object"}`)}
}

// systemView is a view over messages that a no-head transcript contributed.
func systemView(messages ...Message) contextView {
	var view contextView
	for i, m := range messages {
		e := Entry{ID: EntryID(i + 1), Kind: "pi.user", Message: &m}
		if m.Role == "system" {
			e.Kind = "pi.system"
		}
		view.entries = append(view.entries, e)
		view.contributions = append(view.contributions, []Message{m})
		view.messages = append(view.messages, m)
	}
	return view
}

func TestPlanSystemEntries(t *testing.T) {
	a, b := tool("a", "A"), tool("b", "B")
	// The first request declares every section and tool.
	planned := planSystemEntries(systemView(Message{Role: "user", Content: "hi"}), texts("preamble", "P", "cwd", "C"), []ToolSpec{a})
	want := []Entry{{Kind: "pi.system", Message: &Message{Role: "system",
		Sections: []SystemSection{{Name: "preamble", Text: str("P")}, {Name: "cwd", Text: str("C")}}, ToolsAdded: []ToolSpec{a}}}}
	if !reflect.DeepEqual(planned, want) {
		t.Fatalf("baseline=%+v", planned[0].Message)
	}
	shown := systemView(*want[0].Message, Message{Role: "user", Content: "hi"})
	if planned := planSystemEntries(shown, texts("preamble", "P", "cwd", "C"), []ToolSpec{a}); len(planned) != 0 {
		t.Fatalf("an unchanged prompt planned %+v", planned)
	}
	// A changed value and a removed section: the minimal patch, with the tool change on it.
	planned = planSystemEntries(shown, texts("preamble", "P2"), []ToolSpec{a, b})
	if len(planned) != 1 || !reflect.DeepEqual(planned[0].Message.Sections, []SystemSection{{Name: "preamble", Text: str("P2")}, {Name: "cwd"}}) ||
		!reflect.DeepEqual(planned[0].Message.ToolsAdded, []ToolSpec{b}) || len(planned[0].Message.ToolsRemoved) != 0 {
		t.Fatalf("patch=%+v", planned[0].Message)
	}
	// Another order: remove every section, then re-add them in order.
	planned = planSystemEntries(shown, texts("cwd", "C", "preamble", "P"), []ToolSpec{a})
	if len(planned) != 2 || !reflect.DeepEqual(planned[0].Message.Sections, []SystemSection{{Name: "preamble"}, {Name: "cwd"}}) ||
		!reflect.DeepEqual(planned[1].Message.Sections, []SystemSection{{Name: "cwd", Text: str("C")}, {Name: "preamble", Text: str("P")}}) {
		t.Fatalf("reorder=%+v / %+v", planned[0].Message, planned[1].Message)
	}
	// A changed declaration is removed and re-added; another tool order re-adds every tool.
	if _, added := planTools([]ToolSpec{a, b}, []ToolSpec{a, tool("b", "B2")}); len(added) != 1 || added[0].Description != "B2" {
		t.Fatalf("changed declaration added=%+v", added)
	}
	if removed, added := planTools([]ToolSpec{a, b}, []ToolSpec{b, a}); !reflect.DeepEqual(removed, []string{"a", "b"}) || len(added) != 2 || added[0].Name != "b" {
		t.Fatalf("reordered tools removed=%v added=%+v", removed, added)
	}
}

func TestPlanSystemEntriesRebaselinesAfterAHeadMarker(t *testing.T) {
	retained := Entry{ID: 3, Kind: "pi.system", Message: &Message{Role: "system", Sections: []SystemSection{{Name: "preamble", Text: str("P")}}}}
	head := Entry{ID: 5, Kind: "pi.compaction"}
	view := contextView{head: &head, entries: []Entry{head, retained}, messages: []Message{*retained.Message}}
	planned := planSystemEntries(view, texts("preamble", "P"), nil)
	if len(planned) != 1 || !reflect.DeepEqual(planned[0].Edits, []ContextEdit{{Target: 3, Action: "omit"}}) ||
		!reflect.DeepEqual(planned[0].Message.Sections, []SystemSection{{Name: "preamble", Text: str("P")}}) {
		t.Fatalf("rebaseline=%+v", planned)
	}
}

func TestDeriveContextEditsAndTheLeadingBaseline(t *testing.T) {
	baseline := Message{Role: "system", Sections: []SystemSection{{Name: "preamble", Text: str("P")}}}
	history := []Entry{
		{ID: 1, Kind: "pi.user", Message: &Message{Role: "user", Content: "hi"}},
		{ID: 2, Kind: "pi.system", Message: &baseline},
		{ID: 3, Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: "hello"}},
		{ID: 4, Kind: "pi.user", Message: &Message{Role: "user", Content: "secret"}},
		{ID: 5, Kind: "app.redact", Edits: []ContextEdit{{Target: 4, Action: "replace", Messages: []Message{{Role: "user", Content: "[redacted]"}}}, {Target: 3, Action: "omit"}}},
	}
	view, err := deriveContext(history)
	if err != nil {
		t.Fatal(err)
	}
	var roles []string
	for _, m := range view.messages {
		roles = append(roles, m.Role+":"+m.Content)
	}
	if strings.Join(roles, ",") != "system:,user:hi,user:[redacted]" {
		t.Fatalf("messages=%v", roles)
	}
}

func TestPositionalSystemMessagesAcrossRequests(t *testing.T) {
	var mu sync.Mutex
	var requests []ModelRequest
	model := taskModelFunc(func(_ context.Context, req ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, req)
		return ModelResponse{Content: "ok"}, nil
	})
	var preamble sync.Map
	preamble.Store("text", "first")
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "prompt", Sections: []Section{{Name: "preamble", Render: func(context.Context, ConversationRecord, *Harness) (string, error) {
		text, _ := preamble.Load("text")
		return text.(string), nil
	}}}}); err != nil {
		t.Fatal(err)
	}
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}, Extensions: []string{"prompt"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "one"}))
	preamble.Store("text", "second")
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "two"}))
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("%d requests", len(requests))
	}
	var shape []string
	for _, m := range requests[1].Messages {
		if m.Role == "system" {
			shape = append(shape, "system:"+SystemText(m))
		} else {
			shape = append(shape, m.Role+":"+m.Content)
		}
	}
	want := "system:<preamble>\nfirst\n</preamble>,user:one,assistant:ok,user:two,system:<preamble>\nsecond\n</preamble>"
	if strings.Join(shape, ",") != want {
		t.Fatalf("second request:\n%s\nwant\n%s", strings.Join(shape, ","), want)
	}
	if got := promptText(requests[1].Messages); got != "<preamble>\nsecond\n</preamble>" {
		t.Fatalf("folded prompt=%q", got)
	}
}

func TestAnEditedBaselineDoesNotLead(t *testing.T) {
	baseline := Message{Role: "system", Sections: []SystemSection{{Name: "preamble", Text: str("P")}}}
	history := []Entry{
		{ID: 1, Kind: "pi.user", Message: &Message{Role: "user", Content: "hi"}},
		{ID: 2, Kind: "pi.assistant", Message: &Message{Role: "assistant", Content: "hello"}},
		{ID: 3, Kind: "pi.system", Message: &baseline},
		{ID: 4, Kind: "app.edit", Edits: []ContextEdit{{Target: 3, Action: "replace", Messages: []Message{{Role: "system", Content: "replaced"}}}}},
	}
	view, err := deriveContext(history)
	if err != nil {
		t.Fatal(err)
	}
	if roles := []string{view.messages[0].Role, view.messages[2].Role}; roles[0] != "user" || roles[1] != "system" || view.messages[2].Content != "replaced" {
		t.Fatalf("messages=%+v", view.messages)
	}
}
