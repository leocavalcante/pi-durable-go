package durable

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestChangeAgentChangesOnlyWhatItNames(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "p", ID: "m"}, Extensions: []string{"a"}, Tools: []string{"t"}, Cwd: "/work", MaxTurns: 3}})
	if err != nil {
		t.Fatal(err)
	}
	level := "high"
	if err := root.ChangeAgent(ctx, AgentChange{ThinkingLevel: &level, ExtensionEdits: &SelectionEdits{Add: []string{"b"}}, RemoveTools: &[]string{"x"}, Clear: []string{"cwd"}}); err != nil {
		t.Fatal(err)
	}
	agent, err := root.Agent(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := Agent{Model: ModelRef{Provider: "p", ID: "m"}, ThinkingLevel: "high", ExtensionEdits: &SelectionEdits{Add: []string{"b"}}, RemoveTools: []string{"x"}, MaxTurns: 3}
	if !reflect.DeepEqual(agent, want) {
		t.Fatalf("agent=%+v\nwant %+v", agent, want)
	}
	// The exact forms replace the relative ones.
	if err := root.ChangeAgent(ctx, AgentChange{Extensions: &[]string{"c"}, Tools: &[]string{"y"}}); err != nil {
		t.Fatal(err)
	}
	agent, _ = root.Agent(ctx)
	if agent.ExtensionEdits != nil || agent.RemoveTools != nil || !reflect.DeepEqual(agent.Extensions, []string{"c"}) || !reflect.DeepEqual(agent.Tools, []string{"y"}) {
		t.Fatalf("agent=%+v", agent)
	}
	for name, change := range map[string]AgentChange{
		"maxTurns 0":          {MaxTurns: ptr(0)},
		"a fractional count":  {Compaction: &CompactionSettings{KeepRecentTokens: ptr(1.5)}},
		"a negative override": {Compaction: &CompactionSettings{ModelOverrides: map[string]CompactionSettings{"p/m": {ReserveTokens: ptr(-1.0)}}}},
		"an unknown field":    {Clear: []string{"color"}},
	} {
		if err := root.ChangeAgent(ctx, change); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// The agent is a rewindable document (upstream AgentDoc), not transcript
// entries: a fork at an earlier entry gets the agent of that time.
func TestTheAgentIsARewindableDocument(t *testing.T) {
	ctx := context.Background()
	h := testHarness(t)
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "first"}}})
	if err != nil {
		t.Fatal(err)
	}
	var at EntryID
	if err := h.Commit(ctx, func(tx *Tx) error {
		var err error
		at, err = tx.Append(root.ID(), Entry{Kind: "app.note", Data: json.RawMessage(`{}`)})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := root.Configure(ctx, Agent{Model: ModelRef{Provider: "fake", ID: "second"}}); err != nil {
		t.Fatal(err)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Kind == "pi.agent" {
			t.Fatalf("an agent entry in the transcript: %+v", e)
		}
	}
	type agentDoc struct{ Model ModelRef }
	if got, err := ReadDocument[agentDoc](ctx, h, agentDocument, uint64(root.ID())); err != nil || got.Model.ID != "second" {
		t.Fatalf("agent document=%+v error=%v", got, err)
	}
	child, err := root.Fork(ctx, at, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := child.Agent(ctx)
	if err != nil || agent.Model.ID != "first" {
		t.Fatalf("forked agent=%+v error=%v", agent, err)
	}
}

// Stores from before the agent document kept agent changes as pi.agent
// entries; forks still read them.
func TestForksReadLegacyAgentEntries(t *testing.T) {
	ctx := context.Background()
	at := time.Unix(1, 0).UTC()
	agent := func(id string) json.RawMessage {
		data, _ := json.Marshal(Agent{Model: ModelRef{Provider: "fake", ID: id}})
		return data
	}
	legacy := &State{Version: stateVersion, Sequence: 1, NextID: 10, Root: 1,
		Conversations: map[ConversationID]*ConversationRecord{1: {ID: 1, Head: 4, Agent: Agent{Model: ModelRef{Provider: "fake", ID: "second"}}, CreatedAt: at}},
		Entries: map[EntryID]*Entry{
			2: {ID: 2, Conversation: 1, Kind: "pi.agent", Data: agent("first"), Sequence: 1, CreatedAt: at},
			3: {ID: 3, Conversation: 1, Parent: 2, Kind: "app.note", Data: json.RawMessage(`{}`), Sequence: 1, CreatedAt: at},
			4: {ID: 4, Conversation: 1, Parent: 3, Kind: "pi.agent", Data: agent("second"), Sequence: 1, CreatedAt: at},
		},
		Tasks: map[TaskID]*TaskRecord{}, Submissions: map[SubmissionID]*SubmissionRecord{}, Documents: map[string]*DocumentRecord{}}
	store := NewMemoryStorage()
	if err := store.Commit(ctx, 0, legacy); err != nil {
		t.Fatal(err)
	}
	h, err := Open(ctx, store, HarnessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	root, err := h.Root(ctx, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	child, err := root.Fork(ctx, 3, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := child.Agent(ctx); err != nil || got.Model.ID != "first" {
		t.Fatalf("forked agent=%+v error=%v", got, err)
	}
}
