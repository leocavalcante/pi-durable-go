package durable

import (
	"context"
	"errors"
	"reflect"
	"testing"
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
