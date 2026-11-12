package durable

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func selectionRegistry(t *testing.T, names ...string) *RegistrySnapshot {
	t.Helper()
	registry := NewRegistry()
	for _, name := range names {
		if err := registry.Install(Extension{Name: name, Tools: []ToolDefinition{
			{ToolSpec: ToolSpec{Name: name + "-1", Parameters: json.RawMessage(`{"type":"object"}`)}, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) { return ToolResult{}, nil }},
			{ToolSpec: ToolSpec{Name: name + "-2", Parameters: json.RawMessage(`{"type":"object"}`)}, Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) { return ToolResult{}, nil }},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return registry.Snapshot()
}

func definitionNames(tools []ToolDefinition) []string {
	names := []string{}
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	return names
}

func TestAgentSelectionsEditTheDefaults(t *testing.T) {
	snapshot := selectionRegistry(t, "a", "b", "c")
	for _, tc := range []struct {
		name     string
		defaults []string
		agent    Agent
		want     []string
	}{
		{"every installed extension", nil, Agent{}, []string{"a-1", "a-2", "b-1", "b-2", "c-1", "c-2"}},
		{"edits of every installed extension", nil, Agent{ExtensionEdits: &SelectionEdits{Remove: []string{"b"}}}, []string{"a-1", "a-2", "c-1", "c-2"}},
		{"edits of the host default", []string{"a", "b"}, Agent{ExtensionEdits: &SelectionEdits{Add: []string{"c", "a"}, Remove: []string{"a"}}}, []string{"b-1", "b-2", "c-1", "c-2"}},
		{"an exact list wins", []string{"a"}, Agent{Extensions: []string{"c"}, ExtensionEdits: &SelectionEdits{Add: []string{"b"}}}, []string{"c-1", "c-2"}},
		{"removed tools", nil, Agent{Extensions: []string{"a", "b"}, RemoveTools: []string{"a-2", "b-1"}}, []string{"a-1", "b-2"}},
		{"an exact tool list wins", nil, Agent{Extensions: []string{"a"}, Tools: []string{"a-2"}, RemoveTools: []string{"a-2"}}, []string{"a-2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := *snapshot
			view.defaults = tc.defaults
			if got := definitionNames(view.Tools(tc.agent)); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("tools=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestAddToolsTakesALoadedToolOutOfTheRemovedOnes(t *testing.T) {
	h := testHarness(t)
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{RemoveTools: []string{"x", "y"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Commit(context.Background(), func(tx *Tx) error { return addTools(tx, root.ID(), []string{"y"}) }); err != nil {
		t.Fatal(err)
	}
	agent, err := root.Agent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(agent.RemoveTools, []string{"x"}) || agent.ModelTools == nil || !reflect.DeepEqual(agent.ModelTools.Add, []string{"y"}) {
		t.Fatalf("agent=%+v modelTools=%+v", agent, agent.ModelTools)
	}
}
