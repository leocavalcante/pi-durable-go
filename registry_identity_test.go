package durable

import (
	"context"
	"encoding/json"
	"testing"
)

func registryIdentityTask(kind string, version int) TaskDefinition {
	return TaskDefinition{
		Kind:    kind,
		Version: version,
		Run:     func(context.Context, *TaskRuntime, TaskRecord) error { return nil },
	}
}

func TestRegistryResolvedTaskIdentityLifecycle(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "alpha", Tasks: []TaskDefinition{
		registryIdentityTask("alpha.one", 1),
		registryIdentityTask("alpha.two", 1),
	}}); err != nil {
		t.Fatal(err)
	}

	beforeReplacement := registry.Snapshot()
	first, ok := beforeReplacement.resolved("alpha.one")
	if !ok {
		t.Fatal("alpha.one was not resolved")
	}
	second, ok := beforeReplacement.resolved("alpha.two")
	if !ok {
		t.Fatal("alpha.two was not resolved")
	}
	if first.Identity == 0 || first.Identity != second.Identity {
		t.Fatalf("definitions from one install should share a nonzero identity: %d, %d", first.Identity, second.Identity)
	}
	if first.Extension != "alpha" || second.Extension != "alpha" {
		t.Fatalf("wrong installer ownership: %q, %q", first.Extension, second.Extension)
	}
	publicTask, ok := beforeReplacement.Task("alpha.one")
	if !ok || publicTask.Kind != first.Definition.Kind || publicTask.Version != first.Definition.Version {
		t.Fatalf("public Task lookup changed: task=%+v, ok=%v", publicTask, ok)
	}

	if err := registry.Install(Extension{Name: "unrelated", Tasks: []TaskDefinition{registryIdentityTask("unrelated.task", 1)}}); err != nil {
		t.Fatal(err)
	}
	if got, ok := registry.Snapshot().resolved("alpha.one"); !ok || got.Identity != first.Identity {
		t.Fatalf("unrelated install changed alpha identity: got=%+v, ok=%v", got, ok)
	}
	registry.Uninstall("unrelated")
	if got, ok := registry.Snapshot().resolved("alpha.one"); !ok || got.Identity != first.Identity {
		t.Fatalf("unrelated uninstall changed alpha identity: got=%+v, ok=%v", got, ok)
	}

	if err := registry.Install(Extension{Name: "alpha", Tasks: []TaskDefinition{
		registryIdentityTask("alpha.one", 2),
		registryIdentityTask("alpha.two", 2),
	}}); err != nil {
		t.Fatal(err)
	}
	replaced, ok := registry.Snapshot().resolved("alpha.one")
	if !ok || replaced.Identity <= first.Identity {
		t.Fatalf("replacement did not get a new increasing identity: first=%d, replaced=%+v, ok=%v", first.Identity, replaced, ok)
	}
	if got, ok := registry.Snapshot().resolved("alpha.two"); !ok || got.Identity != replaced.Identity {
		t.Fatalf("definitions in replacement install have different identities: %+v, ok=%v", got, ok)
	}
	if firstAgain, ok := beforeReplacement.resolved("alpha.one"); !ok || firstAgain.Identity != first.Identity || firstAgain.Definition.Version != 1 {
		t.Fatalf("old snapshot changed after replacement: %+v, ok=%v", firstAgain, ok)
	}

	if err := registry.Install(Extension{Name: "alpha", Tasks: []TaskDefinition{
		registryIdentityTask("alpha.one", 2),
		registryIdentityTask("alpha.two", 2),
	}}); err != nil {
		t.Fatal(err)
	}
	unchangedReinstall, ok := registry.Snapshot().resolved("alpha.one")
	if !ok || unchangedReinstall.Identity <= replaced.Identity || unchangedReinstall.Definition.Version != replaced.Definition.Version {
		t.Fatalf("reinstalling unchanged definitions did not create a new identity: replaced=%+v, reinstall=%+v, ok=%v", replaced, unchangedReinstall, ok)
	}

	registry.Uninstall("alpha")
	if _, ok := registry.Snapshot().resolved("alpha.one"); ok {
		t.Fatal("uninstalled task remained in the current snapshot")
	}
	if err := registry.Install(Extension{Name: "alpha", Tasks: []TaskDefinition{registryIdentityTask("alpha.one", 2)}}); err != nil {
		t.Fatal(err)
	}
	reinstalled, ok := registry.Snapshot().resolved("alpha.one")
	if !ok || reinstalled.Identity <= unchangedReinstall.Identity {
		t.Fatalf("reinstall did not get a new increasing identity: previous=%d, reinstalled=%+v, ok=%v", unchangedReinstall.Identity, reinstalled, ok)
	}
}

func TestRegistryIdentityInstallIsAtomicOnFailure(t *testing.T) {
	registry := NewRegistry()
	listenerCalls := 0
	registry.Subscribe(func() { listenerCalls++ })
	if err := registry.Install(Extension{Name: "existing", Tasks: []TaskDefinition{registryIdentityTask("occupied", 1)}}); err != nil {
		t.Fatal(err)
	}
	before := registry.Snapshot()
	original, ok := before.resolved("occupied")
	if !ok {
		t.Fatal("occupied task was not resolved")
	}

	err := registry.Install(Extension{Name: "rejected", Tasks: []TaskDefinition{registryIdentityTask("occupied", 1)}})
	if err == nil {
		t.Fatal("install with a duplicate kind succeeded")
	}
	if listenerCalls != 1 {
		t.Fatalf("failed install notified listeners: calls=%d", listenerCalls)
	}
	current := registry.Snapshot()
	got, ok := current.resolved("occupied")
	if !ok || got.Identity != original.Identity || got.Extension != original.Extension {
		t.Fatalf("failed install changed existing resolution: got=%+v, ok=%v", got, ok)
	}
	if _, ok := current.resolved("rejected"); ok {
		t.Fatal("failed extension became visible")
	}
	if extensions := current.Extensions(); len(extensions) != 1 || extensions[0].Name != "existing" {
		t.Fatalf("failed install changed installed extensions: %+v", extensions)
	}

	if err := registry.Install(Extension{Name: "next", Tasks: []TaskDefinition{registryIdentityTask("next.task", 1)}}); err != nil {
		t.Fatal(err)
	}
	next, ok := registry.Snapshot().resolved("next.task")
	if !ok || next.Identity != original.Identity+1 {
		t.Fatalf("failed install consumed an identity: previous=%d, next=%+v, ok=%v", original.Identity, next, ok)
	}
	if listenerCalls != 2 {
		t.Fatalf("successful install listener count=%d, want 2", listenerCalls)
	}
}

func TestRegistryPhaseToolDefinitionsAreDetached(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "tools", Tools: []ToolDefinition{{
		ToolSpec: ToolSpec{Name: "tool", Parameters: json.RawMessage(`{"type":"object"}`)},
		Execute:  func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) { return ToolResult{}, nil },
	}}}); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot()
	tools := snapshot.Tools(Agent{})
	tools[0].Parameters[0] = '['
	if got := snapshot.Tools(Agent{}); string(got[0].Parameters) != `{"type":"object"}` {
		t.Fatal("tool metadata mutation changed the phase snapshot")
	}
}

func TestRegistryResolvedSnapshotIsDetached(t *testing.T) {
	registry := NewRegistry()
	ext := Extension{Name: "detached", Tasks: []TaskDefinition{registryIdentityTask("detached.task", 1)}}
	if err := registry.Install(ext); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot()

	ext.Tasks[0].Kind = "mutated.input"
	ext.Tasks[0].Version = 99
	published := snapshot.Extensions()
	published[0].Tasks[0].Kind = "mutated.output"
	published[0].Tasks[0].Version = 100

	resolved, ok := snapshot.resolved("detached.task")
	if !ok || resolved.Definition.Kind != "detached.task" || resolved.Definition.Version != 1 {
		t.Fatalf("snapshot definition was mutated through detached slices: %+v, ok=%v", resolved, ok)
	}
	if _, ok := snapshot.resolved("mutated.input"); ok {
		t.Fatal("mutation of the install argument changed the snapshot")
	}
	if _, ok := snapshot.resolved("mutated.output"); ok {
		t.Fatal("mutation of Extensions() output changed the snapshot")
	}
	current, ok := registry.Snapshot().resolved("detached.task")
	if !ok || current.Definition.Version != 1 || current.Identity != resolved.Identity {
		t.Fatalf("detached mutation changed the registry: %+v, ok=%v", current, ok)
	}
}
