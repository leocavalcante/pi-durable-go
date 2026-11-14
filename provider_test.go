package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

// As upstream's creation hook, a conversation starts with empty pi.live,
// pi.inbox, and pi.usage and a fresh pi.provider, whose session ID its
// requests carry; a fork starts with a fresh identity.
func TestBuiltinDocumentsAreCreatedWithTheConversation(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var sessions []string
	model := taskModelFunc(func(_ context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		sessions = append(sessions, request.SessionID)
		mu.Unlock()
		return ModelResponse{Content: "ok"}, nil
	})
	h, root := runHarness(t, model, Settings{})
	for _, def := range []DocumentDefinition{liveDocument, inboxDocument, usageDocument, providerDocument} {
		if _, err := ReadDocument[json.RawMessage](ctx, h, def, uint64(root.ID())); err != nil {
			t.Fatalf("%s: %v", def.Kind, err)
		}
	}
	usage, _ := ReadDocument[json.RawMessage](ctx, h, usageDocument, uint64(root.ID()))
	if string(usage) != `{"models":{},"tools":{}}` {
		t.Fatalf("pi.usage=%s", usage)
	}
	provider, err := root.Provider(ctx)
	if err != nil || len(provider.SessionID) != 36 || provider.SessionID[14] != '7' {
		t.Fatalf("provider=%+v err=%v", provider, err)
	}
	state, _ := h.Snapshot(ctx)
	if state.Conversations[root.ID()].ProviderSessionID != provider.SessionID {
		t.Fatalf("record=%q document=%q", state.Conversations[root.ID()].ProviderSessionID, provider.SessionID)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	entries, _ := root.Entries(ctx)
	fork, err := root.Fork(ctx, entries[len(entries)-1].ID, ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	forked, _ := fork.Provider(ctx)
	if forked.SessionID == "" || forked.SessionID == provider.SessionID {
		t.Fatalf("fork identity=%q, root %q", forked.SessionID, provider.SessionID)
	}
	if len(sessions) != 1 || sessions[0] != provider.SessionID {
		t.Fatalf("request sessions=%v", sessions)
	}
}

// A conversation from before pi.provider keeps the identity its requests had:
// its first request writes the document with its record's session ID.
func TestALegacyConversationKeepsItsProviderIdentity(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var sessions []string
	model := taskModelFunc(func(_ context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		sessions = append(sessions, request.SessionID)
		mu.Unlock()
		return ModelResponse{Content: "ok"}, nil
	})
	h, root := runHarness(t, model, Settings{})
	// A store from before the document: the record holds the identity.
	if err := h.Commit(ctx, func(tx *Tx) error { return tx.RetireDocument(providerDocument, uint64(root.ID())) }); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDocument[ProviderState](ctx, h, providerDocument, uint64(root.ID())); !errors.Is(err, ErrNotFound) {
		t.Fatalf("document still there: %v", err)
	}
	state, _ := h.Snapshot(ctx)
	legacy := state.Conversations[root.ID()].ProviderSessionID
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	provider, err := ReadDocument[ProviderState](ctx, h, providerDocument, uint64(root.ID()))
	if err != nil || provider.SessionID != legacy || len(sessions) != 1 || sessions[0] != legacy {
		t.Fatalf("provider=%+v err=%v sessions=%v legacy=%q", provider, err, sessions, legacy)
	}
}
