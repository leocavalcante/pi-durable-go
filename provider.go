package durable

import (
	"context"
	"encoding/json"
	"errors"
)

// ProviderState is a conversation's provider-facing identity, its pi.provider
// document (upstream ProviderState): a session ID made when the conversation
// is created, fresh for every fork, which its model requests carry.
type ProviderState struct {
	SessionID string `json:"sessionId"`
}

// providerDocument is pi.provider: every fork starts with a fresh identity
// instead of copying its parent's.
var providerDocument = DocumentDefinition{Kind: "pi.provider", Version: 1, Scope: "conversation", History: "latest", Fork: "initial",
	Initial: func() any { return ProviderState{} }}

// providerSessionID is the session ID a conversation's requests carry: its
// pi.provider document's, or, for a conversation from before the document,
// its record's, which stored is false for.
func providerSessionID(state *State, c *ConversationRecord) (id string, stored bool) {
	if doc, err := state.document(documentKey(providerDocument, uint64(c.ID))); err == nil && doc != nil {
		var provider ProviderState
		if json.Unmarshal(doc.Value, &provider) == nil && provider.SessionID != "" {
			return provider.SessionID, true
		}
	}
	return c.ProviderSessionID, false
}

// storeProviderSessionID writes a conversation from before pi.provider its
// document, keeping the identity its requests had (upstream migrates such a
// conversation in one commit before its first request).
func storeProviderSessionID(tx *Tx, c *ConversationRecord) error {
	id, stored := providerSessionID(tx.state, c)
	if stored {
		return nil
	}
	if id == "" {
		var err error
		if id, err = newSessionID(tx.clock()); err != nil {
			return err
		}
	}
	return putJSONDocument(tx, providerDocument, uint64(c.ID), ProviderState{SessionID: id})
}

// createBuiltinDocuments is upstream's creation hook for its built-in
// documents, in every commit that creates or forks a conversation: empty
// pi.live, pi.inbox, and pi.usage, and a fresh pi.provider.
func createBuiltinDocuments(tx *Tx, id ConversationID, sessionID string) error {
	for _, doc := range []struct {
		def   DocumentDefinition
		value any
	}{
		{liveDocument, LiveState{}},
		{inboxDocument, InboxState{Items: []InboxItem{}}},
		{usageDocument, UsageState{Models: map[string]Usage{}, Tools: map[string]Usage{}}},
		{providerDocument, ProviderState{SessionID: sessionID}},
	} {
		if err := putJSONDocument(tx, doc.def, uint64(id), doc.value); err != nil {
			return err
		}
	}
	return nil
}

func putJSONDocument(tx *Tx, def DocumentDefinition, id uint64, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.putDocument(def, id, "", data)
}

// Provider reads this conversation's provider-facing identity; a
// conversation from before pi.provider reads its record's.
func (c *Conversation) Provider(ctx context.Context) (ProviderState, error) {
	provider, err := ReadDocument[ProviderState](ctx, c.h, providerDocument, uint64(c.id))
	if errors.Is(err, ErrNotFound) {
		state, err := c.h.read(ctx)
		if err != nil {
			return ProviderState{}, err
		}
		record, err := state.conversation(c.id)
		if err != nil || record == nil {
			return ProviderState{}, ErrNotFound
		}
		return ProviderState{SessionID: record.ProviderSessionID}, nil
	}
	return provider, err
}
