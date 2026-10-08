package durable

import (
	"context"
	"encoding/json"
	"fmt"
)

func documentKey(def DocumentDefinition, id uint64) string {
	if def.Scope == "session" {
		id = 0
	}
	return fmt.Sprintf("%s:%d:%s", def.Scope, id, def.Kind)
}
func validateDocument(def DocumentDefinition) error {
	if def.Kind == "" || def.Version < 1 {
		return fmt.Errorf("%w: document kind/version", ErrInvalid)
	}
	switch def.Scope {
	case "session", "task":
		if def.History != "" && def.History != "latest" || def.Fork != "" {
			return fmt.Errorf("%w: only conversation documents fork or rewind", ErrInvalid)
		}
	case "conversation":
		if def.History != "" && def.History != "latest" && def.History != "rewindable" {
			return ErrInvalid
		}
		if def.Fork != "" && def.Fork != "initial" && def.Fork != "current" && def.Fork != "asOf" {
			return ErrInvalid
		}
		if def.Fork == "asOf" && def.History != "rewindable" {
			return fmt.Errorf("%w: asOf requires rewindable history", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: document scope", ErrInvalid)
	}
	return nil
}
func migratedDocument(def DocumentDefinition, doc *DocumentRecord) (json.RawMessage, error) {
	if doc.Scope != def.Scope || doc.History != normalizeHistory(def.History) || doc.Fork != normalizeFork(def.Scope, def.Fork) {
		return nil, fmt.Errorf("%w: changed document policy", ErrInvalid)
	}
	value := append(json.RawMessage(nil), doc.Value...)
	if doc.Version == def.Version {
		return value, nil
	}
	if doc.Version > def.Version || def.Migrate == nil {
		return nil, fmt.Errorf("%w: document %s version %d", ErrBlocked, def.Kind, doc.Version)
	}
	migrated, err := def.Migrate(doc.Version, value)
	if err != nil {
		return nil, err
	}
	if !json.Valid(migrated) {
		return nil, fmt.Errorf("%w: migration produced invalid JSON", ErrInvalid)
	}
	return migrated, nil
}

// Document explicitly creates absent state. ReadDocument does not.
func (tx *Tx) Document(def DocumentDefinition, id uint64) (json.RawMessage, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	if err := validateDocument(def); err != nil {
		return nil, err
	}
	key := documentKey(def, id)
	doc := tx.state.Documents[key]
	if doc == nil {
		var value any = map[string]any{}
		if def.Initial != nil {
			value = def.Initial()
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		if err := tx.PutDocument(def, id, data); err != nil {
			return nil, err
		}
		return append(json.RawMessage(nil), data...), nil
	}
	if doc.Scope != def.Scope || doc.History != normalizeHistory(def.History) || doc.Fork != normalizeFork(def.Scope, def.Fork) {
		return nil, fmt.Errorf("%w: changed document policy", ErrInvalid)
	}
	value, err := migratedDocument(def, doc)
	if err != nil {
		return nil, err
	}
	if doc.Version != def.Version {
		if err := tx.PutDocument(def, id, value); err != nil {
			return nil, err
		}
	}
	return value, nil
}
func normalizeHistory(s string) string {
	if s == "" {
		return "latest"
	}
	return s
}
func normalizeFork(scope, s string) string {
	if scope != "conversation" {
		return ""
	}
	if s == "" {
		return "initial"
	}
	return s
}
func (tx *Tx) PutDocument(def DocumentDefinition, id uint64, value json.RawMessage) error {
	if err := tx.check(); err != nil {
		return err
	}
	if err := validateDocument(def); err != nil {
		return err
	}
	if !json.Valid(value) {
		return fmt.Errorf("%w: document JSON", ErrInvalid)
	}
	switch def.Scope {
	case "session":
		id = 0
	case "conversation":
		if _, err := tx.Conversation(ConversationID(id)); err != nil {
			return err
		}
	case "task":
		t, err := tx.Task(TaskID(id))
		if err != nil {
			return err
		}
		if t.Status == "terminal" {
			return fmt.Errorf("%w: terminal task document", ErrInvalid)
		}
	}
	key := documentKey(def, id)
	doc := tx.state.Documents[key]
	if doc == nil {
		doc = &DocumentRecord{Kind: def.Kind, Version: def.Version, Scope: def.Scope, ScopeID: id, History: normalizeHistory(def.History), Fork: normalizeFork(def.Scope, def.Fork)}
		tx.state.Documents[key] = doc
	}
	if doc.Scope != def.Scope || doc.History != normalizeHistory(def.History) || doc.Fork != normalizeFork(def.Scope, def.Fork) {
		return fmt.Errorf("%w: changed document policy", ErrInvalid)
	}
	if doc.Version > def.Version {
		return ErrBlocked
	}
	doc.Value = append(json.RawMessage(nil), value...)
	doc.Version = def.Version
	if doc.History == "rewindable" {
		rev := DocumentRevision{Version: def.Version, Sequence: tx.state.Sequence + 1, Value: append(json.RawMessage(nil), value...)}
		if n := len(doc.Revisions); n > 0 && doc.Revisions[n-1].Sequence == rev.Sequence {
			doc.Revisions[n-1] = rev
		} else {
			doc.Revisions = append(doc.Revisions, rev)
		}
	}
	return nil
}

// UpdateDocument is a typed, transaction-scoped read/edit/write operation.
func UpdateDocument[T any](tx *Tx, def DocumentDefinition, id uint64, update func(*T) error) error {
	data, err := tx.Document(def, id)
	if err != nil {
		return err
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if err := update(&value); err != nil {
		return err
	}
	data, err = json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.PutDocument(def, id, data)
}

// ReadDocument returns ErrNotFound for absent documents and never creates state.
// Migrations are evaluated in memory; a later transactional access persists them.
func ReadDocument[T any](ctx context.Context, h *Harness, def DocumentDefinition, id uint64) (T, error) {
	var value T
	if err := validateDocument(def); err != nil {
		return value, err
	}
	s, err := h.Snapshot(ctx)
	if err != nil {
		return value, err
	}
	doc := s.Documents[documentKey(def, id)]
	if doc == nil {
		return value, ErrNotFound
	}
	data, err := migratedDocument(def, doc)
	if err != nil {
		return value, err
	}
	err = json.Unmarshal(data, &value)
	return value, err
}

// WatchDocument delivers detached latest values; nil means absent/retired state.
// It is convergent observation, not an audit journal.
func WatchDocument(ctx context.Context, h *Harness, def DocumentDefinition, id uint64) (<-chan json.RawMessage, func(), error) {
	if err := validateDocument(def); err != nil {
		return nil, nil, err
	}
	states, stop, err := h.session.Watch(ctx)
	if err != nil {
		return nil, nil, err
	}
	out := make(chan json.RawMessage, 1)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(out)
		defer stop()
		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-states:
				if !ok {
					return
				}
				var value json.RawMessage
				if doc := s.Documents[documentKey(def, id)]; doc != nil {
					value = append(json.RawMessage(nil), doc.Value...)
				}
				select {
				case out <- value:
				default:
					select {
					case <-out:
					default:
					}
					select {
					case out <- value:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, func() { cancel(); stop() }, nil
}
