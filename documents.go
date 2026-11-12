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

// memberKey is the storage key of a family member.
func memberKey(def DocumentDefinition, id uint64, member string) string {
	return documentKey(def, id) + "/" + member
}

// keyOf is a document's storage key: a singleton's, or a member's.
func keyOf(def DocumentDefinition, id uint64, member string) (string, error) {
	switch {
	case def.Family && member == "":
		return "", fmt.Errorf("%w: a family document needs a member key", ErrInvalid)
	case !def.Family && member != "":
		return "", fmt.Errorf("%w: %s is not a document family", ErrInvalid, def.Kind)
	case def.Family:
		return memberKey(def, id, member), nil
	}
	return documentKey(def, id), nil
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
	if _, err := keyOf(def, id, ""); err != nil {
		return nil, err
	}
	key := documentKey(def, id)
	doc, err := tx.state.document(key)
	if err != nil {
		return nil, err
	}
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
	return tx.putDocument(def, id, "", value)
}

// PutFamilyDocument writes a member of a document family.
func (tx *Tx) PutFamilyDocument(def DocumentDefinition, id uint64, member string, value json.RawMessage) error {
	return tx.putDocument(def, id, member, value)
}

// FamilyDocument reads a member of a document family, creating it from seed
// with InitialSeed when it is absent (upstream tx.doc(family, scope, key,
// seed)).
func (tx *Tx) FamilyDocument(def DocumentDefinition, id uint64, member string, seed json.RawMessage) (json.RawMessage, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	if err := validateDocument(def); err != nil {
		return nil, err
	}
	key, err := keyOf(def, id, member)
	if err != nil {
		return nil, err
	}
	if def.Scope == "session" {
		id = 0
	}
	doc, err := tx.state.document(key)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		var value any = map[string]any{}
		switch {
		case def.InitialSeed != nil:
			value = def.InitialSeed(seed)
		case def.Initial != nil:
			value = def.Initial()
		}
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		if err := tx.putDocument(def, id, member, data); err != nil {
			return nil, err
		}
		return append(json.RawMessage(nil), data...), nil
	}
	value, err := migratedDocument(def, doc)
	if err != nil {
		return nil, err
	}
	if doc.Version != def.Version {
		if err := tx.putDocument(def, id, member, value); err != nil {
			return nil, err
		}
	}
	return value, nil
}

// RetireDocument removes a singleton document (upstream retireDoc); later
// reads find it absent.
func (tx *Tx) RetireDocument(def DocumentDefinition, id uint64) error {
	return tx.retireDocument(def, id, "")
}

// RetireFamilyDocument removes a member of a document family.
func (tx *Tx) RetireFamilyDocument(def DocumentDefinition, id uint64, member string) error {
	return tx.retireDocument(def, id, member)
}

func (tx *Tx) retireDocument(def DocumentDefinition, id uint64, member string) error {
	if err := tx.check(); err != nil {
		return err
	}
	if err := validateDocument(def); err != nil {
		return err
	}
	key, err := keyOf(def, id, member)
	if err != nil {
		return err
	}
	if doc, kept := tx.state.Documents[key]; kept || tx.state.cold == nil {
		if kept {
			tx.retired(key, doc)
		}
		delete(tx.state.Documents, key)
		return nil
	}
	// A document a bounded Session does not keep: the delta removes it.
	stored, err := tx.state.document(key)
	if err != nil || stored == nil {
		return err
	}
	tx.state.notePreDocument(key, stored)
	tx.retired(key, stored)
	if tx.state.removedDocuments == nil {
		tx.state.removedDocuments = map[string]bool{}
	}
	tx.state.removedDocuments[key] = true
	return nil
}

// retired records a rewindable document's retirement in its history: reads
// as of a later entry find it absent, earlier ones its value then.
func (tx *Tx) retired(key string, doc *DocumentRecord) {
	if doc.History != "rewindable" {
		return
	}
	sequence := tx.state.Sequence + 1
	if tx.state.Revisions == nil {
		tx.state.Revisions = map[string]*DocumentRevision{}
	}
	tx.state.Revisions[revisionKey(key, sequence)] = &DocumentRevision{Version: doc.Version, Sequence: sequence, Retired: true, Fork: doc.Fork}
}

func (tx *Tx) putDocument(def DocumentDefinition, id uint64, member string, value json.RawMessage) error {
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
	key, err := keyOf(def, id, member)
	if err != nil {
		return err
	}
	doc, err := tx.state.mutStoredDocument(key)
	if err != nil {
		return err
	}
	if doc == nil {
		doc = &DocumentRecord{Kind: def.Kind, Version: def.Version, Scope: def.Scope, ScopeID: id, History: normalizeHistory(def.History), Fork: normalizeFork(def.Scope, def.Fork), Member: member}
		tx.state.Documents[key] = doc
	}
	delete(tx.state.removedDocuments, key)
	if doc.Scope != def.Scope || doc.History != normalizeHistory(def.History) || doc.Fork != normalizeFork(def.Scope, def.Fork) {
		return fmt.Errorf("%w: changed document policy", ErrInvalid)
	}
	if doc.Version > def.Version {
		return ErrBlocked
	}
	doc.Value = append(json.RawMessage(nil), value...)
	doc.Version = def.Version
	if doc.History == "rewindable" {
		// A revision of its own: the document's history lives apart from it.
		sequence := tx.state.Sequence + 1
		if tx.state.Revisions == nil {
			tx.state.Revisions = map[string]*DocumentRevision{}
		}
		tx.state.Revisions[revisionKey(key, sequence)] = &DocumentRevision{Version: def.Version, Sequence: sequence, Value: append(json.RawMessage(nil), value...), Fork: doc.Fork}
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
	if _, err := keyOf(def, id, ""); err != nil {
		return value, err
	}
	s, err := h.read(ctx)
	if err != nil {
		return value, err
	}
	doc, err := s.document(documentKey(def, id))
	if err != nil {
		return value, err
	}
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
// It is convergent observation, not an audit journal. A task document's
// watch closes once its task is terminal, after delivering the retirement.
func WatchDocument(ctx context.Context, h *Harness, def DocumentDefinition, id uint64) (<-chan json.RawMessage, func(), error) {
	if err := validateDocument(def); err != nil {
		return nil, nil, err
	}
	states, stop, err := h.session.watchShared(ctx)
	if err != nil {
		return nil, nil, err
	}
	out := make(chan json.RawMessage, 1)
	ctx, cancel := context.WithCancel(ctx)
	unpin := h.session.pinDocument(documentKey(def, id))
	go func() {
		defer close(out)
		defer stop()
		defer unpin()
		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-states:
				if !ok {
					return
				}
				var value json.RawMessage
				if doc, _ := s.document(documentKey(def, id)); doc != nil {
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
				// A task document's watch ends with its task, after the
				// retirement it delivered (upstream ended).
				if def.Scope == "task" {
					if task, _ := s.task(TaskID(id)); task == nil || task.Status == "terminal" {
						return
					}
				}
			}
		}
	}()
	return out, func() { cancel(); stop() }, nil
}

// ReadFamilyDocument reads a member of a document family from the committed
// state; ErrNotFound when it is absent.
func ReadFamilyDocument[T any](ctx context.Context, h *Harness, def DocumentDefinition, id uint64, member string) (T, error) {
	var value T
	if err := validateDocument(def); err != nil {
		return value, err
	}
	key, err := keyOf(def, id, member)
	if err != nil {
		return value, err
	}
	s, err := h.read(ctx)
	if err != nil {
		return value, err
	}
	doc, err := s.document(key)
	if err != nil {
		return value, err
	}
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

// ReadDocumentAsOf reads a rewindable conversation document as it was when
// entry at was committed (upstream snapshotAsOf): the document of the
// conversation that owns the entry, which must be visible from conversation
// id (its own or one its lineage forked from). ErrNotFound when the document
// had no value then.
func ReadDocumentAsOf[T any](ctx context.Context, h *Harness, def DocumentDefinition, id uint64, at EntryID) (T, error) {
	return readDocumentAsOf[T](ctx, h, def, id, "", at)
}

// ReadFamilyDocumentAsOf is ReadDocumentAsOf for a member of a rewindable
// conversation document family.
func ReadFamilyDocumentAsOf[T any](ctx context.Context, h *Harness, def DocumentDefinition, id uint64, member string, at EntryID) (T, error) {
	return readDocumentAsOf[T](ctx, h, def, id, member, at)
}

func readDocumentAsOf[T any](ctx context.Context, h *Harness, def DocumentDefinition, id uint64, member string, at EntryID) (T, error) {
	var value T
	if err := validateDocument(def); err != nil {
		return value, err
	}
	if def.Scope != "conversation" {
		return value, fmt.Errorf("%w: reading as of an entry requires a conversation document", ErrInvalid)
	}
	if normalizeHistory(def.History) != "rewindable" {
		return value, fmt.Errorf("%w: reading as of an entry requires rewindable history", ErrInvalid)
	}
	if _, err := keyOf(def, id, member); err != nil {
		return value, err
	}
	s, err := h.read(ctx)
	if err != nil {
		return value, err
	}
	entries, err := entriesFromState(s, ConversationID(id))
	if err != nil {
		return value, err
	}
	var entry *Entry
	for i := range entries {
		if entries[i].ID == at {
			entry = &entries[i]
			break
		}
	}
	if entry == nil {
		return value, fmt.Errorf("%w: entry %d is not visible from conversation %d", ErrInvalid, at, id)
	}
	key, err := keyOf(def, uint64(entry.Conversation), member)
	if err != nil {
		return value, err
	}
	// The document's history: a retired document reads as it was before.
	revision, err := revisionAt(s, key, entry.Sequence)
	if err != nil {
		return value, err
	}
	if revision == nil || revision.Retired {
		return value, ErrNotFound
	}
	then := DocumentRecord{Kind: def.Kind, Version: revision.Version, Scope: def.Scope, ScopeID: uint64(entry.Conversation),
		History: normalizeHistory(def.History), Fork: normalizeFork(def.Scope, def.Fork), Member: member, Value: revision.Value}
	if current, err := s.document(key); err != nil {
		return value, err
	} else if current != nil {
		// A changed policy is refused, as for the current value.
		then.History, then.Fork = current.History, current.Fork
	}
	data, err := migratedDocument(def, &then)
	if err != nil {
		return value, err
	}
	err = json.Unmarshal(data, &value)
	return value, err
}
