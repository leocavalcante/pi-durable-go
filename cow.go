package durable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"sort"
)

// Copy-on-write transactions (plan 0006 M1). The committed State is never
// mutated: a transaction works on a shallow candidate whose maps hold the
// committed record pointers, and a record is cloned into the candidate on its
// first mutable access. Read a record from the maps; write it only through a
// mut* accessor (or a Tx accessor, which uses them).

// shallowState returns a candidate for a transaction over base: new maps
// sharing base's records.
func shallowState(base *State) *State {
	out := *base
	out.Conversations = cloneMap(base.Conversations)
	out.Entries = cloneMap(base.Entries)
	out.Tasks = cloneMap(base.Tasks)
	out.Submissions = cloneMap(base.Submissions)
	out.Documents = cloneMap(base.Documents)
	out.Revisions = cloneMap(base.Revisions)
	out.base = base
	return &out
}

func cloneMap[K comparable, V any](m map[K]*V) map[K]*V {
	if m == nil {
		return map[K]*V{}
	}
	return maps.Clone(m)
}

// cloneRecord deep-copies a record through JSON. Committed records are
// always valid JSON, so a failure is an invariant violation.
func cloneRecord[V any](v *V) *V {
	out, err := strictClone(v)
	if err != nil {
		panic(fmt.Sprintf("durable: committed record is not JSON: %v", err))
	}
	return out
}

// strictClone round-trips a record through strict JSON: it validates the
// record as cloneState validates a State, and detaches it from every value a
// caller still holds.
func strictClone[V any](v *V) (*V, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("durable: state is not strict JSON: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	out := new(V)
	if err := dec.Decode(out); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("durable: trailing JSON data")
	}
	return out, nil
}

// mutable returns the candidate's record at key, cloning a committed one into
// the candidate first. Without a base (a standalone State), it is the record.
func mutable[K comparable, V any](candidate, base map[K]*V, key K) *V {
	v := candidate[key]
	if v == nil || base == nil || base[key] != v {
		return v
	}
	c := cloneRecord(v)
	candidate[key] = c
	return c
}

func (s *State) mutConversation(id ConversationID) *ConversationRecord {
	if s.base == nil {
		return s.Conversations[id]
	}
	return mutable(s.Conversations, s.base.Conversations, id)
}

// mutStoredConversation is mutConversation that first loads into the
// candidate a conversation record a bounded Session does not keep.
func (s *State) mutStoredConversation(id ConversationID) (*ConversationRecord, error) {
	if s.Conversations[id] == nil && s.cold != nil && s.base != nil && id != 0 {
		stored, err := s.cold.conversation(id)
		if err != nil {
			return nil, err
		}
		if stored != nil {
			if s.preConversations == nil {
				s.preConversations = map[ConversationID]*ConversationRecord{}
			}
			if _, noted := s.preConversations[id]; !noted {
				s.preConversations[id] = stored
			}
			s.Conversations[id] = cloneRecord(stored)
			return s.Conversations[id], nil
		}
	}
	return s.mutConversation(id), nil
}

func (s *State) mutEntry(id EntryID) *Entry {
	if s.base == nil {
		return s.Entries[id]
	}
	return mutable(s.Entries, s.base.Entries, id)
}

func (s *State) mutTask(id TaskID) *TaskRecord {
	if s.base == nil {
		return s.Tasks[id]
	}
	return mutable(s.Tasks, s.base.Tasks, id)
}

func (s *State) mutSubmission(id SubmissionID) *SubmissionRecord {
	if s.base == nil {
		return s.Submissions[id]
	}
	return mutable(s.Submissions, s.base.Submissions, id)
}

func (s *State) mutDocument(key string) *DocumentRecord {
	if s.base == nil {
		return s.Documents[key]
	}
	s.noteRead(key)
	return mutable(s.Documents, s.base.Documents, key)
}

// mutStoredDocument is mutDocument that first loads into the candidate a
// document a bounded Session does not keep, so the commit changes the
// stored record.
func (s *State) mutStoredDocument(key string) (*DocumentRecord, error) {
	if s.Documents[key] == nil && s.cold != nil && s.base != nil && !s.removedDocuments[key] {
		stored, err := s.cold.document(key)
		if err != nil {
			return nil, err
		}
		s.notePreDocument(key, stored)
		if stored != nil {
			s.Documents[key] = cloneRecord(stored)
			return s.Documents[key], nil
		}
	}
	return s.mutDocument(key), nil
}

// notePreDocument remembers a stored document's value before the
// transaction (nil: absent), for reads while its commit is stored but not
// published.
func (s *State) notePreDocument(key string, stored *DocumentRecord) {
	if s.preDocuments == nil {
		s.preDocuments = map[string]*DocumentRecord{}
	}
	if _, noted := s.preDocuments[key]; !noted {
		s.preDocuments[key] = stored
	}
}

// detachChanges validates and detaches every record the candidate added or
// cloned, collects them with the removed keys as the commit's delta, and
// reports whether the candidate differs from base at all. With compare (a
// read-only Harness), a clone whose JSON equals the committed record is no
// change.
func detachChanges(base, candidate *State, compare bool) (bool, *StateDelta, error) {
	delta := &StateDelta{Version: candidate.Version, NextID: candidate.NextID, Root: candidate.Root}
	changed := base.Version != candidate.Version || base.NextID != candidate.NextID || base.Root != candidate.Root
	var err error
	var c bool
	if delta.Conversations, c, err = detachTable(base.Conversations, candidate.Conversations, compare); err != nil {
		return false, nil, err
	}
	changed = changed || c
	if delta.Entries, c, err = detachTable(base.Entries, candidate.Entries, compare); err != nil {
		return false, nil, err
	}
	changed = changed || c
	if delta.Tasks, c, err = detachTable(base.Tasks, candidate.Tasks, compare); err != nil {
		return false, nil, err
	}
	changed = changed || c
	if delta.Submissions, c, err = detachTable(base.Submissions, candidate.Submissions, compare); err != nil {
		return false, nil, err
	}
	changed = changed || c
	if delta.Documents, c, err = detachTable(base.Documents, candidate.Documents, compare); err != nil {
		return false, nil, err
	}
	changed = changed || c
	if delta.Revisions, c, err = detachTable(base.Revisions, candidate.Revisions, compare); err != nil {
		return false, nil, err
	}
	changed = changed || c
	delta.Removed = RemovedKeys{
		Conversations: removedKeys(base.Conversations, candidate.Conversations),
		Entries:       removedKeys(base.Entries, candidate.Entries),
		Tasks:         removedKeys(base.Tasks, candidate.Tasks),
		Submissions:   removedKeys(base.Submissions, candidate.Submissions),
		Documents:     removedKeys(base.Documents, candidate.Documents),
	}
	// Documents a bounded Session retired without holding them.
	if len(candidate.removedDocuments) > 0 {
		for key := range candidate.removedDocuments {
			if candidate.Documents[key] == nil {
				delta.Removed.Documents = append(delta.Removed.Documents, key)
			}
		}
		sort.Strings(delta.Removed.Documents)
		candidate.removedDocuments = nil
	}
	changed = changed || len(delta.Removed.Conversations)+len(delta.Removed.Entries)+len(delta.Removed.Tasks)+
		len(delta.Removed.Submissions)+len(delta.Removed.Documents) > 0
	return changed, delta, nil
}

// detachTable detaches the table's added or cloned records and returns them.
func detachTable[K comparable, V any](base, candidate map[K]*V, compare bool) (map[K]*V, bool, error) {
	var touched map[K]*V
	changed := false
	for key, v := range candidate {
		committed, existed := base[key]
		if existed && committed == v {
			continue
		}
		if v == nil {
			return nil, false, fmt.Errorf("%w: nil record", ErrInvalid)
		}
		detached, err := strictClone(v)
		if err != nil {
			return nil, false, err
		}
		candidate[key] = detached
		if touched == nil {
			touched = map[K]*V{}
		}
		touched[key] = detached
		if changed {
			continue
		}
		if !compare || !existed {
			changed = true
			continue
		}
		before, err := json.Marshal(committed)
		if err != nil {
			return nil, false, err
		}
		after, err := json.Marshal(detached)
		if err != nil {
			return nil, false, err
		}
		changed = !bytes.Equal(before, after)
	}
	return touched, changed, nil
}

// Detached copies of shared records, for values that leave a read of the
// committed State. Strings are immutable and stay shared; slices, raw JSON,
// and pointers are copied.

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage{}, raw...)
}

func (m Message) detached() Message {
	out := m
	if m.ToolCalls != nil {
		out.ToolCalls = make([]ToolCall, len(m.ToolCalls))
		for i, call := range m.ToolCalls {
			call.Arguments = cloneRaw(call.Arguments)
			out.ToolCalls[i] = call
		}
	}
	if m.DurationMs != nil {
		d := *m.DurationMs
		out.DurationMs = &d
	}
	out.ProviderData = cloneRaw(m.ProviderData)
	if m.Images != nil {
		out.Images = append([]ImageContent{}, m.Images...)
	}
	if m.ImageOffsets != nil {
		out.ImageOffsets = append([]int{}, m.ImageOffsets...)
	}
	if m.Sections != nil {
		out.Sections = make([]SystemSection, len(m.Sections))
		for i, section := range m.Sections {
			if section.Text != nil {
				text := *section.Text
				section.Text = &text
			}
			out.Sections[i] = section
		}
	}
	if m.ToolsAdded != nil {
		out.ToolsAdded = make([]ToolSpec, len(m.ToolsAdded))
		for i, tool := range m.ToolsAdded {
			tool.Parameters = cloneRaw(tool.Parameters)
			tool.ConstrainedSampling = cloneRaw(tool.ConstrainedSampling)
			out.ToolsAdded[i] = tool
		}
	}
	if m.ToolsRemoved != nil {
		out.ToolsRemoved = append([]string{}, m.ToolsRemoved...)
	}
	return out
}

func (e Entry) detached() Entry {
	out := e
	if e.Message != nil {
		m := e.Message.detached()
		out.Message = &m
	}
	out.Data = cloneRaw(e.Data)
	if e.Edits != nil {
		out.Edits = make([]ContextEdit, len(e.Edits))
		for i, edit := range e.Edits {
			messages := make([]Message, len(edit.Messages))
			for j, m := range edit.Messages {
				messages[j] = m.detached()
			}
			if edit.Messages == nil {
				messages = nil
			}
			edit.Messages = messages
			out.Edits[i] = edit
		}
	}
	return out
}
