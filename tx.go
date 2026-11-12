package durable

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Tx is valid only during its Commit callback. All retained values are detached
// before publication. Never call a model, network service, or another Commit
// while holding a Tx: commits are serialized and nested commits deadlock.
type Tx struct {
	state   *State
	active  bool
	now     func() time.Time
	created func(*Tx, ConversationID) error
	// trace injects the telemetry carrier of the commit's span, for records
	// that inherit it; nil without propagation.
	trace func() json.RawMessage
}

func (tx *Tx) check() error {
	if !tx.active {
		return ErrClosed
	}
	return nil
}
func (tx *Tx) clock() time.Time {
	if tx.now != nil {
		return tx.now().UTC()
	}
	return time.Now().UTC()
}
func (tx *Tx) id() uint64 { n := tx.state.NextID; tx.state.NextID++; return n }
func (tx *Tx) Conversation(id ConversationID) (*ConversationRecord, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	c := tx.state.mutConversation(id)
	if c == nil {
		return nil, ErrNotFound
	}
	return c, nil
}
func (tx *Tx) Entry(id EntryID) (*Entry, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	e := tx.state.mutEntry(id)
	if e == nil {
		// An older entry a bounded Session no longer keeps: a detached
		// copy, as entries do not change once appended.
		cold, err := tx.state.entry(id)
		if err != nil {
			return nil, err
		}
		if cold == nil {
			return nil, ErrNotFound
		}
		copy := cold.detached()
		return &copy, nil
	}
	return e, nil
}
func (tx *Tx) Task(id TaskID) (*TaskRecord, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	t := tx.state.mutTask(id)
	if t == nil {
		// An ended task a bounded Session no longer keeps: a detached copy,
		// as a terminal task never changes.
		cold, err := tx.state.task(id)
		if err != nil {
			return nil, err
		}
		if cold == nil {
			return nil, ErrNotFound
		}
		return cloneRecord(cold), nil
	}
	return t, nil
}
func (tx *Tx) Submission(id SubmissionID) (*SubmissionRecord, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	s := tx.state.mutSubmission(id)
	if s == nil {
		// A settled submission a bounded Session no longer keeps: a
		// detached copy, as a settled submission never changes.
		cold, err := tx.state.submission(id)
		if err != nil {
			return nil, err
		}
		if cold == nil {
			return nil, ErrNotFound
		}
		return cloneRecord(cold), nil
	}
	return s, nil
}
func (tx *Tx) CreateConversation(opts ConversationOptions) (ConversationID, error) {
	if err := tx.check(); err != nil {
		return 0, err
	}
	agent := opts.Agent
	if opts.Owner.Task != 0 {
		task, err := tx.Task(opts.Owner.Task)
		if err != nil {
			return 0, err
		}
		if task.Status == "terminal" || task.AbortRequested {
			return 0, fmt.Errorf("%w: owner cannot accept children", ErrInvalid)
		}
		// Child agent choices are a one-time copy, not a live parent reference.
		parent, err := tx.Conversation(task.Conversation)
		if err != nil {
			return 0, err
		}
		if agent.Model.ID == "" && agent.Model.Provider == "" && agent.Extensions == nil && agent.ExtensionEdits == nil && agent.Tools == nil && agent.RemoveTools == nil && agent.Instructions == nil && agent.Cwd == "" && agent.ThinkingLevel == "" {
			agent = parent.Agent
		}
	}
	sessionID, err := newSessionID(tx.clock())
	if err != nil {
		return 0, err
	}
	id := ConversationID(tx.id())
	tx.state.Conversations[id] = &ConversationRecord{ID: id, Owner: opts.Owner, Agent: agent, ProviderSessionID: sessionID, CreatedAt: tx.clock()}
	data, err := json.Marshal(agent)
	if err != nil {
		return 0, err
	}
	if _, err := tx.Append(id, Entry{Kind: "pi.agent", Data: data}); err != nil {
		return 0, err
	}
	if tx.created != nil {
		if err := tx.created(tx, id); err != nil {
			return 0, err
		}
	}
	if opts.Init != nil {
		if err := opts.Init(tx, id); err != nil {
			return 0, err
		}
	}
	return id, nil
}
func (tx *Tx) Append(cid ConversationID, e Entry) (EntryID, error) {
	c, err := tx.Conversation(cid)
	if err != nil {
		return 0, err
	}
	if e.Kind == "" {
		return 0, fmt.Errorf("%w: entry kind required", ErrInvalid)
	}
	id := EntryID(tx.id())
	e.ID = id
	e.Conversation = cid
	e.Parent = c.Head
	e.Sequence = tx.state.Sequence + 1
	e.CreatedAt = tx.clock()
	tx.state.Entries[id] = &e
	c.Head = id
	return id, nil
}
func (tx *Tx) CreateTask(cid ConversationID, def TaskDefinition, input json.RawMessage, opts TaskOptions) (TaskID, error) {
	if _, err := tx.Conversation(cid); err != nil {
		return 0, err
	}
	if def.Kind == "" || def.Version < 1 || def.Run == nil {
		return 0, fmt.Errorf("%w: task definition", ErrInvalid)
	}
	if opts.Background && opts.Owner.Task != 0 {
		return 0, fmt.Errorf("%w: background tasks must be conversation-owned", ErrInvalid)
	}
	if opts.Owner.Task != 0 {
		owner, err := tx.Task(opts.Owner.Task)
		if err != nil {
			return 0, err
		}
		if owner.Conversation != cid || owner.Status == "terminal" || owner.AbortRequested {
			return 0, fmt.Errorf("%w: task owner", ErrInvalid)
		}
	}
	if len(input) == 0 {
		input = json.RawMessage(`null`)
	}
	if !json.Valid(input) {
		return 0, fmt.Errorf("%w: task input JSON", ErrInvalid)
	}
	checkpoint := json.RawMessage(`{}`)
	if def.Initial != nil {
		var err error
		checkpoint, err = def.Initial(input)
		if err != nil {
			return 0, err
		}
	}
	if !json.Valid(checkpoint) {
		return 0, fmt.Errorf("%w: checkpoint JSON", ErrInvalid)
	}
	trace := opts.Trace
	if len(trace) == 0 && !opts.TraceRoot && tx.trace != nil {
		trace = tx.trace() // inherit the creator's span
	}
	var links []json.RawMessage
	for _, link := range opts.Links {
		links = append(links, append(json.RawMessage(nil), link...))
	}
	id := TaskID(tx.id())
	tx.state.Tasks[id] = &TaskRecord{ID: id, Conversation: cid, Kind: def.Kind, Version: def.Version, Owner: opts.Owner, Background: opts.Background, AbandonOnRestart: opts.AbandonOnRestart, Status: "pending", Input: append(json.RawMessage(nil), input...), Checkpoint: append(json.RawMessage(nil), checkpoint...),
		Trace: append(json.RawMessage(nil), trace...), Links: links}
	return id, nil
}

func newSessionID(now time.Time) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	ms := uint64(now.UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	b[6] = (b[6] & 15) | 0x70
	b[8] = (b[8] & 63) | 0x80
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:], nil
}

// ConversationsByOwner finds a task's existing children before creating new
// ones. Replay-safe delegation uses this with a deterministic submission key.
func (tx *Tx) ConversationsByOwner(owner TaskID) ([]ConversationRecord, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	var result []ConversationRecord
	for _, c := range tx.state.Conversations {
		if c.Owner.Task == owner {
			// Detached: the record may be the committed one.
			result = append(result, *cloneRecord(c))
		}
	}
	return result, nil
}

// Entries returns the active transcript, including inherited entries. It detects
// corrupt parent cycles rather than hanging on malformed persisted state.
