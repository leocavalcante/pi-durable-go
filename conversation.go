package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Conversation is an ID-based handle, not an independently running process.
type Conversation struct {
	h  *Harness
	id ConversationID
}

func (c *Conversation) ID() ConversationID { return c.id }
func (c *Conversation) Harness() *Harness  { return c.h }
func (h *Harness) Root(ctx context.Context, opts ConversationOptions) (*Conversation, error) {
	var id ConversationID
	err := h.Commit(ctx, func(tx *Tx) error {
		id = tx.state.Root
		if id != 0 {
			return nil
		}
		var err error
		id, err = tx.CreateConversation(opts)
		if err != nil {
			return err
		}
		tx.state.Root = id
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Conversation{h: h, id: id}, nil
}
func (h *Harness) CreateConversation(ctx context.Context, opts ConversationOptions) (*Conversation, error) {
	var id ConversationID
	err := h.Commit(ctx, func(tx *Tx) error { var err error; id, err = tx.CreateConversation(opts); return err })
	if err != nil {
		return nil, err
	}
	return &Conversation{h: h, id: id}, nil
}
func (h *Harness) Conversation(ctx context.Context, id ConversationID) (*Conversation, error) {
	s, err := h.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if s.Conversations[id] == nil {
		return nil, ErrNotFound
	}
	return &Conversation{h: h, id: id}, nil
}
func (c *Conversation) Configure(ctx context.Context, agent Agent) error {
	return c.h.Commit(ctx, func(tx *Tx) error {
		rec, err := tx.Conversation(c.id)
		if err != nil {
			return err
		}
		data, err := json.Marshal(agent)
		if err != nil {
			return err
		}
		rec.Agent = agent
		_, err = tx.Append(c.id, Entry{Kind: "pi.agent", Data: data})
		return err
	})
}
func (c *Conversation) Agent(ctx context.Context) (Agent, error) {
	s, err := c.h.Snapshot(ctx)
	if err != nil {
		return Agent{}, err
	}
	rec := s.Conversations[c.id]
	if rec == nil {
		return Agent{}, ErrNotFound
	}
	return rec.Agent, nil
}
func (c *Conversation) Entries(ctx context.Context) ([]Entry, error) {
	s, err := c.h.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return entriesFromState(s, c.id)
}
func (c *Conversation) Context(ctx context.Context) ([]Message, error) {
	s, err := c.h.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return contextFromState(s, c.id)
}
func contextFromState(s *State, cid ConversationID) ([]Message, error) {
	entries, err := entriesFromState(s, cid)
	if err != nil {
		return nil, err
	}
	var messages []Message
	for _, e := range entries {
		switch e.Kind {
		case "pi.reset":
			messages = nil
			if e.Message != nil && e.Message.Content != "" {
				messages = append(messages, *e.Message)
			}
		case "pi.compaction":
			var comp struct {
				Summary string  `json:"summary"`
				Cut     EntryID `json:"cut"`
			}
			if err := json.Unmarshal(e.Data, &comp); err != nil {
				return nil, err
			}
			// Reconstruct everything after the cut, including messages that arrived
			// while the summary was generated. A summary never deletes stored history.
			var next []Message
			next = append(next, Message{Role: "user", Content: "Conversation summary:\n" + comp.Summary})
			seen := false
			for _, older := range entries {
				if older.ID == comp.Cut {
					seen = true
					continue
				}
				if older.ID == e.ID {
					break
				}
				if seen && older.Message != nil && !older.Message.Aborted {
					next = append(next, *older.Message)
				}
			}
			if !seen {
				return nil, fmt.Errorf("%w: compaction cut", ErrInvalid)
			}
			messages = next
		default:
			if e.Message != nil && !e.Message.Aborted {
				messages = append(messages, *e.Message)
			}
		}
	}
	return messages, nil
}
func (c *Conversation) Fork(ctx context.Context, at EntryID, opts ConversationOptions) (*Conversation, error) {
	var id ConversationID
	err := c.h.Commit(ctx, func(tx *Tx) error {
		parent, err := tx.Conversation(c.id)
		if err != nil {
			return err
		}
		entries, err := entriesFromState(tx.state, c.id)
		if err != nil {
			return err
		}
		var found *Entry
		agent := parent.Agent
		for _, e := range entries {
			if e.Kind == "pi.agent" {
				agent = Agent{}
				if err := json.Unmarshal(e.Data, &agent); err != nil {
					return err
				}
			}
			if e.ID == at {
				copy := e
				found = &copy
				break
			}
		}
		if found == nil {
			return fmt.Errorf("%w: fork entry not visible", ErrInvalid)
		}
		// A zero agent means inherit the parent's choices at this entry.
		if opts.Agent.Model.ID != "" || opts.Agent.Model.Provider != "" || opts.Agent.Tools != nil || opts.Agent.Extensions != nil || opts.Agent.Instructions != "" || opts.Agent.Cwd != "" || opts.Agent.ThinkingLevel != "" {
			agent = opts.Agent
		}
		create := ConversationOptions{Owner: opts.Owner, Agent: agent}
		creationHook := tx.created
		tx.created = nil
		id, err = tx.CreateConversation(create)
		tx.created = creationHook
		if err != nil {
			return err
		}
		child := tx.state.Conversations[id]
		delete(tx.state.Entries, child.Head) // replace the unobserved creation baseline
		child.Agent = agent
		child.Parent = c.id
		child.ForkAt = at
		child.Head = at
		baseline, err := json.Marshal(agent)
		if err != nil {
			return err
		}
		if _, err := tx.Append(id, Entry{Kind: "pi.agent", Data: baseline}); err != nil {
			return err
		}
		// Copy only conversation documents; task/session documents never fork.
		var docs []*DocumentRecord
		for _, doc := range tx.state.Documents {
			if doc.Scope == "conversation" && doc.ScopeID == uint64(c.id) {
				docs = append(docs, doc)
			}
		}
		for _, doc := range docs {
			if doc.Kind == "pi.live" || doc.Kind == "pi.inbox" || doc.Kind == "pi.usage" || doc.Kind == "pi.provider" || doc.Kind == "pi.agent" {
				continue
			}
			def := DocumentDefinition{Kind: doc.Kind, Version: doc.Version, Scope: "conversation", History: doc.History, Fork: doc.Fork}
			var value json.RawMessage
			switch doc.Fork {
			case "current":
				value = doc.Value
			case "asOf":
				for _, rev := range doc.Revisions {
					if rev.Sequence <= found.Sequence {
						value = rev.Value
						if rev.Version > 0 {
							def.Version = rev.Version
						}
					}
				}
			default:
				continue
			}
			if value != nil {
				if err := tx.PutDocument(def, uint64(id), value); err != nil {
					return err
				}
			}
		}
		if creationHook != nil {
			if err := creationHook(tx, id); err != nil {
				return err
			}
		}
		if opts.Init != nil {
			return opts.Init(tx, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Conversation{h: c.h, id: id}, nil
}

// Submission is durable admission. Cancelling Wait does not cancel its work.
type Submission struct {
	h  *Harness
	id SubmissionID
}

func (s *Submission) ID() SubmissionID { return s.id }
func (h *Harness) Submission(ctx context.Context, id SubmissionID) (*Submission, error) {
	state, err := h.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	if state.Submissions[id] == nil {
		return nil, ErrNotFound
	}
	return &Submission{h: h, id: id}, nil
}
func (c *Conversation) Submit(ctx context.Context, request SubmissionRequest) (*Submission, error) {
	if request.Type == "" {
		request.Type = "input"
	}
	if request.Type != "input" && request.Type != "write" {
		return nil, fmt.Errorf("%w: submission type", ErrInvalid)
	}
	if request.WhenBusy != "" && request.WhenBusy != "follow-up" && request.WhenBusy != "steer" && request.WhenBusy != "reject" {
		return nil, fmt.Errorf("%w: busy mode", ErrInvalid)
	}
	if request.Type == "write" && (request.Entry == nil || request.Entry.Kind == "") {
		return nil, fmt.Errorf("%w: write requires entry", ErrInvalid)
	}
	if request.Type == "input" && request.Entry != nil {
		entry := request.Entry
		if entry.Kind == "" || strings.HasPrefix(entry.Kind, "pi.") || entry.Message != nil || entry.ID != 0 || entry.Conversation != 0 || entry.Parent != 0 || entry.Sequence != 0 || !entry.CreatedAt.IsZero() {
			return nil, fmt.Errorf("%w: input entry must contain only a non-reserved kind and data", ErrInvalid)
		}
		if len(entry.Data) > 0 && !json.Valid(entry.Data) {
			return nil, fmt.Errorf("%w: input entry data must be JSON", ErrInvalid)
		}
	}
	var id SubmissionID
	err := c.h.Commit(ctx, func(tx *Tx) error {
		rec, err := tx.Conversation(c.id)
		if err != nil {
			return err
		}
		if request.RequestID != "" {
			for _, existing := range tx.state.Submissions {
				if existing.Conversation == c.id && existing.Request.RequestID == request.RequestID {
					if existing.Request.Type != request.Type {
						return fmt.Errorf("%w: requestId reused for another type", ErrInvalid)
					}
					id = existing.ID
					return nil
				}
			}
		}
		if rec.Busy != 0 && request.WhenBusy == "reject" {
			return ErrBusy
		}
		id = SubmissionID(tx.id())
		tx.state.Submissions[id] = &SubmissionRecord{ID: id, Conversation: c.id, Request: request, Status: "queued", CreatedAt: tx.clock()}
		if rec.Busy == 0 {
			return advanceInbox(tx, c.id)
		}
		if isDisplayOnlyWrite(request) && !queuedResetExists(tx, c.id) {
			return placeWrite(tx, c.id, tx.state.Submissions[id])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	c.h.Resume()
	return &Submission{h: c.h, id: id}, nil
}
func (s *Submission) Record(ctx context.Context) (SubmissionRecord, error) {
	state, err := s.h.Snapshot(ctx)
	if err != nil {
		return SubmissionRecord{}, err
	}
	rec := state.Submissions[s.id]
	if rec == nil {
		return SubmissionRecord{}, ErrNotFound
	}
	return *rec, nil
}
func (s *Submission) Wait(ctx context.Context) (SubmissionRecord, error) {
	states, stop, err := s.h.session.Watch(ctx)
	if err != nil {
		return SubmissionRecord{}, err
	}
	defer stop()
	s.h.Resume()
	for {
		select {
		case <-ctx.Done():
			return SubmissionRecord{}, ctx.Err()
		case state, ok := <-states:
			if !ok {
				if ctx.Err() != nil {
					return SubmissionRecord{}, ctx.Err()
				}
				return SubmissionRecord{}, ErrClosed
			}
			rec := state.Submissions[s.id]
			if rec == nil {
				return SubmissionRecord{}, ErrNotFound
			}
			if rec.Status == "done" || rec.Status == "unanswered" {
				return *rec, nil
			}
		}
	}
}
func (s *Submission) Abort(ctx context.Context) error {
	return s.h.Commit(ctx, func(tx *Tx) error {
		rec, err := tx.Submission(s.id)
		if err != nil {
			return err
		}
		if rec.Status != "queued" {
			return fmt.Errorf("%w: only queued submissions can be withdrawn", ErrInvalid)
		}
		rec.Status = "unanswered"
		rec.Reason = "aborted"
		return nil
	})
}
func (c *Conversation) Reset(ctx context.Context, note string) (*Submission, error) {
	var msg *Message
	if note != "" {
		msg = &Message{Role: "user", Content: note}
	}
	return c.Submit(ctx, SubmissionRequest{Type: "write", Entry: &Entry{Kind: "pi.reset", Message: msg}})
}
func (c *Conversation) Abort(ctx context.Context, includeBackground bool) error {
	state, err := c.h.Snapshot(ctx)
	if err != nil {
		return err
	}
	var ids []TaskID
	for id, t := range state.Tasks {
		if t.Conversation == c.id && t.Owner.Task == 0 && t.Status != "terminal" && (includeBackground || !t.Background) {
			ids = append(ids, id)
		}
	}
	if err := c.h.Commit(ctx, func(tx *Tx) error {
		for _, sub := range tx.state.Submissions {
			if sub.Conversation == c.id && sub.Status == "queued" && sub.Request.Type == "input" {
				sub.Status = "unanswered"
				sub.Reason = "aborted"
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, id := range ids {
		if err := c.h.AbortTask(ctx, id); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if _, err := c.h.WaitTask(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
func viewFromState(state *State, id ConversationID) (View, error) {
	rec := state.Conversations[id]
	if rec == nil {
		return View{}, ErrNotFound
	}
	entries, err := entriesFromState(state, id)
	if err != nil {
		return View{}, err
	}
	view := View{Sequence: state.Sequence, Conversation: *rec, Entries: entries, Documents: map[string]json.RawMessage{}}
	for _, t := range state.Tasks {
		if t.Conversation == id && t.Status != "terminal" {
			view.Tasks = append(view.Tasks, *t)
		}
	}
	for _, s := range state.Submissions {
		if s.Conversation == id && (s.Status == "queued" || s.Status == "running") {
			view.Inbox = append(view.Inbox, *s)
		}
	}
	for _, d := range state.Documents {
		if d.Scope == "conversation" && d.ScopeID == uint64(id) {
			view.Documents[d.Kind] = d.Value
		}
	}
	sort.Slice(view.Tasks, func(i, j int) bool { return view.Tasks[i].ID < view.Tasks[j].ID })
	sort.Slice(view.Inbox, func(i, j int) bool { return view.Inbox[i].ID < view.Inbox[j].ID })
	return view, nil
}
func (c *Conversation) View(ctx context.Context) (View, error) {
	s, err := c.h.Snapshot(ctx)
	if err != nil {
		return View{}, err
	}
	return viewFromState(s, c.id)
}
func (c *Conversation) Watch(ctx context.Context) (<-chan View, func(), error) {
	states, stop, err := c.h.session.Watch(ctx)
	if err != nil {
		return nil, nil, err
	}
	out := make(chan View, 1)
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
				view, err := viewFromState(s, c.id)
				if err != nil {
					return
				}
				select {
				case out <- view:
				default:
					select {
					case <-out:
					default:
					}
					select {
					case out <- view:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, func() { cancel(); stop() }, nil
}
