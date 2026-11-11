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
	s, err := h.read(ctx)
	if err != nil {
		return nil, err
	}
	if s.Conversations[id] == nil {
		return nil, ErrNotFound
	}
	return &Conversation{h: h, id: id}, nil
}

// Configure replaces the conversation's agent in its own commit.
func (c *Conversation) Configure(ctx context.Context, agent Agent) error {
	return c.h.Commit(ctx, func(tx *Tx) error { return tx.Configure(c.id, agent) })
}

// Configure replaces a conversation's agent inside the caller's commit
// (upstream configure(tx, conversationId, change)): read the current one
// with Tx.Conversation, change what should change, and configure it, so the
// change applies atomically with the commit's other writes. The agent is
// recorded as a pi.agent entry.
func (tx *Tx) Configure(cid ConversationID, agent Agent) error {
	if agent.MaxTurns < 0 {
		return fmt.Errorf("%w: MaxTurns must be 1 or more, or 0 for no bound", ErrInvalid)
	}
	rec, err := tx.Conversation(cid)
	if err != nil {
		return err
	}
	data, err := json.Marshal(agent)
	if err != nil {
		return err
	}
	rec.Agent = agent
	_, err = tx.Append(cid, Entry{Kind: "pi.agent", Data: data})
	return err
}
func (c *Conversation) Agent(ctx context.Context) (Agent, error) {
	s, err := c.h.read(ctx)
	if err != nil {
		return Agent{}, err
	}
	rec := s.Conversations[c.id]
	if rec == nil {
		return Agent{}, ErrNotFound
	}
	return *cloneRecord(&rec.Agent), nil
}
func (c *Conversation) Entries(ctx context.Context) ([]Entry, error) {
	s, err := c.h.read(ctx)
	if err != nil {
		return nil, err
	}
	return entriesFromState(s, c.id)
}
func (c *Conversation) Context(ctx context.Context) ([]Message, error) {
	s, err := c.h.read(ctx)
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
			summary, cut, err := compactionSummary(e)
			if err != nil {
				return nil, err
			}
			// Reconstruct everything after the cut, including messages that arrived
			// while the summary was generated. A summary never deletes stored history.
			next := []Message{summary}
			seen := false
			for _, older := range entries {
				if older.ID == cut {
					seen = true
					continue
				}
				if older.ID == e.ID {
					break
				}
				if seen && older.Message != nil && contributesContext(older.Message) {
					next = append(next, *older.Message)
				}
			}
			if !seen {
				return nil, fmt.Errorf("%w: compaction cut", ErrInvalid)
			}
			messages = next
		default:
			if e.Message != nil && contributesContext(e.Message) {
				messages = append(messages, *e.Message)
			}
		}
	}
	return orderToolResults(messages), nil
}

// orderToolResults normalizes each assistant's tool round independently. Result
// messages are moved next to their calls; malformed, duplicate, and orphaned
// results are omitted from the derived context without changing stored entries.
func orderToolResults(messages []Message) []Message {
	if len(messages) == 0 {
		return messages
	}

	ordered := make([]Message, 0, len(messages))
	for start := 0; start < len(messages); {
		if messages[start].Role != "assistant" {
			if messages[start].Role != "tool" {
				ordered = append(ordered, messages[start])
			}
			start++
			continue
		}

		end := start + 1
		for end < len(messages) && messages[end].Role != "assistant" {
			end++
		}

		assistant := messages[start]
		ordered = append(ordered, assistant)

		callIDs := make(map[string]struct{}, len(assistant.ToolCalls))
		for _, call := range assistant.ToolCalls {
			callIDs[call.ID] = struct{}{}
		}
		results := make(map[string]Message, len(callIDs))
		for i := start + 1; i < end; i++ {
			result := messages[i]
			if result.Role != "tool" {
				continue
			}
			if _, matches := callIDs[result.ToolCallID]; !matches {
				continue
			}
			if _, alreadyMatched := results[result.ToolCallID]; !alreadyMatched {
				results[result.ToolCallID] = result
			}
		}

		for _, call := range assistant.ToolCalls {
			if result, ok := results[call.ID]; ok {
				ordered = append(ordered, result)
				continue
			}
			ordered = append(ordered, Message{
				Role:       "tool",
				ToolCallID: call.ID,
				ToolName:   call.Name,
				IsError:    true,
				Content:    "Tool result unavailable: history ends before this call completed.",
			})
		}

		for i := start + 1; i < end; i++ {
			if messages[i].Role != "tool" {
				ordered = append(ordered, messages[i])
			}
		}
		start = end
	}
	return ordered
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
		child := tx.state.mutConversation(id)
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

// SubmitTx admits a submission inside the caller's commit, with Submit's
// rules, and returns its ID: the admission is stored with that commit, or
// not at all when the callback fails later or the commit fails. A rejection
// (ErrBusy, ErrIdle) returns before any write, so the callback may handle it.
// Call Resume after the commit for the run it starts to make progress.
func (c *Conversation) SubmitTx(tx *Tx, request SubmissionRequest) (SubmissionID, error) {
	if err := tx.check(); err != nil {
		return 0, err
	}
	if request.Type == "" {
		request.Type = "input"
	}
	if request.WhenBusy == "follow-up" {
		request.WhenBusy = "followUp"
	}
	if err := validateSubmission(request); err != nil {
		return 0, err
	}
	return c.h.admitSubmission(tx, c.id, request)
}

// Submission is durable admission. Cancelling Wait does not cancel its work.
type Submission struct {
	h  *Harness
	id SubmissionID
}

func (s *Submission) ID() SubmissionID { return s.id }
func (h *Harness) Submission(ctx context.Context, id SubmissionID) (*Submission, error) {
	state, err := h.read(ctx)
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
	if request.WhenBusy == "follow-up" {
		request.WhenBusy = "followUp" // the earlier Go spelling
	}
	if err := validateSubmission(request); err != nil {
		return nil, err
	}
	// Submitting asks for progress: the run it starts needs scheduling.
	c.h.Resume()
	ctx, sp := c.h.span(ctx, "pi.durable.submit", map[string]any{"submission.type": request.Type, "conversation.id": uint64(c.id)})
	var id SubmissionID
	err := c.h.Commit(ctx, func(tx *Tx) error {
		var err error
		id, err = c.h.admitSubmission(tx, c.id, request)
		return err
	})
	if err != nil {
		sp.end(err)
		return nil, err
	}
	if state, serr := c.h.read(ctx); serr == nil && state.Submissions[id] != nil {
		result := state.Submissions[id].Status
		if result == "unanswered" {
			result = "done"
		}
		sp.set(map[string]any{"submission.id": uint64(id), "result": result})
	}
	sp.end(nil)
	return &Submission{h: c.h, id: id}, nil
}
func (s *Submission) Record(ctx context.Context) (SubmissionRecord, error) {
	state, err := s.h.read(ctx)
	if err != nil {
		return SubmissionRecord{}, err
	}
	rec := state.Submissions[s.id]
	if rec == nil {
		return SubmissionRecord{}, ErrNotFound
	}
	return *cloneRecord(rec), nil
}
func (s *Submission) Wait(ctx context.Context) (SubmissionRecord, error) {
	states, stop, err := s.h.session.watchWait(ctx)
	if err != nil {
		return SubmissionRecord{}, err
	}
	defer stop()
	s.h.Resume()
	for {
		select {
		case <-ctx.Done():
			return SubmissionRecord{}, ctx.Err()
		case <-s.h.session.closing:
			return SubmissionRecord{}, s.h.Err()
		case state, ok := <-states:
			if !ok {
				if ctx.Err() != nil {
					return SubmissionRecord{}, ctx.Err()
				}
				return SubmissionRecord{}, s.h.Err()
			}
			if err := s.h.Err(); err != nil {
				return SubmissionRecord{}, err
			}
			rec := state.Submissions[s.id]
			if rec == nil {
				return SubmissionRecord{}, ErrNotFound
			}
			if rec.Status == "done" || rec.Status == "unanswered" {
				return *cloneRecord(rec), nil
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
	state, err := c.h.read(ctx)
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
		for id, sub := range tx.state.Submissions {
			if sub.Conversation == c.id && sub.Status == "queued" && sub.Request.Type == "input" {
				sub = tx.state.mutSubmission(id)
				sub.Status = "unanswered"
				sub.Reason = "aborted"
			}
		}
		// Mark the whole selected scope before the first abort enables scheduling.
		for _, id := range ids {
			markAbortTree(tx.state, id)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := c.h.AbortTask(ctx, id); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if _, err := c.h.WaitTask(ctx, id); err != nil {
			return err
		}
	}
	// Upstream resolves once the scope is idle.
	return c.WaitForIdle(ctx)
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
	// Everything returned is detached: state may be the shared committed one.
	view := View{Sequence: state.Sequence, Conversation: *cloneRecord(rec), Entries: entries, Documents: map[string]json.RawMessage{}}
	for _, t := range state.Tasks {
		if t.Conversation == id && t.Status != "terminal" {
			view.Tasks = append(view.Tasks, *cloneRecord(t))
		}
	}
	for _, s := range state.Submissions {
		if s.Conversation == id && (s.Status == "queued" || s.Status == "placed") {
			view.Inbox = append(view.Inbox, *cloneRecord(s))
		}
	}
	for _, d := range state.Documents {
		if d.Scope == "conversation" && d.ScopeID == uint64(id) {
			view.Documents[d.Kind] = cloneRaw(d.Value)
		}
	}
	sort.Slice(view.Tasks, func(i, j int) bool { return view.Tasks[i].ID < view.Tasks[j].ID })
	sort.Slice(view.Inbox, func(i, j int) bool { return view.Inbox[i].ID < view.Inbox[j].ID })
	return view, nil
}
func (c *Conversation) View(ctx context.Context) (View, error) {
	s, err := c.h.read(ctx)
	if err != nil {
		return View{}, err
	}
	return viewFromState(s, c.id)
}
func (c *Conversation) Watch(ctx context.Context) (<-chan View, func(), error) {
	states, stop, err := c.h.session.watchShared(ctx)
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

// contributesContext excludes assistant attempts that did not finish: aborted
// or failed messages never reach later model context (spec §2.1).
func contributesContext(message *Message) bool {
	return !message.Aborted && message.StopReason != "error" && message.StopReason != "aborted"
}

// validateSubmission checks a submission's type, modes, and entry.
func validateSubmission(request SubmissionRequest) error {
	switch request.Type {
	case "input":
		if request.WhenBusy != "" && request.WhenBusy != "followUp" && request.WhenBusy != "steer" && request.WhenBusy != "reject" {
			return fmt.Errorf("%w: busy mode", ErrInvalid)
		}
		if request.WhenIdle != "" && request.WhenIdle != "reject" {
			return fmt.Errorf("%w: idle mode", ErrInvalid)
		}
	case "write", "continue":
		if request.WhenBusy != "" || request.WhenIdle != "" {
			return fmt.Errorf("%w: busy and idle modes apply to input only", ErrInvalid)
		}
		if request.Type == "continue" && (request.Content != "" || request.Entry != nil) {
			return fmt.Errorf("%w: a continue has no content or entry", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: submission type", ErrInvalid)
	}
	if request.Type == "write" && (request.Entry == nil || request.Entry.Kind == "") {
		return fmt.Errorf("%w: write requires entry", ErrInvalid)
	}
	if request.Type == "input" && request.Entry != nil {
		entry := request.Entry
		if entry.Kind == "" || strings.HasPrefix(entry.Kind, "pi.") || entry.Message != nil || entry.ID != 0 || entry.Conversation != 0 || entry.Parent != 0 || entry.Sequence != 0 || !entry.CreatedAt.IsZero() {
			return fmt.Errorf("%w: input entry must contain only a non-reserved kind and data", ErrInvalid)
		}
		if len(entry.Data) > 0 && !json.Valid(entry.Data) {
			return fmt.Errorf("%w: input entry data must be JSON", ErrInvalid)
		}
	}
	return nil
}

// EntryQuery bounds and orders a scan of a conversation's fork-aware
// history: MinEntryID and MaxEntryID (0: unbounded), and Order "descending"
// (default) or "ascending".
type EntryQuery struct {
	MinEntryID EntryID
	MaxEntryID EntryID
	Order      string
}

// EntryCursor continues a scan; it carries the scan's order.
type EntryCursor struct {
	After EntryID
	Order string
}

// EntryPage is one page of a scan; Next is nil after the last page.
type EntryPage struct {
	Items []Entry
	Next  *EntryCursor
}

// ScanEntries pages through the conversation's raw history, every entry
// including those a summary or reset left behind, newest first unless the
// query is ascending. Pass the previous page's Next to continue.
func (c *Conversation) ScanEntries(ctx context.Context, query EntryQuery, limit int, cursor *EntryCursor) (EntryPage, error) {
	if limit <= 0 {
		return EntryPage{}, fmt.Errorf("%w: limit must be positive", ErrInvalid)
	}
	order := query.Order
	if cursor != nil {
		order = cursor.Order
	}
	if order == "" {
		order = "descending"
	}
	if order != "ascending" && order != "descending" {
		return EntryPage{}, fmt.Errorf("%w: order", ErrInvalid)
	}
	entries, err := c.Entries(ctx)
	if err != nil {
		return EntryPage{}, err
	}
	if order == "descending" {
		for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
			entries[i], entries[j] = entries[j], entries[i]
		}
	}
	var page EntryPage
	for _, e := range entries {
		if query.MinEntryID != 0 && e.ID < query.MinEntryID || query.MaxEntryID != 0 && e.ID > query.MaxEntryID {
			continue
		}
		if cursor != nil && (order == "ascending" && e.ID <= cursor.After || order == "descending" && e.ID >= cursor.After) {
			continue
		}
		if len(page.Items) == limit {
			last := page.Items[len(page.Items)-1].ID
			page.Next = &EntryCursor{After: last, Order: order}
			break
		}
		page.Items = append(page.Items, e)
	}
	return page, nil
}
