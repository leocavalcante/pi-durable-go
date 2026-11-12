package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
)

func inputEntry(request SubmissionRequest) Entry {
	kind := "pi.user"
	var data json.RawMessage
	if request.Entry != nil {
		kind = request.Entry.Kind
		data = request.Entry.Data
	}
	return Entry{Kind: kind, Message: &Message{Role: "user", Content: request.Content, Images: request.Images, ImageOffsets: request.ImageOffsets}, Data: data}
}

// isDisplayOnlyWrite: a write that changes nothing the model sees (no
// message, not a reset or compaction summary) needs no boundary.
func isDisplayOnlyWrite(request SubmissionRequest) bool {
	return request.Type == "write" && request.Entry != nil && request.Entry.Kind != "pi.reset" && request.Entry.Kind != "pi.compaction" && request.Entry.Message == nil
}

func queuedResetExists(tx *Tx, cid ConversationID) bool {
	for _, submission := range tx.state.Submissions {
		if submission.Conversation == cid && submission.Status == "queued" && submission.Request.Type == "write" && submission.Request.Entry != nil && submission.Request.Entry.Kind == "pi.reset" {
			return true
		}
	}
	return false
}

// A queued compaction must be validated at placement, not only when its task
// queues it: a reset or newer summary may have arrived in between.
func placeWrite(tx *Tx, cid ConversationID, submission *SubmissionRecord) error {
	entry := submission.Request.Entry
	if entry == nil {
		return ErrInvalid
	}
	if entry.Kind == "pi.compaction" {
		var summary struct {
			Cut EntryID `json:"cut"`
		}
		if err := json.Unmarshal(entry.Data, &summary); err != nil {
			return err
		}
		entries, err := entriesFromState(tx.state, cid)
		if err != nil {
			return err
		}
		positions := map[EntryID]int{}
		for i, e := range entries {
			positions[e.ID] = i
		}
		cut, visible := positions[summary.Cut]
		stale := !visible
		for i, e := range entries {
			if visible && i > cut && e.Kind == "pi.reset" {
				stale = true
			}
			if e.Kind == "pi.compaction" {
				var other struct {
					Cut EntryID `json:"cut"`
				}
				if err := json.Unmarshal(e.Data, &other); err != nil {
					return err
				}
				if index, ok := positions[other.Cut]; ok && index >= cut {
					stale = true
				}
			}
		}
		if stale {
			submission.Status = "unanswered"
			submission.Reason = "stale"
			return nil
		}
	}
	id, err := tx.Append(cid, *entry)
	if err != nil {
		return err
	}
	submission.Status = "done"
	submission.Entry = id
	return nil
}

func sortSubmissions(items []*SubmissionRecord) {
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
}

// InboxItem is one queued submission in pi.inbox: user input for a run
// (mode steer or followUp, with its content and the entry it is placed as
// when not pi.user), or an entry write (mode write).
type InboxItem struct {
	ID      SubmissionID   `json:"id"`
	Mode    string         `json:"mode"`
	Content string         `json:"content,omitempty"`
	Images  []ImageContent `json:"images,omitempty"`
	// ImageOffsets place Images in Content (Message.ImageOffsets).
	ImageOffsets []int  `json:"imageOffsets,omitempty"`
	Entry        *Entry `json:"entry,omitempty"`
}

// InboxState is the built-in pi.inbox document: one conversation's
// submissions waiting for a boundary, in ID order.
type InboxState struct {
	Items []InboxItem `json:"items"`
}

var inboxDocument = DocumentDefinition{Kind: "pi.inbox", Version: 1, Scope: "conversation", History: "latest", Fork: "initial",
	Initial: func() any { return InboxState{Items: []InboxItem{}} }}

// syncInboxes keeps each conversation's pi.inbox equal to its queued
// submissions, in the commit that changed them.
func syncInboxes(tx *Tx) error {
	queued := map[ConversationID][]*SubmissionRecord{}
	for _, sub := range tx.state.Submissions {
		if sub.Status == "queued" {
			queued[sub.Conversation] = append(queued[sub.Conversation], sub)
		}
	}
	conversations := map[ConversationID]bool{}
	for cid := range queued {
		conversations[cid] = true
	}
	for key, doc := range tx.state.Documents {
		if doc.Kind == inboxDocument.Kind && doc.Scope == "conversation" {
			_ = key
			conversations[ConversationID(doc.ScopeID)] = true
		}
	}
	for cid := range conversations {
		if tx.state.Conversations[cid] == nil {
			continue
		}
		items := []InboxItem{}
		subs := queued[cid]
		sortSubmissions(subs)
		for _, sub := range subs {
			item := InboxItem{ID: sub.ID}
			switch sub.Request.Type {
			case "write":
				item.Mode, item.Entry = "write", sub.Request.Entry
			default:
				item.Mode, item.Content, item.Images, item.ImageOffsets = queueMode(sub.Request), sub.Request.Content, sub.Request.Images, sub.Request.ImageOffsets
				if sub.Request.Entry != nil {
					item.Entry = &Entry{Kind: sub.Request.Entry.Kind, Data: sub.Request.Entry.Data}
				}
			}
			items = append(items, item)
		}
		data, err := json.Marshal(InboxState{Items: items})
		if err != nil {
			return err
		}
		doc := tx.state.Documents[documentKey(inboxDocument, uint64(cid))]
		if doc == nil && len(items) == 0 {
			continue
		}
		if doc != nil && jsonEqual(doc.Value, data) {
			continue
		}
		if err := tx.PutDocument(inboxDocument, uint64(cid), data); err != nil {
			return err
		}
	}
	return nil
}

// Inbox reads this conversation's queued submissions as pi.inbox shows them.
func (c *Conversation) Inbox(ctx context.Context) (InboxState, error) {
	inbox, err := ReadDocument[InboxState](ctx, c.h, inboxDocument, uint64(c.id))
	if errors.Is(err, ErrNotFound) {
		return InboxState{Items: []InboxItem{}}, nil
	}
	return inbox, err
}
