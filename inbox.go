package durable

import "encoding/json"

func inputEntry(request SubmissionRequest) Entry {
	kind := "pi.user"
	var data json.RawMessage
	if request.Entry != nil {
		kind = request.Entry.Kind
		data = request.Entry.Data
	}
	return Entry{Kind: kind, Message: &Message{Role: "user", Content: request.Content}, Data: data}
}

func isDisplayOnlyWrite(request SubmissionRequest) bool {
	return request.Type == "write" && request.Entry != nil && request.Entry.Kind != "pi.reset" && request.Entry.Message == nil
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
	submission.Answer = id
	return nil
}
