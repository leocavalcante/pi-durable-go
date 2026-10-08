package durable

import (
	"context"
	"encoding/json"
)

var usageDocument = DocumentDefinition{Kind: "pi.usage", Version: 1, Scope: "conversation", History: "latest", Fork: "initial", Initial: func() any { return UsageState{Models: map[string]Usage{}, Tools: map[string]Usage{}} }}

func sumUsage(a, b Usage) Usage {
	return Usage{InputTokens: a.InputTokens + b.InputTokens, OutputTokens: a.OutputTokens + b.OutputTokens, CacheReadTokens: a.CacheReadTokens + b.CacheReadTokens, CacheWriteTokens: a.CacheWriteTokens + b.CacheWriteTokens, Cost: a.Cost + b.Cost}
}
func recordModelUsage(tx *Tx, cid ConversationID, model ModelRef, usage Usage) error {
	return UpdateDocument(tx, usageDocument, uint64(cid), func(s *UsageState) error {
		if s.Models == nil {
			s.Models = map[string]Usage{}
		}
		key := model.Provider + "/" + model.ID
		s.Models[key] = sumUsage(s.Models[key], usage)
		return nil
	})
}
func recordToolUsage(tx *Tx, cid ConversationID, name string, usage Usage) error {
	return UpdateDocument(tx, usageDocument, uint64(cid), func(s *UsageState) error {
		if s.Tools == nil {
			s.Tools = map[string]Usage{}
		}
		s.Tools[name] = sumUsage(s.Tools[name], usage)
		return nil
	})
}

// Usage sums each conversation's own ledger, never counting owned child usage twice.
func (h *Harness) Usage(ctx context.Context) (UsageState, error) {
	state, err := h.Snapshot(ctx)
	if err != nil {
		return UsageState{}, err
	}
	total := UsageState{Models: map[string]Usage{}, Tools: map[string]Usage{}}
	for cid := range state.Conversations {
		doc := state.Documents[documentKey(usageDocument, uint64(cid))]
		if doc == nil {
			continue
		}
		var value UsageState
		if err := json.Unmarshal(doc.Value, &value); err != nil {
			return UsageState{}, err
		}
		for k, u := range value.Models {
			total.Models[k] = sumUsage(total.Models[k], u)
		}
		for k, u := range value.Tools {
			total.Tools[k] = sumUsage(total.Tools[k], u)
		}
	}
	return total, nil
}
