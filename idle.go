package durable

import (
	"context"
	"errors"
)

// WaitForIdle resolves when the ordinary ownership scope of every ownerless
// conversation has no live non-background task. It asks for progress, so it
// enables scheduling. Cancelling ctx cancels only this wait.
func (h *Harness) WaitForIdle(ctx context.Context) error {
	return h.waitIdle(ctx, func(state *State) bool { return idleEverywhere(state) })
}

// WaitForIdle resolves when this conversation's ordinary ownership scope has
// no live non-background task: the conversation and the conversations owned,
// transitively, by its non-background tasks.
func (c *Conversation) WaitForIdle(ctx context.Context) error {
	return c.h.waitIdle(ctx, func(state *State) bool {
		if state.Conversations[c.id] == nil {
			return true
		}
		return idleConversation(state, newOwnershipGraph(state), c.id)
	})
}

func (h *Harness) waitIdle(ctx context.Context, idle func(*State) bool) error {
	if h == nil || h.session == nil {
		return ErrClosed
	}
	states, stop, err := h.session.watchWait(ctx)
	if err != nil {
		return err
	}
	defer stop()
	h.Resume()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-h.session.closing:
			return h.Err()
		case state, ok := <-states:
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err := h.Err(); err != nil {
					return err
				}
				return errors.New("durable: watch ended")
			}
			if err := h.Err(); err != nil {
				return err
			}
			if idle(state) {
				return nil
			}
		}
	}
}

// idleEverywhere: every ownerless conversation's ordinary scope is idle.
func idleEverywhere(state *State) bool {
	graph := newOwnershipGraph(state)
	for id, conversation := range state.Conversations {
		if conversation.Owner.Task == 0 && !idleConversation(state, graph, id) {
			return false
		}
	}
	return true
}

// idleConversation walks down from a conversation: its top-level tasks and
// their ordinary owned work, without entering background subtrees.
func idleConversation(state *State, graph *ownershipGraph, cid ConversationID) bool {
	for _, id := range graph.topLevel[cid] {
		task := state.Tasks[id]
		if task == nil || task.Background {
			continue
		}
		if task.Status != "terminal" || len(graph.ownedLive(id)) > 0 {
			return false
		}
	}
	return true
}
