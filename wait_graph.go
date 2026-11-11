package durable

import (
	"fmt"
	"strconv"
	"strings"
)

// checkWait validates a wait (Pi Durable §5.5): it names existing tasks,
// failFast only tasks the waiter owns, and it may not close a cycle in the
// wait-for graph, whose edges run from each task to what it waits on and to
// its live ordinary owned work. Waiting on itself, or on an owner holding it,
// are the shortest such cycles. Invariant: the graph is acyclic, since only a
// wait adds an edge between existing tasks, so checking each wait keeps it.
func checkWait(state *State, waiter TaskID, on []TaskID, policy string) error {
	var targets []TaskID
	for _, id := range on {
		member := state.Tasks[id]
		if member == nil {
			return fmt.Errorf("%w: Task %d does not exist", ErrInvalid, id)
		}
		if policy == "failFast" && member.Owner.Task != waiter {
			return fmt.Errorf("%w: Task %d can wait failFast only on tasks it owns; %d is not one", ErrInvalid, waiter, id)
		}
		// A live child already holds the waiter: waiting on it adds no edge.
		// A terminal task has no edges.
		if member.Status != "terminal" && member.Owner.Task != waiter {
			targets = append(targets, id)
		}
	}
	if len(targets) == 0 {
		return nil
	}
	if cycle := waitCycle(state, waiter, targets); cycle != nil {
		steps := make([]string, 0, len(cycle)+1)
		steps = append(steps, strconv.FormatUint(uint64(waiter), 10))
		for _, id := range cycle {
			steps = append(steps, strconv.FormatUint(uint64(id), 10))
		}
		return fmt.Errorf("%w: Task %d cannot wait on %d: %s is a wait cycle", ErrInvalid, waiter, cycle[0], strings.Join(steps, " -> "))
	}
	return nil
}

// waitCycle returns the path from a member of on back to task in the
// wait-for graph, if the wait would close a cycle. An edge X -> Y means X
// cannot settle before Y: Y is waited on by X while X waits and is not
// abort-marked (abort runs bottom-up), or Y is live ordinary owned work of X,
// which holds X as completing. Each task is visited once.
func waitCycle(state *State, task TaskID, on []TaskID) []TaskID {
	graph := newOwnershipGraph(state)
	from := make(map[TaskID]TaskID, len(on))
	reached := make(map[TaskID]bool, len(on))
	var pending []TaskID
	reach := func(id, via TaskID) {
		if reached[id] {
			return
		}
		reached[id] = true
		from[id] = via
		pending = append(pending, id)
	}
	for _, id := range on {
		reach(id, 0)
	}
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if id == task {
			var path []TaskID
			for at := id; at != 0; at = from[at] {
				path = append([]TaskID{at}, path...)
			}
			return path
		}
		record := state.Tasks[id]
		if record == nil || record.Status == "terminal" {
			continue
		}
		if record.Status == "waiting" && !record.AbortRequested {
			for _, member := range record.WaitOn {
				reach(member, id)
			}
		}
		for _, below := range graph.ownedLive(id) {
			reach(below, id)
		}
	}
	return nil
}

// ownershipGraph indexes owner edges once, for repeated ordinary-scope walks.
type ownershipGraph struct {
	state         *State
	children      map[TaskID][]TaskID
	conversations map[TaskID][]ConversationID
	topLevel      map[ConversationID][]TaskID
}

func newOwnershipGraph(state *State) *ownershipGraph {
	g := &ownershipGraph{state: state, children: map[TaskID][]TaskID{}, conversations: map[TaskID][]ConversationID{}, topLevel: map[ConversationID][]TaskID{}}
	for id, task := range state.Tasks {
		if task.Owner.Task != 0 {
			g.children[task.Owner.Task] = append(g.children[task.Owner.Task], id)
		} else {
			g.topLevel[task.Conversation] = append(g.topLevel[task.Conversation], id)
		}
	}
	for id, conversation := range state.Conversations {
		if conversation.Owner.Task != 0 {
			g.conversations[conversation.Owner.Task] = append(g.conversations[conversation.Owner.Task], id)
		}
	}
	return g
}

// ownedLive lists the live ordinary owned work directly below root: owned
// tasks and the top-level tasks of owned conversations, without descending
// into background subtrees or past terminal-free edges twice.
func (g *ownershipGraph) ownedLive(root TaskID) []TaskID {
	var out []TaskID
	seen := map[TaskID]bool{root: true}
	var visit func(TaskID)
	visit = func(id TaskID) {
		if seen[id] {
			return
		}
		seen[id] = true
		task := g.state.Tasks[id]
		if task == nil || task.Background {
			return
		}
		if task.Status != "terminal" {
			out = append(out, id)
		}
		for _, child := range g.children[id] {
			visit(child)
		}
		for _, cid := range g.conversations[id] {
			for _, child := range g.topLevel[cid] {
				visit(child)
			}
		}
	}
	for _, child := range g.children[root] {
		visit(child)
	}
	for _, cid := range g.conversations[root] {
		for _, child := range g.topLevel[cid] {
			visit(child)
		}
	}
	return out
}
