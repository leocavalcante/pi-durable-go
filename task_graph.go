package durable

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

// TaskGraphNode is one live task of the task graph (upstream spec §9.4):
// its durable state without checkpoint and outcome payloads.
type TaskGraphNode struct {
	ID             TaskID         `json:"id"`
	Kind           string         `json:"kind"`
	Conversation   ConversationID `json:"conversationId"`
	Owner          TaskID         `json:"owner,omitempty"`
	Background     bool           `json:"background"`
	AbortRequested bool           `json:"abortRequested"`
	// Status is pending, running, waiting, sleeping, or completing.
	Status string `json:"status"`
	// Phase is the checkpoint's phase, when it has one.
	Phase string `json:"phase,omitempty"`
	// On and Policy are a waiting task's wait.
	On     []TaskID `json:"on,omitempty"`
	Policy string   `json:"policy,omitempty"`
	// Until is a sleeping task's deadline.
	Until *time.Time `json:"until,omitempty"`
	// Outcome is a completing task's held outcome status.
	Outcome string `json:"outcome,omitempty"`
	// Blocked says why no registered definition can take it, when none can.
	Blocked string `json:"blocked,omitempty"`
	// Conversations this task owns, in ID order.
	Conversations []ConversationID `json:"conversations"`
}

// TaskGraph is every live task of the Session, keyed by ID.
type TaskGraph struct {
	Tasks map[TaskID]TaskGraphNode `json:"tasks"`
}

// TaskGraph reads the live task graph from one snapshot. Blocked reasons are
// derived from the current registry, as Inspect does.
func (h *Harness) TaskGraph(ctx context.Context) (TaskGraph, error) {
	inspection, err := h.Inspect(ctx)
	if err != nil {
		return TaskGraph{}, err
	}
	state, err := h.read(ctx)
	if err != nil {
		return TaskGraph{}, err
	}
	return taskGraphOf(state, inspection), nil
}

func taskGraphOf(state *State, inspection HarnessInspection) TaskGraph {
	owned := map[TaskID][]ConversationID{}
	for id, conversation := range state.Conversations {
		if conversation.Owner.Task != 0 {
			owned[conversation.Owner.Task] = append(owned[conversation.Owner.Task], id)
		}
	}
	graph := TaskGraph{Tasks: map[TaskID]TaskGraphNode{}}
	for _, item := range inspection.Tasks {
		task := item.Record
		node := TaskGraphNode{ID: task.ID, Kind: task.Kind, Conversation: task.Conversation, Owner: task.Owner.Task,
			Background: task.Background, AbortRequested: task.AbortRequested, Status: task.Status, Conversations: []ConversationID{}}
		if conversations := owned[task.ID]; len(conversations) > 0 {
			sort.Slice(conversations, func(i, j int) bool { return conversations[i] < conversations[j] })
			node.Conversations = conversations
		}
		switch task.Status {
		case "completing":
			if task.Outcome != nil {
				node.Outcome = task.Outcome.Status
			}
		default:
			var cp struct {
				Phase string `json:"phase"`
			}
			_ = json.Unmarshal(task.Checkpoint, &cp)
			node.Phase = cp.Phase
			if task.Status == "waiting" {
				node.On, node.Policy = append([]TaskID(nil), task.WaitOn...), task.WaitPolicy
				if node.Policy == "" {
					node.Policy = "allSettled"
				}
			}
			if task.Status == "sleeping" && task.WakeAt != nil {
				until := *task.WakeAt
				node.Until = &until
			}
		}
		if item.State.Kind == "blocked" {
			node.Blocked = item.State.Reason
		}
		graph.Tasks[task.ID] = node
	}
	return graph
}

// WatchTaskGraph delivers the task graph after each commit that changes it,
// coalescing to the latest; stop ends the watch.
func (h *Harness) WatchTaskGraph(ctx context.Context) (<-chan TaskGraph, func(), error) {
	states, stop, err := h.session.Watch(ctx)
	if err != nil {
		return nil, nil, err
	}
	out := make(chan TaskGraph, 1)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(out)
		defer stop()
		var last []byte
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-states:
				if !ok {
					return
				}
				graph, err := h.TaskGraph(ctx)
				if err != nil {
					return
				}
				data, _ := json.Marshal(graph)
				if string(data) == string(last) {
					continue
				}
				last = data
				select {
				case out <- graph:
				default:
					select {
					case <-out:
					default:
					}
					select {
					case out <- graph:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, func() { cancel(); stop() }, nil
}
