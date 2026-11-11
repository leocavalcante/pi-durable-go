package durable

import "sort"

// ordinaryOwned follows owner edges, never history parents. Terminal task nodes
// retain their edges; background tasks cut off their entire subtree. The
// explicitly addressed root is traversed even when it is background; only its
// descendants are returned.
func ordinaryOwned(state *State, root TaskID) ([]TaskID, []ConversationID) {
	children := make(map[TaskID][]TaskID)
	conversations := make(map[TaskID][]ConversationID)
	topLevel := make(map[ConversationID][]TaskID)
	for id, task := range state.Tasks {
		if task.Owner.Task != 0 {
			children[task.Owner.Task] = append(children[task.Owner.Task], id)
		} else {
			topLevel[task.Conversation] = append(topLevel[task.Conversation], id)
		}
	}
	for id, conversation := range state.Conversations {
		if conversation.Owner.Task != 0 {
			conversations[conversation.Owner.Task] = append(conversations[conversation.Owner.Task], id)
		}
	}
	var tasks []TaskID
	var scopes []ConversationID
	seen := make(map[TaskID]bool)
	var visit func(TaskID)
	visit = func(id TaskID) {
		task := state.Tasks[id]
		if task == nil || seen[id] || id != root && task.Background {
			return
		}
		seen[id] = true
		if id != root && task.Status != "terminal" {
			tasks = append(tasks, id)
		}
		for _, child := range children[id] {
			visit(child)
		}
		for _, cid := range conversations[id] {
			scopes = append(scopes, cid)
			for _, child := range topLevel[cid] {
				visit(child)
			}
		}
	}
	visit(root)
	sort.Slice(tasks, func(i, j int) bool { return tasks[i] < tasks[j] })
	sort.Slice(scopes, func(i, j int) bool { return scopes[i] < scopes[j] })
	return tasks, scopes
}

func ownedLive(state *State, id TaskID) []TaskID {
	tasks, _ := ordinaryOwned(state, id)
	return tasks
}

func markAbortTree(state *State, id TaskID) { markAbortTreeReason(state, id, "") }

func markAbortTreeReason(state *State, id TaskID, reason string) {
	task := state.Tasks[id]
	if task == nil || task.Status == "terminal" {
		return
	}
	// A request always wins over an abandonment, including below this root.
	if task.AbortRequested && task.AbortReason == "" {
		reason = ""
	}
	if !task.AbortRequested || task.AbortReason != reason {
		task.Restarts = 0
	}
	task.AbortRequested = true
	task.AbortReason = reason
	markOwned(state, id, cancellationReason(task))
}

func cancellationIntent(task *TaskRecord) bool {
	return task.Status != "terminal" && (task.AbortRequested || task.Status == "completing" && task.Outcome != nil && task.Outcome.Status != "completed")
}

func cancellationReason(task *TaskRecord) string {
	if task.Status == "completing" && task.Outcome != nil && task.Outcome.Status != "completed" {
		return ""
	}
	return task.AbortReason
}

func markOwned(state *State, id TaskID, reason string) bool {
	tasks, scopes := ordinaryOwned(state, id)
	changed := false
	for _, child := range tasks {
		task := state.Tasks[child]
		if task.AbortRequested && (task.AbortReason == "" || reason != "") {
			continue
		}
		task.AbortRequested = true
		task.AbortReason = reason
		task.Restarts = 0
		changed = true
	}
	// The owner's own conversation keeps its queue. Only scopes below it are
	// withdrawn, and queued writes remain available for display/history.
	owned := make(map[ConversationID]bool, len(scopes))
	for _, cid := range scopes {
		owned[cid] = true
	}
	for _, sub := range state.Submissions {
		if owned[sub.Conversation] && sub.Status == "queued" && sub.Request.Type == "input" {
			sub.Status = "unanswered"
			sub.Reason = "aborted"
			changed = true
		}
	}
	return changed
}

func cascadeCancellation(state *State) bool {
	changed := false
	for id, task := range state.Tasks {
		if cancellationIntent(task) && markOwned(state, id, cancellationReason(task)) {
			changed = true
		}
	}
	return changed
}

// belowCancellationIntent is checked again in the reservation/commit callback,
// so a stale dispatch snapshot cannot run code below a newly cancelled owner.
func belowCancellationIntent(state *State, task *TaskRecord) bool {
	seen := make(map[TaskID]bool)
	for task != nil && !seen[task.ID] {
		seen[task.ID] = true
		if task.Background {
			return false
		}
		owner := task.Owner.Task
		if owner == 0 {
			if conversation := state.Conversations[task.Conversation]; conversation != nil {
				owner = conversation.Owner.Task
			}
		}
		task = state.Tasks[owner]
		if task != nil && cancellationIntent(task) {
			return true
		}
	}
	return false
}
