package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

// LiveState is the built-in pi.live conversation document (Pi Durable
// §8.2): run control and presentation of the current generation and tool
// round. Clients may read it; only the Harness writes it.
type LiveState struct {
	// Run is present exactly while the conversation is busy.
	Run *LiveRun `json:"run,omitempty"`
	// Generation presents the current generation attempt.
	Generation *LiveGeneration `json:"generation,omitempty"`
	// Tools presents the current tool round in call order, from the
	// tool-calling answer until the round ends.
	Tools []ToolSlot `json:"tools,omitempty"`
	// NestedTools presents nested calls of running tool calls, in creation
	// order, so a parent precedes its children; removed when the caller
	// settles.
	NestedTools []NestedToolSlot `json:"nestedTools,omitempty"`
	// Compactions lists live compaction tasks in task ID order.
	Compactions []CompactionStatus `json:"compactions,omitempty"`
	// CompactionFailure is the newest compaction that failed since a
	// compaction last produced a summary.
	CompactionFailure *CompactionFailure `json:"compactionFailure,omitempty"`
	// RunFailure says why the newest run ended without an answer. A later
	// commit that starts a run removes it, except a run started in the same
	// commit (AfterFailedRun "continue", or after a reset).
	RunFailure *RunFailure `json:"runFailure,omitempty"`
}

// LiveRun is run control (Pi Durable LiveRun).
type LiveRun struct {
	// TaskID is the generation that settles the run's inputs, handed from
	// one turn's generation to the next.
	TaskID TaskID `json:"taskId"`
	// Inputs are the run's placed submissions, settled when it ends.
	Inputs []SubmissionID `json:"inputs"`
	// Turns counts tool rounds so far, which Agent.MaxTurns bounds; an
	// OnYield continuation starts the count over.
	Turns int `json:"turns"`
	// First is the run's first entry: the first one the boundary that
	// started it placed, a continue's the failed run's; otherwise its first
	// tool-calling answer.
	First EntryID `json:"first,omitempty"`
	// AfterRun is present once the run answered and its generation waits for
	// the tasks AfterRun hooks asked for, listed once created. The run then
	// ends without a failure, however it ends.
	AfterRun *LiveAfterRun `json:"afterRun,omitempty"`
}

// LiveAfterRun lists a held run's after-run tasks.
type LiveAfterRun struct {
	Tasks []TaskID `json:"tasks,omitempty"`
}

// LiveGeneration presents the current generation attempt.
type LiveGeneration struct {
	Attempt int `json:"attempt"`
	// Message is the committed, throttled partial of the in-flight response.
	Message *Message `json:"message,omitempty"`
	// Retry is the durable backoff before the next attempt.
	Retry *LiveRetry `json:"retry,omitempty"`
}

// LiveRetry is a backoff: At in Unix milliseconds on the Harness clock, and
// the error that caused it.
type LiveRetry struct {
	At    int64  `json:"at"`
	Error string `json:"error"`
}

// ToolSlot presents one tool call of the current round.
type ToolSlot struct {
	CallID string `json:"callId"`
	Name   string `json:"name"`
	// Status is pending, running, or done.
	Status string `json:"status"`
	// TaskID is the call's tool task.
	TaskID TaskID `json:"taskId,omitempty"`
	// Output is the retained running output; Details the last details value.
	Output  string          `json:"output,omitempty"`
	Details json.RawMessage `json:"details,omitempty"`
	// DroppedBytes and DroppedLines count running output the slot's bound
	// dropped from its beginning.
	DroppedBytes int `json:"droppedBytes,omitempty"`
	DroppedLines int `json:"droppedLines,omitempty"`
	// Entry is the result entry once done; absent when the tool task faulted
	// or was orphaned.
	Entry EntryID `json:"entry,omitempty"`
	// Unoffered marks a call to a tool its request did not offer.
	Unoffered bool `json:"unoffered,omitempty"`
}

// CompactionStatus presents one live compaction task.
type CompactionStatus struct {
	TaskID TaskID `json:"taskId"`
	// Trigger is what started it: manual, threshold, or overflow.
	Trigger string `json:"trigger"`
	// Blocking says whether a generation waits for it.
	Blocking bool       `json:"blocking"`
	Attempt  int        `json:"attempt"`
	Retry    *LiveRetry `json:"retry,omitempty"`
}

// CompactionFailure is the newest failed compaction.
type CompactionFailure struct {
	TaskID  TaskID `json:"taskId"`
	Trigger string `json:"trigger"`
	Error   string `json:"error"`
}

// Run failure reasons (Pi Durable RunFailureReason).
const (
	RunModelError      = "model_error"
	RunNoModel         = "no_model"
	RunAborted         = "aborted"
	RunReset           = "reset"
	RunFaulted         = "faulted"
	RunOrphaned        = "orphaned"
	RunInterrupted     = "interrupted"
	RunRestartLimit    = "restart_limit"
	RunNothingToAnswer = "nothing_to_answer"
	RunTurnLimit       = "turn_limit"
)

// RunFailure is the newest run that ended without an answer.
type RunFailure struct {
	// TaskID is the task that ended the run.
	TaskID TaskID `json:"taskId"`
	// Inputs are the run's inputs, settled unanswered.
	Inputs []SubmissionID `json:"inputs"`
	Reason string         `json:"reason"`
	// Detail: model_error the error text; no_model which model is missing;
	// faulted the error message; orphaned why the task could not run;
	// interrupted and restart_limit what was cut off and how often;
	// turn_limit the tool rounds the run took.
	Detail string `json:"detail,omitempty"`
	// First is the failed run's First, so a continue sees its history too.
	First EntryID `json:"first,omitempty"`
}

// liveDocument is pi.live. Go numbers its own versions: version 1 stored
// tool slots as a map keyed by task ID, version 2 is upstream's LiveState.
var liveDocument = DocumentDefinition{Kind: "pi.live", Version: 2, Scope: "conversation", History: "latest", Fork: "initial",
	Initial: func() any { return LiveState{} }, Migrate: migrateLive}

func migrateLive(from int, value json.RawMessage) (json.RawMessage, error) {
	if from != 1 {
		return nil, fmt.Errorf("pi.live has no migration from version %d", from)
	}
	return liveFromVersion1(value)
}

// liveFromVersion1 converts Go's version-1 pi.live, whose tools map held the
// slots of running tool tasks, to LiveState; other fields are dropped.
func liveFromVersion1(value json.RawMessage) (json.RawMessage, error) {
	var old struct {
		Tools map[string]struct {
			CallID  string          `json:"callId"`
			Name    string          `json:"name"`
			Output  string          `json:"output"`
			Details json.RawMessage `json:"details"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(value, &old); err != nil {
		return nil, err
	}
	var live LiveState
	for key, slot := range old.Tools {
		id, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			continue
		}
		live.Tools = append(live.Tools, ToolSlot{CallID: slot.CallID, Name: slot.Name, Status: "running", TaskID: TaskID(id), Output: slot.Output, Details: slot.Details})
	}
	sort.Slice(live.Tools, func(i, j int) bool { return live.Tools[i].TaskID < live.Tools[j].TaskID })
	return json.Marshal(live)
}

// readLive reads pi.live in a transaction without creating it.
func readLive(tx *Tx, cid ConversationID) (*LiveState, error) {
	live := &LiveState{}
	doc := tx.state.Documents[documentKey(liveDocument, uint64(cid))]
	if doc == nil {
		return live, nil
	}
	value, err := migratedDocument(liveDocument, doc)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(value, live); err != nil {
		return nil, err
	}
	return live, nil
}

// writeLive stores pi.live; an empty state is written too, so readers see it.
func writeLive(tx *Tx, cid ConversationID, live *LiveState) error {
	data, err := json.Marshal(live)
	if err != nil {
		return err
	}
	return tx.PutDocument(liveDocument, uint64(cid), data)
}

// updateLive is a read-modify-write of pi.live within a transaction.
func updateLive(tx *Tx, cid ConversationID, update func(*LiveState) error) error {
	live, err := readLive(tx, cid)
	if err != nil {
		return err
	}
	if err := update(live); err != nil {
		return err
	}
	return writeLive(tx, cid, live)
}

// toolSlot finds the slot of tool task id.
func (l *LiveState) toolSlot(id TaskID) *ToolSlot {
	for i := range l.Tools {
		if l.Tools[i].TaskID == id {
			return &l.Tools[i]
		}
	}
	return nil
}

func (l *LiveState) compaction(id TaskID) *CompactionStatus {
	for i := range l.Compactions {
		if l.Compactions[i].TaskID == id {
			return &l.Compactions[i]
		}
	}
	return nil
}

func (l *LiveState) removeCompaction(id TaskID) {
	for i := range l.Compactions {
		if l.Compactions[i].TaskID == id {
			l.Compactions = append(l.Compactions[:i], l.Compactions[i+1:]...)
			break
		}
	}
	if len(l.Compactions) == 0 {
		l.Compactions = nil
	}
}

// failCompaction removes a compaction's status and records its failure.
func (l *LiveState) failCompaction(id TaskID, message string) {
	if status := l.compaction(id); status != nil {
		l.CompactionFailure = &CompactionFailure{TaskID: id, Trigger: status.Trigger, Error: message}
	}
	l.removeCompaction(id)
}

// readRunLive reads pi.live for a generation. A run without pi.live.run whose
// conversation is busy with this generation is a version-2 run, which listed
// its inputs in the generation's input: it is adopted as the live run.
func readRunLive(tx *Tx, task TaskRecord) (*LiveState, error) {
	live, err := readLive(tx, task.Conversation)
	if err != nil {
		return nil, err
	}
	if live.Run != nil {
		return live, nil
	}
	conv, err := tx.Conversation(task.Conversation)
	if err != nil {
		return nil, err
	}
	if conv.Busy == task.ID {
		var input generationInput
		_ = json.Unmarshal(task.Input, &input)
		live.Run = &LiveRun{TaskID: task.ID, Inputs: append([]SubmissionID{}, input.Submissions...)}
	}
	return live, nil
}

// Live reads this conversation's pi.live run control and presentation; an
// absent document reads as empty.
func (c *Conversation) Live(ctx context.Context) (LiveState, error) {
	live, err := ReadDocument[LiveState](ctx, c.h, liveDocument, uint64(c.id))
	if errors.Is(err, ErrNotFound) {
		return LiveState{}, nil
	}
	return live, err
}
