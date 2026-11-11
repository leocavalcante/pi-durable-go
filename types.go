// Package durable implements checkpointed agent conversations, tasks, and documents.
// It is an experimental Go port of Pi Durable, not a Pi CLI implementation.
package durable

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type ConversationID uint64
type EntryID uint64
type TaskID uint64
type SubmissionID uint64

var (
	ErrClosed   = errors.New("durable: closed")
	ErrPoisoned = errors.New("durable: uncertain storage failure; reopen required")
	ErrConflict = errors.New("durable: revision conflict")
	ErrNotFound = errors.New("durable: not found")
	ErrBusy     = errors.New("durable: conversation busy")
	ErrInvalid  = errors.New("durable: invalid operation")
	ErrBlocked  = errors.New("durable: missing or incompatible definition")
)

// RejectedError certifies a storage write was not applied. Other write errors
// are uncertain and poison the Session; implementations must not misclassify them.
// Scheduler bookkeeping fails the Session even on certified rejection, except
// for a caller-cancelled, certified-unapplied write.
type RejectedError struct{ Err error }

func (e *RejectedError) Error() string { return e.Err.Error() }
func (e *RejectedError) Unwrap() error { return e.Err }

type State struct {
	Version       int                                    `json:"version"`
	Sequence      uint64                                 `json:"sequence"`
	NextID        uint64                                 `json:"nextId"`
	Root          ConversationID                         `json:"root"`
	Conversations map[ConversationID]*ConversationRecord `json:"conversations"`
	Entries       map[EntryID]*Entry                     `json:"entries"`
	Tasks         map[TaskID]*TaskRecord                 `json:"tasks"`
	Submissions   map[SubmissionID]*SubmissionRecord     `json:"submissions"`
	Documents     map[string]*DocumentRecord             `json:"documents"`
}

// Storage is owned by one Session. Commit must atomically publish next only if
// its current sequence is expected. Implementations must detach retained values.
type Storage interface {
	Load(context.Context) (*State, error)
	Commit(context.Context, uint64, *State) error
	Close() error
}

type Owner struct {
	Task TaskID `json:"task,omitempty"`
}
type ModelRef struct {
	Provider string `json:"provider"`
	ID       string `json:"id"`
}
type Agent struct {
	Model         ModelRef `json:"model"`
	ThinkingLevel string   `json:"thinkingLevel,omitempty"`
	Extensions    []string `json:"extensions"`
	Tools         []string `json:"tools"`
	Instructions  string   `json:"instructions,omitempty"`
	Cwd           string   `json:"cwd,omitempty"`
	// MaxTurns bounds a run's tool rounds; a run that reaches it ends
	// unanswered with turn_limit, and a continue resumes it. Zero: no bound.
	MaxTurns int `json:"maxTurns,omitempty"`
}
type ConversationRecord struct {
	ID                ConversationID `json:"id"`
	Parent            ConversationID `json:"parent,omitempty"`
	ForkAt            EntryID        `json:"forkAt,omitempty"`
	Owner             Owner          `json:"owner"`
	Agent             Agent          `json:"agent"`
	ProviderSessionID string         `json:"providerSessionId"`
	Head              EntryID        `json:"head,omitempty"`
	Busy              TaskID         `json:"busy,omitempty"`
	CreatedAt         time.Time      `json:"createdAt"`
}
type ConversationOptions struct {
	Owner Owner
	Agent Agent
	Init  func(*Tx, ConversationID) error
}

type Entry struct {
	ID           EntryID         `json:"id"`
	Conversation ConversationID  `json:"conversation"`
	Parent       EntryID         `json:"parent,omitempty"`
	Kind         string          `json:"kind"`
	Message      *Message        `json:"message,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
	Sequence     uint64          `json:"sequence"`
	CreatedAt    time.Time       `json:"createdAt"`
}
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	Thinking   string     `json:"thinking,omitempty"`
	ToolCalls  []ToolCall `json:"toolCalls,omitempty"`
	ToolCallID string     `json:"toolCallId,omitempty"`
	ToolName   string     `json:"toolName,omitempty"`
	IsError    bool       `json:"isError,omitempty"`
	// Aborted marks an assistant attempt that did not finish; with an error
	// StopReason it failed. Neither reaches later model context.
	Aborted bool `json:"aborted,omitempty"`
	// StopReason is an assistant message's ending: stop, length, toolUse,
	// error, or aborted (pi-ai StopReason). Empty in messages from before it.
	StopReason string `json:"stopReason,omitempty"`
	// ErrorMessage is a failed attempt's error text.
	ErrorMessage string `json:"errorMessage,omitempty"`
	// ThinkingLevel is the level the attempt requested, as pi-agent-core's
	// agent loop records it.
	ThinkingLevel string `json:"thinkingLevel,omitempty"`
	Usage         Usage  `json:"usage,omitempty"`
	DurationMs    *int64 `json:"durationMs,omitempty"`
	// ProviderData is adapter-owned assistant replay data (for example signed
	// thinking blocks). Persist it with the message; never put credentials here.
	ProviderData json.RawMessage `json:"providerData,omitempty"`
}
type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}
type Usage struct {
	InputTokens      int64   `json:"inputTokens"`
	OutputTokens     int64   `json:"outputTokens"`
	CacheReadTokens  int64   `json:"cacheReadTokens,omitempty"`
	CacheWriteTokens int64   `json:"cacheWriteTokens,omitempty"`
	Cost             float64 `json:"cost,omitempty"`
}
type UsageState struct {
	Models map[string]Usage `json:"models"`
	Tools  map[string]Usage `json:"tools"`
}

// SubmissionRequest is a host submission (Pi Durable SubmissionDraft): user
// input that may start a run, a passive entry write, or a continue, which
// starts a run on the context as it is, without a new user message.
type SubmissionRequest struct {
	Type      string `json:"type"` // input, write, or continue
	Content   string `json:"content,omitempty"`
	Entry     *Entry `json:"entry,omitempty"`
	RequestID string `json:"requestId,omitempty"`
	// WhenBusy (input only): followUp (default) queues for the next run, steer
	// joins the current run at its next boundary, reject fails with ErrBusy.
	WhenBusy string `json:"whenBusy,omitempty"`
	// WhenIdle (input only): reject fails with ErrIdle instead of starting a
	// run in a conversation without one.
	WhenIdle string `json:"whenIdle,omitempty"`
}
type SubmissionRecord struct {
	ID           SubmissionID      `json:"id"`
	Conversation ConversationID    `json:"conversation"`
	Request      SubmissionRequest `json:"request"`
	Status       string            `json:"status"` // queued, placed, done, unanswered
	// Entry is the transcript entry the submission was placed as: an input's
	// user message, or a write's entry.
	Entry     EntryID         `json:"entry,omitempty"`
	Answer    EntryID         `json:"answer,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Detail    json.RawMessage `json:"detail,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
}

type TaskRecord struct {
	ID             TaskID          `json:"id"`
	Conversation   ConversationID  `json:"conversation"`
	Kind           string          `json:"kind"`
	Version        int             `json:"version"`
	Owner          Owner           `json:"owner"`
	Background     bool            `json:"background,omitempty"`
	Status         string          `json:"status"` // pending, running, waiting, completing, terminal
	Input          json.RawMessage `json:"input"`
	Checkpoint     json.RawMessage `json:"checkpoint"`
	WaitOn         []TaskID        `json:"waitOn,omitempty"`
	WaitPolicy     string          `json:"waitPolicy,omitempty"`
	WakeAt         *time.Time      `json:"wakeAt,omitempty"`
	AbortRequested bool            `json:"abortRequested,omitempty"`
	// AbortReason is "restart" for abandoned work; empty means an abort request.
	AbortReason      string `json:"abortReason,omitempty"`
	AbandonOnRestart bool   `json:"abandonOnRestart,omitempty"`
	// Restarts counts consecutive crashes without a task state change. The
	// scheduler owns it: clean closes do not count, and progress, a new abort
	// mark, or an outcome resets it. Document and memo writes are not progress.
	Restarts  int                        `json:"restarts,omitempty"`
	Outcome   *Outcome                   `json:"outcome,omitempty"`
	Memos     map[string]json.RawMessage `json:"memos,omitempty"`
	StartedAt *time.Time                 `json:"startedAt,omitempty"`
	EndedAt   *time.Time                 `json:"endedAt,omitempty"`
}

const (
	OutcomeCompleted = "completed"
	OutcomeFailed    = "failed"
	OutcomeAborted   = "aborted"
	OutcomeOrphaned  = "orphaned" // scheduler only
	OutcomeFaulted   = "faulted"  // scheduler only
)

// Outcome distinguishes application decisions from scheduler failures. Error is
// the failed/faulted message; ErrorDetail is optional JSON, never a stack trace.
type Outcome struct {
	Status      string          `json:"status"`
	Result      json.RawMessage `json:"result,omitempty"`
	Error       string          `json:"error,omitempty"`
	ErrorDetail json.RawMessage `json:"errorDetail,omitempty"`
	Reason      string          `json:"reason,omitempty"`
}

type AbortResult string

const (
	AbortMarked   AbortResult = "marked"
	AbortTerminal AbortResult = "terminal"
)

type TaskOptions struct {
	Owner      Owner
	Background bool
	// AbandonOnRestart aborts a still-live task when a later Harness starts
	// scheduling. Opening for inspection alone does not mark it. Use for work
	// whose creator waits only in memory and cannot resume after a restart.
	AbandonOnRestart bool
}
type TaskTransition struct {
	Status     string
	Checkpoint json.RawMessage
	WaitOn     []TaskID
	WaitPolicy string
	WakeAt     *time.Time
	Outcome    *Outcome
}
type TaskDefinition struct {
	Kind    string
	Version int
	Initial func(json.RawMessage) (json.RawMessage, error)
	Run     func(context.Context, *TaskRuntime, TaskRecord) error
	Abort   func(context.Context, *TaskRuntime, TaskRecord) error
	// Migrate runs on the Session line. It must be pure, bounded, and must not
	// call Session/registry APIs or perform external effects. Reinstall retries
	// a failed migration; a successful abort eligibility probe may run again.
	Migrate func(fromVersion int, input, checkpoint json.RawMessage) (MigratedTask, error)
}

type MigratedTask struct {
	Input      json.RawMessage
	Checkpoint json.RawMessage
}

type DocumentDefinition struct {
	Kind    string
	Version int
	Scope   string // conversation, session, task
	History string // latest or rewindable
	Fork    string // initial, current, asOf
	Initial func() any
	Migrate func(int, json.RawMessage) (json.RawMessage, error)
}
type DocumentRevision struct {
	Version  int             `json:"version"`
	Sequence uint64          `json:"sequence"`
	Value    json.RawMessage `json:"value"`
}
type DocumentRecord struct {
	Kind      string             `json:"kind"`
	Version   int                `json:"version"`
	Scope     string             `json:"scope"`
	ScopeID   uint64             `json:"scopeId"`
	History   string             `json:"history"`
	Fork      string             `json:"fork"`
	Value     json.RawMessage    `json:"value"`
	Revisions []DocumentRevision `json:"revisions,omitempty"`
}

type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}
type ToolDefinition struct {
	ToolSpec
	ReplaySafe bool
	// ExecutionMode "sequential" runs a round containing a call to this tool
	// one call at a time, in call order; empty runs calls in parallel.
	ExecutionMode string
	Execute       func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error)
}

// ToolDiagnostic is a note the Harness or a tool attaches to a result:
// severity info, warn, or error, an optional code, and a message. The model
// sees them in a <harness> block after the content.
type ToolDiagnostic struct {
	Severity string `json:"severity"`
	Message  string `json:"message"`
	Code     string `json:"code,omitempty"`
}

type ToolResult struct {
	Content string `json:"content"`
	// Diagnostics follow the content for the model. The Harness's own error
	// results carry one error diagnostic with a code: tool_unavailable,
	// invalid_arguments, blocked, interrupted, restart_limit, aborted, or
	// abandoned.
	Diagnostics []ToolDiagnostic `json:"diagnostics,omitempty"`
	Details     json.RawMessage  `json:"details,omitempty"`
	IsError     bool             `json:"isError,omitempty"`
	Usage       Usage            `json:"usage,omitempty"`
	DurationMs  *int64           `json:"durationMs,omitempty"`
	Terminate   bool             `json:"terminate,omitempty"`
	Handoff     string           `json:"handoff,omitempty"`
}
type ModelRequest struct {
	Model         ModelRef   `json:"model"`
	SessionID     string     `json:"sessionId"`
	ThinkingLevel string     `json:"thinkingLevel,omitempty"`
	Messages      []Message  `json:"messages"`
	Tools         []ToolSpec `json:"tools,omitempty"`
}
type ModelResponse struct {
	Content   string     `json:"content"`
	Thinking  string     `json:"thinking,omitempty"`
	ToolCalls []ToolCall `json:"toolCalls,omitempty"`
	Usage     Usage      `json:"usage"`
	// StopReason is stop, length (the output limit cut the answer off), or
	// toolUse. Empty means toolUse with tool calls and stop without. A length
	// answer is an answer: tool calls it was cut off in never run.
	StopReason string `json:"stopReason,omitempty"`
	// ProviderData is copied to the committed assistant Message for later replay.
	ProviderData json.RawMessage `json:"providerData,omitempty"`
}

// Model implementations must honor cancellation. Delta callbacks persist progress
// before publishing it; returning an error from a callback must stop the request.
type Model interface {
	Generate(context.Context, ModelRequest, func(string) error) (ModelResponse, error)
}

type Section struct {
	Name   string
	Render func(context.Context, ConversationRecord, *Harness) (string, error)
}
type Hooks struct {
	BeforeRequest func(context.Context, *ModelRequest, *TaskRuntime) error
	AfterResponse func(context.Context, *ModelResponse, *TaskRuntime) error
	BeforeTool    func(context.Context, *ToolCall, *ToolRuntime) error
	AfterTool     func(context.Context, *ToolResult, *ToolRuntime) error
	AfterTools    func(context.Context, []ToolResult, *TaskRuntime) error
	OnYield       func(context.Context, ModelResponse, *TaskRuntime) (string, error)
	// AfterRun runs once a run answered, or every call of its tool round asked
	// to terminate, before the run's final boundary. The tasks it asks for are
	// created as the generation's children, and the run ends once they are
	// terminal; queued items stay queued meanwhile. A crash asks again: nothing
	// was created yet. A failing hook or rejected request is reported as
	// extension_error and skipped.
	AfterRun func(context.Context, RunSummary, *TaskRuntime) ([]RunEndTask, error)
}

// RunSummary says how a run ended, for AfterRun hooks. Read its history from
// First through Last.
type RunSummary struct {
	// Reason is answer, or terminate for a tool round whose every call asked
	// to terminate.
	Reason string `json:"reason"`
	// First is the run's first entry; the answer for a run that has none.
	First EntryID `json:"first"`
	// Answer is the answer, or the terminating round's tool-calling answer.
	Answer EntryID `json:"answer"`
	// Last is the run's last entry.
	Last EntryID `json:"last"`
}

// RunEndTask is a task an AfterRun hook asks for: an installed, non-built-in
// kind and its JSON input. It is created from the installed definition.
type RunEndTask struct {
	Kind  string
	Input json.RawMessage
}
type Extension struct {
	Name      string
	Tools     []ToolDefinition
	Sections  []Section
	Hooks     Hooks
	Tasks     []TaskDefinition
	WrapTools map[string]func(ToolDefinition) ToolDefinition
}
type Settings struct {
	// Retry is Pi Durable's settings.retry policy for generation and
	// compaction model attempts, merged field by field over its default.
	Retry RetrySettings
	// SteeringMode and FollowUpMode select how many queued steers or
	// follow-ups a boundary places: QueueOneAtATime (default) or QueueAll.
	SteeringMode string
	FollowUpMode string
	// AfterFailedRun decides what a run that fails (model_error, no_model,
	// interrupted, restart_limit, nothing_to_answer, turn_limit) does with the
	// inputs queued behind it: AfterFailedRunKeep (default) leaves them
	// queued until the next input or queued write, AfterFailedRunContinue
	// starts the next run with them, and AfterFailedRunWithdraw settles them
	// unanswered with reason run_failed.
	AfterFailedRun string
	// Progress paces presentation commits (upstream settings.progress).
	Progress ProgressSettings
	// ToolExecution "sequential" runs every tool round one call at a time;
	// "parallel" (default) starts all calls at once, unless a called tool's
	// ExecutionMode is sequential.
	ToolExecution string
	// RestartLimit ends a task cut off this many times in a row by crashes,
	// following Pi Durable's restartLimit number setting: values below 1 and NaN
	// select the default of 5, fractions round down, and math.Inf(1) never ends
	// a task. A clean Close is not a crash and is never counted.
	RestartLimit float64
}

// ProgressSettings paces commits of streamed progress. A nil field keeps
// upstream's default.
type ProgressSettings struct {
	// PartialIntervalMs: a streamed partial answer is committed to
	// pi.live.generation.message at most this often (default 100).
	PartialIntervalMs *float64
}

type HarnessOptions struct {
	Model               Model
	Registry            *Registry
	Settings            Settings
	Environment         func(ConversationRecord) (ExecutionEnv, error)
	ConversationCreated func(*Tx, ConversationID) error
	// Now is the Harness clock. Sleeping tasks store deadlines on it, so every
	// process that opens the same storage must use the same clock.
	Now func() time.Time
	// SetTimer arms the scheduler's timer for the earliest sleep and the
	// timers of TaskRuntime.SetTimer: call callback once, about delay from
	// now, and return a function that disarms it. Default time.AfterFunc.
	// Tests can pass fake timers.
	SetTimer func(callback func(), delay time.Duration) (stop func())
	// OnWake wakes the process for sleeping tasks instead of a SetTimer
	// timer. With it, the Harness arms no timer for sleeps: it offers Wake
	// after open and whenever Harness.Wake changes, from a goroutine that never
	// holds the Session line, and the host arms its own wakeup and calls
	// Resume when it fires. It must not block; a panic is reported as
	// host_error and the value is offered again at the next change.
	OnWake func(Wake)
	// OnReport receives best-effort FIFO diagnostics on an independent worker.
	// Shutdown does not join delivery. Handlers should return promptly.
	OnReport func(Report)
}
type ExecutionEnv interface {
	ReadFile(context.Context, string) ([]byte, error)
	WriteFile(context.Context, string, []byte) error
	Exec(context.Context, string, func(string) error) (ExecResult, error)
}
type ExecResult struct {
	ExitCode int
	Output   string
}
type View struct {
	Sequence     uint64                     `json:"sequence"`
	Conversation ConversationRecord         `json:"conversation"`
	Entries      []Entry                    `json:"entries"`
	Tasks        []TaskRecord               `json:"tasks"`
	Inbox        []SubmissionRecord         `json:"inbox"`
	Documents    map[string]json.RawMessage `json:"documents"`
}
