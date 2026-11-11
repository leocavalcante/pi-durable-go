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
	Aborted    bool       `json:"aborted,omitempty"`
	Usage      Usage      `json:"usage,omitempty"`
	DurationMs *int64     `json:"durationMs,omitempty"`
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

type SubmissionRequest struct {
	Type      string `json:"type"` // input or write
	Content   string `json:"content,omitempty"`
	Entry     *Entry `json:"entry,omitempty"`
	RequestID string `json:"requestId,omitempty"`
	WhenBusy  string `json:"whenBusy,omitempty"` // follow-up (default), steer, reject
}
type SubmissionRecord struct {
	ID           SubmissionID      `json:"id"`
	Conversation ConversationID    `json:"conversation"`
	Request      SubmissionRequest `json:"request"`
	Status       string            `json:"status"` // queued, running, done, unanswered
	Answer       EntryID           `json:"answer,omitempty"`
	Reason       string            `json:"reason,omitempty"`
	Detail       json.RawMessage   `json:"detail,omitempty"`
	CreatedAt    time.Time         `json:"createdAt"`
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
	Execute    func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error)
}
type ToolResult struct {
	Content    string          `json:"content"`
	Details    json.RawMessage `json:"details,omitempty"`
	IsError    bool            `json:"isError,omitempty"`
	Usage      Usage           `json:"usage,omitempty"`
	DurationMs *int64          `json:"durationMs,omitempty"`
	Terminate  bool            `json:"terminate,omitempty"`
	Handoff    string          `json:"handoff,omitempty"`
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
	RetryMax   int
	RetryDelay time.Duration
	MaxTurns   int // zero defaults to 100; prevents unbounded runaway calls
}
type HarnessOptions struct {
	Model               Model
	Registry            *Registry
	Settings            Settings
	Environment         func(ConversationRecord) (ExecutionEnv, error)
	ConversationCreated func(*Tx, ConversationID) error
	Now                 func() time.Time
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
