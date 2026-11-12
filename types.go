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
	// ErrReadOnly refuses writes and scheduling of a read-only Harness.
	ErrReadOnly = errors.New("durable: read-only Harness")
)

// RejectedError certifies a storage write was not applied. Like any storage
// error, it fails the Session (upstream: storage failures are final); a
// storage returns it when it knows nothing was written, which reopening can
// rely on.
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
	// base is the committed State a transaction candidate shares records
	// with; nil outside a transaction (see cow.go).
	base *State
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
	Instructions  *string  `json:"instructions,omitempty"`
	Cwd           string   `json:"cwd,omitempty"`
	// MaxTurns bounds a run's tool rounds; a run that reaches it ends
	// unanswered with turn_limit, and a continue resumes it. Zero: no bound.
	MaxTurns int `json:"maxTurns,omitempty"`
	// Compaction overrides Settings.Compaction for this conversation.
	Compaction *CompactionSettings `json:"compaction,omitempty"`
	// ModelTools filters the enabled tools the model may call: the tools
	// offered to it. It never widens; other tools can still call the ones it
	// leaves out. Nil offers every such tool except deferred ones.
	ModelTools *ModelTools `json:"modelTools,omitempty"`
}

// ModelTools selects the tools offered to the model (upstream modelTools).
// With Only set, exactly those names in that order, deferred ones too.
// Otherwise every model-callable tool except deferred ones and Remove, in
// registry order, followed by the deferred tools in Add that are not in
// Remove, in Add order; a tool result's AddTools appends to Add.
type ModelTools struct {
	Only   []string `json:"only,omitempty"`
	Remove []string `json:"remove,omitempty"`
	Add    []string `json:"add,omitempty"`
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
	// Edits omit or replace earlier entries' messages in model context
	// (upstream ContextEdit); the newest edit of a target wins.
	Edits     []ContextEdit `json:"edits,omitempty"`
	Sequence  uint64        `json:"sequence"`
	CreatedAt time.Time     `json:"createdAt"`
}

// ContextEdit changes what an earlier entry contributes to model context:
// Action "omit" contributes nothing, "replace" contributes Messages instead.
// Stored history is unchanged.
type ContextEdit struct {
	Target   EntryID   `json:"target"`
	Action   string    `json:"action"`
	Messages []Message `json:"messages,omitempty"`
}

// SystemSection is one named prompt section of a system message; a nil Text
// removes the section.
type SystemSection struct {
	Name string  `json:"name"`
	Text *string `json:"text"`
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
	// Images of a user or tool-result message, after its text.
	Images []ImageContent `json:"images,omitempty"`
	// A system message's prompt sections and tool changes (pi-ai
	// SystemMessage): Sections set or remove named sections in order, and
	// ToolsAdded and ToolsRemoved change the tools on offer from this point.
	// Replaying the system messages in order gives the current prompt and
	// tools; a model that takes no system messages mid-conversation gets them
	// folded into one leading message (CollapseSystemMessages).
	Sections     []SystemSection `json:"sections,omitempty"`
	ToolsAdded   []ToolSpec      `json:"toolsAdded,omitempty"`
	ToolsRemoved []string        `json:"toolsRemoved,omitempty"`
}

// ImageContent is an image in a message: base64 data and its MIME type,
// such as image/png. A model that takes no images sees a placeholder.
type ImageContent struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
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
	Type    string `json:"type"` // input, write, or continue
	Content string `json:"content,omitempty"`
	// Images of an input, after its content.
	Images    []ImageContent `json:"images,omitempty"`
	Entry     *Entry         `json:"entry,omitempty"`
	RequestID string         `json:"requestId,omitempty"`
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
	// Trace is the telemetry carrier of the admission span; absent without
	// propagation.
	Trace json.RawMessage `json:"trace,omitempty"`
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
	// Trace and Links are the task's telemetry carrier and creation-time
	// links (TaskOptions); absent without propagation.
	Trace json.RawMessage   `json:"trace,omitempty"`
	Links []json.RawMessage `json:"links,omitempty"`
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
	// Trace is the telemetry carrier the task's phase spans continue
	// (TelemetryPropagation). Empty inherits the creator's span; with
	// TraceRoot, the task starts at the adapter root with no carrier.
	Trace     json.RawMessage
	TraceRoot bool
	// Links are carriers linked to the spans of the task's first invocation.
	Links []json.RawMessage
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
	// Family makes the kind a family of documents keyed by a member name
	// within each scope (upstream defineDocFamily); InitialSeed creates an
	// absent member from the seed it is first read with.
	Family      bool
	InitialSeed func(seed json.RawMessage) any
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
	// Member is a family member's key; empty for a singleton.
	Member string `json:"member,omitempty"`
}

type ToolSpec struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	// ConstrainedSampling is pi-ai's Tool.constrainedSampling: false, or a
	// configuration object; part of the declaration.
	ConstrainedSampling json.RawMessage `json:"constrainedSampling,omitempty"`
}
type ToolDefinition struct {
	ToolSpec
	ReplaySafe bool
	// ExecutionMode "sequential" runs a round containing a call to this tool
	// one call at a time, in call order; empty runs calls in parallel.
	ExecutionMode string
	// Callers says who may call the tool: "model", "tools" (nested calls
	// through ToolRuntime.ExecuteTool), or both (empty, the default).
	Callers []string
	// OutputLimits bound the content of the tool's results; nil fields keep
	// the defaults of 50 KiB and 2000 lines, retaining the beginning.
	OutputLimits *OutputLimits
	// Deferred tools are not offered to the model until loaded: until
	// Agent.ModelTools names them, which a tool result's AddTools (from a
	// tool such as tool_search) writes. Other tools can call them any time.
	Deferred bool
	// StructuredOutputSchema, when set, is the JSON Schema of the
	// StructuredOutput programs get from a successful result; nested callers
	// otherwise get the result's content as a JSON string.
	StructuredOutputSchema json.RawMessage
	Execute                func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error)
}

// OutputLimits bound a tool result's content: MaxBytes and MaxLines (zero
// keeps the default), retaining the "head" (default) or the "tail".
type OutputLimits struct {
	MaxBytes int
	MaxLines int
	Retain   string
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
	// Images follow the content in the tool-result message.
	Images []ImageContent `json:"images,omitempty"`
	// Diagnostics follow the content for the model. The Harness's own error
	// results carry one error diagnostic with a code: tool_unavailable,
	// invalid_arguments, blocked, interrupted, restart_limit, aborted, or
	// abandoned.
	Diagnostics []ToolDiagnostic `json:"diagnostics,omitempty"`
	// StructuredOutput is the value for programs (nested callers) of a tool
	// with a StructuredOutputSchema, validated against it.
	StructuredOutput json.RawMessage `json:"structuredOutput,omitempty"`
	Details          json.RawMessage `json:"details,omitempty"`
	IsError          bool            `json:"isError,omitempty"`
	Usage            Usage           `json:"usage,omitempty"`
	DurationMs       *int64          `json:"durationMs,omitempty"`
	Terminate        bool            `json:"terminate,omitempty"`
	Handoff          string          `json:"handoff,omitempty"`
	// AddTools enables and offers these tools from the next request on: they
	// are appended to Agent.Tools and to Agent.ModelTools, loading deferred
	// tools. Applied when the round ends.
	AddTools []string `json:"addTools,omitempty"`
}
type ModelRequest struct {
	Model         ModelRef   `json:"model"`
	SessionID     string     `json:"sessionId"`
	ThinkingLevel string     `json:"thinkingLevel,omitempty"`
	Messages      []Message  `json:"messages"`
	Tools         []ToolSpec `json:"tools,omitempty"`
	// Identity is HarnessOptions.Identity, set as the request is sent.
	Identity *ClientIdentity `json:"identity,omitempty"`
	// Stream is Settings.Stream, set as the request is sent.
	Stream *StreamOptions `json:"stream,omitempty"`
}

// StreamOptions are curated pi-ai request options (upstream
// ConversationStreamOptions); zero fields keep the adapter defaults.
type StreamOptions struct {
	// Transport is the preferred transport where a provider has several;
	// the Pi bridge defaults to sse.
	Transport string `json:"transport,omitempty"`
	// TimeoutMs is the HTTP request timeout of providers that support one.
	TimeoutMs int64 `json:"timeoutMs,omitempty"`
	// WebsocketConnectTimeoutMs bounds a WebSocket handshake.
	WebsocketConnectTimeoutMs int64 `json:"websocketConnectTimeoutMs,omitempty"`
	// ThinkingBudgets are token budgets per thinking level.
	ThinkingBudgets map[string]int `json:"thinkingBudgets,omitempty"`
	// MaxRetries and MaxRetryDelayMs are provider retries inside one
	// attempt (default none: Durable retries attempts itself).
	MaxRetries      *int  `json:"maxRetries,omitempty"`
	MaxRetryDelayMs int64 `json:"maxRetryDelayMs,omitempty"`
	// Headers are extra request headers.
	Headers map[string]string `json:"headers,omitempty"`
	// Metadata is provider request metadata.
	Metadata json.RawMessage `json:"metadata,omitempty"`
	// CacheRetention is pi-ai's prompt cache retention: none, short, or long.
	CacheRetention string `json:"cacheRetention,omitempty"`
}

func (s StreamOptions) isZero() bool {
	return s.Transport == "" && s.TimeoutMs == 0 && s.WebsocketConnectTimeoutMs == 0 && len(s.ThinkingBudgets) == 0 &&
		s.MaxRetries == nil && s.MaxRetryDelayMs == 0 && len(s.Headers) == 0 && len(s.Metadata) == 0 && s.CacheRetention == ""
}

// ClientIdentity is pi-ai's ClientIdentity: how model requests identify the
// calling app to providers. Its fields are plain pi-ai JSON.
type ClientIdentity struct {
	// UserAgent is "short", "detailed" (the default), or {"custom": "..."}.
	UserAgent json.RawMessage `json:"userAgent,omitempty"`
	// Attribution is {"name", "url", "categories"} for provider attribution
	// headers; null turns off attribution.
	Attribution json.RawMessage `json:"attribution,omitempty"`
	// Version is the version in the detailed User-Agent.
	Version string `json:"version,omitempty"`
}
type ModelResponse struct {
	Content   string     `json:"content"`
	Thinking  string     `json:"thinking,omitempty"`
	ToolCalls []ToolCall `json:"toolCalls,omitempty"`
	Usage     Usage      `json:"usage"`
	// StopReason is stop, length (the output limit cut the answer off), or
	// toolUse. Empty means toolUse with tool calls and stop without. A length
	// answer is an answer: tool calls it was cut off in never run. Hooks see
	// a failed attempt as error, with ErrorMessage.
	StopReason   string `json:"stopReason,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	// ProviderData is copied to the committed assistant Message for later replay.
	ProviderData json.RawMessage `json:"providerData,omitempty"`
}

// Model implementations must honor cancellation. Delta callbacks report
// progress; returning an error from a callback must stop the request.
type Model interface {
	Generate(context.Context, ModelRequest, func(string) error) (ModelResponse, error)
}

// Section is a part of the agent's system prompt, rendered for every request:
// wrapped as <Name>\n...\n</Name>, or as is when Untagged (upstream
// tag: false). Render returns ErrOmitSection to leave the section out (upstream
// undefined); any text, "" included, is the section. A Render that fails is
// reported as extension_error and keeps the text the model was shown.
type Section struct {
	Name     string
	Untagged bool
	Render   func(context.Context, ConversationRecord, *Harness) (string, error)
}

// ErrOmitSection, returned by a Section's Render, leaves the section out of the
// prompt.
var ErrOmitSection = errors.New("durable: section omitted")

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
	// BeforeCompact runs when a compaction selected its range, before the
	// summary request. The first hook that decides wins: Decline completes
	// the compaction without a summary, and a Summary is placed instead of
	// requesting one. A failing hook is reported as extension_error.
	BeforeCompact func(context.Context, CompactionRequest, *TaskRuntime) (CompactionDecision, error)
}

// CompactionRequest is what a compaction is about to summarize.
type CompactionRequest struct {
	Reason       string // manual, threshold, or overflow
	Entries      []Entry
	Messages     []Message
	FirstKept    EntryID
	Instructions string
}

// CompactionDecision is a BeforeCompact hook's answer; the zero value
// decides nothing.
type CompactionDecision struct {
	Decline bool
	Summary string
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
	// Stream holds the pi-ai request options of every model request
	// (settings.stream); zero fields keep the adapter defaults.
	Stream StreamOptions
	// Extensions is the default extension selection of an agent that names
	// none (settings.extensions); nil selects every installed extension, in
	// install order.
	Extensions []string
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
	// Compaction is the Harness-wide compaction policy: threshold and
	// overflow compaction, and the range a summary keeps.
	Compaction CompactionSettings
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
	// OutputIntervalMs: running tool output is committed to its slot at
	// most this often (default 100); 0 commits each Output call at once.
	OutputIntervalMs *float64
	// PersistWithinMs: how long these progress commits, and other deferred
	// commits without their own WithinMs, may stay in memory before they are
	// stored (default 1,000; 0 stores them at once; math.Inf(1) stores them
	// only with the next stored commit or at close). A crash loses at most
	// about this much of an answer or of tool output.
	PersistWithinMs *float64
}

// CommitOptions ask for a commit's persistence (upstream CommitPersistence).
// A deferred commit that changes only existing conversation documents with
// latest history and the initial fork is adopted and shown at once, and its
// documents are stored later: with the next stored commit, within WithinMs
// (default Settings.Progress.PersistWithinMs), or at close; a crash loses
// them. Any other deferred commit is stored like an immediate one.
type CommitOptions struct {
	Deferred bool
	WithinMs *float64
}

type HarnessOptions struct {
	Model Model
	// ModelFunc, when set, returns the model at each use instead of Model
	// (upstream's function-valued HarnessOptions.models).
	ModelFunc func() Model
	Registry  *Registry
	Settings  Settings
	// SettingsFunc, when set, replaces Settings and is read at each use, as
	// upstream reads function-valued settings: a change applies to the next
	// decision that reads it. It must not block or call the Harness.
	SettingsFunc        func() Settings
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
	// Telemetry receives content-free spans; see Telemetry.
	Telemetry Telemetry
	// Identity is set on every model request through the Harness, including
	// those tools make through ToolRuntime.Model (pi-ai's ClientIdentity).
	Identity *ClientIdentity
	// ReadOnly opens for reading only, as a viewer of a session another
	// process drives. Open recovers nothing: a running task stays as committed.
	// Every commit with writes fails with ErrReadOnly before it reaches
	// storage, and scheduling never starts; reads and watches work. Wake is
	// always inactive and OnWake is never called.
	ReadOnly bool
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
