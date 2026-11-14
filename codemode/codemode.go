package codemode

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"unicode/utf16"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/js"
	"github.com/leocavalcante/pi-durable-go/tools"
)

// Durable code mode (upstream codemode.ts): a tool that runs model-written
// JavaScript in a sandbox, whose script survives a restart by deterministic
// replay. The VM only runs when the host hands it a result; given the same
// start data, clock, seed, and results in the same order, it makes the same
// calls with the same arguments. After a restart the replay-safe tool runs
// again, starts the script again from the stored start data, and hands it
// the results it got before, in the order it got them, from a journal
// written before each delivery. Nested tool calls reattach to their tasks by
// key ("c<VM call id>"), so they never run twice unless their own replay
// policy reruns them. Then the script continues live.
//
// A call is launched only once the count of calls issued in the drain that
// made it is committed, so every key an earlier attempt admitted is at most
// its last recorded count. A rerun refuses to replay when this module's
// version or the VM's fingerprint changed, when the deadline passed, or when
// the script diverges.

// ModuleVersion is the version of everything here that turns stored results
// into what a script sees; a call started under another version does not
// resume.
const ModuleVersion = 2

// ToolName is the code mode tool's name.
const ToolName = "codemode"

// Global is a function scripts call as a top-level global; its result is
// journaled. Spread passes the script's arguments as a list. A nil result is
// undefined.
type Global struct {
	Name    string
	Spread  bool
	Execute func(ctx context.Context, args json.RawMessage, r *durable.ToolRuntime) (json.RawMessage, error)
}

// Options configure the code mode tool and extension.
type Options struct {
	// Sandbox runs the scripts: a QuickJSSandbox in Go, or a NodeSandbox
	// with Node and an installed Pi. Nil is a shared QuickJSSandbox.
	Sandbox Sandbox
	// Namespaces group tools by name prefix, such as MCP servers, for the
	// codemode section and tool search.
	Namespaces []Namespace
	// InlineBudget is the estimated tokens the tool declarations in the
	// codemode section may use; nil is DefaultInlineBudget.
	InlineBudget *int
	// Models gives scripts models.*: the model catalog, classifiers, and
	// image generation. Nil leaves models.* out, as upstream's models: false.
	Models Models
	// Globals are host functions scripts call as globals.
	Globals []Global
}

// Documents scoped to the code mode call's task, retired with it.
var (
	// RunDocument holds the start data, the output directory, and the
	// decided outcome.
	RunDocument = durable.DocumentDefinition{Kind: "pi.codemode.run", Version: 1, Scope: "task",
		Initial: func() any { return map[string]any{} }}
	// JournalDocument holds one entry per delivered result.
	JournalDocument = durable.DocumentDefinition{Kind: "pi.codemode.journal", Version: 1, Scope: "task",
		Initial: func() any { return journalState{Entries: []journalEntry{}} }}
	// StoreDocument holds what scripts keep with store() and read with
	// load(), per conversation; a fork sees the values as of its entry.
	StoreDocument = durable.DocumentDefinition{Kind: "pi.codemode.store", Version: 1, Scope: "conversation",
		History: "rewindable", Fork: "asOf", Initial: func() any { return storeState{Values: map[string]json.RawMessage{}} }}
)

// declaredTool is one tool as the script sees it: tools.<JSName>.
// Structured: the tool has a structured output schema, so an error result
// with a value resolves with it. Hidden: an internal tool, which scripts
// reach only through a global.
type declaredTool struct {
	Name        string `json:"name"`
	JSName      string `json:"jsName"`
	Description string `json:"description"`
	Structured  bool   `json:"structured,omitempty"`
	Hidden      bool   `json:"hidden,omitempty"`
}

type startNamespace struct {
	Name         string   `json:"name"`
	Description  string   `json:"description,omitempty"`
	Instructions string   `json:"instructions,omitempty"`
	Tools        []string `json:"tools"`
}

type startGlobal struct {
	Name   string `json:"name"`
	Spread bool   `json:"spread"`
}

// startData is what a call starts with; a rerun starts the VM from exactly
// this.
type startData struct {
	Version   int     `json:"version"`
	StartedAt float64 `json:"startedAt"`
	Seed      uint32  `json:"seed"`
	// Fingerprint is the VM's, recorded by the first run before any call is
	// admitted.
	Fingerprint string `json:"fingerprint,omitempty"`
	// Deadline is startedAt + timeout_ms when the script set one.
	Deadline        *float64                   `json:"deadline,omitempty"`
	Tools           []declaredTool             `json:"tools"`
	Namespaces      []startNamespace           `json:"namespaces"`
	Globals         []startGlobal              `json:"globals"`
	MaxOutputTokens float64                    `json:"maxOutputTokens"`
	Store           map[string]json.RawMessage `json:"store"`
}

type runState struct {
	Start *startData `json:"start,omitempty"`
	// OutputDir holds the result's files, created once: a rerun writes the
	// same files again.
	OutputDir string `json:"outputDir,omitempty"`
	// Result is the outcome, decided once.
	Result *durable.ToolResult `json:"result,omitempty"`
}

type journalEntry struct {
	// ID is the VM's call ID; its result is the entry's position + 1.
	ID int `json:"id"`
	// At is the VM clock while it processes this result.
	At float64 `json:"at"`
	// IssuedAfter counts the calls the VM had issued after processing this
	// result; absent until acknowledged.
	IssuedAfter *int `json:"issuedAfter,omitempty"`
	// Global is a global's call and outcome; a tool's result is its nested
	// call's.
	Global *journalGlobal `json:"global,omitempty"`
}

type journalGlobal struct {
	Name    string          `json:"name"`
	Args    json.RawMessage `json:"args,omitempty"`
	Outcome Outcome         `json:"outcome"`
}

type journalState struct {
	Entries []journalEntry `json:"entries"`
	// FirstIssued counts the calls the VM had issued after the script's
	// first run, before any result; absent until acknowledged.
	FirstIssued *int `json:"firstIssued,omitempty"`
}

type storeState struct {
	Values map[string]json.RawMessage `json:"values"`
}

// toolSchema is the code mode tool's parameters.
var toolSchema = json.RawMessage(`{"type":"object","properties":{"code":{"type":"string","description":"Raw JavaScript source."}},"required":["code"]}`)

// toolLimits bound outputs by the script's own token budget; the Harness's
// limits only stop a runaway.
var toolLimits = outputLimits{maxBytes: 4 * 1024 * 1024, maxLines: 100_000}

// discoveryGlobals find tools, answered from the start data, so a replay
// sees what the first run saw.
var discoveryGlobals = []string{"searchTools", "describeTool", "describeNamespace"}

const (
	defaultSearchLimit = 8
	argsPreviewChars   = 200
	errorPreviewChars  = 500
)

// Tool is the code mode tool. Scripts call the conversation's callable
// tools as tools.<name>(args).
func Tool(options Options) durable.ToolDefinition {
	sampling, _ := json.Marshal(map[string]any{"type": "grammar", "variants": map[string]string{"openai_lark": SourceGrammar}})
	return durable.ToolDefinition{
		ToolSpec: durable.ToolSpec{Name: ToolName, Description: Description(options.Models != nil), Parameters: toolSchema,
			// Capable models write the script as raw text instead of a
			// JSON-escaped string.
			ConstrainedSampling: sampling},
		OutputLimits: &durable.OutputLimits{MaxBytes: toolLimits.maxBytes, MaxLines: toolLimits.maxLines},
		Callers:      []string{"model"},
		ReplaySafe:   true,
		Execute: func(ctx context.Context, args json.RawMessage, r *durable.ToolRuntime) (durable.ToolResult, error) {
			var input struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(args, &input); err != nil {
				return durable.ToolResult{}, err
			}
			return newCall(options, r, ctx).execute(input.Code)
		},
	}
}

// Extension is the codemode extension: the code mode tool, and the codemode
// prompt section listing the tools scripts can call.
func Extension(options Options) durable.Extension {
	definitions := []durable.ToolDefinition{Tool(options)}
	if options.Models != nil {
		definitions = append(definitions, modelTools(options.Models)...)
	}
	budget := DefaultInlineBudget
	if options.InlineBudget != nil {
		budget = *options.InlineBudget
	}
	return durable.Extension{
		Name:  "codemode",
		Tools: definitions,
		Sections: []durable.Section{{Name: "codemode", Render: func(_ context.Context, input durable.PromptInput) (string, error) {
			// Deferred tools are found with searchTools().
			var callable []durable.ToolDefinition
			for _, tool := range input.Agent.Callable {
				if !strings.HasPrefix(tool.Name, InternalPrefix) && !tool.Deferred {
					callable = append(callable, tool)
				}
			}
			section, ok := Section(callable, options.Namespaces, budget)
			if !ok {
				return "", durable.ErrOmitSection
			}
			return section, nil
		}}},
	}
}

// outcomeOf is what a script gets for a nested call: a tool with a schema
// resolves with its value, also on an error result that carries one; a
// schema-less tool resolves with its text output. Any other error rejects,
// with its text: a schema-less tool's output, else its error diagnostics.
func outcomeOf(name string, structured bool, result durable.NestedToolResult) Outcome {
	var value json.RawMessage
	if structured {
		if len(result.StructuredOutput) > 0 {
			value = result.StructuredOutput
		}
	} else {
		value, _ = json.Marshal(textOf(result.StructuredOutput))
	}
	if !result.IsError || structured && value != nil {
		return Outcome{OK: true, Value: value}
	}
	message := ""
	if !structured {
		message = textOf(result.StructuredOutput)
	}
	var errors []string
	for _, diagnostic := range result.Diagnostics {
		if diagnostic.Severity == "error" {
			errors = append(errors, diagnostic.Message)
		}
	}
	if message == "" {
		message = strings.Join(errors, "\n")
	}
	if message == "" {
		message = "Tool " + name + " failed"
	}
	outcome := Outcome{OK: false, Message: message}
	if value != nil {
		outcome.Properties = map[string]json.RawMessage{"structuredOutput": value}
	}
	return outcome
}

// textOf is the text of a schema-less tool's collapsed output: a string, one
// image, or an array of text and image blocks.
func textOf(value json.RawMessage) string {
	var text string
	if json.Unmarshal(value, &text) == nil {
		return text
	}
	var items []json.RawMessage
	if json.Unmarshal(value, &items) != nil {
		return ""
	}
	var texts []string
	for _, raw := range items {
		var block struct {
			Type string  `json:"type"`
			Text *string `json:"text"`
		}
		if json.Unmarshal(raw, &block) == nil && block.Type == "text" {
			if block.Text != nil {
				texts = append(texts, *block.Text)
			} else {
				texts = append(texts, "undefined")
			}
		}
	}
	return strings.Join(texts, "\n")
}

func errorResult(message string) durable.ToolResult {
	return durable.ToolResult{Content: message, IsError: true}
}

// cut keeps at most max UTF-16 code units of text, ending in "...".
func cut(text string, max int) string {
	units := utf16.Encode([]rune(text))
	if len(units) <= max {
		return text
	}
	return string(utf16.Decode(units[:max-3])) + "..."
}

func describeCall(call HostCall) string {
	prefix := ""
	if call.Target == "tool" {
		prefix = "tools."
	}
	args := ""
	if len(call.Args) > 0 {
		args = js.StringifyRaw(call.Args)
	}
	return prefix + call.Name + "(" + args + ")"
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// asToolCall is a script's call of models.classify() or
// models.generateImages() as the call of its internal tool.
func asToolCall(call HostCall) HostCall {
	if call.Target != "global" {
		return call
	}
	tool, ok := modelCalls[call.Name]
	if !ok {
		return call
	}
	var list []json.RawMessage
	_ = json.Unmarshal(call.Args, &list)
	argument := func(i int) json.RawMessage {
		if i < len(list) && len(list[i]) > 0 {
			return list[i]
		}
		return json.RawMessage("null")
	}
	args, _ := json.Marshal(struct {
		Model   json.RawMessage `json:"model"`
		Context json.RawMessage `json:"context"`
	}{argument(0), argument(1)})
	return HostCall{ID: call.ID, Target: "tool", Name: tool, Args: args}
}

// loop runs the call's continuations one at a time, as upstream's single
// JavaScript thread runs its promise callbacks.
type loop struct {
	mu     sync.Mutex
	queue  []func()
	closed bool
	signal chan struct{}
}

func (l *loop) post(fn func()) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.queue = append(l.queue, fn)
	l.mu.Unlock()
	select {
	case l.signal <- struct{}{}:
	default:
	}
}

func (l *loop) drain() {
	for {
		l.mu.Lock()
		if len(l.queue) == 0 {
			l.mu.Unlock()
			return
		}
		fn := l.queue[0]
		l.queue = l.queue[1:]
		l.mu.Unlock()
		fn()
	}
}

func (l *loop) close() {
	l.mu.Lock()
	l.closed = true
	l.queue = nil
	l.mu.Unlock()
}

type queued struct {
	id      int
	at      float64
	outcome Outcome
	global  *journalGlobal
}

type ack struct{ seq, issued int }

// call is one execution of the code mode tool: a first run, or a rerun after
// a restart that replays the journal. Its state belongs to its loop.
type call struct {
	options Options
	r       *durable.ToolRuntime
	ctx     context.Context
	loop    *loop
	run     Run
	start   startData
	// finished: the loop has the run's result. The events the VM sent
	// before it are handled first, as upstream handles them before its
	// result settles; Run.Finished could be true already.
	finished bool
	// aborted: the call aborted its VM (upstream controller.signal.aborted).
	aborted      bool
	abortMessage string
	// replayed is the journal as this attempt found it.
	replayed      []journalEntry
	journalLength int
	nextReplayed  int
	replaying     bool
	// rerun: an earlier attempt ran this call, so its nested call keys may
	// already exist.
	rerun bool
	// released: live calls may launch.
	released      bool
	firstIssued   *int
	durableIssued int
	waiting       []HostCall
	replayIndex   map[int]int
	issued        int
	ready         map[int]Outcome
	held          []HostCall
	queue         []queued
	acks          []ack
	committing    bool
	// ended is an outcome the call decided other than the script's own.
	ended   *durable.ToolResult
	failure error
	// models.classify() and models.generateImages() calls in flight, and
	// those waiting for a slot.
	modelCalls      int
	modelWaiters    []func()
	generatedImages int
	rows            map[int]*NestedCall
	gateOpen        bool
	gateWaiters     []func()
	// outputDir caches the run's output directory for this attempt.
	outputDir     string
	outputDirErr  error
	outputDirDone bool
}

func newCall(options Options, r *durable.ToolRuntime, ctx context.Context) *call {
	return &call{options: options, r: r, ctx: ctx, loop: &loop{signal: make(chan struct{}, 1)},
		replayIndex: map[int]int{}, ready: map[int]Outcome{}, rows: map[int]*NestedCall{}}
}

func (c *call) taskID() uint64 { return uint64(c.r.ID) }

func (c *call) now() float64 { return float64(c.r.Now().UnixMilli()) }

// async runs work off the loop, then its continuation on the loop.
func (c *call) async(work func() (any, error), then func(any, error)) {
	go func() {
		value, err := work()
		c.loop.post(func() { then(value, err) })
	}()
}

// afterGate runs fn once the VM's fingerprint is checked or recorded.
func (c *call) afterGate(fn func()) {
	if c.gateOpen {
		c.loop.post(fn)
		return
	}
	c.gateWaiters = append(c.gateWaiters, fn)
}

func (c *call) openGate() {
	c.gateOpen = true
	for _, fn := range c.gateWaiters {
		c.loop.post(fn)
	}
	c.gateWaiters = nil
}

func readJSON[T any](tx *durable.Tx, def durable.DocumentDefinition, id uint64) (T, error) {
	var value T
	raw, err := tx.Document(def, id)
	if err != nil {
		return value, err
	}
	err = json.Unmarshal(raw, &value)
	return value, err
}

func writeJSON(tx *durable.Tx, def durable.DocumentDefinition, id uint64, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.PutDocument(def, id, data)
}

func randomSeed() uint32 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return binary.LittleEndian.Uint32(b[:])
}

func (c *call) execute(source string) (durable.ToolResult, error) {
	code, sourceOptions, err := ParseSource(source)
	if err != nil {
		return durable.ToolResult{}, err
	}
	agent, err := c.r.Agent(c.ctx)
	if err != nil {
		return durable.ToolResult{}, err
	}
	declared := make([]declaredTool, 0, len(agent.Callable))
	var callable []durable.ToolDefinition
	for _, tool := range agent.Callable {
		internal := strings.HasPrefix(tool.Name, InternalPrefix)
		declared = append(declared, declaredTool{Name: tool.Name, JSName: Identifier(tool.Name), Description: sampleOf(tool),
			Structured: len(tool.StructuredOutputSchema) > 0, Hidden: internal})
		if !internal {
			callable = append(callable, tool)
		}
	}
	var globals []startGlobal
	for _, name := range discoveryGlobals {
		globals = append(globals, startGlobal{Name: name, Spread: true})
	}
	if c.options.Models != nil {
		for _, name := range append(slices.Clone(catalogGlobals), "models.classify", "models.generateImages") {
			globals = append(globals, startGlobal{Name: name, Spread: true})
		}
	}
	for _, global := range c.options.Globals {
		globals = append(globals, startGlobal{Name: global.Name, Spread: global.Spread})
	}
	namespaces := []startNamespace{}
	for _, tool := range callable {
		namespace := namespaceOf(tool.Name, c.options.Namespaces)
		if namespace == nil {
			continue
		}
		index := slices.IndexFunc(namespaces, func(entry startNamespace) bool { return entry.Name == namespace.Name })
		if index == -1 {
			namespaces = append(namespaces, startNamespace{Name: namespace.Name, Description: namespace.Description,
				Instructions: namespace.Instructions, Tools: []string{}})
			index = len(namespaces) - 1
		}
		namespaces[index].Tools = append(namespaces[index].Tools, tool.Name)
	}
	now := c.now()
	seed := randomSeed()
	var done *durable.ToolResult
	var start startData
	var journal journalState
	rerun := false
	err = c.r.CommitChange(c.ctx, func(tx *durable.Tx) error {
		done, rerun = nil, false
		run, err := readJSON[runState](tx, RunDocument, c.taskID())
		if err != nil {
			return err
		}
		if run.Result != nil {
			done = run.Result
			return nil
		}
		rerun = run.Start != nil
		if run.Start == nil {
			store, err := readJSON[storeState](tx, StoreDocument, uint64(c.r.ConversationID))
			if err != nil {
				return err
			}
			if store.Values == nil {
				store.Values = map[string]json.RawMessage{}
			}
			maxOutputTokens := float64(DefaultMaxOutputTokens)
			if sourceOptions.MaxOutputTokens != nil {
				maxOutputTokens = *sourceOptions.MaxOutputTokens
			}
			run.Start = &startData{Version: ModuleVersion, StartedAt: now, Seed: seed, Tools: declared,
				Namespaces: namespaces, Globals: globals, MaxOutputTokens: maxOutputTokens, Store: store.Values}
			if sourceOptions.TimeoutMs != nil {
				deadline := now + *sourceOptions.TimeoutMs
				run.Start.Deadline = &deadline
			}
			if err := writeJSON(tx, RunDocument, c.taskID(), run); err != nil {
				return err
			}
		}
		start = *run.Start
		journal, err = readJSON[journalState](tx, JournalDocument, c.taskID())
		return err
	})
	if err != nil {
		return durable.ToolResult{}, err
	}
	if done != nil {
		return *done, nil
	}
	c.start = start
	c.replayed = journal.Entries
	for index, entry := range journal.Entries {
		c.replayIndex[entry.ID] = index
	}
	c.journalLength = len(journal.Entries)
	c.replaying = len(journal.Entries) > 0
	c.rerun = rerun
	c.firstIssued = journal.FirstIssued
	if journal.FirstIssued != nil {
		c.durableIssued = *journal.FirstIssued
	}
	for _, entry := range journal.Entries {
		if entry.IssuedAfter != nil {
			c.durableIssued = max(c.durableIssued, *entry.IssuedAfter)
		}
	}

	if c.start.Version != ModuleVersion {
		return c.decide(errorResult(fmt.Sprintf("The script cannot resume: it started under code mode version %d, this is %d.", c.start.Version, ModuleVersion)))
	}
	var remaining *float64
	if c.start.Deadline != nil {
		left := *c.start.Deadline - c.now()
		if left <= 0 {
			return c.decide(errorResult("Script timed out: its deadline passed while it was interrupted."))
		}
		remaining = &left
	}

	var visible []DeclaredTool
	for _, tool := range c.start.Tools {
		if !tool.Hidden {
			visible = append(visible, DeclaredTool{Name: tool.Name, JSName: tool.JSName, Description: tool.Description})
		}
	}
	declaredGlobals := make([]DeclaredGlobal, len(c.start.Globals))
	for i, global := range c.start.Globals {
		declaredGlobals[i] = DeclaredGlobal{Name: global.Name, Spread: global.Spread}
	}
	if c.ctx.Err() != nil {
		c.aborted = true
	}
	sandbox := c.options.Sandbox
	if sandbox == nil {
		sandbox = defaultSandbox
	}
	run, err := sandbox.Start(code, RunOptions{
		StartedAt: c.start.StartedAt, Seed: c.start.Seed, Tools: visible, Globals: declaredGlobals,
		Store: c.start.Store, TimeoutMs: remaining,
		OnStarted: func(fingerprint string) { c.loop.post(func() { c.checkFingerprint(fingerprint) }) },
		OnCall:    func(made HostCall) { c.loop.post(func() { c.onCall(made) }) },
		OnIdle:    func(seq, issued int) { c.loop.post(func() { c.onIdle(seq, issued) }) },
	})
	if err != nil {
		return durable.ToolResult{}, err
	}
	c.run = run
	if c.aborted {
		run.Abort("Execution aborted")
	}
	result := c.wait()
	c.loop.close()
	if c.failure != nil {
		return durable.ToolResult{}, c.failure
	}
	if c.ended != nil {
		return *c.ended, nil
	}
	if err := c.ctx.Err(); err != nil {
		return durable.ToolResult{}, err
	}
	// Aborted, but not by this call: the host closed the sandbox, for example
	// while shutting down before closing the Harness. That decides nothing
	// about the script: store nothing and end with this invocation, so a
	// later attempt replays it.
	if !result.OK && result.Error != nil && result.Error.Kind == "aborted" && !c.aborted {
		<-c.ctx.Done()
		return durable.ToolResult{}, c.ctx.Err()
	}
	// A replay that ends where the live run went on is a divergence.
	if during := result.During; c.rerun && during != nil && during.Seq <= len(c.replayed) {
		var continued bool
		if during.Seq == 0 {
			continued = len(c.replayed) > 0 || c.firstIssued != nil
		} else {
			continued = c.replayed[during.Seq-1].IssuedAfter != nil
		}
		for _, entry := range c.replayed[during.Seq:] {
			continued = continued || entry.ID > during.Issued
		}
		if continued {
			return c.decide(errorResult(fmt.Sprintf("The script took a different path when resumed after a restart: it ended after result %d, where it went on before.", during.Seq)))
		}
	}
	return c.complete(result)
}

// wait runs the loop until the VM has its result, aborting the VM once the
// call's context ends.
func (c *call) wait() Result {
	results := make(chan Result, 1)
	go func() {
		result, _ := c.run.Wait(context.Background())
		results <- result
	}()
	done := c.ctx.Done()
	for {
		select {
		case <-c.loop.signal:
			c.loop.drain()
		case result := <-results:
			c.loop.drain()
			c.finished = true
			return result
		case <-done:
			done = nil
			c.abortVM("Execution aborted")
		}
	}
}

// abortVM ends the VM with an error the script cannot catch.
func (c *call) abortVM(message string) {
	if c.aborted && c.run != nil {
		return
	}
	c.aborted, c.abortMessage = true, message
	if c.run != nil {
		c.run.Abort(message)
	}
}

// withDetails is a decided outcome with the call's details.
func (c *call) withDetails(decided durable.ToolResult) durable.ToolResult {
	details, _ := json.Marshal(c.details())
	decided.Details = details
	return decided
}

// store keeps a decided outcome unless one is stored.
func (c *call) store(result durable.ToolResult) error {
	return c.r.CommitChange(c.ctx, func(tx *durable.Tx) error {
		run, err := readJSON[runState](tx, RunDocument, c.taskID())
		if err != nil {
			return err
		}
		if run.Result != nil {
			return nil
		}
		run.Result = &result
		return writeJSON(tx, RunDocument, c.taskID(), run)
	})
}

// decide stores an outcome the call decided and returns it.
func (c *call) decide(decided durable.ToolResult) (durable.ToolResult, error) {
	result := c.withDetails(decided)
	if err := c.store(result); err != nil {
		return durable.ToolResult{}, err
	}
	return result, nil
}

// diverge ends the call with message: it stores the outcome, then stops the
// VM with an error the script cannot catch.
func (c *call) diverge(message string) {
	if c.ended != nil || c.failure != nil {
		return
	}
	result := errorResult("The script took a different path when resumed after a restart: " + message)
	c.ended = &result
	stored := c.withDetails(result)
	c.async(func() (any, error) { return nil, c.store(stored) }, func(_ any, err error) {
		if err != nil {
			c.fail(err)
			return
		}
		c.abortVM(message)
	})
}

// stopped: nothing more may happen; the run has its outcome, or the call
// decided or failed.
func (c *call) stopped() bool {
	return c.ended != nil || c.failure != nil || c.finished
}

func (c *call) fail(err error) {
	if c.failure != nil {
		return
	}
	c.failure = err
	c.abortVM(errorMessage(err))
}

// complete stores the script's own outcome with its store writes in one
// transaction.
func (c *call) complete(result Result) (durable.ToolResult, error) {
	details := c.details()
	items, fullOutputPath, err := formatResult(result, formatOptions{generatedImages: c.generatedImages,
		wallMs: c.now() - c.start.StartedAt, maxOutputTokens: c.start.MaxOutputTokens, calls: details.Calls,
		limits: toolLimits, write: c.outputFiles()})
	if err != nil {
		return durable.ToolResult{}, err
	}
	content, images, offsets := toolResultContent(items)
	details.FullOutputPath = fullOutputPath
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return durable.ToolResult{}, err
	}
	tool := durable.ToolResult{Content: content, Images: images, ImageOffsets: offsets, IsError: !result.OK, Details: detailsJSON}
	var stored *durable.ToolResult
	err = c.r.CommitChange(c.ctx, func(tx *durable.Tx) error {
		stored = nil
		run, err := readJSON[runState](tx, RunDocument, c.taskID())
		if err != nil {
			return err
		}
		if run.Result != nil {
			stored = run.Result
			return nil
		}
		run.Result = &tool
		if err := writeJSON(tx, RunDocument, c.taskID(), run); err != nil {
			return err
		}
		if writes := result.StoreWrites; result.OK && (len(writes.Set) > 0 || len(writes.Delete) > 0) {
			store, err := readJSON[storeState](tx, StoreDocument, uint64(c.r.ConversationID))
			if err != nil {
				return err
			}
			if store.Values == nil {
				store.Values = map[string]json.RawMessage{}
			}
			for _, key := range writes.Delete {
				delete(store.Values, key)
			}
			for key, value := range writes.Set {
				store.Values[key] = value
			}
			return writeJSON(tx, StoreDocument, uint64(c.r.ConversationID), store)
		}
		return nil
	})
	if err != nil {
		return durable.ToolResult{}, err
	}
	if stored != nil {
		return *stored, nil
	}
	return tool, nil
}

// outputFiles writes the result's files under fixed names into the run's
// output directory, so a rerun overwrites them.
func (c *call) outputFiles() outputFile {
	return func(name string, data []byte) (string, string) {
		env, err := c.r.Env()
		if err != nil || env == nil {
			return "", "no execution environment"
		}
		if !c.outputDirDone {
			c.outputDir, c.outputDirErr = c.makeOutputDir(env)
			c.outputDirDone = true
		}
		if c.outputDirErr != nil {
			return "", errorMessage(c.outputDirErr)
		}
		path := c.outputDir + "/" + name
		if err := env.WriteFile(c.ctx, path, data); err != nil {
			return "", errorMessage(err)
		}
		return path, ""
	}
}

// makeOutputDir is the run's output directory: the recorded one, else a new
// temp directory, recorded before any file is written.
func (c *call) makeOutputDir(env durable.ExecutionEnv) (string, error) {
	var recorded string
	if err := c.r.CommitChange(c.ctx, func(tx *durable.Tx) error {
		run, err := readJSON[runState](tx, RunDocument, c.taskID())
		recorded = run.OutputDir
		return err
	}); err != nil {
		return "", err
	}
	if recorded != "" {
		return recorded, nil
	}
	temp, ok := env.(durable.TempDirEnv)
	if !ok {
		return "", errors.New("the execution environment cannot create temporary directories")
	}
	created, err := temp.CreateTempDir(c.ctx, "pi-codemode-")
	if err != nil {
		return "", err
	}
	var dir string
	err = c.r.CommitChange(c.ctx, func(tx *durable.Tx) error {
		run, err := readJSON[runState](tx, RunDocument, c.taskID())
		if err != nil {
			return err
		}
		if run.OutputDir == "" {
			run.OutputDir = created
			if err := writeJSON(tx, RunDocument, c.taskID(), run); err != nil {
				return err
			}
		}
		dir = run.OutputDir
		return nil
	})
	return dir, err
}

func (c *call) checkFingerprint(fingerprint string) {
	if recorded := c.start.Fingerprint; recorded != "" {
		if recorded != fingerprint {
			ended := errorResult(fmt.Sprintf("The script cannot resume: the sandbox changed since it started (%s -> %s).", recorded, fingerprint))
			c.ended = &ended
			stored := c.withDetails(ended)
			c.async(func() (any, error) { return nil, c.store(stored) }, func(_ any, err error) {
				if err != nil {
					c.fail(err)
				}
				c.abortVM("Sandbox fingerprint changed")
			})
			return
		}
		c.afterFingerprint()
		return
	}
	c.async(func() (any, error) {
		return nil, c.r.CommitChange(c.ctx, func(tx *durable.Tx) error {
			run, err := readJSON[runState](tx, RunDocument, c.taskID())
			if err != nil || run.Start == nil {
				return err
			}
			run.Start.Fingerprint = fingerprint
			return writeJSON(tx, RunDocument, c.taskID(), run)
		})
	}, func(_ any, err error) {
		if err != nil {
			c.fail(err)
			return
		}
		c.afterFingerprint()
	})
}

// afterFingerprint opens the gate: nothing was delivered or admitted before.
// A rerun without a journal releases its calls at the script's first
// acknowledgment instead.
func (c *call) afterFingerprint() {
	c.openGate()
	if c.replaying {
		c.deliverReplayed()
	} else if !c.rerun {
		c.release()
	}
}

func (c *call) onCall(made HostCall) {
	if c.stopped() {
		return
	}
	call := asToolCall(made)
	if call.Target == "tool" {
		c.started(call)
	}
	c.issued = max(c.issued, call.ID)
	index := -1
	if c.replaying {
		if i, ok := c.replayIndex[call.ID]; ok {
			index = i
		}
	}
	if index == -1 {
		if c.released {
			c.waiting = append(c.waiting, call)
			c.launchCovered()
		} else {
			c.held = append(c.held, call)
		}
		return
	}
	entry := c.replayed[index]
	if entry.Global != nil {
		if call.Target != "global" || call.Name != entry.Global.Name || !sameJSON(call.Args, entry.Global.Args) {
			c.diverge(fmt.Sprintf("call %d is %s, but was a call to %s", call.ID, describeCall(call), entry.Global.Name))
			return
		}
		c.ready[call.ID] = entry.Global.Outcome
		c.afterGate(c.deliverReplayed)
		return
	}
	if call.Target != "tool" {
		c.diverge(fmt.Sprintf("call %d is %s, but was a tool call", call.ID, describeCall(call)))
		return
	}
	c.afterGate(func() {
		c.callTool(call, func(outcome Outcome, err error) {
			if err == nil {
				c.ready[call.ID] = outcome
				c.deliverReplayed()
				return
			}
			if c.stopped() {
				return
			}
			if c.ctx.Err() != nil {
				c.fail(err)
			} else {
				// The key names another tool or other arguments.
				c.diverge(fmt.Sprintf("call %d is %s: %s", call.ID, describeCall(call), errorMessage(err)))
			}
		})
	})
}

// deliver hands the VM a result; a delivery the sandbox refuses is a bug.
func (c *call) deliver(seq int, now float64, id int, outcome Outcome) {
	if err := c.run.Deliver(seq, now, id, outcome); err != nil {
		c.fail(err)
	}
}

// deliverReplayed delivers journaled results in journal order, each once the
// VM has issued its call.
func (c *call) deliverReplayed() {
	for c.nextReplayed < len(c.replayed) && !c.stopped() {
		entry := c.replayed[c.nextReplayed]
		outcome, ok := c.ready[entry.ID]
		if entry.ID > c.issued || !ok {
			return
		}
		delete(c.ready, entry.ID)
		c.nextReplayed++
		c.deliver(c.nextReplayed, entry.At, entry.ID, outcome)
	}
}

func (c *call) setAck(seq, issued int) {
	for i := range c.acks {
		if c.acks[i].seq == seq {
			c.acks[i].issued = issued
			return
		}
	}
	c.acks = append(c.acks, ack{seq, issued})
}

func (c *call) onIdle(seq, issued int) {
	if c.stopped() {
		return
	}
	// A count an earlier attempt recorded must match; a missing one is
	// recorded now.
	if c.rerun && seq <= len(c.replayed) {
		var recorded *int
		if seq == 0 {
			recorded = c.firstIssued
		} else {
			recorded = c.replayed[seq-1].IssuedAfter
		}
		if recorded != nil && *recorded != issued {
			if seq == 0 {
				c.diverge(fmt.Sprintf("its first run made %d calls, where it had made %d", issued, *recorded))
			} else {
				c.diverge(fmt.Sprintf("after result %d it had made %d calls, where it had made %d", seq, issued, *recorded))
			}
			return
		}
	}
	c.setAck(seq, issued)
	if !c.replaying {
		// A rerun without a journal: every call an earlier attempt could have
		// admitted is made by now.
		if seq == 0 && c.rerun {
			c.release()
		}
		// Committed at once when the script has processed everything
		// delivered: a script waiting on a long call is where a restart
		// usually finds it. Otherwise with the next commit.
		if seq == c.journalLength {
			c.pump()
		}
		return
	}
	// Caught up: the VM processed everything delivered and waits.
	if seq != c.nextReplayed {
		return
	}
	if seq < len(c.replayed) {
		if next := c.replayed[seq]; next.ID > issued {
			c.diverge(fmt.Sprintf("it waits without making call %d, which it made before", next.ID))
		}
		return
	}
	// The last replayed result is acknowledged: continue live, admitting the
	// calls the journal did not have once their count is committed.
	c.replaying = false
	c.pump()
	c.release()
}

// release lets the held calls launch, and later live calls, each once its
// issued count is committed. On a rerun, every held call is first checked
// against the call an earlier attempt admitted under its key, and a
// divergence launches none.
func (c *call) release() {
	c.afterGate(func() {
		if c.stopped() || c.released {
			return
		}
		calls := c.held
		c.held = nil
		var check func(i int)
		check = func(i int) {
			if c.rerun && i < len(calls) {
				call := calls[i]
				c.async(func() (any, error) { return c.earlier(call) }, func(value any, err error) {
					if err != nil {
						c.fail(err)
						return
					}
					if c.stopped() {
						return
					}
					if other, _ := value.(string); other != "" {
						c.diverge(fmt.Sprintf("call %d is %s, but was %s", call.ID, describeCall(call), other))
						return
					}
					check(i + 1)
				})
				return
			}
			c.released = true
			c.waiting = append(c.waiting, calls...)
			c.waiting = append(c.waiting, c.held...)
			c.held = nil
			c.launchCovered()
		}
		check(0)
	})
}

// launchCovered launches the waiting calls whose issued count is committed.
func (c *call) launchCovered() {
	if !c.released || c.stopped() {
		return
	}
	var covered, rest []HostCall
	for _, call := range c.waiting {
		if call.ID <= c.durableIssued {
			covered = append(covered, call)
		} else {
			rest = append(rest, call)
		}
	}
	if len(covered) == 0 {
		return
	}
	c.waiting = rest
	for _, call := range covered {
		c.launch(call)
	}
}

// earlier is the call an earlier attempt made under this call's key when it
// is another one: another target, tool, or arguments; "" when it is the same
// or there is none.
func (c *call) earlier(call HostCall) (string, error) {
	record, found, err := c.r.NestedCall(c.ctx, fmt.Sprintf("c%d", call.ID))
	if err != nil || !found || record.Call.Name == "" {
		return "", err
	}
	args := call.Args
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	if call.Target == "tool" && record.Call.Name == call.Name && sameJSON(record.Call.Arguments, args) {
		return "", nil
	}
	return "tools." + record.Call.Name + "(" + js.StringifyRaw(record.Call.Arguments) + ")", nil
}

// launch runs one live call and journals its result.
func (c *call) launch(call HostCall) {
	if c.stopped() {
		return
	}
	finish := func(outcome Outcome, global *journalGlobal, err error) {
		if err == nil {
			// A result after the outcome is decided has nowhere to go.
			if c.stopped() {
				return
			}
			c.queue = append(c.queue, queued{id: call.ID, at: c.now(), outcome: outcome, global: global})
			c.pump()
			return
		}
		// Stopped meanwhile: the outcome is decided, or a waiting model call
		// gave up.
		if c.stopped() {
			return
		}
		if c.ctx.Err() != nil {
			c.fail(err)
		} else {
			// A key an earlier attempt admitted with another tool or other
			// arguments: this attempt took another path.
			c.diverge(fmt.Sprintf("call %d is %s: %s", call.ID, describeCall(call), errorMessage(err)))
		}
	}
	if call.Target == "tool" {
		c.callTool(call, func(outcome Outcome, err error) { finish(outcome, nil, err) })
		return
	}
	c.callGlobal(call, func(outcome Outcome, err error) {
		finish(outcome, &journalGlobal{Name: call.Name, Args: call.Args, Outcome: outcome}, err)
	})
}

// callTool runs a nested call and answers with its outcome, on the loop.
func (c *call) callTool(call HostCall, done func(Outcome, error)) {
	model := strings.HasPrefix(call.Name, InternalPrefix)
	proceed := func() {
		// The call may have stopped while this one waited: a finished script
		// admits no new paid call.
		if model && c.stopped() {
			c.releaseModelSlot()
			done(Outcome{}, errors.New("The script has ended"))
			return
		}
		// Passed as the script gave them; validation reports arguments that
		// are not an object.
		args := call.Args
		if len(args) == 0 {
			args = json.RawMessage("{}")
		}
		c.async(func() (any, error) {
			return c.r.ExecuteTool(c.ctx, call.Name, args, durable.NestedCallOptions{Key: fmt.Sprintf("c%d", call.ID)})
		}, func(value any, err error) {
			if model {
				c.releaseModelSlot()
			}
			if err != nil {
				done(Outcome{}, err)
				return
			}
			result := value.(durable.NestedToolResult)
			if call.Name == GenerateImagesTool {
				c.generatedImages += countImages(result.StructuredOutput)
			}
			structured := false
			for _, tool := range c.start.Tools {
				if tool.Name == call.Name {
					structured = tool.Structured
					break
				}
			}
			outcome := outcomeOf(call.Name, structured, result)
			c.finishRow(call.ID, outcome, result)
			done(outcome, nil)
		})
	}
	if model {
		c.modelSlot(proceed)
	} else {
		proceed()
	}
}

// started: the script made a tool call; its row appears, running, also
// while it waits to launch.
func (c *call) started(call HostCall) {
	if _, ok := c.rows[call.ID]; ok {
		return
	}
	args := ""
	if len(call.Args) > 0 {
		args = js.StringifyRaw(call.Args)
	}
	name := strings.TrimPrefix(call.Name, InternalPrefix)
	c.rows[call.ID] = &NestedCall{ID: call.ID, Name: name, Args: cut(args, argsPreviewChars), Status: "running"}
}

// finishRow: an error result is an error row, also when the script got its
// value (bash's nonzero exit).
func (c *call) finishRow(id int, outcome Outcome, result durable.NestedToolResult) {
	row, ok := c.rows[id]
	if !ok {
		return
	}
	row.Status = "ok"
	if result.IsError {
		row.Status = "error"
	}
	if result.DurationMs != nil {
		duration := *result.DurationMs
		row.DurationMs = &duration
	}
	var errors []string
	for _, diagnostic := range result.Diagnostics {
		if diagnostic.Severity == "error" {
			errors = append(errors, diagnostic.Message)
		}
	}
	text := strings.Join(errors, "\n")
	if !outcome.OK {
		text = outcome.Message
	}
	if result.IsError && text != "" {
		row.Error = cut(text, errorPreviewChars)
	}
}

// details are the rows in call order; calls still running when the script
// ended were cancelled.
func (c *call) details() ToolDetails {
	ids := make([]int, 0, len(c.rows))
	for id := range c.rows {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	calls := make([]NestedCall, 0, len(ids))
	for _, id := range ids {
		row := *c.rows[id]
		if row.Status == "running" {
			row.Status = "cancelled"
		}
		calls = append(calls, row)
	}
	return ToolDetails{Calls: calls}
}

// callGlobal answers a global's call, on the loop.
func (c *call) callGlobal(call HostCall, done func(Outcome, error)) {
	discovery := slices.Contains(discoveryGlobals, call.Name)
	catalog := slices.Contains(catalogGlobals, call.Name) && c.options.Models != nil
	var global *Global
	if !discovery && !catalog {
		for i := range c.options.Globals {
			if c.options.Globals[i].Name == call.Name {
				global = &c.options.Globals[i]
				break
			}
		}
		if global == nil {
			done(Outcome{OK: false, Message: "Unknown global " + call.Name}, nil)
			return
		}
	}
	answer := func(value json.RawMessage, err error) {
		if err != nil {
			if c.ctx.Err() != nil {
				done(Outcome{}, err)
				return
			}
			done(Outcome{OK: false, Message: errorMessage(err)}, nil)
			return
		}
		done(Outcome{OK: true, Value: value}, nil)
	}
	if discovery {
		answer(c.discover(call))
		return
	}
	c.async(func() (any, error) {
		if catalog {
			return catalogCall(c.ctx, call.Name, call.Args, c.options.Models)
		}
		return global.Execute(c.ctx, call.Args, c.r)
	}, func(value any, err error) {
		raw, _ := value.(json.RawMessage)
		answer(raw, err)
	})
}

func (c *call) modelSlot(then func()) {
	if c.modelCalls >= MaxConcurrentModelCalls {
		c.modelWaiters = append(c.modelWaiters, func() {
			c.modelCalls++
			then()
		})
		return
	}
	c.modelCalls++
	then()
}

func (c *call) releaseModelSlot() {
	c.modelCalls--
	if len(c.modelWaiters) > 0 {
		next := c.modelWaiters[0]
		c.modelWaiters = c.modelWaiters[1:]
		c.loop.post(next)
	}
}

// discover answers searchTools(), describeTool(), and describeNamespace()
// from the start data.
func (c *call) discover(call HostCall) (json.RawMessage, error) {
	var args []json.RawMessage
	_ = json.Unmarshal(call.Args, &args)
	argument := func(i int) json.RawMessage {
		if i < len(args) {
			return args[i]
		}
		return nil
	}
	var visible []declaredTool
	for _, tool := range c.start.Tools {
		if !tool.Hidden {
			visible = append(visible, tool)
		}
	}
	namespaces := c.start.Namespaces
	namespaceOfTool := func(name string) *startNamespace {
		for i := range namespaces {
			if slices.Contains(namespaces[i].Tools, name) {
				return &namespaces[i]
			}
		}
		return nil
	}
	matchesNamespace := func(namespace *startNamespace, query string) bool {
		return namespace.Name == query || Identifier(namespace.Name) == Identifier(query)
	}
	stringArgument := func(i int) (string, bool) {
		value, err := js.Parse(argument(i))
		text, ok := value.(string)
		return text, err == nil && ok
	}
	switch call.Name {
	case "describeTool":
		name, ok := stringArgument(0)
		if !ok {
			return nil, errors.New("describeTool() expects a tool name")
		}
		for _, tool := range visible {
			if tool.Name == name || tool.JSName == name {
				return json.Marshal(tool.Description)
			}
		}
		return nil, nil
	case "describeNamespace":
		name, ok := stringArgument(0)
		if !ok {
			return nil, errors.New("describeNamespace() expects a namespace name")
		}
		for i := range namespaces {
			namespace := &namespaces[i]
			if !matchesNamespace(namespace, name) {
				continue
			}
			identifiers := make([]string, len(namespace.Tools))
			for j, tool := range namespace.Tools {
				identifiers[j] = Identifier(tool)
			}
			return json.Marshal(startNamespace{Name: namespace.Name, Description: namespace.Description,
				Instructions: namespace.Instructions, Tools: identifiers})
		}
		return nil, nil
	}
	query, ok := stringArgument(0)
	if !ok {
		return nil, errors.New("searchTools() expects a query string")
	}
	var options struct {
		Limit     json.RawMessage `json:"limit"`
		Namespace json.RawMessage `json:"namespace"`
	}
	_ = json.Unmarshal(argument(1), &options)
	// options?.limit ?? 8: null is the default too.
	limit := defaultSearchLimit
	if len(options.Limit) > 0 && string(options.Limit) != "null" {
		var n float64
		if json.Unmarshal(options.Limit, &n) != nil || n != math.Trunc(n) || n <= 0 {
			return nil, errors.New("searchTools() limit must be a positive integer")
		}
		limit = int(min(n, math.MaxInt32))
	}
	var namespace *string
	if len(options.Namespace) > 0 && string(options.Namespace) != "null" {
		value, err := js.Parse(options.Namespace)
		text, ok := value.(string)
		if err != nil || !ok {
			return nil, errors.New("searchTools() namespace must be a string")
		}
		namespace = &text
	}
	var documents []tools.SearchDocument
	for _, tool := range visible {
		own := namespaceOfTool(tool.Name)
		if namespace != nil && (own == nil || !matchesNamespace(own, *namespace)) {
			continue
		}
		namespaceText := ""
		if own != nil {
			namespaceText = strings.Join([]string{own.Name, own.Description, own.Instructions}, " ")
		}
		documents = append(documents, tools.SearchDocument{Name: tool.Name, Text: tool.Description, NamespaceText: namespaceText})
	}
	type found struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	results := []found{}
	for _, name := range tools.RankDocuments(query, documents, limit) {
		for _, tool := range visible {
			if tool.Name == name {
				results = append(results, found{Name: tool.JSName, Description: tool.Description})
				break
			}
		}
	}
	return json.Marshal(results)
}

// pump journals finished calls and acknowledged counts in one commit, then
// delivers the calls and launches the calls the counts cover; one commit in
// flight at a time, so what arrives meanwhile goes into the next.
func (c *call) pump() {
	if c.committing || c.stopped() {
		return
	}
	if len(c.queue) == 0 && len(c.acks) == 0 {
		return
	}
	c.committing = true
	batch, acks := c.queue, c.acks
	c.queue, c.acks = nil, nil
	c.async(func() (any, error) {
		return nil, c.r.CommitChange(c.ctx, func(tx *durable.Tx) error {
			journal, err := readJSON[journalState](tx, JournalDocument, c.taskID())
			if err != nil {
				return err
			}
			for _, a := range acks {
				issued := a.issued
				if a.seq == 0 {
					if journal.FirstIssued == nil {
						journal.FirstIssued = &issued
					}
				} else if a.seq-1 < len(journal.Entries) && journal.Entries[a.seq-1].IssuedAfter == nil {
					journal.Entries[a.seq-1].IssuedAfter = &issued
				}
			}
			for _, item := range batch {
				journal.Entries = append(journal.Entries, journalEntry{ID: item.id, At: item.at, Global: item.global})
			}
			if journal.Entries == nil {
				journal.Entries = []journalEntry{}
			}
			return writeJSON(tx, JournalDocument, c.taskID(), journal)
		})
	}, func(_ any, err error) {
		if err != nil {
			c.fail(err)
			return
		}
		for _, a := range acks {
			c.durableIssued = max(c.durableIssued, a.issued)
		}
		for _, item := range batch {
			c.journalLength++
			if !c.run.Finished() {
				c.deliver(c.journalLength, item.at, item.id, item.outcome)
			}
		}
		c.launchCovered()
		c.committing = false
		c.pump()
	})
}

// countImages counts the image blocks in a models.generateImages() result.
func countImages(value json.RawMessage) int {
	var result struct {
		Output []struct {
			Type string `json:"type"`
		} `json:"output"`
	}
	if json.Unmarshal(value, &result) != nil {
		return 0
	}
	count := 0
	for _, block := range result.Output {
		if block.Type == "image" {
			count++
		}
	}
	return count
}
