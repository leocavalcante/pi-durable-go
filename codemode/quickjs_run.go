package codemode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// QuickJSSandbox runs scripts in Go, as pi-codemode's CodemodeSandbox runs
// them in a worker thread: each run gets its own QuickJS VM (QuickJS-NG
// compiled to WebAssembly, run by wazero) on its own goroutine, driven as
// pi-codemode 1.5.2 drives it with its own prelude. Scripts see the same
// environment, clock, and randomness as with NodeSandbox, and the VM reports
// the same fingerprint, so a script started in one replays in the other. It
// needs no Node. Close aborts the runs in flight.
type QuickJSSandbox struct {
	// MemoryLimitBytes bounds each VM's heap; 0 sets no limit.
	MemoryLimitBytes int64

	engine  *engine
	mu      sync.Mutex
	closed  bool
	running map[*execution]struct{}
}

var _ Sandbox = (*QuickJSSandbox)(nil)

var globalPart = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)

var reservedGlobals = map[string]bool{"tools": true, "ALL_TOOLS": true, "console": true, "text": true, "image": true,
	"exit": true, "globalThis": true, "store": true, "load": true}

// checkGlobals is upstream's: identifiers or namespace.member, not shadowing
// a built-in global or each other.
func checkGlobals(globals []DeclaredGlobal) error {
	names, namespaces := map[string]bool{}, map[string]bool{}
	for _, global := range globals {
		parts := strings.Split(global.Name, ".")
		valid := len(parts) <= 2 && !reservedGlobals[parts[0]]
		for _, part := range parts {
			valid = valid && globalPart.MatchString(part)
		}
		if !valid {
			return fmt.Errorf("Invalid global name %q", global.Name)
		}
		if names[global.Name] {
			return fmt.Errorf("Global %q is already registered", global.Name)
		}
		if len(parts) == 2 {
			namespaces[parts[0]] = true
		}
		names[global.Name] = true
	}
	for name := range namespaces {
		if names[name] {
			return fmt.Errorf("Global %q conflicts with the namespace %q", name, name)
		}
	}
	return nil
}

// Start validates the run as upstream's start() does and starts its VM.
func (s *QuickJSSandbox) Start(code string, options RunOptions) (Run, error) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, errors.New("Sandbox is closed")
	}
	if math.IsNaN(options.StartedAt) || math.IsInf(options.StartedAt, 0) || options.StartedAt < 0 {
		return nil, errors.New("startedAt must be a non-negative number of milliseconds since the epoch")
	}
	names, scriptNames := map[string]bool{}, map[string]bool{}
	for _, tool := range options.Tools {
		names[tool.Name] = true
		jsName := tool.JSName
		if jsName == "" {
			jsName = Identifier(tool.Name)
		}
		scriptNames[jsName] = true
	}
	if len(names) != len(options.Tools) {
		return nil, errors.New("Tool names must be unique")
	}
	if len(scriptNames) != len(options.Tools) {
		return nil, errors.New("Tool script names must be unique")
	}
	if err := checkGlobals(options.Globals); err != nil {
		return nil, err
	}
	engine, err := sharedEngine()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.engine = engine
	s.mu.Unlock()
	e := newExecution(s, code, options)
	s.mu.Lock()
	if s.running == nil {
		s.running = map[*execution]struct{}{}
	}
	s.running[e] = struct{}{}
	s.mu.Unlock()
	e.launch()
	return e, nil
}

// Close aborts the runs in flight, which end with an "aborted" error
// ("Sandbox closed"), and rejects new ones.
func (s *QuickJSSandbox) Close() error {
	s.mu.Lock()
	s.closed = true
	runs := make([]*execution, 0, len(s.running))
	for e := range s.running {
		runs = append(runs, e)
	}
	s.mu.Unlock()
	for _, e := range runs {
		e.Abort("Sandbox closed")
	}
	return nil
}

// The module is compiled once per process; every run instantiates it.
var (
	engineOnce sync.Once
	engineOf   *engine
	engineErr  error
)

func sharedEngine() (*engine, error) {
	engineOnce.Do(func() { engineOf, engineErr = newEngine(context.Background()) })
	return engineOf, engineErr
}

// defaultSandbox runs the scripts of a tool without a Sandbox.
var defaultSandbox = &QuickJSSandbox{}

// vmMessage is a message from the VM to the host (protocol.ts).
type vmMessage struct {
	kind        string
	item        OutputItem
	id          int
	target      string
	name        string
	args        *string
	fingerprint string
	seq, issued int
	ok          bool
	value       *string
	writes      string
	err         string
	during      *During
	message     string
}

// vmDelivery is a result for the VM (protocol.ts HostToWorkerMessage).
type vmDelivery struct {
	id         int
	ok         bool
	payload    *string
	seq        int
	now        float64
	properties *string
}

type pendingCall struct {
	record    *CallRecord
	startedAt time.Time
}

// execution is the host side of one run (upstream Execution), and its Run.
type execution struct {
	sandbox *QuickJSSandbox
	options RunOptions
	code    string

	mu        sync.Mutex
	output    []OutputItem
	calls     []*CallRecord
	pending   map[int]*pendingCall
	delivered map[int]bool
	finished  bool
	result    Result
	done      chan struct{}
	timer     *time.Timer

	// interrupted stops the VM at its next poll.
	interrupted atomic.Bool
	deliveries  chan vmDelivery
	// events orders the VM's messages: the host handles them one at a time.
	events chan vmMessage
}

func newExecution(s *QuickJSSandbox, code string, options RunOptions) *execution {
	return &execution{sandbox: s, options: options, code: code, pending: map[int]*pendingCall{}, delivered: map[int]bool{},
		done: make(chan struct{}), deliveries: make(chan vmDelivery, 64), events: make(chan vmMessage, 64)}
}

func (e *execution) launch() {
	if e.options.TimeoutMs != nil && !math.IsInf(*e.options.TimeoutMs, 1) {
		timeout := *e.options.TimeoutMs
		e.timer = time.AfterFunc(time.Duration(timeout*float64(time.Millisecond)), func() {
			e.finish(&Error{Kind: "timeout", Message: fmt.Sprintf("Execution timed out after %s ms", js.Stringify(timeout))}, nil, nil, nil)
		})
	}
	go e.receive()
	go e.runVM()
}

// receive handles the VM's messages in order.
func (e *execution) receive() {
	for {
		select {
		case message := <-e.events:
			e.handle(message)
		case <-e.done:
			return
		}
	}
}

func (e *execution) post(message vmMessage) {
	select {
	case e.events <- message:
	case <-e.done:
	}
}

func (e *execution) handle(message vmMessage) {
	e.mu.Lock()
	if e.finished {
		e.mu.Unlock()
		return
	}
	switch message.kind {
	case "output":
		e.output = append(e.output, message.item)
		e.mu.Unlock()
	case "call":
		if e.pending[message.id] != nil {
			e.mu.Unlock()
			e.finish(&Error{Kind: "sandbox", Message: fmt.Sprintf("Sandbox bridge broken: duplicate call id %d. The script may have modified built-ins such as a prototype's toJSON.", message.id)}, nil, nil, nil)
			return
		}
		var args json.RawMessage
		if message.args != nil {
			if !json.Valid([]byte(*message.args)) {
				e.mu.Unlock()
				e.finish(&Error{Kind: "sandbox", Message: "Sandbox bridge broken: call arguments is not valid JSON. The script may have modified built-ins such as a prototype's toJSON."}, nil, nil, nil)
				return
			}
			args = json.RawMessage(*message.args)
		}
		var record *CallRecord
		if message.target == "tool" {
			record = &CallRecord{Name: message.name, Status: "cancelled"}
			e.calls = append(e.calls, record)
		}
		e.pending[message.id] = &pendingCall{record: record, startedAt: time.Now()}
		e.mu.Unlock()
		if e.options.OnCall != nil {
			e.options.OnCall(HostCall{ID: message.id, Target: message.target, Name: message.name, Args: args})
		}
	case "started":
		e.mu.Unlock()
		if e.options.OnStarted != nil {
			e.options.OnStarted(message.fingerprint)
		}
	case "idle":
		e.mu.Unlock()
		if e.options.OnIdle != nil {
			e.options.OnIdle(message.seq, message.issued)
		}
	case "done":
		e.mu.Unlock()
		e.handleDone(message)
	case "crash":
		e.mu.Unlock()
		e.finish(&Error{Kind: "sandbox", Message: message.message}, nil, nil, nil)
	default:
		e.mu.Unlock()
	}
}

// handleDone decodes a finished script before finishing (upstream
// handleDone, parseScriptError, parseStoreWrites).
func (e *execution) handleDone(message vmMessage) {
	broken := func(what string) {
		e.finish(&Error{Kind: "sandbox", Message: "Sandbox bridge broken: " + what + ". The script may have modified built-ins such as a prototype's toJSON."}, nil, nil, nil)
	}
	if !message.ok {
		var parsed struct {
			Name    *string `json:"name"`
			Message *string `json:"message"`
			Stack   *string `json:"stack"`
		}
		if json.Unmarshal([]byte(message.err), &parsed) != nil {
			broken("script error is not valid JSON")
			return
		}
		if parsed.Message == nil {
			broken("script error is malformed")
			return
		}
		failure := &Error{Kind: "script", Message: *parsed.Message}
		if parsed.Name != nil {
			failure.Name = *parsed.Name
		}
		if parsed.Stack != nil {
			failure.Stack = *parsed.Stack
		}
		e.finish(failure, nil, nil, message.during)
		return
	}
	var value json.RawMessage
	if message.value != nil {
		if !json.Valid([]byte(*message.value)) {
			broken("return value is not valid JSON")
			return
		}
		value = json.RawMessage(*message.value)
	}
	var entries []json.RawMessage
	if json.Unmarshal([]byte(message.writes), &entries) != nil {
		broken("store writes are not an array")
		return
	}
	writes := StoreWrites{Set: map[string]json.RawMessage{}, Delete: []string{}}
	for _, raw := range entries {
		var entry []*string
		if json.Unmarshal(raw, &entry) != nil || len(entry) < 1 || len(entry) > 2 || entry[0] == nil || len(entry) == 2 && entry[1] == nil {
			broken("store writes contain a malformed entry")
			return
		}
		if len(entry) == 1 {
			writes.Delete = append(writes.Delete, *entry[0])
			continue
		}
		if !json.Valid([]byte(*entry[1])) {
			broken(fmt.Sprintf("store value for %s is not valid JSON", js.Stringify(*entry[0])))
			return
		}
		writes.Set[*entry[0]] = json.RawMessage(*entry[1])
	}
	e.finish(nil, value, &writes, message.during)
}

// finish decides the run's result once and stops its VM.
func (e *execution) finish(failure *Error, value json.RawMessage, writes *StoreWrites, during *During) {
	e.mu.Lock()
	if e.finished {
		e.mu.Unlock()
		return
	}
	e.finished = true
	if e.timer != nil {
		e.timer.Stop()
	}
	now := time.Now()
	for _, call := range e.pending {
		if call.record != nil {
			call.record.DurationMs = float64(now.Sub(call.startedAt).Microseconds()) / 1000
		}
	}
	e.pending = map[int]*pendingCall{}
	calls := make([]CallRecord, len(e.calls))
	for i, record := range e.calls {
		calls[i] = *record
	}
	output := append([]OutputItem{}, e.output...)
	result := Result{Output: output, Calls: calls, During: during}
	if failure != nil {
		result.Error = failure
	} else {
		result.OK, result.Value = true, value
		if writes == nil {
			writes = &StoreWrites{Set: map[string]json.RawMessage{}, Delete: []string{}}
		}
		result.StoreWrites = *writes
	}
	e.result = result
	e.mu.Unlock()
	// Stop the VM: it is interrupted at its next poll.
	e.interrupted.Store(true)
	close(e.done)
	e.sandbox.mu.Lock()
	delete(e.sandbox.running, e)
	e.sandbox.mu.Unlock()
}

// Deliver is upstream's deliver(), with its checks.
func (e *execution) Deliver(seq int, now float64, id int, outcome Outcome) error {
	e.mu.Lock()
	if e.finished {
		e.mu.Unlock()
		return nil
	}
	if math.IsNaN(now) || math.IsInf(now, 0) {
		e.mu.Unlock()
		return fmt.Errorf("The clock value of result %d is not a finite number", seq)
	}
	if seq < 1 || e.delivered[seq] {
		e.mu.Unlock()
		return fmt.Errorf("Result number %d is not a positive integer used once", seq)
	}
	pending := e.pending[id]
	if pending == nil {
		e.mu.Unlock()
		return fmt.Errorf("Call %d is not waiting for a result", id)
	}
	// Serialized as JavaScript would, so the VM reads upstream's text.
	var payload, properties *string
	if outcome.OK {
		if len(outcome.Value) > 0 {
			text := js.StringifyRaw(outcome.Value)
			payload = &text
		}
	} else {
		message := outcome.Message
		payload = &message
		if outcome.Properties != nil {
			object := &js.Object{Values: map[string]any{}}
			for key, raw := range outcome.Properties {
				value, err := js.Parse(raw)
				if err != nil {
					e.mu.Unlock()
					return fmt.Errorf("The properties of result %d are not an object", seq)
				}
				object.Keys = append(object.Keys, key)
				object.Values[key] = value
			}
			sortKeys(object)
			text := js.Stringify(object)
			properties = &text
		}
	}
	e.delivered[seq] = true
	delete(e.pending, id)
	if pending.record != nil {
		pending.record.Status = "ok"
		if !outcome.OK {
			pending.record.Status = "error"
		}
		pending.record.DurationMs = float64(time.Since(pending.startedAt).Microseconds()) / 1000
	}
	// Sent without the lock: the VM may be waiting for the host, which
	// takes it.
	e.mu.Unlock()
	select {
	case e.deliveries <- vmDelivery{id: id, ok: outcome.OK, payload: payload, seq: seq, now: now, properties: properties}:
	case <-e.done:
	}
	return nil
}

// sortKeys orders an object's keys as JavaScript enumerates an object built
// from sorted Go map keys.
func sortKeys(object *js.Object) {
	keys := object.Keys
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	object.Order()
}

func (e *execution) Abort(message string) {
	e.finish(&Error{Kind: "aborted", Message: message}, nil, nil, nil)
}

func (e *execution) Finished() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.finished
}

func (e *execution) Wait(ctx context.Context) (Result, error) {
	select {
	case <-e.done:
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.result, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

// jobsPerPoll is how many promise jobs run between interrupt polls.
const jobsPerPoll = 50

// runVM drives the run's VM (upstream startVm in deterministic mode) on this
// goroutine until the host finishes.
func (e *execution) runVM() {
	crash := func(err error) {
		var thrown *jsError
		if errors.As(err, &thrown) {
			e.post(vmMessage{kind: "crash", message: thrown.name + ": " + thrown.message})
			return
		}
		e.post(vmMessage{kind: "crash", message: "Error: " + err.Error()})
	}
	ctx := context.Background()
	v, err := e.sandbox.engine.newVM(ctx, e.sandbox.MemoryLimitBytes, e.options.StartedAt, e.options.Seed, e.interrupted.Load)
	if err != nil {
		e.finish(&Error{Kind: "sandbox", Message: err.Error()}, nil, nil, nil)
		return
	}
	defer v.close()
	issued, current := 0, 0
	var api, settle, run, stalled uint32
	// drain runs queued jobs, then fails a script that waits on nothing
	// that can ever resume it.
	drain := func() error {
		for jobs := 1; v.call("qjs_is_job_pending") != 0; jobs++ {
			if int32(v.call("qjs_execute_pending_job")) < 0 {
				return errors.New("Promise job failed")
			}
			if v.trap != nil {
				return v.trap
			}
			if jobs%jobsPerPoll == 0 && e.interrupted.Load() {
				return errInterrupted
			}
		}
		result, err := v.callFunction(stalled, api)
		v.free(result)
		return err
	}
	bridge := v.newFunction("bridge", func(args []uint32) error {
		arg := func(i int) uint32 {
			if i < len(args) {
				return args[i]
			}
			return 0
		}
		present := func(i int) bool { return arg(i) != 0 && !v.isUndefined(arg(i)) }
		switch kind := v.toString(arg(0)); kind {
		case "call", "global":
			issued++
			message := vmMessage{kind: "call", id: int(v.toNumber(arg(1))), target: "global", name: v.toString(arg(2))}
			if kind == "call" {
				message.target = "tool"
			}
			if present(3) {
				text := v.toString(arg(3))
				message.args = &text
			}
			e.post(message)
		case "output":
			var item OutputItem
			switch v.toString(arg(1)) {
			case "image":
				item = OutputItem{Type: "image", Data: v.toString(arg(2)), MimeType: v.toString(arg(3))}
			case "console":
				item = OutputItem{Type: "text", Text: v.toString(arg(2)), Console: true}
			default:
				item = OutputItem{Type: "text", Text: v.toString(arg(2))}
			}
			e.post(vmMessage{kind: "output", item: item})
		case "done":
			// The host stops the VM on done, so this drain's idle never
			// arrives: its numbers come with done.
			during := &During{Seq: current, Issued: issued}
			if v.toBoolean(arg(1)) {
				message := vmMessage{kind: "done", ok: true, writes: v.toString(arg(3)), during: during}
				if present(2) {
					text := v.toString(arg(2))
					message.value = &text
				}
				e.post(message)
			} else {
				e.post(vmMessage{kind: "done", ok: false, err: v.toString(arg(2)), during: during})
			}
		}
		return v.trap
	})
	if v.trap != nil {
		crash(v.trap)
		return
	}
	fingerprint := fmt.Sprintf("quickjs-wasi %s, quickjs %s, prelude %s, deterministic 1, stack %d, heap %s, budget none",
		quickjsWasiVersion, v.quickjsVersion(), preludeHash, maxStackSize, heapOf(e.sandbox.MemoryLimitBytes))
	tools, globals, store, err := e.data()
	if err != nil {
		crash(err)
		return
	}
	prelude, err := v.eval(preludeSource, "codemode-prelude.js")
	if err != nil {
		crash(err)
		return
	}
	toolsValue, globalsValue, storeValue := v.newString(tools), v.newString(globals), v.newString(store)
	api, err = v.callFunction(prelude, v.undefined(), bridge, toolsValue, globalsValue, storeValue)
	v.free(prelude)
	v.free(toolsValue)
	v.free(globalsValue)
	v.free(storeValue)
	if err != nil {
		crash(err)
		return
	}
	settle, run, stalled = v.getProp(api, "settle"), v.getProp(api, "run"), v.getProp(api, "stalled")
	e.post(vmMessage{kind: "started", fingerprint: fingerprint})
	// The prefix shares the first line with the script, so reported line
	// numbers match the script as written.
	fn, err := v.eval("(async (tools, console) => {"+e.code+"\n})", "codemode.js")
	if err != nil {
		var thrown *jsError
		if errors.As(err, &thrown) {
			e.post(vmMessage{kind: "done", ok: false, err: describeException(thrown)})
			e.waitDone()
			return
		}
		crash(err)
		return
	}
	result, err := v.callFunction(run, api, fn)
	v.free(result)
	v.free(fn)
	if err == nil {
		err = drain()
	}
	if err != nil {
		crash(err)
		return
	}
	e.post(vmMessage{kind: "idle", seq: 0, issued: issued})
	// Results apply strictly in seq order; early ones wait.
	nextSeq := 1
	early := map[int]vmDelivery{}
	for {
		select {
		case <-e.done:
			return
		case d := <-e.deliveries:
			if d.seq < nextSeq || early[d.seq].seq != 0 {
				crash(fmt.Errorf("Result %d has sequence number %d, expected %d or later, once", d.id, d.seq, nextSeq))
				return
			}
			early[d.seq] = d
			for next, ok := early[nextSeq]; ok; next, ok = early[nextSeq] {
				delete(early, nextSeq)
				// Never backwards: a resumed host's clock may be behind.
				v.clock.now = math.Max(v.clock.now, next.now)
				current = nextSeq
				args := []uint32{v.newNumber(float64(next.id)), v.boolean(next.ok), v.undefined(), v.undefined()}
				owned := []uint32{args[0]}
				if next.payload != nil {
					args[2] = v.newString(*next.payload)
					owned = append(owned, args[2])
				}
				if next.properties != nil {
					args[3] = v.newString(*next.properties)
					owned = append(owned, args[3])
				}
				result, err := v.callFunction(settle, api, args...)
				v.free(result)
				for _, value := range owned {
					v.free(value)
				}
				if err == nil {
					err = drain()
				}
				if err != nil {
					crash(err)
					return
				}
				e.post(vmMessage{kind: "idle", seq: nextSeq, issued: issued})
				nextSeq++
			}
		}
	}
}

// waitDone keeps the VM until the host finishes.
func (e *execution) waitDone() { <-e.done }

// data is what the prelude starts from: the tools, the globals, and the
// store, each as JSON text (upstream Execution and serializeStore).
func (e *execution) data() (string, string, string, error) {
	type tool struct {
		Name        string `json:"name"`
		JSName      string `json:"jsName"`
		Description string `json:"description"`
	}
	type global struct {
		Name   string `json:"name"`
		Spread bool   `json:"spread"`
	}
	tools := make([]tool, len(e.options.Tools))
	for i, t := range e.options.Tools {
		jsName := t.JSName
		if jsName == "" {
			jsName = Identifier(t.Name)
		}
		tools[i] = tool{Name: t.Name, JSName: jsName, Description: t.Description}
	}
	globals := make([]global, len(e.options.Globals))
	for i, g := range e.options.Globals {
		globals[i] = global{Name: g.Name, Spread: g.Spread}
	}
	store := &js.Object{Values: map[string]any{}}
	for key, raw := range e.options.Store {
		if len(raw) == 0 {
			continue
		}
		store.Keys = append(store.Keys, key)
		store.Values[key] = js.StringifyRaw(raw)
	}
	sortKeys(store)
	toolsJSON, err := json.Marshal(tools)
	if err != nil {
		return "", "", "", err
	}
	globalsJSON, err := json.Marshal(globals)
	if err != nil {
		return "", "", "", err
	}
	return jsonText(toolsJSON), jsonText(globalsJSON), js.Stringify(store), nil
}

// jsonText is JSON as JSON.stringify writes it (Go escapes <, >, and &).
func jsonText(data []byte) string { return js.StringifyRaw(data) }

func heapOf(limit int64) string {
	if limit <= 0 {
		return "none"
	}
	return fmt.Sprint(limit)
}

// describeException is vm.js's: name, message, and stack as JSON.
func describeException(e *jsError) string {
	head := e.name
	if e.message != "" {
		head = e.name + ": " + e.message
	}
	stack := head
	if trimmed := js.TrimEnd(e.stack); e.hasStack && trimmed != "" {
		stack = head + "\n" + trimmed
	}
	data, _ := json.Marshal(struct {
		Name    string `json:"name"`
		Message string `json:"message"`
		Stack   string `json:"stack"`
	}{e.name, e.message, stack})
	return js.StringifyRaw(data)
}
