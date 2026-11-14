package codemode

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/leocavalcante/pi-durable-go/internal/piinstall"
)

//go:embed bridge.mjs
var bridgeSource string

// bridgeURL is the bridge as a module Node imports from a CommonJS --eval:
// with --input-type=module, the sandbox's worker threads would inherit an
// option they cannot use.
var bridgeURL = "data:text/javascript;base64," + base64.StdEncoding.EncodeToString([]byte(bridgeSource))

// maxBridgeLine bounds one message from the bridge: a result carries the
// script's output, up to 16M characters plus images.
const maxBridgeLine = 256 << 20

// abortGrace is how long an aborted run may take to report its result
// before its process is killed.
const abortGrace = 5 * time.Second

// NodeSandbox runs scripts with pi-codemode's own CodemodeSandbox, installed
// with Pi: each run is a Node process whose worker thread hosts a QuickJS VM,
// so the script environment, the VM's determinism, and its fingerprint are
// upstream's. It needs Node.js 22.19+ and Pi 1.5.2. Do not change its fields
// once it is in use.
type NodeSandbox struct {
	// NodePath defaults to node on PATH, then Pi's managed Node installation.
	NodePath string
	// PackageDir is an installed pi-coding-agent package directory, whose
	// dependencies provide pi-codemode. Empty discovers it as model.Pi does.
	PackageDir string
	// CodemodeDir selects a pi-codemode package directory instead.
	CodemodeDir string
	// MemoryLimitBytes bounds each VM's heap; 0 sets no limit.
	MemoryLimitBytes int64

	mu      sync.Mutex
	closed  bool
	running map[*nodeRun]struct{}
}

var _ Sandbox = (*NodeSandbox)(nil)

type bridgeStart struct {
	PackageDir       string         `json:"packageDir,omitempty"`
	CodemodeDir      string         `json:"codemodeDir,omitempty"`
	MemoryLimitBytes int64          `json:"memoryLimitBytes,omitempty"`
	Code             string         `json:"code"`
	Options          bridgeRunStart `json:"options"`
}

type bridgeRunStart struct {
	StartedAt float64                    `json:"startedAt"`
	Seed      uint32                     `json:"seed"`
	Tools     []DeclaredTool             `json:"tools"`
	Globals   []DeclaredGlobal           `json:"globals"`
	Store     map[string]json.RawMessage `json:"store"`
	TimeoutMs *float64                   `json:"timeoutMs,omitempty"`
}

type bridgeMessage struct {
	Type        string   `json:"type"`
	Message     string   `json:"message"`
	Fingerprint string   `json:"fingerprint"`
	Call        HostCall `json:"call"`
	Seq         int      `json:"seq"`
	Issued      int      `json:"issued"`
	Result      *Result  `json:"result"`
}

// Start launches the run's process and waits until the sandbox accepted the
// script, so options it rejects fail here, with upstream's message.
func (s *NodeSandbox) Start(code string, options RunOptions) (Run, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("Sandbox is closed")
	}
	s.mu.Unlock()
	if options.Tools == nil {
		options.Tools = []DeclaredTool{}
	}
	if options.Globals == nil {
		options.Globals = []DeclaredGlobal{}
	}
	if options.Store == nil {
		options.Store = map[string]json.RawMessage{}
	}
	start, err := json.Marshal(bridgeStart{
		PackageDir: piinstall.PackageDir(s.PackageDir), CodemodeDir: s.CodemodeDir,
		MemoryLimitBytes: s.MemoryLimitBytes, Code: code,
		Options: bridgeRunStart{StartedAt: options.StartedAt, Seed: options.Seed, Tools: options.Tools,
			Globals: options.Globals, Store: options.Store, TimeoutMs: options.TimeoutMs},
	})
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(piinstall.NodePath(s.NodePath), "--eval", "import(process.argv[1])", bridgeURL)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, errors.New("codemode: cannot start Node; configure NodePath or install Node.js 22.19+")
	}
	run := &nodeRun{sandbox: s, cmd: cmd, stdin: stdin, options: options, done: make(chan struct{}),
		pending: map[int]bool{}, delivered: map[int]bool{}, exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(run.exited)
	}()
	if _, err := stdin.Write(append(start, '\n')); err != nil {
		run.kill()
		return nil, errors.New("codemode: the sandbox process ended before it started the script")
	}
	reader := bufio.NewReaderSize(stdout, 64<<10)
	first, err := readLine(reader)
	if err != nil {
		run.kill()
		return nil, errors.New("codemode: the sandbox process ended before it started the script")
	}
	var message bridgeMessage
	if err := json.Unmarshal(first, &message); err != nil {
		run.kill()
		return nil, errors.New("codemode: the sandbox sent a malformed message")
	}
	switch message.Type {
	case "accepted":
	case "rejected":
		run.kill()
		return nil, errors.New(message.Message)
	default:
		run.kill()
		return nil, fmt.Errorf("codemode: pi-codemode unavailable (%s); install Pi 1.5.2 or configure PackageDir", message.Message)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		run.Abort("Sandbox closed")
	} else {
		if s.running == nil {
			s.running = map[*nodeRun]struct{}{}
		}
		s.running[run] = struct{}{}
		s.mu.Unlock()
	}
	go run.read(reader)
	return run, nil
}

// Close aborts the runs in flight, which end with an "aborted" error
// ("Sandbox closed"), and rejects new ones; it waits for their results.
func (s *NodeSandbox) Close() error {
	s.mu.Lock()
	s.closed = true
	runs := make([]*nodeRun, 0, len(s.running))
	for run := range s.running {
		runs = append(runs, run)
	}
	s.mu.Unlock()
	for _, run := range runs {
		run.Abort("Sandbox closed")
	}
	for _, run := range runs {
		<-run.done
	}
	return nil
}

func readLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		line = append(line, chunk...)
		if len(line) > maxBridgeLine {
			return nil, errors.New("codemode: a sandbox message is too large")
		}
		if err == nil {
			return bytes.TrimRight(line, "\r\n"), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
}

type nodeRun struct {
	sandbox *NodeSandbox
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	options RunOptions
	exited  chan struct{}

	// writeMu orders writes to the process; the reader never takes it, so
	// a process blocked writing to a full pipe is always drained.
	writeMu sync.Mutex

	mu        sync.Mutex
	pending   map[int]bool
	delivered map[int]bool
	finished  bool
	aborting  bool
	result    Result
	done      chan struct{}
}

func (r *nodeRun) read(reader *bufio.Reader) {
	for {
		line, err := readLine(reader)
		if err != nil {
			message := "Sandbox process exited before the script settled"
			if !errors.Is(err, io.EOF) {
				message = err.Error()
			}
			r.settle(Result{OK: false, Error: &Error{Kind: "sandbox", Message: message}})
			return
		}
		var message bridgeMessage
		if err := json.Unmarshal(line, &message); err != nil {
			r.settle(Result{OK: false, Error: &Error{Kind: "sandbox", Message: "Sandbox bridge sent a malformed message"}})
			return
		}
		switch message.Type {
		case "started":
			if r.options.OnStarted != nil {
				r.options.OnStarted(message.Fingerprint)
			}
		case "call":
			r.mu.Lock()
			r.pending[message.Call.ID] = true
			r.mu.Unlock()
			if r.options.OnCall != nil {
				r.options.OnCall(message.Call)
			}
		case "idle":
			if r.options.OnIdle != nil {
				r.options.OnIdle(message.Seq, message.Issued)
			}
		case "result":
			if message.Result == nil {
				r.settle(Result{OK: false, Error: &Error{Kind: "sandbox", Message: "Sandbox bridge sent no result"}})
			} else {
				r.settle(*message.Result)
			}
			return
		}
	}
}

// settle records the result once and ends the process.
func (r *nodeRun) settle(result Result) {
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return
	}
	if result.Output == nil {
		result.Output = []OutputItem{}
	}
	if result.Calls == nil {
		result.Calls = []CallRecord{}
	}
	if result.OK && result.StoreWrites.Set == nil {
		result.StoreWrites.Set = map[string]json.RawMessage{}
	}
	if result.OK && result.StoreWrites.Delete == nil {
		result.StoreWrites.Delete = []string{}
	}
	r.finished = true
	r.result = result
	r.pending = map[int]bool{}
	r.mu.Unlock()
	r.writeMu.Lock()
	_ = r.stdin.Close()
	r.writeMu.Unlock()
	go func() {
		select {
		case <-r.exited:
		case <-time.After(abortGrace):
			r.kill()
		}
	}()
	r.sandbox.mu.Lock()
	delete(r.sandbox.running, r)
	r.sandbox.mu.Unlock()
	close(r.done)
}

func (r *nodeRun) kill() {
	r.writeMu.Lock()
	_ = r.stdin.Close()
	r.writeMu.Unlock()
	if r.cmd.Process != nil {
		_ = r.cmd.Process.Kill()
	}
}

func (r *nodeRun) send(message any) {
	data, err := json.Marshal(message)
	if err != nil {
		return
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	// A process that is gone reports through its result.
	_, _ = r.stdin.Write(append(data, '\n'))
}

func (r *nodeRun) Deliver(seq int, now float64, id int, outcome Outcome) error {
	r.mu.Lock()
	if r.finished {
		r.mu.Unlock()
		return nil
	}
	if err := checkDelivery(seq, now, r.delivered, r.pending, id); err != nil {
		r.mu.Unlock()
		return err
	}
	r.delivered[seq] = true
	delete(r.pending, id)
	r.mu.Unlock()
	r.send(struct {
		Type    string  `json:"type"`
		Seq     int     `json:"seq"`
		Now     float64 `json:"now"`
		ID      int     `json:"id"`
		Outcome Outcome `json:"outcome"`
	}{"deliver", seq, now, id, outcome})
	return nil
}

func (r *nodeRun) Abort(message string) {
	r.mu.Lock()
	if r.finished || r.aborting {
		r.mu.Unlock()
		return
	}
	r.aborting = true
	r.mu.Unlock()
	r.send(struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}{"abort", message})
	// A process that does not answer is killed; its result says it was
	// aborted, as the sandbox would have.
	go func() {
		select {
		case <-r.done:
		case <-time.After(abortGrace):
			r.kill()
			r.settle(Result{OK: false, Error: &Error{Kind: "aborted", Message: message}})
		}
	}()
}

func (r *nodeRun) Finished() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finished
}

func (r *nodeRun) Wait(ctx context.Context) (Result, error) {
	select {
	case <-r.done:
		return r.result, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}
