package tools

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

// Running commands, ported from upstream's NodeExecutionEnv.exec
// (@earendil-works/pi-durable/env/node): a string through the environment's
// shell, an argv directly, with options for the working directory, the
// environment, a timeout, and spilling the complete output to a file.

// ExecOptions are how a command runs (upstream ShellExecOptions).
type ExecOptions struct {
	// Cwd is the working directory, relative to the environment's; empty,
	// the environment's.
	Cwd string
	// Env adds variables; with InheritEnv false, they are the whole
	// environment (default: inherited).
	Env        map[string]string
	InheritEnv *bool
	// Timeout ends the command after this many seconds; 0, none.
	Timeout float64
	// OnOutput receives every decoded chunk of stdout and stderr as it
	// arrives, with the stream it came from; an error stops the command.
	OnOutput func(text string, info OutputInfo) error
	// Spill writes the complete raw output to a temporary file once it
	// exceeds either threshold.
	Spill *SpillOptions
}

// OutputInfo names the stream a chunk of output came from.
type OutputInfo struct {
	Stream string // stdout or stderr
}

// SpillOptions are the thresholds after which output spills to a file:
// bytes, and complete or partial lines.
type SpillOptions struct {
	AfterBytes int
	AfterLines int
}

// ShellExecResult is a finished command (upstream ShellExecResult): its
// exit code (128 plus the signal for a killed process), and the file with
// its complete output when it spilled.
type ShellExecResult struct {
	ExitCode  int
	SpillPath string
}

// ExecutionError is a command that failed to run to its end (upstream
// ExecutionError): Code is aborted, timeout, shell_unavailable,
// spawn_error, callback_error, or unknown; SpillPath is the spill file of a
// command that timed out or was aborted after it spilled.
type ExecutionError struct {
	Code      string
	Message   string
	SpillPath string
	Err       error
}

func (e *ExecutionError) Error() string { return e.Message }
func (e *ExecutionError) Unwrap() error { return e.Err }

// ShellRunner runs commands with options (upstream Shell.exec), as
// LocalEnv does; the bash and powershell tools use it when the environment
// implements it.
type ShellRunner interface {
	ExecWith(ctx context.Context, command string, options ExecOptions) (ShellExecResult, error)
	ExecArgvWith(ctx context.Context, argv []string, options ExecOptions) (ShellExecResult, error)
}

var _ ShellRunner = (*LocalEnv)(nil)

// LocalEnvOptions configure a local environment (upstream
// NodeExecutionEnv's options).
type LocalEnvOptions struct {
	// ShellPath is the shell commands run through; default /bin/bash, else
	// bash on PATH, else sh.
	ShellPath string
	// ShellEnv adds variables to every command's inherited environment.
	ShellEnv map[string]string
	// Watch is how Watch watches.
	Watch WatchOptions
}

// NewLocalEnvWith is NewLocalEnv with options.
func NewLocalEnvWith(cwd string, options LocalEnvOptions) (*LocalEnv, error) {
	env, err := NewLocalEnv(cwd)
	if err != nil {
		return nil, err
	}
	env.options = options
	return env, nil
}

// ExecWith runs command through the environment's shell.
func (e *LocalEnv) ExecWith(ctx context.Context, command string, options ExecOptions) (ShellExecResult, error) {
	if ctx.Err() != nil {
		return ShellExecResult{}, &ExecutionError{Code: "aborted", Message: "aborted", Err: ctx.Err()}
	}
	if err := checkTimeout(options.Timeout); err != nil {
		return ShellExecResult{}, err
	}
	shell, args, err := e.shellConfig()
	if err != nil {
		return ShellExecResult{}, err
	}
	return e.execute(ctx, append(append([]string{shell}, args...), command), options)
}

// ExecArgvWith runs argv[0] directly with the rest as its arguments,
// without a shell, so they reach the program unparsed.
func (e *LocalEnv) ExecArgvWith(ctx context.Context, argv []string, options ExecOptions) (ShellExecResult, error) {
	if ctx.Err() != nil {
		return ShellExecResult{}, &ExecutionError{Code: "aborted", Message: "aborted", Err: ctx.Err()}
	}
	if err := checkTimeout(options.Timeout); err != nil {
		return ShellExecResult{}, err
	}
	if len(argv) == 0 {
		return ShellExecResult{}, &ExecutionError{Code: "spawn_error", Message: "Empty argv: no program to run", Err: ErrSpawn}
	}
	return e.execute(ctx, argv, options)
}

func checkTimeout(timeout float64) error {
	if timeout == 0 {
		return nil
	}
	if math.IsNaN(timeout) || math.IsInf(timeout, 0) || timeout < 0 {
		return &ExecutionError{Code: "timeout", Message: "Invalid timeout: must be a finite number of seconds"}
	}
	if timeout > maxTimeoutSeconds {
		return &ExecutionError{Code: "timeout", Message: fmt.Sprintf("Invalid timeout: maximum is %s seconds", strconv.FormatFloat(maxTimeoutSeconds, 'f', -1, 64))}
	}
	return nil
}

// shellConfig is the shell strings run through (upstream getShellConfig).
func (e *LocalEnv) shellConfig() (string, []string, error) {
	if path := e.options.ShellPath; path != "" {
		if _, err := os.Stat(path); err != nil {
			return "", nil, &ExecutionError{Code: "shell_unavailable", Message: "Custom shell path not found: " + path}
		}
		return path, []string{"-c"}, nil
	}
	if runtime.GOOS == "windows" {
		var candidates []string
		for _, variable := range []string{"ProgramFiles", "ProgramFiles(x86)"} {
			if dir := os.Getenv(variable); dir != "" {
				candidates = append(candidates, dir+`\Git\bin\bash.exe`)
			}
		}
		for _, candidate := range candidates {
			if _, err := os.Stat(candidate); err == nil {
				return candidate, []string{"-c"}, nil
			}
		}
		if bash, err := exec.LookPath("bash.exe"); err == nil {
			return bash, []string{"-c"}, nil
		}
		searched := ""
		for _, candidate := range candidates {
			searched += "\n  " + candidate
		}
		return "", nil, &ExecutionError{Code: "shell_unavailable", Message: "No bash shell found. Options:\n" +
			"  1. Install Git for Windows: https://git-scm.com/download/win\n" +
			"  2. Add your bash to PATH (Cygwin, MSYS2, etc.)\n" +
			"  3. Configure an explicit shellPath\n\n" +
			"Searched Git Bash in:" + searched}
	}
	if _, err := os.Stat("/bin/bash"); err == nil {
		return "/bin/bash", []string{"-c"}, nil
	}
	if bash, err := exec.LookPath("bash"); err == nil {
		return bash, []string{"-c"}, nil
	}
	return "sh", []string{"-c"}, nil
}

// commandEnv is a command's environment (upstream getShellEnv).
func (e *LocalEnv) commandEnv(options ExecOptions) []string {
	vars := map[string]string{}
	var order []string
	set := func(name, value string) {
		if _, ok := vars[name]; !ok {
			order = append(order, name)
		}
		vars[name] = value
	}
	if options.InheritEnv == nil || *options.InheritEnv {
		for _, entry := range os.Environ() {
			if i := bytes.IndexByte([]byte(entry), '='); i > 0 {
				set(entry[:i], entry[i+1:])
			}
		}
		for name, value := range e.options.ShellEnv {
			set(name, value)
		}
	}
	for name, value := range options.Env {
		set(name, value)
	}
	out := make([]string, 0, len(order))
	for _, name := range order {
		out = append(out, name+"="+vars[name])
	}
	return out
}

// spawnError is a program that could not start, worded as Node's spawn
// errors are.
func spawnError(program string, err error) error {
	code := ""
	switch {
	case errors.Is(err, exec.ErrNotFound), errors.Is(err, fs.ErrNotExist):
		code = "ENOENT"
	case errors.Is(err, fs.ErrPermission):
		code = "EACCES"
	case errors.Is(err, syscall.ENOTDIR):
		code = "ENOTDIR"
	}
	message := err.Error()
	if code != "" {
		message = "spawn " + program + " " + code
	}
	return &ExecutionError{Code: "spawn_error", Message: message, Err: fmt.Errorf("%w: %v", ErrSpawn, err)}
}

// execute runs argv with options.
func (e *LocalEnv) execute(ctx context.Context, argv []string, options ExecOptions) (ShellExecResult, error) {
	cwd := e.cwd
	if options.Cwd != "" {
		cwd = e.resolve(options.Cwd)
	}
	if _, err := os.Stat(cwd); err != nil {
		return ShellExecResult{}, &ExecutionError{Code: "spawn_error", Message: "Working directory does not exist: " + cwd + "\nCannot execute bash commands.", Err: fmt.Errorf("%w: %v", ErrSpawn, err)}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = cwd
	cmd.Env = e.commandEnv(options)
	configureProcess(cmd)

	var mu sync.Mutex
	var callbackErr, spillErr error
	var timedOut atomic.Bool
	spill := &rawSpill{env: e, options: options.Spill}
	emit := func(text string, stream string) {
		if text == "" || options.OnOutput == nil || callbackErr != nil {
			return
		}
		if err := options.OnOutput(text, OutputInfo{Stream: stream}); err != nil {
			callbackErr = &ExecutionError{Code: "callback_error", Message: err.Error(), Err: err}
			cancel()
		}
	}
	// One decoder per stream, so a character split across chunks of one
	// stream survives interleaving.
	var stdout, stderr streamDecoder
	feed := func(stream string, decoder *streamDecoder) writerFunc {
		return func(p []byte) (int, error) {
			mu.Lock()
			defer mu.Unlock()
			emit(decoder.decode(p, false), stream)
			if spillErr == nil {
				if err := spill.write(p); err != nil {
					spillErr = &ExecutionError{Code: "unknown", Message: "Failed to preserve complete shell output: " + err.Error(), Err: err}
					cancel()
				}
			}
			// Keep consuming after a failure, so the child cannot block on
			// a full pipe.
			return len(p), nil
		}
	}
	cmd.Stdout = feed("stdout", &stdout)
	cmd.Stderr = feed("stderr", &stderr)
	if err := cmd.Start(); err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return ShellExecResult{}, &ExecutionError{Code: "aborted", Message: "aborted", Err: ctx.Err()}
		}
		return ShellExecResult{}, spawnError(argv[0], err)
	}
	e.track(cmd)
	var timer *time.Timer
	if options.Timeout > 0 {
		timer = time.AfterFunc(time.Duration(options.Timeout*float64(time.Second)), func() {
			timedOut.Store(true)
			cancel()
		})
	}
	waitErr := cmd.Wait()
	e.untrack(cmd)
	if timer != nil {
		timer.Stop()
	}
	mu.Lock()
	defer mu.Unlock()
	spill.close()
	emit(stdout.decode(nil, true), "stdout")
	emit(stderr.decode(nil, true), "stderr")
	switch {
	case callbackErr != nil:
		return ShellExecResult{}, callbackErr
	case timedOut.Load():
		return ShellExecResult{}, &ExecutionError{Code: "timeout", Message: "timeout:" + strconv.FormatFloat(options.Timeout, 'f', -1, 64), SpillPath: spill.path}
	case ctx.Err() != nil:
		return ShellExecResult{}, &ExecutionError{Code: "aborted", Message: "aborted", SpillPath: spill.path, Err: ctx.Err()}
	case spillErr != nil:
		return ShellExecResult{}, spillErr
	}
	result := ShellExecResult{SpillPath: spill.path}
	var exitErr *exec.ExitError
	switch {
	case waitErr == nil:
	case errors.As(waitErr, &exitErr):
		result.ExitCode = exitErr.ExitCode()
		// A process killed by a signal has no exit code: 128 plus the
		// signal, as shells report it.
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			result.ExitCode = 128 + int(status.Signal())
		}
	default:
		return ShellExecResult{}, &ExecutionError{Code: "spawn_error", Message: waitErr.Error(), Err: waitErr}
	}
	return result, nil
}

// rawSpill writes a command's complete raw output to a temporary file once
// it passes the thresholds (upstream's spill): counted over both streams
// in arrival order, the output before then written first.
type rawSpill struct {
	env      *LocalEnv
	options  *SpillOptions
	bytes    int
	newlines int
	prefix   [][]byte
	file     *os.File
	path     string
}

func (s *rawSpill) write(chunk []byte) error {
	if s.options == nil || len(chunk) == 0 {
		return nil
	}
	if s.file != nil {
		_, err := s.file.Write(chunk)
		return err
	}
	s.bytes += len(chunk)
	s.newlines += bytes.Count(chunk, []byte{'\n'})
	lines := s.newlines
	if chunk[len(chunk)-1] != '\n' {
		lines++
	}
	if s.bytes <= s.options.AfterBytes && lines <= s.options.AfterLines {
		s.prefix = append(s.prefix, append([]byte(nil), chunk...))
		return nil
	}
	path, err := s.env.CreateTempFile(context.Background(), "pi-output-", ".log")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	s.file, s.path = f, path
	for _, prefix := range s.prefix {
		if _, err := f.Write(prefix); err != nil {
			return err
		}
	}
	s.prefix = nil
	_, err = f.Write(chunk)
	return err
}

func (s *rawSpill) close() {
	if s.file != nil {
		_ = s.file.Close()
	}
}

// execResult runs a command for the plain Exec API: its decoded output's
// tail and exit code, context errors as they are, and spawn failures as
// ErrSpawn.
func (e *LocalEnv) execResult(ctx context.Context, run func(ExecOptions) (ShellExecResult, error), onOutput func(string) error) (durable.ExecResult, error) {
	var output tailBuffer
	result, err := run(ExecOptions{OnOutput: func(text string, _ OutputInfo) error {
		output.Write([]byte(text))
		if onOutput == nil {
			return nil
		}
		return onOutput(text)
	}})
	var execErr *ExecutionError
	if errors.As(err, &execErr) {
		switch execErr.Code {
		case "aborted":
			return durable.ExecResult{Output: output.String()}, ctx.Err()
		case "callback_error":
			return durable.ExecResult{Output: output.String()}, execErr.Err
		}
	}
	if err != nil {
		return durable.ExecResult{Output: output.String()}, err
	}
	return durable.ExecResult{Output: output.String(), ExitCode: result.ExitCode}, nil
}
