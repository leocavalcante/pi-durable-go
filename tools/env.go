// Package tools provides a local execution environment and coding tools.
//
// LocalEnv uses cwd to resolve relative paths and as the process working
// directory. It is not a sandbox: absolute paths, parent traversal, and shell
// commands can access anything permitted to the current process.
package tools

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	durable "github.com/leocavalcante/pi-durable-go"
)

const maxExecOutput = 64 * 1024

// LocalEnv implements durable.ExecutionEnv on Linux and POSIX systems. Exec
// requires /bin/sh and is unsupported on Windows. CWD is a working directory,
// not a filesystem or process sandbox.
type LocalEnv struct {
	cwd string
}

var _ durable.ExecutionEnv = (*LocalEnv)(nil)

// NewLocalEnv canonicalizes cwd and verifies that it names a directory.
func NewLocalEnv(cwd string) (*LocalEnv, error) {
	if cwd == "" {
		return nil, errors.New("tools: cwd must not be empty")
	}
	absolute, err := filepath.Abs(cwd)
	if err != nil {
		return nil, fmt.Errorf("tools: resolve cwd: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("tools: resolve cwd: %w", err)
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return nil, fmt.Errorf("tools: inspect cwd: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("tools: cwd is not a directory")
	}
	return &LocalEnv{cwd: canonical}, nil
}

func (e *LocalEnv) resolve(path string) string {
	return resolveLocalPath(e.cwd, path)
}

// AbsolutePath resolves path as upstream's Node environment does (PathResolver).
func (e *LocalEnv) AbsolutePath(ctx context.Context, path string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return e.resolve(path), nil
}

// Exists reports whether path exists (PathResolver).
func (e *LocalEnv) Exists(ctx context.Context, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, err := os.Lstat(e.resolve(path))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// ReadFile reads the named file; relative paths are based at CWD.
func (e *LocalEnv) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(e.resolve(path))
}

// OpenFile opens path for streaming reads (FileOpener).
func (e *LocalEnv) OpenFile(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.Open(e.resolve(path))
}

// WriteFile writes data to path as upstream's Node environment does
// (fs.writeFile): its directory is created, a symbolic link is written
// through, and an existing file keeps its inode, links, and mode; a new one
// gets 0666 less the umask. Relative paths are based at CWD. The write is in
// place, not atomic.
func (e *LocalEnv) WriteFile(ctx context.Context, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path = e.resolve(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o777); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o666)
}

// Exec executes command through bash, or /bin/sh without bash. Output is combined and retained as a
// 64 KiB tail. Callback errors terminate the process but continue draining its
// pipes to avoid blocked children. Shell execution is unavailable on Windows.
func (e *LocalEnv) Exec(ctx context.Context, command string, onOutput func(string) error) (durable.ExecResult, error) {
	if err := ctx.Err(); err != nil {
		return durable.ExecResult{}, err
	}
	if command == "" {
		return durable.ExecResult{}, errors.New("tools: command must not be empty")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Like upstream, bash when it is installed; otherwise the POSIX shell.
	shell := "/bin/sh"
	if bash, err := exec.LookPath("bash"); err == nil {
		shell = bash
	}
	return e.run(ctx, runCtx, cancel, []string{shell, "-c", command}, onOutput)
}

// ExecArgv runs a program directly with its arguments, so no shell parses
// them (ArgvExecutor). A program that cannot start fails with
// ErrSpawn.
func (e *LocalEnv) ExecArgv(ctx context.Context, argv []string, onOutput func(string) error) (durable.ExecResult, error) {
	if err := ctx.Err(); err != nil {
		return durable.ExecResult{}, err
	}
	if len(argv) == 0 {
		return durable.ExecResult{}, errors.New("tools: command must not be empty")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	return e.run(ctx, runCtx, cancel, argv, onOutput)
}

// ArgvExecutor is optionally implemented by an ExecutionEnv to run a program
// directly, as the powershell tool does.
type ArgvExecutor interface {
	ExecArgv(ctx context.Context, argv []string, onOutput func(string) error) (durable.ExecResult, error)
}

// ErrSpawn marks a program that could not start.
var ErrSpawn = errors.New("tools: program could not start")

func (e *LocalEnv) run(ctx, runCtx context.Context, cancel context.CancelFunc, argv []string, onOutput func(string) error) (durable.ExecResult, error) {
	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = e.cwd
	configureProcess(cmd)
	var output tailBuffer
	var callbackMu sync.Mutex
	var callbackErr error
	writer := func(p []byte) (int, error) {
		output.Write(p)
		if onOutput == nil {
			return len(p), nil
		}
		callbackMu.Lock()
		defer callbackMu.Unlock()
		if callbackErr == nil {
			callbackErr = onOutput(string(p))
			if callbackErr != nil {
				cancel()
			}
		}
		// Keep consuming after failure, so the child cannot block on a full pipe.
		return len(p), nil
	}
	cmd.Stdout = writerFunc(writer)
	cmd.Stderr = writerFunc(writer)
	if err := cmd.Start(); err != nil {
		return durable.ExecResult{}, fmt.Errorf("%w: %v", ErrSpawn, err)
	}
	err := cmd.Wait()
	callbackMu.Lock()
	cbErr := callbackErr
	callbackMu.Unlock()
	if cbErr != nil {
		return durable.ExecResult{Output: output.String()}, cbErr
	}
	if ctx.Err() != nil {
		return durable.ExecResult{Output: output.String()}, ctx.Err()
	}
	result := durable.ExecResult{Output: output.String()}
	if err == nil {
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, nil
	}
	return result, err
}

type writerFunc func([]byte) (int, error)

func (w writerFunc) Write(p []byte) (int, error) { return w(p) }

type tailBuffer struct {
	mu sync.Mutex
	b  []byte
}

func (t *tailBuffer) Write(p []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(p) >= maxExecOutput {
		t.b = append(t.b[:0], p[len(p)-maxExecOutput:]...)
		return
	}
	if excess := len(t.b) + len(p) - maxExecOutput; excess > 0 {
		copy(t.b, t.b[excess:])
		t.b = t.b[:len(t.b)-excess]
	}
	t.b = append(t.b, p...)
}
func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(bytes.Clone(t.b))
}

// mutations is the tail of each file's mutation chain (upstream's
// withFileMutationQueue): a call waits for the one before it, so calls on
// one file run in arrival order; a file's entry leaves with its last call.
var mutations = struct {
	sync.Mutex
	tails map[string]chan struct{}
}{tails: make(map[string]chan struct{})}

// mutationKey is a file's canonical path; for a file that does not exist
// yet, its canonical parent joined with its name, so a write that creates a
// file and a later edit share one key even under a symlinked directory.
func mutationKey(env *LocalEnv, path string) string {
	canonical := env.resolve(path)
	if resolved, err := filepath.EvalSymlinks(canonical); err == nil {
		canonical = resolved
	} else if parent, err := filepath.EvalSymlinks(filepath.Dir(canonical)); err == nil {
		canonical = filepath.Join(parent, filepath.Base(canonical))
	}
	return filepath.Clean(canonical)
}

// queueMutation waits for the earlier mutations of the file and returns the
// release that lets the next one run.
func queueMutation(env *LocalEnv, path string) func() {
	key := mutationKey(env, path)
	done := make(chan struct{})
	mutations.Lock()
	previous := mutations.tails[key]
	mutations.tails[key] = done
	mutations.Unlock()
	if previous != nil {
		<-previous
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			mutations.Lock()
			if mutations.tails[key] == done {
				delete(mutations.tails, key)
			}
			mutations.Unlock()
			close(done)
		})
	}
}

var _ io.Writer = writerFunc(nil)
