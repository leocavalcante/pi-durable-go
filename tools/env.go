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
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(e.cwd, filepath.Clean(path))
}

// ReadFile reads the named file; relative paths are based at CWD.
func (e *LocalEnv) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return os.ReadFile(e.resolve(path))
}

// WriteFile atomically replaces path using a temporary file in its directory.
// Relative paths are based at CWD. Existing permissions are retained.
func (e *LocalEnv) WriteFile(ctx context.Context, path string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path = e.resolve(path)
	if canonical, err := filepath.EvalSymlinks(path); err == nil {
		path = canonical
	} else if info, lstatErr := os.Lstat(path); lstatErr == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("tools: cannot write dangling symbolic link: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	mode := os.FileMode(0o644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".durable-write-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Rename(temp, path)
}

// Exec executes command through /bin/sh. Output is combined and retained as a
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
	cmd := exec.CommandContext(runCtx, "/bin/sh", "-c", command)
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
		return durable.ExecResult{}, err
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

var mutations = struct {
	sync.Mutex
	locks map[string]*sync.Mutex
}{locks: make(map[string]*sync.Mutex)}

func mutationLock(env *LocalEnv, path string) *sync.Mutex {
	canonical := env.resolve(path)
	if resolved, err := filepath.EvalSymlinks(canonical); err == nil {
		canonical = resolved
	} else if parent, err := filepath.EvalSymlinks(filepath.Dir(canonical)); err == nil {
		canonical = filepath.Join(parent, filepath.Base(canonical))
	}
	key := filepath.Clean(canonical)
	mutations.Lock()
	defer mutations.Unlock()
	if lock := mutations.locks[key]; lock != nil {
		return lock
	}
	lock := new(sync.Mutex)
	mutations.locks[key] = lock
	return lock
}

var _ io.Writer = writerFunc(nil)
