// Package tools provides a local execution environment and coding tools.
//
// LocalEnv uses cwd to resolve relative paths and as the process working
// directory. It is not a sandbox: absolute paths, parent traversal, and shell
// commands can access anything permitted to the current process.
package tools

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/js"
)

const maxExecOutput = 64 * 1024

// LocalEnv implements durable.ExecutionEnv on Linux and POSIX systems. Exec
// requires /bin/sh and is unsupported on Windows. CWD is a working directory,
// not a filesystem or process sandbox.
type LocalEnv struct {
	cwd string
	// processes are the running commands, which Cleanup ends.
	processes struct {
		sync.Mutex
		running map[*exec.Cmd]func() error
	}
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

// Exists reports whether path exists, as its FileInfo says (upstream
// exists, PathResolver): a missing path is false, other failures, a file
// of an unsupported kind included, are errors.
func (e *LocalEnv) Exists(ctx context.Context, path string) (bool, error) {
	_, err := e.FileInfo(ctx, path)
	var fileError *FileError
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &fileError) && fileError.Code == "not_found":
		return false, nil
	}
	return false, err
}

// CreateTempDir creates a directory in the system's temporary directory
// named prefix and six random letters and digits, as Node's mkdtemp does
// (upstream NodeExecutionEnv.createTempDir).
func (e *LocalEnv) CreateTempDir(ctx context.Context, prefix string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return mkdtemp(prefix)
}

// CreateTempFile creates an empty file named prefix, a random UUID, and
// suffix in a new temporary directory (upstream
// NodeExecutionEnv.createTempFile).
func (e *LocalEnv) CreateTempFile(ctx context.Context, prefix, suffix string) (string, error) {
	dir, err := e.CreateTempDir(ctx, "tmp-")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, prefix+randomUUID()+suffix)
	if err := os.WriteFile(path, nil, 0o666); err != nil {
		return "", nodeError(err)
	}
	return path, nil
}

// tempNameChars are the characters mkdtemp(3) fills its template with.
const tempNameChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// mkdtemp creates the directory prefix, joined to the system's temporary
// directory as Node's path.join joins it, with six random characters.
func mkdtemp(prefix string) (string, error) {
	base := filepath.Join(os.TempDir(), prefix)
	if prefix != "" && os.IsPathSeparator(prefix[len(prefix)-1]) && !os.IsPathSeparator(base[len(base)-1]) {
		base += string(filepath.Separator)
	}
	var err error
	for range 100 {
		var random [6]byte
		_, _ = rand.Read(random[:])
		for i, b := range random {
			random[i] = tempNameChars[int(b)%len(tempNameChars)]
		}
		dir := base + string(random[:])
		if err = os.Mkdir(dir, 0o700); err == nil {
			return dir, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			break
		}
	}
	// Node names the template in its error.
	return "", syscallError("mkdtemp", base+"XXXXXX", err)
}

// randomUUID is a random (version 4) UUID, as crypto.randomUUID makes.
func randomUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ReadFile reads the named file; relative paths are based at CWD.
func (e *LocalEnv) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(e.resolve(path))
	return data, nodeError(err)
}

// OpenFile opens path for streaming reads (FileOpener).
func (e *LocalEnv) OpenFile(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(e.resolve(path))
	if err != nil {
		return nil, nodeError(err)
	}
	return nodeFile{f}, nil
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
	if err := mkdirAll(filepath.Dir(path)); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nodeError(os.WriteFile(path, data, 0o666))
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
	// emit passes decoded text on; callbackMu is held.
	emit := func(text string) {
		if text == "" {
			return
		}
		output.Write([]byte(text))
		if onOutput == nil || callbackErr != nil {
			return
		}
		if callbackErr = onOutput(text); callbackErr != nil {
			cancel()
		}
	}
	// One decoder per stream, so a character split across chunks of one
	// stream survives interleaving (upstream).
	var stdout, stderr streamDecoder
	stream := func(decoder *streamDecoder) writerFunc {
		return func(p []byte) (int, error) {
			callbackMu.Lock()
			defer callbackMu.Unlock()
			emit(decoder.decode(p, false))
			// Keep consuming after failure, so the child cannot block on a full pipe.
			return len(p), nil
		}
	}
	cmd.Stdout = stream(&stdout)
	cmd.Stderr = stream(&stderr)
	if err := cmd.Start(); err != nil {
		return durable.ExecResult{}, fmt.Errorf("%w: %v", ErrSpawn, err)
	}
	e.track(cmd)
	err := cmd.Wait()
	e.untrack(cmd)
	callbackMu.Lock()
	emit(stdout.decode(nil, true))
	emit(stderr.decode(nil, true))
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

// streamDecoder decodes one output stream as upstream's StreamDecoder: as
// UTF-8 decoded whole, a character split across chunks held back, and a
// byte-order mark at the stream's very start dropped.
type streamDecoder struct {
	decoder js.UTF8Decoder
	started bool
}

func (d *streamDecoder) decode(p []byte, end bool) string {
	text := d.decoder.Decode(p, !end)
	if d.started || text == "" {
		return text
	}
	d.started = true
	return strings.TrimPrefix(text, "\ufeff")
}

// track records a started command for Cleanup.
func (e *LocalEnv) track(cmd *exec.Cmd) {
	cancel := cmd.Cancel
	if cancel == nil {
		cancel = cmd.Process.Kill
	}
	e.processes.Lock()
	defer e.processes.Unlock()
	if e.processes.running == nil {
		e.processes.running = map[*exec.Cmd]func() error{}
	}
	e.processes.running[cmd] = cancel
}

func (e *LocalEnv) untrack(cmd *exec.Cmd) {
	e.processes.Lock()
	defer e.processes.Unlock()
	delete(e.processes.running, cmd)
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
// file and a later edit share one key even under a symlinked directory
// (upstream canonical). A path that cannot be resolved otherwise fails, as
// Node's realpath reports it.
func mutationKey(env *LocalEnv, path string) (string, error) {
	return canonicalPath(filepath.Clean(env.resolve(path)))
}

func canonicalPath(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", realpathError(path, err)
	}
	parent := filepath.Dir(path)
	if parent == path {
		return path, nil
	}
	canonicalParent, err := canonicalPath(parent)
	if err != nil {
		return "", err
	}
	return filepath.Join(canonicalParent, filepath.Base(path)), nil
}

// queueMutation waits for the earlier mutations of the file and returns the
// release that lets the next one run.
func queueMutation(env *LocalEnv, path string) (func(), error) {
	key, err := mutationKey(env, path)
	if err != nil {
		return nil, err
	}
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
	}, nil
}

var _ io.Writer = writerFunc(nil)
