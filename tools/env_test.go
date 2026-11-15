package tools

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestLocalEnvReadWriteAndRelativePaths(t *testing.T) {
	cwd := t.TempDir()
	env, err := NewLocalEnv(cwd)
	if err != nil {
		t.Fatal(err)
	}
	if err := env.WriteFile(context.Background(), "nested/file.txt", []byte("initial")); err != nil {
		t.Fatal(err)
	}
	got, err := env.ReadFile(context.Background(), "nested/file.txt")
	if err != nil || string(got) != "initial" {
		t.Fatalf("read=%q err=%v", got, err)
	}
	entries, err := os.ReadDir(filepath.Join(cwd, "nested"))
	if err != nil || len(entries) != 1 || entries[0].Name() != "file.txt" {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
}

func TestLocalEnvExecOutputAndStatus(t *testing.T) {
	env, err := NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var streamed strings.Builder
	res, err := env.Exec(context.Background(), "printf out; printf err >&2; exit 7", func(s string) error { streamed.WriteString(s); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if res.ExitCode != 7 || len(res.Output) != 6 || streamed.Len() != 6 {
		t.Fatalf("result=%+v stream=%q", res, streamed.String())
	}
}

func TestLocalEnvExecOutputBoundedAndCallbackFailure(t *testing.T) {
	env, err := NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	res, err := env.Exec(context.Background(), "head -c 100000 /dev/zero | tr '\\000' x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Output) != maxExecOutput || !strings.HasSuffix(res.Output, "xxx") {
		t.Fatalf("output size=%d", len(res.Output))
	}
	want := errors.New("callback failed")
	_, err = env.Exec(context.Background(), "while :; do echo x; done", func(string) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error=%v", err)
	}
}

func TestLocalEnvExecCancellationKillsProcessGroup(t *testing.T) {
	env, err := NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := env.Exec(ctx, "sleep 30 & wait", nil); done <- err }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("process did not terminate")
	}
}

func TestFileMutationsRunInArrivalOrder(t *testing.T) {
	env, err := NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key, err := mutationKey(env, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	queue := func(path string) func() {
		release, err := queueMutation(env, path)
		if err != nil {
			t.Error(err)
			return func() {}
		}
		return release
	}
	tail := func() chan struct{} {
		mutations.Lock()
		defer mutations.Unlock()
		return mutations.tails[key]
	}
	// queued waits until a call after the tail before is queued.
	queued := func(before chan struct{}) chan struct{} {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if now := tail(); now != before {
				return now
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("a mutation never queued")
		return nil
	}
	first := queue("a.txt")
	order := make(chan int, 3)
	after := func(n int, path string) {
		release := queue(path)
		order <- n
		release()
	}
	second := tail()
	go after(2, "./a.txt") // the same file by another path
	third := queued(second)
	go after(3, "a.txt")
	queued(third)
	// Other files never wait.
	other := queue("b.txt")
	other()
	order <- 1
	first()
	got := []int{<-order, <-order, <-order}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("order=%v", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for tail() != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if tail() != nil {
		t.Fatal("the file's mutation queue was left behind")
	}
}

// Temporary paths are named as Node's environment names them: mkdtemp's six
// letters and digits, and a file named with a random UUID in its own
// directory, which is where bash spills its output.
func TestTempPathsAreNamedAsNodes(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	// As Node's os.tmpdir() gives it: unresolved, as macOS's /var is.
	tmp := os.TempDir()
	cwd := t.TempDir()
	env, err := NewLocalEnv(cwd)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dir, err := env.CreateTempDir(ctx, "pi-x-")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^pi-x-[A-Za-z0-9]{6}$`).MatchString(filepath.Base(dir)) || filepath.Dir(dir) != tmp {
		t.Fatalf("dir %q", dir)
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("dir mode: %v %v", info, err)
	}
	uuidFile := regexp.MustCompile(`^` + regexp.QuoteMeta(tmp) + `/tmp-[A-Za-z0-9]{6}/pi-output-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.log$`)
	file, err := env.CreateTempFile(ctx, "pi-output-", ".log")
	if err != nil {
		t.Fatal(err)
	}
	if !uuidFile.MatchString(file) {
		t.Fatalf("file %q", file)
	}
	if data, err := os.ReadFile(file); err != nil || len(data) != 0 {
		t.Fatalf("file content %q %v", data, err)
	}
	got, _ := runCoding(t, cwd, "bash", `{"command":"seq 1 3000"}`)
	spill := regexp.MustCompile(`Full output: (\S+)`).FindStringSubmatch(got.Content)
	if spill == nil || !uuidFile.MatchString(spill[1]) {
		t.Fatalf("spill in %q", got.Content[len(got.Content)-200:])
	}
	if data, err := os.ReadFile(spill[1]); err != nil || !strings.HasPrefix(string(data), "1\n2\n") || !strings.HasSuffix(string(data), "2999\n3000\n") {
		t.Fatalf("spill content %d bytes %v", len(data), err)
	}
}

// Each output stream is decoded as upstream's StreamDecoder decodes it: a
// byte-order mark at its start dropped, a character split across writes
// joined, an invalid byte replaced, and stdout and stderr decoded apart.
func TestExecDecodesEachStream(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	env, err := NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	run := func(command string) string {
		var got strings.Builder
		if _, err := env.Exec(context.Background(), command, func(text string) error {
			got.WriteString(text)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return got.String()
	}
	if got := run(`printf '\xef\xbb\xbfA\xc3'; sleep 0.05; printf '\xa9\xff\n'`); got != "Aé\ufffd\n" {
		t.Fatalf("stdout: %q", got)
	}
	got := run(`printf '\xc3' >&2; sleep 0.05; printf 'x'; sleep 0.05; printf '\xa9' >&2`)
	if strings.ContainsRune(got, '\ufffd') || !strings.Contains(got, "x") || !strings.Contains(got, "é") {
		t.Fatalf("streams: %q", got)
	}
	if got := run(`printf 'ok\xe2\x9c'`); got != "ok\ufffd" {
		t.Fatalf("cut short: %q", got)
	}
}
