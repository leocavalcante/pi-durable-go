package tools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	key := mutationKey(env, "a.txt")
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
	first := queueMutation(env, "a.txt")
	order := make(chan int, 3)
	after := func(n int, path string) {
		release := queueMutation(env, path)
		order <- n
		release()
	}
	second := tail()
	go after(2, "./a.txt") // the same file by another path
	third := queued(second)
	go after(3, "a.txt")
	queued(third)
	// Other files never wait.
	other := queueMutation(env, "b.txt")
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
