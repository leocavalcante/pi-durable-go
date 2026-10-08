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
