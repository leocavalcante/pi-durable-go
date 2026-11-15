package tools

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Upstream's NodeExecutionEnv tests (test/env-node.test.ts), on a local
// environment.

func nodeTestEnv(t *testing.T) (*LocalEnv, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	env, err := NewLocalEnv(root)
	if err != nil {
		t.Fatal(err)
	}
	return env, root
}

func fileErrorOf(t *testing.T, err error) *FileError {
	t.Helper()
	var fileError *FileError
	if !errors.As(err, &fileError) {
		t.Fatalf("not a FileError: %v", err)
	}
	return fileError
}

func TestNodeEnvReadsWritesListsAndRemoves(t *testing.T) {
	env, root := nodeTestEnv(t)
	ctx := context.Background()
	if got, _ := env.AbsolutePath(ctx, "nested/child"); got != filepath.Join(root, "nested/child") {
		t.Fatalf("absolute %q", got)
	}
	if got := env.JoinPath(root, "nested", "child"); got != filepath.Join(root, "nested", "child") {
		t.Fatalf("join %q", got)
	}
	must(t, env.CreateDir(ctx, "nested/child", true))
	must(t, env.WriteFile(ctx, "nested/child/file.txt", []byte("hel")))
	must(t, env.AppendFile(ctx, "nested/child/file.txt", []byte("lo")))
	if text, err := env.ReadTextFile(ctx, "nested/child/file.txt"); err != nil || text != "hello" {
		t.Fatalf("text %q %v", text, err)
	}
	if lines, err := env.ReadTextLines(ctx, "nested/child/file.txt", 1); err != nil || len(lines) != 1 || lines[0] != "hello" {
		t.Fatalf("lines %q %v", lines, err)
	}
	entries, err := env.ListDir(ctx, "nested/child")
	if err != nil || len(entries) != 1 || entries[0].Name != "file.txt" || entries[0].Path != filepath.Join(root, "nested/child/file.txt") ||
		entries[0].Kind != "file" || entries[0].Size != 5 || entries[0].MtimeMs == 0 {
		t.Fatalf("entries %+v %v", entries, err)
	}
	if ok, err := env.Exists(ctx, "nested/child/file.txt"); !ok || err != nil {
		t.Fatal("the file does not exist")
	}
	must(t, env.Remove(ctx, "nested/child/file.txt", false, false))
	if ok, err := env.Exists(ctx, "nested/child/file.txt"); ok || err != nil {
		t.Fatal("the file still exists")
	}
}

func TestNodeEnvFileInfoDoesNotFollowSymlinks(t *testing.T) {
	env, root := nodeTestEnv(t)
	ctx := context.Background()
	must(t, env.CreateDir(ctx, "dir", true))
	writeText(t, env, "dir/file.txt", "hello")
	must(t, os.Symlink(filepath.Join(root, "dir/file.txt"), filepath.Join(root, "file-link")))
	must(t, os.Symlink(filepath.Join(root, "dir"), filepath.Join(root, "dir-link")))
	for _, c := range []struct{ name, kind string }{{"dir", "directory"}, {"dir/file.txt", "file"}, {"file-link", "symlink"}, {"dir-link", "symlink"}} {
		info, err := env.FileInfo(ctx, c.name)
		if err != nil || info.Kind != c.kind || info.Name != filepath.Base(c.name) || info.Path != filepath.Join(root, c.name) {
			t.Fatalf("%s: %+v %v", c.name, info, err)
		}
	}
	if canonical, err := env.CanonicalPath(ctx, "file-link"); err != nil || canonical != filepath.Join(root, "dir/file.txt") {
		t.Fatalf("canonical %q %v", canonical, err)
	}
	entries, err := env.ListDir(ctx, ".")
	must(t, err)
	kinds := map[string]string{}
	for _, entry := range entries {
		kinds[entry.Name] = entry.Kind
	}
	if kinds["file-link"] != "symlink" || kinds["dir-link"] != "symlink" || kinds["dir"] != "directory" {
		t.Fatalf("kinds %v", kinds)
	}
}

func TestNodeEnvErrorsForMissingPathsAndNonDirectories(t *testing.T) {
	env, root := nodeTestEnv(t)
	ctx := context.Background()
	_, err := env.FileInfo(ctx, "missing.txt")
	if fe := fileErrorOf(t, err); fe.Code != "not_found" || fe.Path != filepath.Join(root, "missing.txt") {
		t.Fatalf("missing %+v", fe)
	}
	if ok, err := env.Exists(ctx, "missing.txt"); ok || err != nil {
		t.Fatalf("exists %v %v", ok, err)
	}
	writeText(t, env, "file.txt", "hello")
	_, err = env.ListDir(ctx, "file.txt")
	if fe := fileErrorOf(t, err); fe.Code != "not_directory" {
		t.Fatalf("listing a file %+v", fe)
	}
	_, err = env.OpenTextLineReader(ctx, "missing.txt")
	if fe := fileErrorOf(t, err); fe.Code != "not_found" || fe.Path != filepath.Join(root, "missing.txt") {
		t.Fatalf("line reader %+v", fe)
	}
}

func TestNodeEnvAppendsRenamesAndCreatesTemporaryFiles(t *testing.T) {
	env, root := nodeTestEnv(t)
	ctx := context.Background()
	must(t, env.AppendFile(ctx, "new/nested/file.txt", []byte("a")))
	must(t, env.AppendFile(ctx, "new/nested/file.txt", []byte("b")))
	if text, _ := env.ReadTextFile(ctx, "new/nested/file.txt"); text != "ab" {
		t.Fatalf("appended %q", text)
	}
	writeText(t, env, "source.txt", "new")
	writeText(t, env, "destination.txt", "old")
	must(t, env.RenameFile(ctx, "source.txt", "destination.txt"))
	if ok, _ := env.Exists(ctx, "source.txt"); ok {
		t.Fatal("the source stayed")
	}
	if text, _ := env.ReadTextFile(ctx, "destination.txt"); text != "new" {
		t.Fatalf("renamed %q", text)
	}
	err := env.RenameFile(ctx, "missing-source.txt", "destination.txt")
	if fe := fileErrorOf(t, err); fe.Code != "not_found" || fe.Path != filepath.Join(root, "missing-source.txt") {
		t.Fatalf("rename of a missing source %+v", fe)
	}
	t.Setenv("TMPDIR", t.TempDir())
	dir, err := env.CreateTempDir(ctx, "node-env-test-")
	must(t, err)
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
	file, err := env.CreateTempFile(ctx, "prefix-", ".txt")
	must(t, err)
	if text, err := env.ReadTextFile(ctx, file); err != nil || text != "" || !strings.HasSuffix(file, ".txt") {
		t.Fatalf("temporary file %q %q %v", file, text, err)
	}
}

func TestNodeEnvCreateDirAndRemoveOptions(t *testing.T) {
	env, _ := nodeTestEnv(t)
	ctx := context.Background()
	if fe := fileErrorOf(t, env.CreateDir(ctx, "missing/child", false)); fe.Code != "not_found" {
		t.Fatalf("non-recursive %+v", fe)
	}
	writeText(t, env, "dir/child/file.txt", "hello")
	if err := env.Remove(ctx, "dir", false, false); err == nil {
		t.Fatal("removed a directory without recursive")
	}
	must(t, env.Remove(ctx, "dir", true, false))
	if ok, _ := env.Exists(ctx, "dir"); ok {
		t.Fatal("the directory stayed")
	}
	if err := env.Remove(ctx, "missing", false, false); err == nil {
		t.Fatal("removed a missing path without force")
	}
	must(t, env.Remove(ctx, "missing", false, true))
}

// Operations of an ended context fail as aborted and change nothing.
func TestNodeEnvAbortedFileOperationsHaveNoEffect(t *testing.T) {
	env, _ := nodeTestEnv(t)
	writeText(t, env, "file.txt", "hello")
	t.Setenv("TMPDIR", t.TempDir())
	ctx := canceled()
	check := func(name string, err error) {
		t.Helper()
		if fe := fileErrorOf(t, err); fe.Code != "aborted" || !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: %+v", name, fe)
		}
	}
	_, err := env.ReadTextFile(ctx, "file.txt")
	check("readTextFile", err)
	_, err = env.ReadTextLines(ctx, "file.txt", -1)
	check("readTextLines", err)
	_, err = env.ReadBinaryFile(ctx, "file.txt")
	check("readBinaryFile", err)
	_, err = env.OpenTextLineReader(ctx, "file.txt")
	check("openTextLineReader", err)
	check("writeFile", env.WriteFile(ctx, "other.txt", []byte("hello")))
	check("appendFile", env.AppendFile(ctx, "file.txt", []byte(" world")))
	check("truncateFile", env.TruncateFile(ctx, "file.txt", 1))
	check("flushFile", env.FlushFile(ctx, "file.txt"))
	check("renameFile", env.RenameFile(ctx, "file.txt", "renamed.txt"))
	_, err = env.FileInfo(ctx, "file.txt")
	check("fileInfo", err)
	_, err = env.ListDir(ctx, ".")
	check("listDir", err)
	_, err = env.CanonicalPath(ctx, "file.txt")
	check("canonicalPath", err)
	_, err = env.Exists(ctx, "file.txt")
	check("exists", err)
	check("createDir", env.CreateDir(ctx, "dir", true))
	check("remove", env.Remove(ctx, "file.txt", false, false))
	_, err = env.CreateTempDir(ctx, "tmp-")
	check("createTempDir", err)
	_, err = env.CreateTempFile(ctx, "", "")
	check("createTempFile", err)
	if text, _ := env.ReadTextFile(context.Background(), "file.txt"); text != "hello" {
		t.Fatalf("the file changed: %q", text)
	}
	entries, _ := env.ListDir(context.Background(), ".")
	if len(entries) != 1 || entries[0].Name != "file.txt" {
		t.Fatalf("entries %+v", entries)
	}
	if temps, _ := os.ReadDir(os.Getenv("TMPDIR")); len(temps) != 0 {
		t.Fatalf("temporary files were made: %v", temps)
	}
}

func TestNodeEnvTruncatesAndFlushes(t *testing.T) {
	env, root := nodeTestEnv(t)
	ctx := context.Background()
	must(t, env.WriteFile(ctx, "file.bin", []byte{0x61, 0xc3, 0xa9, 0x0a, 0x62, 0x0a}))
	// Truncation is byte-exact even when the boundary splits a character.
	must(t, env.TruncateFile(ctx, "file.bin", 2))
	if data, _ := env.ReadBinaryFile(ctx, "file.bin"); string(data) != "\x61\xc3" {
		t.Fatalf("truncated %v", data)
	}
	must(t, env.TruncateFile(ctx, "file.bin", 4))
	if data, _ := env.ReadBinaryFile(ctx, "file.bin"); string(data) != "\x61\xc3\x00\x00" {
		t.Fatalf("extended %v", data)
	}
	writeText(t, env, "file.txt", "hello")
	for _, size := range []int64{-1, maxSafeInteger + 1} {
		if fe := fileErrorOf(t, env.TruncateFile(ctx, "file.txt", size)); fe.Code != "invalid" || fe.Path != filepath.Join(root, "file.txt") {
			t.Fatalf("size %d: %+v", size, fe)
		}
	}
	if fe := fileErrorOf(t, env.TruncateFile(ctx, "missing.txt", 0)); fe.Code != "not_found" {
		t.Fatalf("missing %+v", fe)
	}
	if ok, _ := env.Exists(ctx, "missing.txt"); ok {
		t.Fatal("truncation created a file")
	}
	writeText(t, env, "durable.txt", "durable")
	must(t, env.FlushFile(ctx, "durable.txt"))
	if text, _ := env.ReadTextFile(ctx, "durable.txt"); text != "durable" {
		t.Fatalf("flushed %q", text)
	}
	if fe := fileErrorOf(t, env.FlushFile(ctx, "missing.txt")); fe.Code != "not_found" || fe.Path != filepath.Join(root, "missing.txt") {
		t.Fatalf("flush of a missing file %+v", fe)
	}
	must(t, env.CreateDir(ctx, "dir", true))
	if fe := fileErrorOf(t, env.FlushFile(ctx, "dir")); fe.Code != "is_directory" {
		t.Fatalf("flush of a directory %+v", fe)
	}
	env.Cleanup(ctx)
}

func TestNodeEnvTextLineReader(t *testing.T) {
	env, root := nodeTestEnv(t)
	ctx := context.Background()
	readAll := func(name string) []TextLine {
		t.Helper()
		reader, err := env.OpenTextLineReader(ctx, name)
		must(t, err)
		defer reader.Close()
		var lines []TextLine
		for {
			line, ok, err := reader.ReadLine(ctx)
			must(t, err)
			if !ok {
				return lines
			}
			lines = append(lines, line)
		}
	}
	writeText(t, env, "lines.txt", "one\r\n\ntwo\npartial")
	if got := readAll("lines.txt"); len(got) != 4 || got[0] != (TextLine{"one\r", true}) || got[1] != (TextLine{"", true}) ||
		got[2] != (TextLine{"two", true}) || got[3] != (TextLine{"partial", false}) {
		t.Fatalf("lines %+v", got)
	}
	writeText(t, env, "empty.txt", "")
	writeText(t, env, "newline.txt", "\n")
	if got := readAll("empty.txt"); len(got) != 0 {
		t.Fatalf("empty %+v", got)
	}
	if got := readAll("newline.txt"); len(got) != 1 || got[0] != (TextLine{"", true}) {
		t.Fatalf("lone newline %+v", got)
	}
	// A four-byte character across the first 64 KiB chunk, and a line
	// longer than a chunk.
	first := strings.Repeat("a", 64*1024-2) + "😀tail"
	second := strings.Repeat("é", 100000)
	writeText(t, env, "large.txt", first+"\n"+second)
	if got := readAll("large.txt"); len(got) != 2 || got[0] != (TextLine{first, true}) || got[1] != (TextLine{second, false}) {
		t.Fatalf("large: %d lines", len(got))
	}
	reader, err := env.OpenTextLineReader(ctx, "lines.txt")
	must(t, err)
	must(t, reader.Close())
	must(t, reader.Close())
	_, _, err = reader.ReadLine(ctx)
	if fe := fileErrorOf(t, err); fe.Code != "invalid" || fe.Path != filepath.Join(root, "lines.txt") {
		t.Fatalf("read after close %+v", fe)
	}
	if lines, err := env.ReadTextLines(ctx, "lines.txt", 1); err != nil || len(lines) != 1 || lines[0] != "one\r" {
		t.Fatalf("limited %q %v", lines, err)
	}
}

func execCollect(t *testing.T, env *LocalEnv, ctx context.Context, command string, options ExecOptions) (ShellExecResult, string, error) {
	t.Helper()
	var output strings.Builder
	options.OnOutput = func(text string, _ OutputInfo) error {
		output.WriteString(text)
		return nil
	}
	result, err := env.ExecWith(ctx, command, options)
	return result, output.String(), err
}

func execCode(err error) string {
	var execErr *ExecutionError
	if errors.As(err, &execErr) {
		return execErr.Code
	}
	if err == nil {
		return ""
	}
	return "other: " + err.Error()
}

func TestNodeEnvExecutesInCwdWithVariables(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	env, root := nodeTestEnv(t)
	ctx := context.Background()
	result, output, err := execCollect(t, env, ctx, `printf '%s' "$NODE_ENV_TEST" > cwd-marker.txt; printf '%s:%s' "$PWD" "$NODE_ENV_TEST"`,
		ExecOptions{Env: map[string]string{"NODE_ENV_TEST": "ok"}})
	if err != nil || result.ExitCode != 0 || output != root+":ok" {
		t.Fatalf("%+v %q %v", result, output, err)
	}
	if marker, _ := os.ReadFile(filepath.Join(root, "cwd-marker.txt")); string(marker) != "ok" {
		t.Fatalf("marker %q", marker)
	}
	// Variables over the environment's base ones: a missing one keeps the
	// base, an empty one shadows it, and a value replaces it.
	base := map[string]string{"PI_SESSION_FILE": "/stale/parent.jsonl", "PI_CODING_AGENT": "true", "PI_NODE_ENV_PRESERVED_TEST": "preserved"}
	for _, c := range []struct {
		overrides map[string]string
		want      string
	}{
		{nil, "x:/stale/parent.jsonl"},
		{map[string]string{"PI_SESSION_FILE": ""}, "x:"},
		{map[string]string{"PI_SESSION_FILE": "/sessions/current.jsonl"}, "x:/sessions/current.jsonl"},
	} {
		based, err := NewLocalEnvWith(root, LocalEnvOptions{ShellEnv: base})
		must(t, err)
		_, output, err := execCollect(t, based, ctx, `printf '%s:%s|%s|%s' "${PI_SESSION_FILE+x}" "${PI_SESSION_FILE-}" "$PI_CODING_AGENT" "$PI_NODE_ENV_PRESERVED_TEST"`,
			ExecOptions{Env: c.overrides})
		if err != nil || output != c.want+"|true|preserved" {
			t.Fatalf("%v: %q %v", c.overrides, output, err)
		}
	}
	// Replacing rather than inheriting the environment drops the base too.
	t.Setenv("PI_NODE_ENV_INHERITED_TEST", "host")
	configured, err := NewLocalEnvWith(root, LocalEnvOptions{ShellEnv: map[string]string{"PI_NODE_ENV_CONFIGURED_TEST": "configured"}})
	must(t, err)
	inherit := false
	_, output, err = execCollect(t, configured, ctx, `printf '%s:%s:%s' "${PI_NODE_ENV_INHERITED_TEST-}" "${PI_NODE_ENV_CONFIGURED_TEST-}" "${PI_NODE_ENV_EXPLICIT_TEST-}"`,
		ExecOptions{InheritEnv: &inherit, Env: map[string]string{"PI_NODE_ENV_EXPLICIT_TEST": "explicit"}})
	if err != nil || output != "::explicit" {
		t.Fatalf("replaced %q %v", output, err)
	}
}

func TestNodeEnvExecResultsAndErrors(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	env, root := nodeTestEnv(t)
	ctx := context.Background()
	if result, output, err := execCollect(t, env, ctx, "printf out; printf err >&2", ExecOptions{}); err != nil || result != (ShellExecResult{}) ||
		!strings.Contains(output, "out") || !strings.Contains(output, "err") {
		t.Fatalf("streams %+v %q %v", result, output, err)
	}
	if result, err := env.ExecWith(ctx, "exit 7", ExecOptions{}); err != nil || result != (ShellExecResult{ExitCode: 7}) {
		t.Fatalf("exit %+v %v", result, err)
	}
	if runtime.GOOS != "windows" {
		if result, err := env.ExecWith(ctx, "kill -9 $$", ExecOptions{}); err != nil || result.ExitCode != 128+9 {
			t.Fatalf("killed %+v %v", result, err)
		}
	}
	if _, err := env.ExecWith(ctx, "sleep 5", ExecOptions{Timeout: 0.01}); execCode(err) != "timeout" {
		t.Fatalf("timeout %v", err)
	}
	for _, timeout := range []float64{-1, 2147484} {
		if _, err := env.ExecWith(ctx, "touch spawned", ExecOptions{Timeout: timeout}); execCode(err) != "timeout" || !strings.Contains(err.Error(), "Invalid timeout") {
			t.Fatalf("timeout %v: %v", timeout, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "spawned")); err == nil {
		t.Fatal("an invalid timeout spawned")
	}
	_, err := env.ExecWith(ctx, "printf out", ExecOptions{OnOutput: func(string, OutputInfo) error { return errors.New("callback failed") }})
	if execCode(err) != "callback_error" || err.Error() != "callback failed" {
		t.Fatalf("callback %v", err)
	}
	missing, err := NewLocalEnvWith(root, LocalEnvOptions{ShellPath: filepath.Join(root, "missing-shell")})
	must(t, err)
	if _, err := missing.ExecWith(ctx, "printf ok", ExecOptions{}); execCode(err) != "shell_unavailable" {
		t.Fatalf("missing shell %v", err)
	}
	writeText(t, env, "not-executable-shell", "not executable")
	broken, err := NewLocalEnvWith(root, LocalEnvOptions{ShellPath: filepath.Join(root, "not-executable-shell")})
	must(t, err)
	if _, err := broken.ExecWith(ctx, "printf ok", ExecOptions{}); execCode(err) != "spawn_error" {
		t.Fatalf("spawn %v", err)
	}
	if _, err := env.ExecWith(canceled(), "touch spawned", ExecOptions{}); execCode(err) != "aborted" {
		t.Fatalf("pre-aborted %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "spawned")); err == nil {
		t.Fatal("an aborted command spawned")
	}
	abortCtx, abort := context.WithCancel(ctx)
	go func() {
		time.Sleep(100 * time.Millisecond)
		abort()
	}()
	if _, err := env.ExecWith(abortCtx, "sleep 5", ExecOptions{}); execCode(err) != "aborted" {
		t.Fatalf("aborted %v", err)
	}
}

func TestNodeEnvCleanupTerminatesCommands(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	env, root := nodeTestEnv(t)
	done := make(chan error, 1)
	go func() {
		_, err := env.ExecWith(context.Background(), "touch started; sleep 60", ExecOptions{})
		done <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(root, "started")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the command never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	env.Cleanup(context.Background())
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a cleaned-up command ends as a result: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup did not end the command")
	}
}

func TestNodeEnvSpills(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	env, _ := nodeTestEnv(t)
	ctx := context.Background()
	t.Setenv("TMPDIR", t.TempDir())
	if result, err := env.ExecWith(ctx, "printf short", ExecOptions{Spill: &SpillOptions{AfterBytes: 100, AfterLines: 10}}); err != nil || result.SpillPath != "" {
		t.Fatalf("below the thresholds %+v %v", result, err)
	}
	// The spill keeps the exact raw bytes while output is decoded.
	result, output, err := execCollect(t, env, ctx, `printf 'f\200\000o'`, ExecOptions{Spill: &SpillOptions{AfterBytes: 1, AfterLines: 10}})
	must(t, err)
	if spilled, _ := os.ReadFile(result.SpillPath); string(spilled) != "f\x80\x00o" || output != "f\ufffd\x00o" {
		t.Fatalf("raw spill %q, output %q", spilled, output)
	}
	_, err = env.ExecWith(ctx, "printf 12345678901234567890; sleep 5", ExecOptions{Timeout: 0.3, Spill: &SpillOptions{AfterBytes: 10, AfterLines: 10}})
	var execErr *ExecutionError
	if !errors.As(err, &execErr) || execErr.Code != "timeout" || execErr.SpillPath == "" {
		t.Fatalf("timeout %v", err)
	}
	if spilled, _ := os.ReadFile(execErr.SpillPath); string(spilled) != "12345678901234567890" {
		t.Fatalf("timed-out spill %q", spilled)
	}
	result, err = env.ExecWith(ctx, "head -c 500000 /dev/zero | tr '\\0' x", ExecOptions{Spill: &SpillOptions{AfterBytes: 10, AfterLines: 10}})
	must(t, err)
	if spilled, _ := os.ReadFile(result.SpillPath); len(spilled) != 500000 {
		t.Fatalf("large spill %d bytes", len(spilled))
	}
	result, output, err = execCollect(t, env, ctx, "i=1; while [ $i -le 15000 ]; do echo line-$i; i=$((i+1)); done", ExecOptions{Spill: &SpillOptions{AfterBytes: 1024 * 1024, AfterLines: 100}})
	must(t, err)
	var want strings.Builder
	for i := 1; i <= 15000; i++ {
		want.WriteString("line-" + itoa(i) + "\n")
	}
	if output != want.String() {
		t.Fatalf("streamed %d bytes", len(output))
	}
	if lines, err := env.ReadTextLines(ctx, result.SpillPath, -1); err != nil || len(lines) != 15000 || lines[14999] != "line-15000" {
		t.Fatalf("spilled lines %d %v", len(lines), err)
	}
	// A spill that cannot be written fails the command rather than losing
	// output silently.
	readOnly := t.TempDir()
	must(t, os.Chmod(readOnly, 0o555))
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o755) })
	t.Setenv("TMPDIR", readOnly)
	if os.Geteuid() != 0 {
		_, err = env.ExecWith(ctx, "printf 12345678901234567890", ExecOptions{Spill: &SpillOptions{AfterBytes: 10, AfterLines: 10}})
		if execCode(err) != "unknown" || !strings.Contains(err.Error(), "Failed to preserve complete shell output") {
			t.Fatalf("lost spill %v", err)
		}
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// The legacy WSL launcher reads the command on stdin; other bashes take it
// with -c (upstream #9490). A fake launcher named like Windows' bash.exe
// reports the arguments it got.
func TestNodeEnvSelectsTheCommandTransport(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the fake launcher is a POSIX script")
	}
	if _, err := os.Stat("/bin/bash"); err != nil {
		t.Skip("/bin/bash is missing")
	}
	unset := "<unset>"
	for _, c := range []struct{ shellPath, systemRoot, wantArg string }{
		{`C:\Windows\System32\bash.exe`, unset, "-s"},
		{`D:\WinNT\System32\bash.exe`, `D:\WinNT`, "-s"},
		{`D:\WinNT\Sysnative\bash.exe`, `D:\WinNT`, "-s"},
		{`D:\WinNT\System32\bash.exe`, "", "-c"},
		{`E:\Windows\System32\bash.exe`, `D:\WinNT`, "-c"},
	} {
		t.Run(c.shellPath+" "+c.systemRoot, func(t *testing.T) {
			root := t.TempDir()
			script := "#!/bin/sh\nprintf 'args:%s\\n' \"$*\" >&2\nexec /bin/bash \"$@\"\n"
			must(t, os.WriteFile(filepath.Join(root, c.shellPath), []byte(script), 0o755))
			t.Chdir(root)
			t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("SystemRoot", c.systemRoot)
			if c.systemRoot == unset {
				must(t, os.Unsetenv("SystemRoot"))
			}
			env, err := NewLocalEnvWith(root, LocalEnvOptions{ShellPath: c.shellPath})
			must(t, err)
			result, output, err := execCollect(t, env, context.Background(), `name='World'; echo "Hello, ${name}!"`, ExecOptions{})
			if err != nil || result.ExitCode != 0 || !strings.Contains(output, "Hello, World!") || !strings.Contains(output, "args:"+c.wantArg) {
				t.Fatalf("%+v %q %v", result, output, err)
			}
		})
	}
}
