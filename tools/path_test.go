package tools

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Paths resolve as upstream's tools and Node environment resolve them
// (expectations from TestCrosscheckReadPaths and upstream's resolvePath).
func TestToolPathsResolveAsUpstream(t *testing.T) {
	if got := normalizeToolPath("@src/a\u00a0b\u202fc.go"); got != "src/a b c.go" {
		t.Fatalf("normalizeToolPath=%q", got)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home directory")
	}
	for path, want := range map[string]string{
		"~":                   home,
		"~/notes.md":          filepath.Join(home, "notes.md"),
		"file:///tmp/x.txt":   "/tmp/x.txt",
		"file://host/x.txt":   "/work/file:/host/x.txt",
		"rel/../x.txt":        "/work/x.txt",
		"/abs//y.txt":         "/abs/y.txt",
		"file:///tmp/a%20b.c": "/tmp/a b.c",
	} {
		if got := resolveLocalPath("/work", path); got != filepath.FromSlash(want) {
			t.Errorf("resolveLocalPath(%q)=%q, want %q", path, got, want)
		}
	}
	dir := t.TempDir()
	for name, content := range map[string]string{"Shot at 10.00.00\u202fAM.txt": "screenshot", "it\u2019s.txt": "curly", "cafe\u0301.txt": "decomposed"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for path, want := range map[string]string{"Shot at 10.00.00 AM.txt": "screenshot", "@it's.txt": "curly", "caf\u00e9.txt": "decomposed"} {
		args, _ := json.Marshal(map[string]string{"path": path})
		if result, _ := runCoding(t, dir, "read", string(args)); result.IsError || result.Content != want {
			t.Errorf("read %q: %q (error %v)", path, result.Content, result.IsError)
		}
	}
}

// Writes go in place, as upstream's Node environment writes (fs.writeFile):
// a hard link sees the new content, a symbolic link stays a link and its
// target is written, and a dangling link creates its target.
func TestLocalEnvWritesInPlace(t *testing.T) {
	dir := t.TempDir()
	original := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(original, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(original, filepath.Join(dir, "hard.txt")); err != nil {
		t.Skip("no hard links")
	}
	if err := os.Symlink("a.txt", filepath.Join(dir, "soft.txt")); err != nil {
		t.Skip("no symbolic links")
	}
	if err := os.Symlink("created.txt", filepath.Join(dir, "dangling.txt")); err != nil {
		t.Fatal(err)
	}
	if result, _ := runCoding(t, dir, "write", `{"path":"soft.txt","content":"new"}`); result.IsError {
		t.Fatalf("write: %s", result.Content)
	}
	if result, _ := runCoding(t, dir, "write", `{"path":"dangling.txt","content":"made"}`); result.IsError {
		t.Fatalf("write dangling: %s", result.Content)
	}
	for name, want := range map[string]string{"a.txt": "new", "hard.txt": "new", "created.txt": "made"} {
		if data, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(data) != want {
			t.Errorf("%s=%q err=%v, want %q", name, data, err, want)
		}
	}
	if info, err := os.Lstat(filepath.Join(dir, "soft.txt")); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("soft.txt is no longer a link: %v", err)
	}
	if info, err := os.Stat(original); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
}
