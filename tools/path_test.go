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
