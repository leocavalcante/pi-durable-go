//go:build crosscheck

// Package crosscheck runs upstream Pi Durable's own code, from the pinned
// release's TypeScript sources, on cases the Go tests send, so tests compare
// the port with upstream directly (make crosscheck). It needs Node 22.6+ and
// the checkout scripts/crosscheck-setup.sh prepares, named by
// PI_DURABLE_CROSSCHECK_UPSTREAM; without it the tests skip.
package crosscheck

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Upstream is the prepared upstream checkout, or "" when there is none.
func Upstream() string { return os.Getenv("PI_DURABLE_CROSSCHECK_UPSTREAM") }

// Run runs op on input in upstream's code and decodes its result into
// output; it skips the test without a prepared upstream checkout.
func Run(t testing.TB, op string, input, output any) {
	t.Helper()
	if Upstream() == "" {
		t.Skip("no upstream checkout: run make crosscheck, or set PI_DURABLE_CROSSCHECK_UPSTREAM")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("no node")
	}
	_, file, _, _ := runtime.Caller(0)
	oracle := filepath.Join(filepath.Dir(file), "oracle.mts")
	body, err := json.Marshal(map[string]any{"op": op, "input": input})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(node, "--experimental-strip-types", "--no-warnings", oracle)
	cmd.Stdin = bytes.NewReader(body)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		t.Fatalf("upstream %s: %v\n%s", op, err, stderr.String())
	}
	if err := json.Unmarshal(stdout.Bytes(), output); err != nil {
		t.Fatalf("upstream %s: %v\n%s", op, err, stdout.String())
	}
}

// Cases reads a case file of this package's testdata into v.
func Cases(t testing.TB, name string, v any) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	data, err := os.ReadFile(filepath.Join(filepath.Dir(file), "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

// Same reports whether two JSON values are equal, ignoring object key order
// and number formatting.
func Same(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return bytes.Equal(a, b)
	}
	ja, _ := json.Marshal(x)
	jb, _ := json.Marshal(y)
	return bytes.Equal(ja, jb)
}
