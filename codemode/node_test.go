package codemode

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// installedSandbox is a NodeSandbox over the installed Pi that
// PI_DURABLE_TEST_PI_PACKAGE_DIR names; the test is skipped without one.
func installedSandbox(t *testing.T) *NodeSandbox {
	t.Helper()
	pkg := os.Getenv("PI_DURABLE_TEST_PI_PACKAGE_DIR")
	if pkg == "" {
		t.Skip("set PI_DURABLE_TEST_PI_PACKAGE_DIR to run scripts with an installed Pi 1.5.2")
	}
	sandbox := &NodeSandbox{PackageDir: pkg}
	t.Cleanup(func() { _ = sandbox.Close() })
	return sandbox
}

// closingSandbox is a Sandbox that Close aborts.
type closingSandbox interface {
	Sandbox
	Close() error
}

// sandboxKind makes sandboxes of one kind: QuickJS, or Node over the
// installed Pi.
type sandboxKind struct {
	name string
	make func() closingSandbox
}

// sandboxKinds are QuickJS, and Node when PI_DURABLE_TEST_PI_PACKAGE_DIR
// names an installed Pi.
func sandboxKinds() []sandboxKind {
	kinds := []sandboxKind{{"quickjs", func() closingSandbox { return &QuickJSSandbox{} }}}
	if pkg := os.Getenv("PI_DURABLE_TEST_PI_PACKAGE_DIR"); pkg != "" {
		kinds = append(kinds, sandboxKind{"node", func() closingSandbox { return &NodeSandbox{PackageDir: pkg} }})
	}
	return kinds
}

// eachSandbox runs test with a sandbox of each kind.
func eachSandbox(t *testing.T, test func(t *testing.T, kind sandboxKind, sandbox closingSandbox)) {
	for _, kind := range sandboxKinds() {
		t.Run(kind.name, func(t *testing.T) {
			sandbox := kind.make()
			t.Cleanup(func() { _ = sandbox.Close() })
			test(t, kind, sandbox)
		})
	}
}

// testSandbox is the sandbox the durable tests run scripts in: QuickJS, or
// Node over the installed Pi with PI_DURABLE_TEST_CODEMODE_SANDBOX=node.
func testSandbox(t *testing.T) closingSandbox {
	t.Helper()
	if os.Getenv("PI_DURABLE_TEST_CODEMODE_SANDBOX") == "node" {
		return installedSandbox(t)
	}
	sandbox := &QuickJSSandbox{}
	t.Cleanup(func() { _ = sandbox.Close() })
	return sandbox
}

// recorder collects a run's events.
type recorder struct {
	fingerprints chan string
	calls        chan HostCall
	idles        chan [2]int
}

func newRecorder() *recorder {
	return &recorder{fingerprints: make(chan string, 4), calls: make(chan HostCall, 64), idles: make(chan [2]int, 64)}
}

func (r *recorder) options(options RunOptions) RunOptions {
	options.OnStarted = func(fingerprint string) { r.fingerprints <- fingerprint }
	options.OnCall = func(call HostCall) { r.calls <- call }
	options.OnIdle = func(seq, issued int) { r.idles <- [2]int{seq, issued} }
	return options
}

func next[T any](t *testing.T, events chan T) T {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(30 * time.Second):
		t.Fatal("no event within 30s")
		panic("unreachable")
	}
}

func wait(t *testing.T, run Run) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSandboxRunsAScriptThatCallsTools(t *testing.T) {
	eachSandbox(t, testRunsAScriptThatCallsTools)
}

func testRunsAScriptThatCallsTools(t *testing.T, _ sandboxKind, sandbox closingSandbox) {
	events := newRecorder()
	run, err := sandbox.Start(`const [a, b] = await Promise.all([tools.echo({ text: "one" }), tools.echo({ text: "two" })]);
console.log("got", a);
text(b);
const found = await lookup("key", 2);
store("kept", { found });
store("gone", undefined);
return { a, found };`, events.options(RunOptions{
		StartedAt: 1000, Seed: 7,
		Tools:   []DeclaredTool{{Name: "echo", Description: "echo"}},
		Globals: []DeclaredGlobal{{Name: "lookup", Spread: true}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint := next(t, events.fingerprints); !strings.HasPrefix(fingerprint, "quickjs-wasi ") || !strings.Contains(fingerprint, "deterministic 1") {
		t.Fatalf("fingerprint = %q", fingerprint)
	}
	first, second := next(t, events.calls), next(t, events.calls)
	if first.ID != 1 || first.Target != "tool" || first.Name != "echo" || string(first.Args) != `{"text":"one"}` || second.ID != 2 {
		t.Fatalf("calls = %+v %+v", first, second)
	}
	if idle := next(t, events.idles); idle != [2]int{0, 2} {
		t.Fatalf("first idle = %v", idle)
	}
	// Results in another order than the calls were made.
	if err := run.Deliver(1, 2000, 2, Outcome{OK: true, Value: json.RawMessage(`"echo two"`)}); err != nil {
		t.Fatal(err)
	}
	if idle := next(t, events.idles); idle != [2]int{1, 2} {
		t.Fatalf("idle after the first result = %v", idle)
	}
	if err := run.Deliver(2, 3000, 1, Outcome{OK: true, Value: json.RawMessage(`"echo one"`)}); err != nil {
		t.Fatal(err)
	}
	global := next(t, events.calls)
	if global.ID != 3 || global.Target != "global" || global.Name != "lookup" || string(global.Args) != `["key",2]` {
		t.Fatalf("global call = %+v", global)
	}
	if idle := next(t, events.idles); idle != [2]int{2, 3} {
		t.Fatalf("idle after the second result = %v", idle)
	}
	if err := run.Deliver(3, 4000, 3, Outcome{OK: true, Value: json.RawMessage(`{"value":1}`)}); err != nil {
		t.Fatal(err)
	}
	result := wait(t, run)
	if !result.OK || string(result.Value) != `{"a":"echo one","found":{"value":1}}` {
		t.Fatalf("result = %+v", result)
	}
	if len(result.Output) != 2 || result.Output[0].Text != "got echo one" || !result.Output[0].Console || result.Output[1].Text != "echo two" {
		t.Fatalf("output = %+v", result.Output)
	}
	if string(result.StoreWrites.Set["kept"]) != `{"found":{"value":1}}` || len(result.StoreWrites.Delete) != 1 || result.StoreWrites.Delete[0] != "gone" {
		t.Fatalf("store writes = %+v", result.StoreWrites)
	}
	if result.During == nil || *result.During != (During{Seq: 3, Issued: 3}) {
		t.Fatalf("during = %+v", result.During)
	}
	if !run.Finished() {
		t.Fatal("a run with its result is not finished")
	}
}

// The VM's clock reads the start and then each result's time, and its
// randomness comes from the seed: the same run gives the same values.
func TestSandboxIsDeterministic(t *testing.T) {
	eachSandbox(t, testIsDeterministic)
}

func testIsDeterministic(t *testing.T, _ sandboxKind, sandbox closingSandbox) {
	once := func(seed uint32) string {
		events := newRecorder()
		run, err := sandbox.Start(`const before = [Date.now(), Math.random(), new Date().toISOString()];
await tools.wait({});
return [...before, Date.now(), Math.random(), Math.random()];`, events.options(RunOptions{
			StartedAt: 1700000000000, Seed: seed, Tools: []DeclaredTool{{Name: "wait"}},
		}))
		if err != nil {
			t.Fatal(err)
		}
		call := next(t, events.calls)
		if err := run.Deliver(1, 1700000005000, call.ID, Outcome{OK: true}); err != nil {
			t.Fatal(err)
		}
		result := wait(t, run)
		if !result.OK {
			t.Fatalf("result = %+v", result.Error)
		}
		return string(result.Value)
	}
	first, again, other := once(42), once(42), once(43)
	if first != again {
		t.Fatalf("the same run gave %s, then %s", first, again)
	}
	if first == other {
		t.Fatalf("another seed gave the same values %s", first)
	}
	if !strings.HasPrefix(first, `[1700000000000,`) || !strings.Contains(first, `"2023-11-14T22:13:20.000Z",1700000005000,`) {
		t.Fatalf("clock readings = %s", first)
	}
}

func TestSandboxEndsRuns(t *testing.T) {
	eachSandbox(t, testEndsRuns)
}

func testEndsRuns(t *testing.T, kind sandboxKind, sandbox closingSandbox) {
	t.Run("timeout", func(t *testing.T) {
		timeout := 200.0
		run, err := sandbox.Start(`await tools.never({});`, newRecorder().options(RunOptions{
			StartedAt: 1, Tools: []DeclaredTool{{Name: "never"}}, TimeoutMs: &timeout}))
		if err != nil {
			t.Fatal(err)
		}
		result := wait(t, run)
		if result.OK || result.Error.Kind != "timeout" || result.Error.Message != "Execution timed out after 200 ms" {
			t.Fatalf("result = %+v", result.Error)
		}
	})
	t.Run("abort", func(t *testing.T) {
		events := newRecorder()
		run, err := sandbox.Start(`try { await tools.never({}); } catch { text("caught"); }`, events.options(RunOptions{
			StartedAt: 1, Tools: []DeclaredTool{{Name: "never"}}}))
		if err != nil {
			t.Fatal(err)
		}
		next(t, events.calls)
		run.Abort("stopped by the host")
		result := wait(t, run)
		if result.OK || result.Error.Kind != "aborted" || result.Error.Message != "stopped by the host" || len(result.Output) != 0 {
			t.Fatalf("result = %+v", result)
		}
		// A finished run takes no more results.
		if err := run.Deliver(1, 2, 1, Outcome{OK: true}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("script errors", func(t *testing.T) {
		run, err := sandbox.Start(`const x = ;`, RunOptions{StartedAt: 1})
		if err != nil {
			t.Fatal(err)
		}
		result := wait(t, run)
		if result.OK || result.Error.Kind != "script" || result.Error.Name != "SyntaxError" || result.During != nil {
			t.Fatalf("syntax error = %+v during %+v", result.Error, result.During)
		}
		run, err = sandbox.Start(`throw new RangeError("too far");`, RunOptions{StartedAt: 1})
		if err != nil {
			t.Fatal(err)
		}
		result = wait(t, run)
		if result.OK || result.Error.Kind != "script" || result.Error.Name != "RangeError" || result.Error.Message != "too far" ||
			!strings.HasPrefix(result.Error.Stack, "RangeError: too far\n") || result.During == nil || *result.During != (During{}) {
			t.Fatalf("thrown error = %+v during %+v", result.Error, result.During)
		}
	})
	t.Run("rejected options", func(t *testing.T) {
		_, err := sandbox.Start(`return 1;`, RunOptions{StartedAt: 1, Tools: []DeclaredTool{{Name: "a"}, {Name: "a"}}})
		if err == nil || err.Error() != "Tool names must be unique" {
			t.Fatalf("err = %v", err)
		}
		_, err = sandbox.Start(`return 1;`, RunOptions{StartedAt: 1, Globals: []DeclaredGlobal{{Name: "tools"}}})
		if err == nil || err.Error() != `Invalid global name "tools"` {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("deliveries", func(t *testing.T) {
		events := newRecorder()
		run, err := sandbox.Start(`await tools.a({}); await tools.a({});`, events.options(RunOptions{
			StartedAt: 1, Tools: []DeclaredTool{{Name: "a"}}}))
		if err != nil {
			t.Fatal(err)
		}
		call := next(t, events.calls)
		if err := run.Deliver(1, 2, 9, Outcome{OK: true}); err == nil || err.Error() != "Call 9 is not waiting for a result" {
			t.Fatalf("unknown call: %v", err)
		}
		if err := run.Deliver(0, 2, call.ID, Outcome{OK: true}); err == nil || err.Error() != "Result number 0 is not a positive integer used once" {
			t.Fatalf("result 0: %v", err)
		}
		if err := run.Deliver(1, 2, call.ID, Outcome{OK: true}); err != nil {
			t.Fatal(err)
		}
		second := next(t, events.calls)
		if err := run.Deliver(1, 3, second.ID, Outcome{OK: true}); err == nil || err.Error() != "Result number 1 is not a positive integer used once" {
			t.Fatalf("result 1 again: %v", err)
		}
		if err := run.Deliver(2, 3, second.ID, Outcome{OK: false, Message: "nope", Properties: map[string]json.RawMessage{"code": json.RawMessage(`7`)}}); err != nil {
			t.Fatal(err)
		}
		result := wait(t, run)
		if result.OK || result.Error.Kind != "script" || result.Error.Message != "nope" {
			t.Fatalf("result = %+v", result.Error)
		}
	})
	t.Run("close", func(t *testing.T) {
		closing := kind.make()
		events := newRecorder()
		run, err := closing.Start(`await tools.never({});`, events.options(RunOptions{StartedAt: 1, Tools: []DeclaredTool{{Name: "never"}}}))
		if err != nil {
			t.Fatal(err)
		}
		next(t, events.calls)
		if err := closing.Close(); err != nil {
			t.Fatal(err)
		}
		result := wait(t, run)
		if result.OK || result.Error.Kind != "aborted" || result.Error.Message != "Sandbox closed" {
			t.Fatalf("result = %+v", result.Error)
		}
		if _, err := closing.Start(`return 1;`, RunOptions{StartedAt: 1}); err == nil || err.Error() != "Sandbox is closed" {
			t.Fatalf("start after close: %v", err)
		}
	})
	t.Run("unavailable", func(t *testing.T) {
		if kind.name != "node" {
			t.Skip("only the Node sandbox loads pi-codemode")
		}
		_, err := (&NodeSandbox{CodemodeDir: t.TempDir()}).Start(`return 1;`, RunOptions{StartedAt: 1})
		if err == nil || !strings.Contains(err.Error(), "pi-codemode unavailable") {
			t.Fatalf("err = %v", err)
		}
	})
}
