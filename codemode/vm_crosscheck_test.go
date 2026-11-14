package codemode

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// The Go VM against pi-codemode's own: each script runs in QuickJSSandbox
// and NodeSandbox with the same start data and answers, and their events and
// results must be equal. Runs with PI_DURABLE_TEST_PI_PACKAGE_DIR.

// vmScript is a script and how the host answers its calls, by call ID, in
// the order given; a call without an answer is never answered.
type vmScript struct {
	name    string
	code    string
	tools   []DeclaredTool
	globals []DeclaredGlobal
	store   map[string]json.RawMessage
	timeout float64
	answers []vmAnswer
}

type vmAnswer struct {
	id      int
	now     float64
	outcome Outcome
}

var vmCorpus = []vmScript{
	{name: "tools and globals", code: `const [a, b] = await Promise.all([tools.echo({ text: "one" }), tools.echo({ text: "two" })]);
console.log("got", a, { b }, [1, 2], null, undefined, 1.5, -0, NaN);
console.warn("warn"); console.error(new Error("err"));
text(b); text({ nested: { x: [1, "two"] } }); text(42);
const found = await lookup("key", 2, { deep: true });
return { a, b, found };`, tools: []DeclaredTool{{Name: "echo", Description: "echo"}}, globals: []DeclaredGlobal{{Name: "lookup", Spread: true}},
		answers: []vmAnswer{{2, 2000, Outcome{OK: true, Value: json.RawMessage(`"echo two"`)}}, {1, 3000, Outcome{OK: true, Value: json.RawMessage(`"echo one"`)}},
			{3, 4000, Outcome{OK: true, Value: json.RawMessage(`{"value":[1,{"k":null}]}`)}}}},
	{name: "clock and randomness", code: `const before = [Date.now(), Math.random(), new Date().toString(), new Date(0).toLocaleString(), performance.now()];
await tools.wait({});
return [...before, Date.now(), Math.random(), new Date().toISOString(), performance.now(), typeof WeakRef, typeof FinalizationRegistry];`,
		tools: []DeclaredTool{{Name: "wait"}}, answers: []vmAnswer{{1, 1700000005123.7, Outcome{OK: true}}}},
	{name: "errors with properties", code: `try { await tools.fail({}); } catch (error) { return [error.message, error.name, error.structuredOutput, error instanceof Error, Object.keys(error)]; }`,
		tools: []DeclaredTool{{Name: "fail"}}, answers: []vmAnswer{{1, 5, Outcome{OK: false, Message: "nope", Properties: map[string]json.RawMessage{"structuredOutput": json.RawMessage(`{"exitCode":1}`)}}}}},
	{name: "thrown error", code: `await tools.a({});
function deep(n) { if (n === 0) throw new TypeError("deep failure"); return deep(n - 1); }
deep(3);`, tools: []DeclaredTool{{Name: "a"}}, answers: []vmAnswer{{1, 5, Outcome{OK: true}}}},
	{name: "syntax error", code: `const x = ;`},
	{name: "reference error", code: `return missing + 1;`},
	{name: "thrown values", code: `throw { not: "an error" };`},
	{name: "exit", code: `text("before"); exit(); text("after");`},
	{name: "recursion", code: `function f(n) { return f(n + 1) + 1; } try { f(0); } catch (error) { return [error.name, error.message]; }`},
	{name: "unhandled rejection", code: `Promise.reject(new Error("ignored")); await tools.a({}); return "after";`,
		tools: []DeclaredTool{{Name: "a"}}, answers: []vmAnswer{{1, 5, Outcome{OK: true, Value: json.RawMessage(`1`)}}}},
	{name: "stalled", code: `await new Promise(() => {});`},
	{name: "store and load", code: `store("a", { n: load("kept").n + 1 }); store("kept", undefined);
try { store("big", "x".repeat(300000)); } catch (error) { text(error.message); }
try { store("", 1); } catch (error) { text(error.message); }
try { store("bad", () => 1); } catch (error) { text(error.message); }
return [load("a"), load("kept"), load("missing")];`, store: map[string]json.RawMessage{"kept": json.RawMessage(`{"n":41}`), "other": json.RawMessage(`[1,2]`)}},
	{name: "images and output limits", code: `image("data:image/png;base64,iVBORw0KGgo=");
try { image("not a data url"); } catch (error) { text(error.message); }
image({ type: "image", data: "R0lGODlh", mimeType: "image/gif" });
for (let i = 0; i < 3; i++) console.log("line", i);
return ALL_TOOLS.map((tool) => [tool.name, typeof tools[tool.name]]);`, tools: []DeclaredTool{{Name: "my-tool", JSName: "my_tool", Description: "d"}, {Name: "read"}}},
	{name: "timeout", code: `text("started"); while (true) {}`, timeout: 300},
	{name: "job loop timeout", code: `text("started"); while (true) await null;`, timeout: 300},
	{name: "unicode", code: `text("héllo 😀 \u2028 \ud800 end"); return ["😀".length, "\ud83d", JSON.stringify("\ud83d")];`},
	{name: "spread and namespaced globals", code: `return [await ns.one(1, "two"), await whole({ a: 1 })];`,
		globals: []DeclaredGlobal{{Name: "ns.one", Spread: true}, {Name: "whole"}},
		answers: []vmAnswer{{1, 5, Outcome{OK: true, Value: json.RawMessage(`"one"`)}}, {2, 6, Outcome{OK: true}}}},
}

// runVMScript runs one script and reports its events and result.
func runVMScript(t *testing.T, sandbox Sandbox, script vmScript) string {
	t.Helper()
	calls := make(chan HostCall, 64)
	var events []string
	options := RunOptions{StartedAt: 1700000000000, Seed: 99, Tools: script.tools, Globals: script.globals, Store: script.store,
		OnStarted: func(fingerprint string) { events = append(events, "started "+fingerprint) },
		OnCall: func(call HostCall) {
			events = append(events, fmt.Sprintf("call %d %s %s %s", call.ID, call.Target, call.Name, call.Args))
			calls <- call
		},
		OnIdle: func(seq, issued int) { events = append(events, fmt.Sprintf("idle %d %d", seq, issued)) },
	}
	if script.timeout > 0 {
		options.TimeoutMs = &script.timeout
	}
	run, err := sandbox.Start(script.code, options)
	if err != nil {
		return "start: " + err.Error()
	}
	seen := map[int]bool{}
	for seq, answer := range script.answers {
		for !seen[answer.id] {
			select {
			case call := <-calls:
				seen[call.ID] = true
			case <-time.After(30 * time.Second):
				t.Fatalf("%s: call %d never came", script.name, answer.id)
			}
		}
		if err := run.Deliver(seq+1, answer.now, answer.id, answer.outcome); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := run.Wait(ctx)
	if err != nil {
		t.Fatalf("%s: %v", script.name, err)
	}
	for i := range result.Calls {
		result.Calls[i].DurationMs = 0
	}
	data, _ := json.MarshalIndent(result, "", " ")
	return strings.Join(events, "\n") + "\n" + string(data)
}

func TestQuickJSSandboxRunsAsPiCodemodeDoes(t *testing.T) {
	pkg := os.Getenv("PI_DURABLE_TEST_PI_PACKAGE_DIR")
	if pkg == "" {
		t.Skip("set PI_DURABLE_TEST_PI_PACKAGE_DIR to compare with pi-codemode's sandbox")
	}
	quickjs, node := &QuickJSSandbox{}, &NodeSandbox{PackageDir: pkg}
	defer quickjs.Close()
	defer node.Close()
	for _, script := range vmCorpus {
		t.Run(script.name, func(t *testing.T) {
			got, want := runVMScript(t, quickjs, script), runVMScript(t, node, script)
			if got != want {
				t.Errorf("QuickJSSandbox:\n%s\n\nNodeSandbox:\n%s", got, want)
			}
		})
	}
}
