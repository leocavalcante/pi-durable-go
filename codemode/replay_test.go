package codemode

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/storage"
)

// Ports of upstream's durable code mode tests (test/codemode.test.ts), with
// pi-codemode's sandbox from the installed Pi.

// countingSandbox counts the scripts it starts.
type countingSandbox struct {
	inner   Sandbox
	mu      sync.Mutex
	starts  int
	onStart func(Run)
}

func (s *countingSandbox) Start(code string, options RunOptions) (Run, error) {
	s.mu.Lock()
	s.starts++
	onStart := s.onStart
	s.mu.Unlock()
	run, err := s.inner.Start(code, options)
	if err == nil && onStart != nil {
		onStart(run)
	}
	return run, err
}

func (s *countingSandbox) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts
}

// counts counts runs by key.
type counts struct {
	mu   sync.Mutex
	runs map[string]int
}

func (c *counts) add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runs == nil {
		c.runs = map[string]int{}
	}
	c.runs[key]++
}

func (c *counts) snapshot() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.runs {
		out[k] = v
	}
	return out
}

// holdGate blocks the hold tool while block is set, until its call is
// aborted, and signals reached the first time it blocks.
type holdGate struct {
	mu      sync.Mutex
	block   bool
	reached chan struct{}
	once    *sync.Once
	runs    int
}

func newHoldGate() *holdGate {
	return &holdGate{block: true, reached: make(chan struct{}), once: &sync.Once{}}
}

func (g *holdGate) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reached, g.once = make(chan struct{}), &sync.Once{}
}

func (g *holdGate) setBlock(block bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.block = block
}

func (g *holdGate) wait(t *testing.T) {
	t.Helper()
	g.mu.Lock()
	reached := g.reached
	g.mu.Unlock()
	select {
	case <-reached:
	case <-time.After(60 * time.Second):
		t.Fatal("hold was never reached")
	}
}

func (g *holdGate) execute(ctx context.Context, name string, args json.RawMessage) (durable.ToolResult, error) {
	g.mu.Lock()
	g.runs++
	block, reached, once := g.block, g.reached, g.once
	g.mu.Unlock()
	if block {
		once.Do(func() { close(reached) })
		<-ctx.Done()
		return durable.ToolResult{}, ctx.Err()
	}
	var input struct {
		ID *float64 `json:"id"`
	}
	_ = json.Unmarshal(args, &input)
	id := ""
	if input.ID != nil {
		id = fmt.Sprint(*input.ID)
	}
	return durable.ToolResult{Content: name + " " + id}, nil
}

// slowGates finish the slow tool's call id once released.
type slowGates struct {
	mu       sync.Mutex
	gates    map[float64]chan struct{}
	finished map[float64]chan struct{}
}

func (s *slowGates) gate(m map[float64]chan struct{}, id float64) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m[id] == nil {
		m[id] = make(chan struct{})
	}
	return m[id]
}

func (s *slowGates) release(id float64) { close(s.gate(s.gates, id)) }

type setup struct {
	t       *testing.T
	ctx     context.Context
	sandbox *countingSandbox
	model   *scriptModel
	echo    *counts
	holds   *holdGate
	slows   *slowGates
	tools   []durable.ToolDefinition
	globals []Global
	hooks   durable.Hooks
	path    string
}

func newSetup(t *testing.T, globals ...Global) *setup {
	t.Helper()
	return newSetupWith(t, installedSandbox(t), globals...)
}

func newSetupWith(t *testing.T, sandbox Sandbox, globals ...Global) *setup {
	t.Helper()
	s := &setup{t: t, ctx: testContext(t), sandbox: &countingSandbox{inner: sandbox}, model: &scriptModel{},
		echo: &counts{}, holds: newHoldGate(), slows: &slowGates{gates: map[float64]chan struct{}{}, finished: map[float64]chan struct{}{}},
		globals: globals, path: filepath.Join(t.TempDir(), "session.sqlite")}
	s.tools = []durable.ToolDefinition{
		{ToolSpec: durable.ToolSpec{Name: "echo", Description: "echo", Parameters: parametersSchema},
			Execute: func(ctx context.Context, args json.RawMessage, r *durable.ToolRuntime) (durable.ToolResult, error) {
				var input struct{ Text string }
				_ = json.Unmarshal(args, &input)
				s.echo.add(input.Text)
				return durable.ToolResult{Content: "echo " + input.Text}, nil
			}},
		{ToolSpec: durable.ToolSpec{Name: "hold", Description: "hold", Parameters: parametersSchema}, ReplaySafe: true,
			Execute: func(ctx context.Context, args json.RawMessage, r *durable.ToolRuntime) (durable.ToolResult, error) {
				return s.holds.execute(ctx, "held", args)
			}},
		{ToolSpec: durable.ToolSpec{Name: "slow", Description: "slow", Parameters: parametersSchema},
			Execute: func(ctx context.Context, args json.RawMessage, r *durable.ToolRuntime) (durable.ToolResult, error) {
				var input struct{ ID float64 }
				_ = json.Unmarshal(args, &input)
				select {
				case <-s.slows.gate(s.slows.gates, input.ID):
				case <-ctx.Done():
					return durable.ToolResult{}, ctx.Err()
				}
				close(s.slows.gate(s.slows.finished, input.ID))
				return durable.ToolResult{Content: fmt.Sprintf("slow %v", input.ID)}, nil
			}},
	}
	return s
}

func (s *setup) open(store durable.Storage) testChat {
	s.t.Helper()
	return openChat(s.t, s.ctx, store, s.model,
		durable.Extension{Name: "test", Tools: s.tools, Hooks: s.hooks},
		Extension(Options{Sandbox: s.sandbox, Globals: s.globals}))
}

func (s *setup) openSQLite() testChat {
	s.t.Helper()
	store, err := storage.OpenSQLite(s.ctx, s.path)
	if err != nil {
		s.t.Fatal(err)
	}
	return s.open(store)
}

// scriptTask is the code mode call's task.
func scriptTask(t *testing.T, ctx context.Context, h *durable.Harness) durable.TaskID {
	t.Helper()
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range state.Tasks {
		var input struct {
			Kind string           `json:"kind"`
			Call durable.ToolCall `json:"call"`
		}
		_ = json.Unmarshal(task.Input, &input)
		if task.Kind == "pi.tool" && input.Kind == "" && input.Call.Name == ToolName {
			return task.ID
		}
	}
	t.Fatal("no code mode task")
	return 0
}

// nestedCalls are the nested calls' tasks.
func nestedCalls(t *testing.T, ctx context.Context, h *durable.Harness) []durable.TaskRecord {
	t.Helper()
	state, err := h.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var nested []durable.TaskRecord
	for _, task := range state.Tasks {
		var input struct{ Kind string }
		_ = json.Unmarshal(task.Input, &input)
		if task.Kind == "pi.tool" && input.Kind == "nested" {
			nested = append(nested, *task)
		}
	}
	return nested
}

func editDocument[T any](t *testing.T, ctx context.Context, h *durable.Harness, def durable.DocumentDefinition, task durable.TaskID, edit func(*T)) {
	t.Helper()
	if err := h.Commit(ctx, func(tx *durable.Tx) error {
		value, err := readJSON[T](tx, def, uint64(task))
		if err != nil {
			return err
		}
		edit(&value)
		return writeJSON(tx, def, uint64(task), value)
	}); err != nil {
		t.Fatal(err)
	}
}

func readJournal(t *testing.T, ctx context.Context, h *durable.Harness, task durable.TaskID) journalState {
	t.Helper()
	journal, err := durable.ReadDocument[journalState](ctx, h, JournalDocument, uint64(task))
	if err != nil {
		t.Fatal(err)
	}
	return journal
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// restart runs code until hold blocks, closes the Harness (a restart), lets
// between change the stored state, then reopens with hold no longer blocking
// and waits for the answer.
func (s *setup) restart(code string, between, beforeClose func(*durable.Harness, durable.TaskID)) (codemodeResult, *durable.Harness) {
	t := s.t
	t.Helper()
	s.model.scripts = []string{code}
	chat := s.openSQLite()
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	s.holds.wait(t)
	if beforeClose != nil {
		beforeClose(chat.h, scriptTask(t, s.ctx, chat.h))
	}
	if err := chat.h.Close(s.ctx); err != nil {
		t.Fatal(err)
	}
	s.holds.setBlock(false)
	chat = s.openSQLite()
	t.Cleanup(func() { _ = chat.h.Close(context.Background()) })
	if between != nil {
		between(chat.h, scriptTask(t, s.ctx, chat.h))
	}
	chat.h.Resume()
	return s.answer(chat.h, sub.ID()), chat.h
}

// answer waits for the submission and returns its code mode result.
func (s *setup) answer(h *durable.Harness, id durable.SubmissionID) codemodeResult {
	t := s.t
	t.Helper()
	sub, err := h.Submission(s.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := sub.Wait(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Status != "done" {
		t.Fatalf("submission %+v", settled)
	}
	results := codemodeResults(t, s.ctx, h)
	if len(results) == 0 {
		t.Fatal("no code mode result")
	}
	return results[len(results)-1]
}

// runOnce runs code in a fresh chat and returns its result.
func (s *setup) runOnce(code string) codemodeResult {
	s.t.Helper()
	s.holds.setBlock(false)
	s.model.mu.Lock()
	s.model.scripts, s.model.next = []string{code}, 0
	s.model.mu.Unlock()
	s.path = filepath.Join(s.t.TempDir(), "session.sqlite")
	chat := s.openSQLite()
	s.t.Cleanup(func() { _ = chat.h.Close(context.Background()) })
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		s.t.Fatal(err)
	}
	return s.answer(chat.h, sub.ID())
}

func expectResult(t *testing.T, result codemodeResult, text string, isError bool) {
	t.Helper()
	if result.Content != text || result.IsError != isError {
		t.Fatalf("result (error %v):\n%s\nwant (error %v):\n%s", result.IsError, result.Content, isError, text)
	}
}

func expectFailure(t *testing.T, result codemodeResult, parts ...string) {
	t.Helper()
	if !result.IsError {
		t.Fatalf("result is no error: %s", result.Content)
	}
	for _, part := range parts {
		if !strings.Contains(result.Content, part) {
			t.Fatalf("result %q lacks %q", result.Content, part)
		}
	}
}

func expectCounts(t *testing.T, got map[string]int, want map[string]int) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("runs %v, want %v", got, want)
	}
}

func TestCodemodeJournalsEachResult(t *testing.T) {
	s := newSetup(t)
	result := s.runOnce(`const a = await tools.echo({ text: "a" }); const b = await tools.echo({ text: "b" }); return [a, b];`)
	expectResult(t, result, `["echo a","echo b"]`, false)
}

func TestCodemodeResumesAfterARestart(t *testing.T) {
	s := newSetup(t)
	result, _ := s.restart(`const a = await tools.echo({ text: "a" });
		const held = await tools.hold({ id: 1 });
		const b = await tools.echo({ text: "b" });
		return [a, held, b, Date.now() > 0];`, nil, nil)
	expectResult(t, result, `["echo a","held 1","echo b",true]`, false)
	expectCounts(t, s.echo.snapshot(), map[string]int{"a": 1, "b": 1})
	// The VM ran twice; hold, replay-safe, reran once after the restart.
	if s.sandbox.count() != 2 || s.holds.runs != 2 {
		t.Fatalf("starts %d, hold runs %d", s.sandbox.count(), s.holds.runs)
	}
}

func TestCodemodeReplaysResultsInTheOrderTheyArrived(t *testing.T) {
	s := newSetup(t)
	// 2 finishes first, then 1.
	s.slows.release(2)
	go func() {
		<-s.slows.gate(s.slows.finished, 2)
		s.slows.release(1)
	}()
	result, _ := s.restart(`const order = [];
		await Promise.all([tools.slow({ id: 1 }).then(() => order.push(1)), tools.slow({ id: 2 }).then(() => order.push(2))]);
		await tools.echo({ text: order.join(",") });
		await tools.hold({});
		return order;`, nil, nil)
	expectResult(t, result, "[2,1]", false)
	expectCounts(t, s.echo.snapshot(), map[string]int{"2,1": 1})
}

func TestCodemodeJournalsGlobals(t *testing.T) {
	var mu sync.Mutex
	counter := 0
	s := newSetup(t, Global{Name: "next", Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (json.RawMessage, error) {
		mu.Lock()
		defer mu.Unlock()
		counter++
		return json.RawMessage(fmt.Sprint(counter)), nil
	}})
	result, _ := s.restart(`const n = await next(); await tools.hold({}); return [n, await next()];`, nil, nil)
	// The first next() ran once and was replayed from the journal; the second
	// ran after the restart.
	expectResult(t, result, "[1,2]", false)
	if counter != 2 {
		t.Fatalf("next ran %d times", counter)
	}
}

func TestCodemodeKeepsTheClockAndRandomnessAcrossARestart(t *testing.T) {
	s := newSetup(t)
	result, _ := s.restart(`const before = [Date.now(), Math.random()];
		await tools.hold({});
		await tools.echo({ text: String(before[0]) + "/" + String(before[1]) });
		return "ok";`, nil, nil)
	expectResult(t, result, "ok", false)
	// One echo: the same arguments, so the rerun admitted no second call.
	runs := s.echo.snapshot()
	if len(runs) != 1 {
		t.Fatalf("echo runs %v", runs)
	}
	for _, n := range runs {
		if n != 1 {
			t.Fatalf("echo runs %v", runs)
		}
	}
}

func TestCodemodeEndsDivergingReplays(t *testing.T) {
	t.Run("another count", func(t *testing.T) {
		s := newSetup(t)
		result, _ := s.restart(`await tools.echo({ text: "a" }); await tools.hold({});`, func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, JournalDocument, task, func(journal *journalState) {
				seven := 7
				journal.Entries[0].IssuedAfter = &seven
			})
		}, nil)
		expectFailure(t, result, "after result 1 it had made 2 calls, where it had made 7")
	})
	t.Run("a call it made before", func(t *testing.T) {
		s := newSetup(t)
		var h *durable.Harness
		result, h := s.restart(`await tools.echo({ text: "a" }); await tools.hold({});`, func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, JournalDocument, task, func(journal *journalState) { journal.Entries[0].ID = 99 })
		}, nil)
		expectFailure(t, result, "it waits without making call 99")
		// hold, admitted before the restart, resumed on its own; no call was
		// admitted by the replay.
		if n := len(nestedCalls(t, s.ctx, h)); n != 2 {
			t.Fatalf("%d nested calls", n)
		}
	})
	t.Run("a global with other arguments", func(t *testing.T) {
		s := newSetup(t, Global{Name: "next", Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (json.RawMessage, error) {
			return json.RawMessage("1"), nil
		}})
		result, _ := s.restart(`await next(); await tools.hold({});`, func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, JournalDocument, task, func(journal *journalState) {
				journal.Entries[0].Global.Args = json.RawMessage(`"other"`)
			})
		}, nil)
		expectFailure(t, result, "call 1 is next")
	})
	t.Run("calls the live run never made", func(t *testing.T) {
		s := newSetup(t)
		result, h := s.restart(`await tools.echo({ text: "a" });
			const extra = load("k") ?? 0;
			for (let i = 0; i < extra; i++) tools.echo({ text: "extra" + i });
			await tools.hold({});`, func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
				run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage("3")}
			})
		}, func(h *durable.Harness, task durable.TaskID) {
			eventually(t, func() bool {
				journal := readJournal(t, s.ctx, h, task)
				return len(journal.Entries) > 0 && journal.Entries[0].IssuedAfter != nil && *journal.Entries[0].IssuedAfter == 2
			})
		})
		expectFailure(t, result, "after result 1 it had made 5 calls, where it had made 2")
		expectCounts(t, s.echo.snapshot(), map[string]int{"a": 1})
		if n := len(nestedCalls(t, s.ctx, h)); n != 2 {
			t.Fatalf("%d nested calls", n)
		}
	})
	t.Run("an end where it went on", func(t *testing.T) {
		s := newSetup(t)
		result, _ := s.restart(`await tools.echo({ text: "a" }); if (load("k")) return "early"; await tools.hold({});`,
			func(h *durable.Harness, task durable.TaskID) {
				editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
					run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage("true")}
				})
			}, func(h *durable.Harness, task durable.TaskID) {
				eventually(t, func() bool {
					journal := readJournal(t, s.ctx, h, task)
					return len(journal.Entries) > 0 && journal.Entries[0].IssuedAfter != nil && *journal.Entries[0].IssuedAfter == 2
				})
			})
		expectFailure(t, result, "it ended after result 1, where it went on before")
	})
	t.Run("an end before calls without a count", func(t *testing.T) {
		s := newSetup(t)
		result, _ := s.restart(`await tools.echo({ text: "a" }); if (load("k")) return "early"; await tools.echo({ text: "b" }); await tools.hold({});`,
			func(h *durable.Harness, task durable.TaskID) {
				editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
					run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage("true")}
				})
				// As if the live run crashed before recording result 1's count.
				editDocument(t, s.ctx, h, JournalDocument, task, func(journal *journalState) { journal.Entries[0].IssuedAfter = nil })
			}, nil)
		expectFailure(t, result, "it ended after result 1, where it went on before")
	})
	t.Run("a key admitted with other arguments", func(t *testing.T) {
		s := newSetup(t)
		result, _ := s.restart(`await tools.echo({ text: load("k") ?? "a" }); await tools.hold({});`,
			func(h *durable.Harness, task durable.TaskID) {
				editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
					run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage(`"other"`)}
				})
				// As if the restart came before the first journal commit.
				editDocument(t, s.ctx, h, JournalDocument, task, func(journal *journalState) { journal.Entries = []journalEntry{} })
			}, nil)
		expectFailure(t, result, "different path", "call 1 is tools.echo")
		expectCounts(t, s.echo.snapshot(), map[string]int{"a": 1})
	})
	t.Run("more calls in the first run", func(t *testing.T) {
		s := newSetup(t)
		result, h := s.restart(`const k = load("k");
			tools.echo({ text: k ?? "a" });
			if (k) tools.echo({ text: "new" });
			await tools.hold({});`, func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
				run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage(`"x"`)}
			})
			editDocument(t, s.ctx, h, JournalDocument, task, func(journal *journalState) { journal.Entries = []journalEntry{} })
		}, nil)
		// The first run already made more calls than before.
		expectFailure(t, result, "its first run made 3 calls, where it had made 2")
		if n := len(nestedCalls(t, s.ctx, h)); n != 2 {
			t.Fatalf("%d nested calls", n)
		}
		expectCounts(t, s.echo.snapshot(), map[string]int{"a": 1})
	})
	t.Run("a global where a tool was", func(t *testing.T) {
		s := newSetup(t, Global{Name: "pick", Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (json.RawMessage, error) {
			return json.RawMessage("1"), nil
		}})
		result, _ := s.restart(`if (load("k")) { await pick(); await tools.echo({ text: "new" }); } else { await tools.hold({}); }`,
			func(h *durable.Harness, task durable.TaskID) {
				editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
					run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage("true")}
				})
			}, nil)
		expectFailure(t, result, "call 1 is pick(), but was tools.hold({})")
		expectCounts(t, s.echo.snapshot(), map[string]int{})
	})
	t.Run("fewer calls in the first run", func(t *testing.T) {
		s := newSetup(t, Global{Name: "pick", Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (json.RawMessage, error) {
			return json.RawMessage("1"), nil
		}})
		result, _ := s.restart(`const k = load("k");
			const first = tools.hold({ id: 1 });
			if (!k) tools.hold({ id: 2 });
			await first;
			await pick();
			await tools.echo({ text: "new" });`, func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
				run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage("true")}
			})
		}, func(h *durable.Harness, task durable.TaskID) {
			eventually(t, func() bool {
				journal := readJournal(t, s.ctx, h, task)
				return journal.FirstIssued != nil && *journal.FirstIssued == 2
			})
		})
		expectFailure(t, result, "its first run made 1 calls, where it had made 2")
		expectCounts(t, s.echo.snapshot(), map[string]int{})
	})
	t.Run("an end in the first run", func(t *testing.T) {
		s := newSetup(t)
		result, _ := s.restart(`if (load("k")) { store("x", 1); return "early"; } await tools.hold({});`,
			func(h *durable.Harness, task durable.TaskID) {
				editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
					run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage("true")}
				})
			}, func(h *durable.Harness, task durable.TaskID) {
				eventually(t, func() bool {
					journal := readJournal(t, s.ctx, h, task)
					return journal.FirstIssued != nil && *journal.FirstIssued == 1
				})
			})
		expectFailure(t, result, "it ended after result 0, where it went on before")
	})
	t.Run("every held call checked first", func(t *testing.T) {
		s := newSetup(t, Global{Name: "forever", Execute: func(ctx context.Context, _ json.RawMessage, _ *durable.ToolRuntime) (json.RawMessage, error) {
			<-ctx.Done()
			return json.RawMessage("null"), nil
		}})
		result, h := s.restart(`const k = load("k");
			if (k) tools.echo({ text: "new" }); else forever();
			await tools.hold({ id: k ? 2 : 1 });`, func(h *durable.Harness, task durable.TaskID) {
			editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
				run.Start.Store = map[string]json.RawMessage{"k": json.RawMessage("true")}
			})
		}, nil)
		expectFailure(t, result, `call 2 is tools.hold({"id":2}), but was tools.hold({"id":1})`)
		expectCounts(t, s.echo.snapshot(), map[string]int{})
		if n := len(nestedCalls(t, s.ctx, h)); n != 1 {
			t.Fatalf("%d nested calls", n)
		}
	})
}

func TestCodemodeRefusesToResumeAnotherSandboxOrVersion(t *testing.T) {
	for _, field := range []string{"fingerprint", "version"} {
		t.Run(field, func(t *testing.T) {
			s := newSetup(t)
			result, _ := s.restart(`await tools.hold({}); await tools.echo({ text: "late" });`, func(h *durable.Harness, task durable.TaskID) {
				editDocument(t, s.ctx, h, RunDocument, task, func(run *runState) {
					if field == "fingerprint" {
						run.Start.Fingerprint = "another sandbox"
					} else {
						run.Start.Version = 0
					}
				})
			}, nil)
			expectFailure(t, result, "cannot resume")
			expectCounts(t, s.echo.snapshot(), map[string]int{})
		})
	}
}

func TestCodemodeTimesOutAcrossARestart(t *testing.T) {
	s := newSetup(t)
	// Upstream's 300 ms leave a Node process per run too little time to
	// reach hold.
	result, _ := s.restart(`// @options: {"timeout_ms": 3000}
		await tools.hold({}); await tools.echo({ text: "late" });`, func(*durable.Harness, durable.TaskID) { time.Sleep(3100 * time.Millisecond) }, nil)
	expectFailure(t, result, "deadline passed")
	expectCounts(t, s.echo.snapshot(), map[string]int{})
	if s.sandbox.count() != 1 {
		t.Fatalf("starts %d", s.sandbox.count())
	}
}

func TestCodemodeKeepsUndefinedGlobalResults(t *testing.T) {
	s := newSetup(t, Global{Name: "nothing", Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (json.RawMessage, error) {
		return nil, nil
	}})
	result, _ := s.restart(`const before = await nothing(); await tools.hold({}); return [before === undefined, (await nothing()) === undefined];`, nil, nil)
	expectResult(t, result, "[true,true]", false)
}

func TestCodemodeRecordsACountTheLiveRunNeverRecorded(t *testing.T) {
	s := newSetup(t)
	s.model.scripts = []string{`await tools.echo({ text: "a" }); await tools.hold({});`}
	chat := s.openSQLite()
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	s.holds.wait(t)
	if err := chat.h.Close(s.ctx); err != nil {
		t.Fatal(err)
	}
	// As if result 1's count was never recorded; hold blocks again.
	s.holds.reset()
	chat = s.openSQLite()
	task := scriptTask(t, s.ctx, chat.h)
	editDocument(t, s.ctx, chat.h, JournalDocument, task, func(journal *journalState) { journal.Entries[0].IssuedAfter = nil })
	chat.h.Resume()
	// The replay acknowledges result 1 and goes live; the script waits on
	// hold, and the count is recorded.
	s.holds.wait(t)
	eventually(t, func() bool {
		journal := readJournal(t, s.ctx, chat.h, task)
		return journal.Entries[0].IssuedAfter != nil && *journal.Entries[0].IssuedAfter == 2
	})
	if err := chat.h.Close(s.ctx); err != nil {
		t.Fatal(err)
	}
	s.holds.setBlock(false)
	chat = s.openSQLite()
	defer chat.h.Close(context.Background())
	chat.h.Resume()
	s.answer(chat.h, sub.ID())
}

func TestCodemodeListsTheScriptsCallsInItsDetails(t *testing.T) {
	s := newSetup(t)
	s.tools = append(s.tools, durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: "fails", Description: "fails", Parameters: parametersSchema},
		Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
			return durable.ToolResult{Content: "permission denied", IsError: true}, nil
		}})
	s.holds.setBlock(true)
	s.model.scripts = []string{`await tools.echo({ text: "a" }); try { await tools.fails({}); } catch {} tools.hold({ id: 9 }); return 1;`}
	chat := s.openSQLite()
	defer chat.h.Close(context.Background())
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	result := s.answer(chat.h, sub.ID())
	calls := result.details.Calls
	// A call still running, or not yet launched, when the script ended shows
	// as cancelled.
	if len(calls) != 3 ||
		calls[0] != (NestedCall{ID: 1, Name: "echo", Args: `{"text":"a"}`, Status: "ok", DurationMs: calls[0].DurationMs}) ||
		calls[1] != (NestedCall{ID: 2, Name: "fails", Args: "{}", Status: "error", Error: "permission denied", DurationMs: calls[1].DurationMs}) ||
		calls[2] != (NestedCall{ID: 3, Name: "hold", Args: `{"id":9}`, Status: "cancelled"}) || calls[0].DurationMs == nil {
		t.Fatalf("calls %+v", calls)
	}
}

func TestCodemodeStoresNoOutcomeWhenTheSandboxCloses(t *testing.T) {
	s := newSetup(t)
	node := s.sandbox.inner.(*NodeSandbox)
	s.model.scripts = []string{`await tools.echo({ text: "a" }); await tools.hold({}); return "done";`}
	chat := s.openSQLite()
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	s.holds.wait(t)
	task := scriptTask(t, s.ctx, chat.h)
	// A host shutting down closes the sandbox before the Harness.
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	run, err := durable.ReadDocument[runState](s.ctx, chat.h, RunDocument, uint64(task))
	if err != nil {
		t.Fatal(err)
	}
	if run.Result != nil {
		t.Fatalf("stored %+v", run.Result)
	}
	if err := chat.h.Close(s.ctx); err != nil {
		t.Fatal(err)
	}
	// The next process has a new sandbox.
	s.holds.setBlock(false)
	s.sandbox.inner = &NodeSandbox{PackageDir: node.PackageDir}
	chat = s.openSQLite()
	defer chat.h.Close(context.Background())
	chat.h.Resume()
	expectResult(t, s.answer(chat.h, sub.ID()), `done`, false)
	expectCounts(t, s.echo.snapshot(), map[string]int{"a": 1})
}

func TestCodemodeAbortsCallsStillRunningWhenTheScriptEnds(t *testing.T) {
	s := newSetup(t)
	s.holds.setBlock(true)
	s.model.scripts = []string{`tools.hold({}); await tools.echo({ text: "a" }); return 1;`}
	chat := s.openSQLite()
	defer chat.h.Close(context.Background())
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	expectResult(t, s.answer(chat.h, sub.ID()), "1", false)
	var hold *durable.TaskRecord
	for _, task := range nestedCalls(t, s.ctx, chat.h) {
		if strings.Contains(string(task.Input), `"name":"hold"`) {
			hold = &task
		}
	}
	if hold == nil || hold.Status != "terminal" || hold.Outcome == nil || hold.Outcome.Status != "aborted" || s.holds.runs != 1 {
		t.Fatalf("hold %+v, runs %d", hold, s.holds.runs)
	}
}

func TestCodemodeOutcomesOfFailedCalls(t *testing.T) {
	s := newSetup(t)
	exit := json.RawMessage(`{"type":"object","properties":{"output":{"type":"string"},"exitCode":{"type":"number"}},"required":["output","exitCode"]}`)
	png := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
	s.tools = append(s.tools,
		durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: "fails", Description: "fails", Parameters: parametersSchema},
			Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
				return durable.ToolResult{Content: "permission denied", IsError: true}, nil
			}},
		// Like bash on a nonzero exit: an error result with its value.
		durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: "run", Description: "run", Parameters: json.RawMessage(`{"type":"object"}`)},
			StructuredOutputSchema: exit,
			Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
				return durable.ToolResult{Content: "no matches", StructuredOutput: json.RawMessage(`{"output":"no matches","exitCode":1}`), IsError: true}, nil
			}},
		durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: "denied", Description: "denied", Parameters: json.RawMessage(`{"type":"object"}`)},
			StructuredOutputSchema: exit,
			Execute: func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
				return durable.ToolResult{Content: "permission denied", IsError: true}, nil
			}},
		durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: "picture", Description: "picture", Parameters: json.RawMessage(`{"type":"object","properties":{"caption":{"type":"boolean"}},"required":["caption"]}`)},
			Execute: func(_ context.Context, args json.RawMessage, _ *durable.ToolRuntime) (durable.ToolResult, error) {
				var input struct{ Caption bool }
				_ = json.Unmarshal(args, &input)
				result := durable.ToolResult{Images: []durable.ImageContent{{Data: png, MimeType: "image/png"}}}
				if input.Caption {
					result.Content = "a cat"
				}
				return result, nil
			}})
	t.Run("rejects with the error text and structured output", func(t *testing.T) {
		expectResult(t, s.runOnce(`try { await tools.fails({}); } catch (e) { return [e.message, e.structuredOutput]; }`),
			`["permission denied","permission denied"]`, false)
	})
	t.Run("resolves a schema tool's error result with its value", func(t *testing.T) {
		expectResult(t, s.runOnce(`const r = await tools.run({});
			let e; try { await tools.denied({}); } catch (error) { e = [error.message, error.structuredOutput ?? null]; }
			return [r.exitCode, r.output, e];`), `[1,"no matches",["permission denied",null]]`, false)
	})
	t.Run("resolves a schema-less tool with its text", func(t *testing.T) {
		expectResult(t, s.runOnce(`return [await tools.picture({ caption: true }), await tools.picture({ caption: false })];`),
			`["a cat",""]`, false)
	})
}

func TestCodemodeReturnsADecidedOutcomeAfterARestart(t *testing.T) {
	s := newSetup(t)
	s.holds.setBlock(false)
	stopped := newHoldGate()
	s.hooks.AfterTool = func(ctx context.Context, result *durable.ToolResult, r *durable.ToolRuntime) error {
		if r.Call.Name != ToolName {
			return nil
		}
		stopped.mu.Lock()
		block, once, reached := stopped.block, stopped.once, stopped.reached
		stopped.mu.Unlock()
		if !block {
			return nil
		}
		once.Do(func() { close(reached) })
		<-ctx.Done()
		return ctx.Err()
	}
	s.model.scripts = []string{`return "first"`}
	chat := s.openSQLite()
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	// Stopped after execute returned, so after the outcome is stored, and
	// before the result commits.
	stopped.wait(t)
	task := scriptTask(t, s.ctx, chat.h)
	if run, err := durable.ReadDocument[runState](s.ctx, chat.h, RunDocument, uint64(task)); err != nil || run.Result == nil {
		t.Fatalf("run %+v, %v", run, err)
	}
	if err := chat.h.Close(s.ctx); err != nil {
		t.Fatal(err)
	}
	stopped.setBlock(false)
	chat = s.openSQLite()
	defer chat.h.Close(context.Background())
	chat.h.Resume()
	expectResult(t, s.answer(chat.h, sub.ID()), "first", false)
	if s.sandbox.count() != 1 {
		t.Fatalf("starts %d", s.sandbox.count())
	}
}

func TestCodemodeKeepsStoreValuesPerConversationAsOfAFork(t *testing.T) {
	s := newSetup(t)
	s.holds.setBlock(false)
	s.model.scripts = []string{`store("k", 1); return load("k")`}
	chat := s.openSQLite()
	defer chat.h.Close(context.Background())
	submitAndWait(t, s.ctx, chat, "first")
	state, err := chat.h.Snapshot(s.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var firstUser durable.EntryID
	for _, entry := range state.Entries {
		if entry.Kind == "pi.user" && firstUser == 0 {
			firstUser = entry.ID
		}
	}
	s.model.mu.Lock()
	s.model.scripts = append(s.model.scripts, `return [load("k") ?? null]`)
	s.model.mu.Unlock()
	submitAndWait(t, s.ctx, chat, "second")
	results := codemodeResults(t, s.ctx, chat.h)
	if len(results) != 2 || results[0].Content != "1" || results[1].Content != "[1]" {
		t.Fatalf("results %+v", results)
	}
	// A fork at the first input is from before the first script stored
	// anything.
	s.model.mu.Lock()
	s.model.scripts = append(s.model.scripts, `return [load("k") ?? null]`)
	s.model.mu.Unlock()
	fork, err := chat.root.Fork(s.ctx, firstUser, durable.ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := fork.Submit(s.ctx, durable.SubmissionRequest{Content: "fork"})
	if err != nil {
		t.Fatal(err)
	}
	if settled, err := sub.Wait(s.ctx); err != nil || settled.Status != "done" {
		t.Fatalf("fork %+v %v", settled, err)
	}
	results = codemodeResults(t, s.ctx, chat.h)
	if last := results[len(results)-1]; last.Content != "[null]" {
		t.Fatalf("fork result %q", last.Content)
	}
}

func TestCodemodeJournalsResultsThatFinishTogetherInOneCommit(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	waiting := 0
	s := newSetup(t, Global{Name: "wait", Execute: func(ctx context.Context, _ json.RawMessage, _ *durable.ToolRuntime) (json.RawMessage, error) {
		mu.Lock()
		waiting++
		if waiting == 50 {
			close(release)
		}
		mu.Unlock()
		<-release
		return json.RawMessage("null"), nil
	}})
	s.holds.setBlock(false)
	s.model.scripts = []string{`return (await Promise.all(Array.from({ length: 50 }, () => wait()))).length`}
	chat := s.openSQLite()
	defer chat.h.Close(context.Background())
	journalCommits := 0
	var count sync.Mutex
	stop := chat.h.ObserveCommits(func(delta *durable.StateDelta) {
		for key, document := range delta.Documents {
			if document != nil && document.Kind == "pi.codemode.journal" && !strings.Contains(key, "@") {
				count.Lock()
				journalCommits++
				count.Unlock()
			}
		}
	})
	defer stop()
	sub, err := chat.root.Submit(s.ctx, durable.SubmissionRequest{Content: "go"})
	if err != nil {
		t.Fatal(err)
	}
	expectResult(t, s.answer(chat.h, sub.ID()), "50", false)
	count.Lock()
	defer count.Unlock()
	// The journal's creation and a few groups, not one commit per result.
	if journalCommits >= 10 {
		t.Fatalf("%d journal commits", journalCommits)
	}
}
