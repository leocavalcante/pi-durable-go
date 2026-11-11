package durable

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestBoundOutput(t *testing.T) {
	for _, tc := range []struct {
		text          string
		limits        OutputLimits
		want          string
		lines, nbytes int
	}{
		{"a\nb\nc\n", OutputLimits{MaxLines: 2}, "a\nb\n", 1, 2},
		{"a\nb\nc\n", OutputLimits{MaxLines: 2, Retain: "tail"}, "b\nc\n", 1, 2},
		{"short", OutputLimits{}, "short", 0, 0},
		{"abcdef", OutputLimits{MaxBytes: 4}, "abcd", 1, 2},
		{"界界界", OutputLimits{MaxBytes: 4}, "界", 1, 6},
		{"one\ntwo\nthree", OutputLimits{MaxBytes: 8}, "one\ntwo\n", 1, 5},
	} {
		got, lines, nbytes := boundOutput(tc.text, tc.limits)
		if got != tc.want || lines != tc.lines || nbytes != tc.nbytes {
			t.Errorf("boundOutput(%q, %+v)=(%q, %d, %d), want (%q, %d, %d)", tc.text, tc.limits, got, lines, nbytes, tc.want, tc.lines, tc.nbytes)
		}
	}
}

func TestToolOutputLimitsTruncateWithADiagnostic(t *testing.T) {
	registry := toolExtension("lines", true, func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
		return ToolResult{Content: strings.Repeat("line\n", 10)}, nil
	})
	snapshot := registry.Snapshot()
	tools := snapshot.Extensions()[0].Tools
	tools[0].OutputLimits = &OutputLimits{MaxLines: 3}
	registry = NewRegistry()
	if err := registry.Install(Extension{Name: "tools", Tools: tools}); err != nil {
		t.Fatal(err)
	}
	h := taskTestHarness(t, HarnessOptions{Model: &toolCallingModel{name: "lines"}, Registry: registry})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	want := "line\nline\nline\n\n<harness>\n[warn] Output truncated to its beginning: 7 lines, 35 bytes dropped\n</harness>"
	if got := toolResultEntry(t, h, root.ID()).Message.Content; got != want {
		t.Fatalf("result=%q", got)
	}
}
