package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// sanitizeOutput's answers are upstream chord's sanitizeOutput's.
func TestSanitizeOutputAsChord(t *testing.T) {
	for _, c := range [][2]string{
		{"plain\n", "plain\n"},
		{"nul\x00byte and bell\x07\n", "nulbyte and bell\n"},
		{"\x1b[31mred\x1b[0m and \x1b]0;title\x07osc\n", "red and osc\n"},
		{"crlf line\r\n", "crlf line\n"},
		{"tab\tkept\n", "tab\tkept\n"},
		{"\x1bcreset \x1b(B charset\n", "reset  charset\n"},
		{"annotation\ufff9x\ufffay\ufffbz\n", "annotationxyz\n"},
		{"del\x7f and c1\u0085 kept\n", "del\x7f and c1\u0085 kept\n"},
		{"dcs\x1bPq#0\x1b\\done\n", "dcsdone\n"},
		{"lone esc\x1b\n", "lone esc\n"},
		{"csi \x1b[?25l\x1b[2Jhidden\n", "csi hidden\n"},
		{"\x1b]8;;http://x\x1b\\link\x1b]8;;\x1b\\\n", "link\n"},
	} {
		if got := sanitizeOutput(c[0]); got != c[1] {
			t.Errorf("sanitizeOutput(%q) = %q, want %q", c[0], got, c[1])
		}
	}
}

// A buffer's snapshots are upstream chord's OutputBuffer's.
func TestOutputBufferAsChord(t *testing.T) {
	for _, c := range []struct {
		limits OutputLimits
		chunks []string
		want   boundedOutput
	}{
		{OutputLimits{MaxBytes: 20, MaxLines: 3, Retain: "tail"}, []string{"one\ntw", "o\nthree\nfo", "ur\nfive", "\nsix and more text\n"},
			boundedOutput{text: "six and more text\n", droppedBytes: 24, droppedLines: 5}},
		{OutputLimits{MaxBytes: 12, MaxLines: 5, Retain: "head"}, []string{"ab\ncd\n", "ef\x1b[1mgh\n", "ij\nkl\nmn\n"},
			boundedOutput{text: "ab\ncd\n", droppedBytes: 18, droppedLines: 4}},
		// A character split across chunks survives; one cut short at the
		// end is a replacement character.
		{OutputLimits{MaxBytes: 100, MaxLines: 10, Retain: "tail"}, []string{"caf\xc3", "\xa9 ok\n\xe2\x9c"},
			boundedOutput{text: "café ok\n\ufffd"}},
	} {
		b := newOutputBuffer(c.limits)
		for _, chunk := range c.chunks {
			b.push(chunk)
		}
		b.end()
		if got := b.snapshot(); got != c.want {
			t.Errorf("%q: got %+v, want %+v", c.chunks, got, c.want)
		}
	}
}

// Streamed output is bound once, by the tool's limits, and its note counts
// all it dropped (upstream; tool-streamed-tail-lines-beyond-window).
func TestStreamedOutputIsBoundOnceByItsLimits(t *testing.T) {
	row := "a row of streamed output text that is fairly long\n"
	registry := NewRegistry()
	if err := registry.Install(Extension{Name: "tools", Tools: []ToolDefinition{{
		ToolSpec: ToolSpec{Name: "streamer", Parameters: json.RawMessage(`{"type":"object"}`)}, ReplaySafe: true,
		OutputLimits: &OutputLimits{Retain: "tail", MaxLines: 10},
		Execute: func(ctx context.Context, _ json.RawMessage, r *ToolRuntime) (ToolResult, error) {
			for i := 0; i < 3000; i++ {
				if err := r.Output(ctx, row); err != nil {
					return ToolResult{}, err
				}
			}
			if text, truncated := r.RetainedOutput(ctx); text != strings.Repeat(row, 10) || !truncated {
				return ToolResult{}, fmt.Errorf("retained %d bytes, truncated %v", len(text), truncated)
			}
			return ToolResult{}, nil
		},
	}}}); err != nil {
		t.Fatal(err)
	}
	h := taskTestHarness(t, HarnessOptions{Model: &toolCallingModel{name: "streamer"}, Registry: registry})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
	want := strings.Repeat(row, 10) + "\n<harness>\n[warn] Output truncated to its end: 2990 lines, 149500 bytes dropped\n</harness>"
	if got := toolResultEntry(t, h, root.ID()).Message.Content; got != want {
		t.Fatalf("result:\n%q\nwant\n%q", got[max(0, len(got)-200):], want[len(want)-200:])
	}
}

// Ledgers add usage as upstream's addUsage: every counter and cost part;
// optional counters once either side reports them; the first record's
// provenance of its total stays.
func TestUsageSumsAsUpstream(t *testing.T) {
	n := func(v int64) *int64 { return &v }
	first := Usage{InputTokens: 1, TotalTokens: 1, Cost: 0.5, CostInput: 0.5, CostFromProvider: true}
	second := Usage{InputTokens: 2, OutputTokens: 3, ReasoningTokens: n(3), CacheWrite1hTokens: n(1), TotalTokens: 5, Cost: 0.25, CostOutput: 0.25}
	sum := sumUsage(sumUsage(Usage{}, first), second)
	sum = sumUsage(sum, Usage{ReasoningTokens: n(4)})
	if sum.ReasoningTokens == nil || *sum.ReasoningTokens != 7 || sum.CacheWrite1hTokens == nil || *sum.CacheWrite1hTokens != 1 {
		t.Fatalf("optional counters: %+v", sum)
	}
	if sum.InputTokens != 3 || sum.OutputTokens != 3 || sum.TotalTokens != 6 || sum.Cost != 0.75 || sum.CostInput != 0.5 || sum.CostOutput != 0.25 || !sum.CostFromProvider {
		t.Fatalf("sum: %+v", sum)
	}
	if later := sumUsage(sumUsage(Usage{}, second), first); later.CostFromProvider {
		t.Fatalf("a later record changed the provenance: %+v", later)
	}
	if none := sumUsage(Usage{}, Usage{InputTokens: 1}); none.ReasoningTokens != nil {
		t.Fatalf("reasoning appeared: %+v", none)
	}
}

// A request's size from its usage prefers the reported total (pi-ai
// calculateContextTokens).
func TestUsageTokensPreferTheTotal(t *testing.T) {
	if got := usageTokens(Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 40}); got != 40 {
		t.Fatalf("with a total: %d", got)
	}
	if got := usageTokens(Usage{InputTokens: 10, OutputTokens: 5, CacheReadTokens: 2, CacheWriteTokens: 1}); got != 18 {
		t.Fatalf("without a total: %d", got)
	}
}
