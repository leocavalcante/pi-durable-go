package durable

import (
	"encoding/json"
	"strings"
	"testing"
)

// Expectations are upstream's serializeConversation and pi-ai's
// estimateMessageTokens (1.5.2) for the same messages.
func TestSummarizedTranscriptsAreUpstreams(t *testing.T) {
	long := strings.Repeat("x", 2100) + strings.Repeat("😀", 30)
	messages := []Message{
		{Role: "user", Content: "hello"},
		{Role: "assistant", Content: "calling", ToolCalls: []ToolCall{{ID: "c1", Name: "lookup", Arguments: json.RawMessage(`{"z":1,"a":"\u00e9","10":true,"2":[1.0,2.5e1],"nested":{"b":1,"a":2,"1":0},"s":"line\nnext \"q\" \u0001"}`)}}},
		{Role: "tool", ToolCallID: "c1", ToolName: "lookup", Content: long},
		{Role: "user", Content: "lookhere", Images: []ImageContent{{Data: "AAAA", MimeType: "image/png"}}, ImageOffsets: []int{4}},
		{Role: "assistant", Thinking: "hmm"},
	}
	want := "[User]: hello\n\n[Assistant]: calling\n\n[Assistant tool calls]: lookup(2=[1,25], 10=true, z=1, a=\"é\", nested={\"1\":0,\"b\":1,\"a\":2}, s=\"line\\nnext \\\"q\\\" \\u0001\")\n\n[Tool result]: " +
		strings.Repeat("x", 2000) + "\n\n[... 160 more characters truncated]\n\n[User]: look\nhere"
	if got := serializeConversation(messages); got != want {
		t.Fatalf("serialized:\n%q\nwant\n%q", got, want)
	}
	for i, tokens := range []int{2, 31, 618, 1374, 1} {
		if got := estimateMessageTokens(messages[i]); got != tokens {
			t.Fatalf("message %d: %d tokens, want %d", i, got, tokens)
		}
	}
}

// Expectations are upstream's selectCut and estimateContext (1.5.2) for the
// same views and kept-token targets.
func TestCompactionCutsAreUpstreams(t *testing.T) {
	want := []int{5, 3, 3, 2, 1, -1, 840, 5, 3, 3, 3, 3, -1, 1131, 4, 4, 4, 4, -1, -1, 403, 3, 3, 3, 2, -1, -1, 1072}
	U := func(text string) Message { return Message{Role: "user", Content: text} }
	A := func(text string, calls []string, usage int64) Message {
		m := Message{Role: "assistant", Content: text, StopReason: "stop", Usage: Usage{InputTokens: usage}}
		for _, id := range calls {
			m.ToolCalls = append(m.ToolCalls, ToolCall{ID: id, Name: "t", Arguments: json.RawMessage(`{}`)})
		}
		if len(calls) > 0 {
			m.StopReason = "toolUse"
		}
		return m
	}
	R := func(id, text string) Message {
		return Message{Role: "tool", ToolCallID: id, ToolName: "t", Content: text}
	}
	big := func(n int) string { return strings.Repeat("w", n) }
	views := [][][]Message{
		{{U(big(700))}, {A(big(700), nil, 0)}, {U(big(700))}, {A(big(700), nil, 0)}, {U(big(70))}, {A(big(70), nil, 0)}},
		{{U(big(350))}, {A("", []string{"c1"}, 0)}, {R("c1", big(3500))}, {A(big(35), nil, 0)}, {U(big(35))}, {A(big(35), nil, 0)}},
		{{U(big(350))}, {A("", []string{"c1"}, 0)}, {U("steer " + big(350))}, {R("c1", big(350))}, {A(big(350), nil, 0)}},
		{{U(big(100))}, {A(big(100), nil, 500)}, {U(big(1000))}, {A(big(1000), nil, 0)}},
	}
	heads := []bool{false, false, false, true}
	var got []int
	for v, contributions := range views {
		view := contextView{contributions: contributions}
		for i := range contributions {
			view.entries = append(view.entries, Entry{ID: EntryID(i + 1)})
			view.messages = append(view.messages, contributions[i]...)
		}
		if heads[v] {
			view.head = &view.entries[0]
		}
		for _, keep := range []float64{10, 50, 150, 300, 600, 5000} {
			cut := selectCut(view, keep)
			if cut == 0 {
				cut = -1
			}
			got = append(got, cut)
		}
		got = append(got, estimateContext(view, nil))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: got %d want %d (all: %v)", i, got[i], want[i], got)
			break
		}
	}
}
