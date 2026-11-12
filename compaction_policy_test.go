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
