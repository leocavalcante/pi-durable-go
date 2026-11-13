//go:build crosscheck

package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// Cross-checks with upstream's own code (make crosscheck): the same cases go
// through the port and through the pinned release's TypeScript sources.

// piMessage is a message as pi-ai represents it, for upstream's functions.
func piMessage(m Message) map[string]any {
	blocks := func() []any {
		var out []any
		for _, part := range OrderedContent(m.Content, m.Images, m.ImageOffsets) {
			if part.Image != nil {
				out = append(out, map[string]any{"type": "image", "data": part.Image.Data, "mimeType": part.Image.MimeType})
			} else if part.Text != "" {
				out = append(out, map[string]any{"type": "text", "text": part.Text})
			}
		}
		if out == nil {
			out = []any{}
		}
		return out
	}
	switch m.Role {
	case "user":
		if len(m.Images) == 0 {
			return map[string]any{"role": "user", "content": m.Content, "timestamp": 1}
		}
		return map[string]any{"role": "user", "content": blocks(), "timestamp": 1}
	case "tool":
		return map[string]any{"role": "toolResult", "toolCallId": m.ToolCallID, "toolName": m.ToolName, "content": blocks(), "isError": m.IsError, "timestamp": 3}
	case "assistant":
		content := []any{}
		if m.Thinking != "" {
			content = append(content, map[string]any{"type": "thinking", "thinking": m.Thinking})
		}
		if m.Content != "" {
			content = append(content, map[string]any{"type": "text", "text": m.Content})
		}
		for _, call := range m.ToolCalls {
			var args any = map[string]any{}
			if parsed, err := parseJS(call.Arguments); err == nil {
				args = jsonValueOf(parsed)
			}
			content = append(content, map[string]any{"type": "toolCall", "id": call.ID, "name": call.Name, "arguments": args})
		}
		stop := m.StopReason
		if stop == "" {
			stop = "stop"
		}
		usage := m.Usage
		return map[string]any{"role": "assistant", "content": content, "stopReason": stop, "timestamp": 2,
			"api": "faux", "provider": "faux", "model": "faux-1",
			"usage": map[string]any{"input": usage.InputTokens, "output": usage.OutputTokens, "cacheRead": usage.CacheReadTokens, "cacheWrite": usage.CacheWriteTokens,
				"totalTokens": usage.InputTokens + usage.OutputTokens + usage.CacheReadTokens + usage.CacheWriteTokens,
				"cost":        map[string]any{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}}
	}
	panic("no pi-ai form for role " + m.Role)
}

// jsonValueOf is a parsed value as encoding/json writes it, key order aside:
// ordered objects become an ordered map JSON keeps via jsonObject.
func jsonValueOf(v any) any {
	switch value := v.(type) {
	case *jsObject:
		return orderedJSON{value}
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = jsonValueOf(item)
		}
		return out
	}
	return v
}

// orderedJSON marshals a parsed object in its own key order.
type orderedJSON struct{ object *jsObject }

func (o orderedJSON) MarshalJSON() ([]byte, error) { return []byte(jsStringify(o.object)), nil }

func piMessages(messages []Message) []any {
	out := make([]any, len(messages))
	for i, m := range messages {
		out[i] = piMessage(m)
	}
	return out
}

func TestCrosscheckToolArguments(t *testing.T) {
	var cases []struct {
		Schema json.RawMessage `json:"schema"`
		Args   json.RawMessage `json:"args"`
	}
	crosscheck.Cases(t, "coerce.json", &cases)
	var want []struct {
		OK    bool            `json:"ok"`
		Value json.RawMessage `json:"value"`
	}
	crosscheck.Run(t, "coerce", cases, &want)
	for i, c := range cases {
		got, err := validateToolArguments(ToolSpec{Name: "t", Parameters: c.Schema}, c.Args)
		if (err == nil) != want[i].OK {
			t.Errorf("%s with %s: accepted=%v, upstream %v (%v)", c.Schema, c.Args, err == nil, want[i].OK, err)
			continue
		}
		if err == nil && !crosscheck.Same(got, want[i].Value) {
			t.Errorf("%s with %s: %s, upstream %s", c.Schema, c.Args, got, want[i].Value)
		}
	}
}

func TestCrosscheckToolArgumentErrors(t *testing.T) {
	var cases []struct {
		Name   string          `json:"name"`
		Schema json.RawMessage `json:"schema"`
		Args   json.RawMessage `json:"args"`
	}
	crosscheck.Cases(t, "errors.json", &cases)
	var want []struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	crosscheck.Run(t, "validate", cases, &want)
	for i, c := range cases {
		_, err := validateToolArguments(ToolSpec{Name: c.Name, Parameters: c.Schema}, c.Args)
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != want[i].Error {
			t.Errorf("%s with %s:\n got %q\nwant %q", c.Schema, c.Args, got, want[i].Error)
		}
	}
}

func crosscheckTranscript() []Message {
	long := strings.Repeat("x", 2100) + strings.Repeat("😀", 30)
	return []Message{
		{Role: "user", Content: "hello"},
		{Role: "assistant", Content: "calling", ToolCalls: []ToolCall{{ID: "c1", Name: "lookup", Arguments: json.RawMessage(`{"z":1,"a":"\u00e9","10":true,"2":[1.0,2.5e1],"nested":{"b":1,"a":2,"1":0},"s":"line\nnext \"q\" \u0001"}`)}}, StopReason: "toolUse"},
		{Role: "tool", ToolCallID: "c1", ToolName: "lookup", Content: long},
		{Role: "user", Content: "lookhere", Images: []ImageContent{{Data: "AAAA", MimeType: "image/png"}}, ImageOffsets: []int{4}},
		{Role: "assistant", Thinking: "hmm"},
	}
}

func TestCrosscheckSummarizedTranscripts(t *testing.T) {
	messages := crosscheckTranscript()
	var want struct {
		Serialized string `json:"serialized"`
		Tokens     []int  `json:"tokens"`
	}
	crosscheck.Run(t, "serialize", map[string]any{"messages": piMessages(messages)}, &want)
	if got := serializeConversation(messages); got != want.Serialized {
		t.Errorf("serialized:\n got %q\nwant %q", got, want.Serialized)
	}
	for i, m := range messages {
		if got := estimateMessageTokens(m); got != want.Tokens[i] {
			t.Errorf("message %d: %d tokens, upstream %d", i, got, want.Tokens[i])
		}
	}
}

func TestCrosscheckCompactionCuts(t *testing.T) {
	U := func(text string) Message { return Message{Role: "user", Content: text} }
	A := func(text string, calls []string, usage int64) Message {
		m := Message{Role: "assistant", Content: text, StopReason: "stop", Usage: Usage{InputTokens: usage}}
		for _, id := range calls {
			m.ToolCalls = append(m.ToolCalls, ToolCall{ID: id, Name: "t", Arguments: json.RawMessage(`{}`)})
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
	keeps := []float64{10, 50, 150, 300, 600, 5000}
	type piView struct {
		Head          bool    `json:"head"`
		Contributions [][]any `json:"contributions"`
	}
	input := make([]piView, len(views))
	for v, contributions := range views {
		input[v].Head = heads[v]
		for _, messages := range contributions {
			input[v].Contributions = append(input[v].Contributions, piMessages(messages))
		}
	}
	var want []struct {
		Cuts     []int `json:"cuts"`
		Estimate int   `json:"estimate"`
	}
	crosscheck.Run(t, "cut", map[string]any{"views": input, "keeps": keeps}, &want)
	for v, contributions := range views {
		view := contextView{contributions: contributions}
		for i := range contributions {
			view.entries = append(view.entries, Entry{ID: EntryID(i + 1)})
			view.messages = append(view.messages, contributions[i]...)
		}
		if heads[v] {
			view.head = &view.entries[0]
		}
		for k, keep := range keeps {
			cut := selectCut(view, keep)
			if cut == 0 {
				cut = -1
			}
			if cut != want[v].Cuts[k] {
				t.Errorf("view %d keeping %v: cut %d, upstream %d", v, keep, cut, want[v].Cuts[k])
			}
		}
		if got := estimateContext(view, nil); got != want[v].Estimate {
			t.Errorf("view %d: estimate %d, upstream %d", v, got, want[v].Estimate)
		}
	}
}

func TestCrosscheckToolResultOrdering(t *testing.T) {
	a := func(ids ...string) Message {
		m := Message{Role: "assistant", StopReason: "toolUse"}
		for _, id := range ids {
			m.ToolCalls = append(m.ToolCalls, ToolCall{ID: id, Name: "t", Arguments: json.RawMessage(`{}`)})
		}
		return m
	}
	r := func(id string) Message {
		return Message{Role: "tool", ToolCallID: id, ToolName: "t", Content: "r" + id}
	}
	u := func(text string) Message { return Message{Role: "user", Content: text} }
	sequences := [][]Message{
		{u("q"), a("1", "2"), r("2"), r("1"), u("next")},
		{u("q"), a("1"), u("steer"), r("1")},
		{u("q"), a("1", "2"), r("1"), a("3"), r("3")},
		{a("1"), r("1"), a("1"), r("1")},
		{r("9"), u("q"), a("1")},
		{u("q"), a("1"), r("1"), r("1")},
	}
	input := make([][]any, len(sequences))
	for i, seq := range sequences {
		input[i] = piMessages(seq)
	}
	var want [][]json.RawMessage
	crosscheck.Run(t, "order", input, &want)
	for i, seq := range sequences {
		got := piMessages(orderToolResults(seq))
		if len(got) != len(want[i]) {
			t.Errorf("sequence %d: %d messages, upstream %d", i, len(got), len(want[i]))
			continue
		}
		for j := range got {
			if !samePiMessage(got[j], want[i][j]) {
				data, _ := json.Marshal(got[j])
				t.Errorf("sequence %d message %d:\n got %s\nwant %s", i, j, data, want[i][j])
			}
		}
	}
}

// samePiMessage compares the parts of a pi-ai message the port keeps.
func samePiMessage(got any, want json.RawMessage) bool {
	var w map[string]any
	if json.Unmarshal(want, &w) != nil {
		return false
	}
	data, _ := json.Marshal(got)
	var g map[string]any
	_ = json.Unmarshal(data, &g)
	keep := func(m map[string]any) map[string]any {
		out := map[string]any{}
		for _, key := range []string{"role", "content", "toolCallId", "toolName", "isError"} {
			if value, ok := m[key]; ok {
				out[key] = value
			}
		}
		if m["role"] == "toolResult" {
			if content, ok := out["content"].([]any); ok {
				var texts []string
				for _, block := range content {
					if b, ok := block.(map[string]any); ok && b["type"] == "text" {
						texts = append(texts, b["text"].(string))
					}
				}
				out["content"] = strings.Join(texts, "")
			}
		}
		return out
	}
	return reflect.DeepEqual(keep(g), keep(w))
}

func TestCrosscheckPromptPlans(t *testing.T) {
	sectionCases := [][2][][2]string{
		{{{"a", "1"}, {"b", "2"}}, {{"a", "1"}, {"b", "3"}}},
		{{{"a", "1"}, {"b", "2"}}, {{"b", "2"}, {"a", "1"}}},
		{{{"a", "1"}, {"b", "2"}}, {{"a", "1"}}},
		{{{"a", "1"}}, {{"a", "1"}, {"c", "3"}}},
		{{{"a", "1"}, {"b", "2"}, {"c", "3"}}, {{"a", "1"}, {"c", "3"}, {"d", "4"}}},
		{{}, {}},
		{{{"a", "1"}, {"b", "2"}}, {{"c", "3"}, {"a", "1"}}},
	}
	tool := func(name, description string) ToolSpec {
		return ToolSpec{Name: name, Description: description, Parameters: json.RawMessage(`{"type":"object"}`)}
	}
	toolCases := [][2][]ToolSpec{
		{{tool("x", ""), tool("y", "")}, {tool("y", "")}},
		{{tool("x", ""), tool("y", "")}, {tool("y", ""), tool("x", "")}},
		{{tool("x", ""), tool("y", "")}, {tool("x", "changed"), tool("y", "")}},
		{{tool("x", "")}, {tool("x", ""), tool("z", "")}},
		{{}, {tool("a", ""), tool("b", "")}},
		{{tool("x", ""), tool("y", ""), tool("z", "")}, {tool("x", ""), tool("z", "")}},
		{{tool("x", ""), tool("y", "")}, {tool("y", "changed"), tool("x", "")}},
	}
	piTools := func(tools []ToolSpec) []any {
		out := []any{}
		for _, t := range tools {
			out = append(out, map[string]any{"name": t.Name, "description": t.Description, "parameters": json.RawMessage(t.Parameters)})
		}
		return out
	}
	var toolInput [][][]any
	for _, c := range toolCases {
		toolInput = append(toolInput, [][]any{piTools(c[0]), piTools(c[1])})
	}
	var want struct {
		Sections [][]json.RawMessage `json:"sections"`
		Tools    []struct {
			Removed []string `json:"removed"`
			Added   []string `json:"added"`
		} `json:"tools"`
	}
	crosscheck.Run(t, "plan", map[string]any{"sections": sectionCases, "tools": toolInput}, &want)
	ordered := func(pairs [][2]string) orderedText {
		o := orderedText{text: map[string]string{}}
		for _, p := range pairs {
			o.names = append(o.names, p[0])
			o.text[p[0]] = p[1]
		}
		return o
	}
	for i, c := range sectionCases {
		var got []string
		for _, patch := range planSections(ordered(c[0]), ordered(c[1])) {
			parts := make([]string, 0, len(patch))
			for _, section := range patch {
				key, _ := json.Marshal(section.Name)
				value := []byte("null")
				if section.Text != nil {
					value, _ = json.Marshal(*section.Text)
				}
				parts = append(parts, string(key)+":"+string(value))
			}
			got = append(got, "{"+strings.Join(parts, ",")+"}")
		}
		var wantPatches []string
		for _, patch := range want.Sections[i] {
			wantPatches = append(wantPatches, string(patch))
		}
		if !reflect.DeepEqual(got, wantPatches) {
			t.Errorf("sections %d: %v, upstream %v", i, got, wantPatches)
		}
	}
	for i, c := range toolCases {
		removed, added := planTools(c[0], c[1])
		var addedNames []string
		for _, tool := range added {
			addedNames = append(addedNames, tool.Name)
		}
		if strings.Join(removed, ",") != strings.Join(want.Tools[i].Removed, ",") || strings.Join(addedNames, ",") != strings.Join(want.Tools[i].Added, ",") {
			t.Errorf("tools %d: removed %v added %v, upstream %v %v", i, removed, addedNames, want.Tools[i].Removed, want.Tools[i].Added)
		}
	}
}

func TestCrosscheckErrorClassification(t *testing.T) {
	messages := []string{"503 Service Unavailable", "429 Too Many Requests", "429 insufficient_quota", "overloaded_error", "rate limit exceeded",
		"Connection reset by peer", "fetch failed", "ECONNRESET", "request timed out", "invalid api key",
		"400 prompt is too long: 210000 tokens > 200000 maximum", "This model's maximum context length is 8192 tokens", "context_length_exceeded",
		"Your input exceeds the context window of this model", "500 Internal Server Error", "Internal error", "terminated", "socket hang up",
		"The usage limit has been reached", "You exceeded your current quota", "model is overloaded", "Bad gateway", "502",
		"input is too long for requested model", "prompt too long", "too many tokens", "maximum context length",
		"Provider finish_reason: content_filter", "Provider finish_reason: network_error"}
	var want []struct {
		Retry    bool `json:"retry"`
		Overflow bool `json:"overflow"`
	}
	crosscheck.Run(t, "classify", messages, &want)
	for i, text := range messages {
		if got := IsRetryableModelError(errors.New(text)); got != want[i].Retry {
			t.Errorf("%q: retryable %v, upstream %v", text, got, want[i].Retry)
		}
		if got := IsContextOverflowError(errors.New(text), "openai"); got != want[i].Overflow {
			t.Errorf("%q: overflow %v, upstream %v", text, got, want[i].Overflow)
		}
	}
}

func TestCrosscheckPartialArguments(t *testing.T) {
	partials := []string{"", "{", `{"a"`, `{"a":`, `{"a":1`, `{"a":1,`, `{"a":"x`, `{"a":"x\`, `{"a":"x\u00`, `{"a":[1,2`, `{"a":{"b":`,
		`{"a":tr`, `{"a":nul`, `{"a":-`, `{"a":1e`, `[1,2`, `{"a":"x"}`, `{"a":"x"} extra`, "not json", `{"a":"\n"`,
		`{"a": [ {"b": "c" } , {`, `{"a":1}{`, `  {"k" : "v" ,  "n" : 12.5e`, `{"a":true,"b":fals`, `"str`}
	// Unterminated numbers that pi-ai's parser drops for a moment are a
	// documented difference (docs/upstream-cross-checks.md).
	known := map[string]bool{`{"a":1.`: true, `{"b":2,"a":1.`: true, `{"b":2,"a":1E-`: true}
	var want []string
	crosscheck.Run(t, "partial", partials, &want)
	for i, text := range partials {
		if known[text] {
			continue
		}
		got := partialArguments(text)
		var w any
		_ = json.Unmarshal([]byte(want[i]), &w)
		if _, object := w.(map[string]any); !object {
			w = map[string]any{} // a non-object shows {} as arguments
		}
		wantJSON, _ := json.Marshal(w)
		if !crosscheck.Same(got, wantJSON) {
			t.Errorf("%q: %s, upstream %s", text, got, want[i])
		}
	}
}

// Scenarios: one conversation script both runtimes play, upstream's Harness
// with pi-ai's faux provider (internal/crosscheck/scenario.mts) and this one
// with a scripted model. They report each request's messages, the
// transcript, and each submission's settlement, normalized alike.

type scenarioSpec struct {
	Name          string `json:"name"`
	ContextWindow int    `json:"contextWindow,omitempty"`
	AfterRun      *struct {
		Hold string `json:"hold,omitempty"`
	} `json:"afterRun,omitempty"`
	Documents []struct {
		Kind    string `json:"kind"`
		History string `json:"history"`
		Fork    string `json:"fork"`
	} `json:"documents,omitempty"`
	Models []struct {
		ID            string `json:"id"`
		ContextWindow int    `json:"contextWindow,omitempty"`
		Reasoning     bool   `json:"reasoning,omitempty"`
	} `json:"models,omitempty"`
	Summaries []struct {
		Text string `json:"text"`
		Hold string `json:"hold,omitempty"`
	} `json:"summaries,omitempty"`
	Model []struct {
		Text     string `json:"text,omitempty"`
		Thinking string `json:"thinking,omitempty"`
		Stop     string `json:"stop,omitempty"`
		Error    string `json:"error,omitempty"`
		Hold     string `json:"hold,omitempty"`
		Calls    []struct {
			ID   string          `json:"id"`
			Name string          `json:"name"`
			Args json.RawMessage `json:"args,omitempty"`
			// As pi-ai sets it for malformed arguments text.
			ArgumentsError string `json:"argumentsError,omitempty"`
		} `json:"calls,omitempty"`
	} `json:"model"`
	Tools []struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters,omitempty"`
		Sequential  bool            `json:"sequential,omitempty"`
		ReplaySafe  bool            `json:"replaySafe,omitempty"`
		Results     []struct {
			Text    *string `json:"text,omitempty"`
			IsError bool    `json:"isError,omitempty"`
			Throw   string  `json:"throw,omitempty"`
			Hold    string  `json:"hold,omitempty"`
			// Control and diagnostics, as upstream's result carries them.
			Terminate   bool             `json:"terminate,omitempty"`
			Handoff     string           `json:"handoff,omitempty"`
			AddTools    []string         `json:"addTools,omitempty"`
			Diagnostics []ToolDiagnostic `json:"diagnostics,omitempty"`
			Images      []ImageContent   `json:"images,omitempty"`
			Usage       *struct {
				Input  int64 `json:"input"`
				Output int64 `json:"output"`
			} `json:"usage,omitempty"`
			// Running output, and text built by repetition.
			Stream       []string `json:"stream,omitempty"`
			StreamRepeat *struct {
				Text  string `json:"text"`
				Times int    `json:"times"`
			} `json:"streamRepeat,omitempty"`
			Repeat *struct {
				Text  string `json:"text"`
				Times int    `json:"times"`
			} `json:"repeat,omitempty"`
			// Nested calls in order; the result lists theirs.
			Nested []struct {
				Tool string          `json:"tool"`
				Args json.RawMessage `json:"args,omitempty"`
				Key  string          `json:"key,omitempty"`
			} `json:"nested,omitempty"`
		} `json:"results,omitempty"`
		Callers      []string `json:"callers,omitempty"`
		OutputLimits *struct {
			MaxBytes int    `json:"maxBytes,omitempty"`
			MaxLines int    `json:"maxLines,omitempty"`
			Retain   string `json:"retain,omitempty"`
		} `json:"outputLimits,omitempty"`
	} `json:"tools,omitempty"`
	Settings struct {
		Retry *struct {
			Enabled         *bool    `json:"enabled,omitempty"`
			MaxRetries      *float64 `json:"maxRetries,omitempty"`
			BaseDelayMs     *float64 `json:"baseDelayMs,omitempty"`
			MaxAgentDelayMs *float64 `json:"maxAgentDelayMs,omitempty"`
		} `json:"retry,omitempty"`
		ToolExecution  string              `json:"toolExecution,omitempty"`
		SteeringMode   string              `json:"steeringMode,omitempty"`
		FollowUpMode   string              `json:"followUpMode,omitempty"`
		AfterFailedRun string              `json:"afterFailedRun,omitempty"`
		Compaction     *CompactionSettings `json:"compaction,omitempty"`
	} `json:"settings"`
	Agent struct {
		MaxTurns     int      `json:"maxTurns,omitempty"`
		Instructions *string  `json:"instructions,omitempty"`
		Tools        []string `json:"tools,omitempty"`
	} `json:"agent"`
	Extensions []struct {
		Name     string `json:"name"`
		Sections []struct {
			Key  string `json:"key"`
			Text string `json:"text"`
			Tag  *bool  `json:"tag,omitempty"`
		} `json:"sections,omitempty"`
		BeforeTool []struct {
			Tool  string          `json:"tool"`
			Block *string         `json:"block,omitempty"`
			Args  json.RawMessage `json:"args,omitempty"`
		} `json:"beforeTool,omitempty"`
		AfterTool []struct {
			Tool string `json:"tool"`
			Text string `json:"text"`
		} `json:"afterTool,omitempty"`
		OnYield []struct {
			Continue *string `json:"continue,omitempty"`
		} `json:"onYield,omitempty"`
		BeforeRequest *struct {
			AppendUser *string `json:"appendUser,omitempty"`
		} `json:"beforeRequest,omitempty"`
		AfterResponse *struct {
			ErrorMessage *string `json:"errorMessage,omitempty"`
		} `json:"afterResponse,omitempty"`
		BeforeCompact *struct {
			Decline bool    `json:"decline,omitempty"`
			Summary *string `json:"summary,omitempty"`
		} `json:"beforeCompact,omitempty"`
	} `json:"extensions,omitempty"`
	Steps []struct {
		Submit *struct {
			Type string `json:"type"`
			// A string, or pi-ai content blocks (text and images).
			Content  json.RawMessage `json:"content,omitempty"`
			WhenBusy string          `json:"whenBusy,omitempty"`
			WhenIdle string          `json:"whenIdle,omitempty"`
			Entry    *struct {
				Kind string          `json:"kind"`
				Data json.RawMessage `json:"data,omitempty"`
			} `json:"entry,omitempty"`
		} `json:"submit,omitempty"`
		Compact   json.RawMessage `json:"compact,omitempty"`
		Reset     json.RawMessage `json:"reset,omitempty"`
		Configure *struct {
			Instructions  *string `json:"instructions,omitempty"`
			MaxTurns      *int    `json:"maxTurns,omitempty"`
			Model         *string `json:"model,omitempty"`
			ThinkingLevel *string `json:"thinkingLevel,omitempty"`
		} `json:"configure,omitempty"`
		Reopen bool `json:"reopen,omitempty"`
		Fork   *struct {
			At int `json:"at"`
		} `json:"fork,omitempty"`
		Use json.RawMessage `json:"use,omitempty"`
		Doc *struct {
			Kind  string          `json:"kind"`
			Value json.RawMessage `json:"value"`
		} `json:"doc,omitempty"`
		RetireDoc string          `json:"retireDoc,omitempty"`
		Live      bool            `json:"live,omitempty"`
		Wait      string          `json:"wait,omitempty"`
		WaitHeld  string          `json:"waitHeld,omitempty"`
		Release   string          `json:"release,omitempty"`
		Abort     json.RawMessage `json:"abort,omitempty"`
	} `json:"steps"`
}

// scenarioGates are the holds of a scenario: a step waits at one until the
// scenario releases it.
type scenarioGates struct {
	mu    sync.Mutex
	gates map[string]*scenarioGate
}

type scenarioGate struct {
	reached, released chan struct{}
	reachOnce         sync.Once
}

func (g *scenarioGates) get(name string) *scenarioGate {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gates == nil {
		g.gates = map[string]*scenarioGate{}
	}
	if g.gates[name] == nil {
		g.gates[name] = &scenarioGate{reached: make(chan struct{}), released: make(chan struct{})}
	}
	return g.gates[name]
}

func (g *scenarioGates) hold(ctx context.Context, name string) error {
	gate := g.get(name)
	gate.reachOnce.Do(func() { close(gate.reached) })
	select {
	case <-gate.released:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// scenarioMessage is a message in the normalized form both runners report.
func scenarioMessage(m Message) map[string]any {
	text := func() string {
		var out strings.Builder
		for _, part := range OrderedContent(m.Content, m.Images, m.ImageOffsets) {
			if part.Image != nil {
				out.WriteString("[image]")
			} else {
				out.WriteString(part.Text)
			}
		}
		return out.String()
	}
	switch m.Role {
	case "user":
		return map[string]any{"role": "user", "text": text()}
	case "assistant":
		calls := []any{}
		for _, call := range m.ToolCalls {
			calls = append(calls, map[string]any{"id": call.ID, "name": call.Name, "args": json.RawMessage(call.Arguments)})
		}
		out := map[string]any{"role": "assistant", "text": m.Content, "thinking": m.Thinking, "calls": calls, "stop": m.StopReason}
		if m.ErrorMessage != "" {
			out["error"] = m.ErrorMessage
		}
		return out
	case "tool":
		// The text, and the images apart (see the upstream normalizer).
		return map[string]any{"role": "tool", "id": m.ToolCallID, "name": m.ToolName, "text": m.Content, "images": len(m.Images), "isError": m.IsError}
	case "system":
		sections := map[string]any{}
		for _, section := range m.Sections {
			if section.Text == nil {
				sections[section.Name] = nil
			} else {
				sections[section.Name] = *section.Text
			}
		}
		added := []string{}
		for _, tool := range m.ToolsAdded {
			added = append(added, tool.Name)
		}
		removed := append([]string{}, m.ToolsRemoved...)
		return map[string]any{"role": "system", "sections": sections, "added": added, "removed": removed}
	}
	return map[string]any{"role": m.Role}
}

// runScenario plays a scenario on this Harness.
func runScenario(t *testing.T, sc scenarioSpec) json.RawMessage {
	t.Helper()
	ctx := context.Background()
	gates := &scenarioGates{}
	var mu sync.Mutex
	requests, summaryRequests := []any{}, []any{}
	calls, summaryCalls := 0, 0
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		messages := []any{}
		for _, m := range request.Messages {
			messages = append(messages, scenarioMessage(m))
		}
		offered := []string{}
		for _, spec := range request.Tools {
			offered = append(offered, spec.Name)
		}
		thinking := request.ThinkingLevel
		if thinking == "" {
			thinking = "off"
		}
		record := map[string]any{"model": request.Model.ID, "thinking": thinking, "messages": messages, "tools": offered}
		// Summarization requests take their answers from Summaries.
		if len(request.Messages) > 0 && request.Messages[0].Role == "system" && strings.HasPrefix(SystemText(request.Messages[0]), "You are a context summarization assistant") {
			mu.Lock()
			i := summaryCalls
			summaryCalls++
			summaryRequests = append(summaryRequests, record)
			mu.Unlock()
			if i >= len(sc.Summaries) {
				return ModelResponse{}, errors.New("no scripted response")
			}
			if hold := sc.Summaries[i].Hold; hold != "" {
				if err := gates.hold(ctx, hold); err != nil {
					return ModelResponse{}, err
				}
			}
			response := ModelResponse{Content: sc.Summaries[i].Text, StopReason: "stop"}
			response.Usage = fauxUsage(request.Messages, response)
			return response, nil
		}
		mu.Lock()
		i := calls
		calls++
		requests = append(requests, record)
		mu.Unlock()
		if i >= len(sc.Model) {
			return ModelResponse{}, errors.New("no scripted response")
		}
		step := sc.Model[i]
		if step.Hold != "" {
			if err := gates.hold(ctx, step.Hold); err != nil {
				return ModelResponse{}, err
			}
		}
		if step.Error != "" {
			// A failed answer has usage too, as the faux provider's does.
			return ModelResponse{Usage: fauxUsage(request.Messages, ModelResponse{})}, errors.New(step.Error)
		}
		response := ModelResponse{Content: step.Text, Thinking: step.Thinking, StopReason: step.Stop}
		for _, call := range step.Calls {
			args := call.Args
			if len(args) == 0 {
				args = json.RawMessage(`{}`)
			}
			response.ToolCalls = append(response.ToolCalls, ToolCall{ID: call.ID, Name: call.Name, Arguments: args, ArgumentsError: call.ArgumentsError})
		}
		if response.StopReason == "" {
			response.StopReason = "stop"
			if len(response.ToolCalls) > 0 {
				response.StopReason = "toolUse"
			}
		}
		// The usage pi-ai's faux provider reports, which context estimates
		// prefer over character counts.
		response.Usage = fauxUsage(request.Messages, response)
		return response, nil
	})
	// The faux provider's models: faux-1 and the scenario's own.
	windows := map[string]int{"faux-1": 128000}
	if sc.ContextWindow > 0 {
		windows["faux-1"] = sc.ContextWindow
	}
	for _, m := range sc.Models {
		windows[m.ID] = 128000
		if m.ContextWindow > 0 {
			windows[m.ID] = m.ContextWindow
		}
	}
	var harnessModel Model = scenarioModel{generate: model, windows: windows}
	registry := NewRegistry()
	var tools []ToolDefinition
	for _, spec := range sc.Tools {
		spec := spec
		description := spec.Description
		if description == "" {
			description = "The " + spec.Name + " tool"
		}
		parameters := spec.Parameters
		if len(parameters) == 0 {
			parameters = json.RawMessage(`{"type":"object"}`)
		}
		var toolCalls int
		var toolMu sync.Mutex
		definition := ToolDefinition{ToolSpec: ToolSpec{Name: spec.Name, Description: description, Parameters: parameters}, ReplaySafe: spec.ReplaySafe, Callers: spec.Callers,
			Execute: func(ctx context.Context, args json.RawMessage, rt *ToolRuntime) (ToolResult, error) {
				toolMu.Lock()
				i := toolCalls
				toolCalls++
				toolMu.Unlock()
				if len(spec.Results) == 0 {
					return ToolResult{Content: jsStringifyRaw(args)}, nil // JSON.stringify(args)
				}
				result := spec.Results[min(i, len(spec.Results)-1)]
				if result.Hold != "" {
					if err := gates.hold(ctx, result.Hold); err != nil {
						return ToolResult{}, err
					}
				}
				if result.Throw != "" {
					return ToolResult{}, errors.New(result.Throw)
				}
				if len(result.Nested) > 0 {
					// Nested calls in order; their results, as programs read them.
					type nestedView struct {
						StructuredOutput json.RawMessage `json:"structuredOutput"`
						IsError          bool            `json:"isError"`
						Diagnostics      []string        `json:"diagnostics"`
					}
					views := []nestedView{}
					for _, call := range result.Nested {
						args := call.Args
						if len(args) == 0 {
							args = json.RawMessage(`{}`)
						}
						done, err := rt.ExecuteTool(ctx, call.Tool, args, NestedCallOptions{Key: call.Key})
						if err != nil {
							return ToolResult{}, err
						}
						view := nestedView{StructuredOutput: done.StructuredOutput, IsError: done.IsError, Diagnostics: []string{}}
						if len(view.StructuredOutput) == 0 {
							view.StructuredOutput = json.RawMessage(`null`)
						}
						for _, d := range done.Diagnostics {
							view.Diagnostics = append(view.Diagnostics, "["+d.Severity+"] "+d.Message)
						}
						views = append(views, view)
					}
					encoded, err := json.Marshal(views)
					if err != nil {
						return ToolResult{}, err
					}
					return ToolResult{Content: jsStringifyRaw(encoded)}, nil // JSON.stringify
				}
				for _, chunk := range result.Stream {
					if err := rt.Output(ctx, chunk); err != nil {
						return ToolResult{}, err
					}
				}
				if r := result.StreamRepeat; r != nil {
					for range r.Times {
						if err := rt.Output(ctx, r.Text); err != nil {
							return ToolResult{}, err
						}
					}
				}
				out := ToolResult{IsError: result.IsError, Diagnostics: result.Diagnostics, Terminate: result.Terminate, Handoff: result.Handoff, AddTools: result.AddTools, Images: result.Images}
				if result.Text != nil {
					out.Content = *result.Text
				}
				if r := result.Repeat; r != nil {
					out.Content = strings.Repeat(r.Text, r.Times)
				}
				if u := result.Usage; u != nil {
					out.Usage = Usage{InputTokens: u.Input, OutputTokens: u.Output}
				}
				return out, nil
			}}
		if spec.Sequential {
			definition.ExecutionMode = "sequential"
		}
		if l := spec.OutputLimits; l != nil {
			definition.OutputLimits = &OutputLimits{MaxBytes: l.MaxBytes, MaxLines: l.MaxLines, Retain: l.Retain}
		}
		tools = append(tools, definition)
	}
	if len(tools) > 0 {
		if err := registry.Install(Extension{Name: "scenario", Tools: tools}); err != nil {
			t.Fatal(err)
		}
	}
	// An AfterRun hook asking for one task per run: it appends a
	// scenario.after-run entry with its label and completes.
	var askedMu sync.Mutex
	var asked []RunSummary
	if spec := sc.AfterRun; spec != nil {
		task := TaskDefinition{Kind: "scenario.after-run", Version: 1, Run: func(ctx context.Context, rt *TaskRuntime, task TaskRecord) error {
			if spec.Hold != "" {
				if err := gates.hold(ctx, spec.Hold); err != nil {
					return err
				}
			}
			var input struct {
				Label string `json:"label"`
			}
			if err := json.Unmarshal(task.Input, &input); err != nil {
				return err
			}
			label, err := json.Marshal(input.Label)
			if err != nil {
				return err
			}
			return rt.Commit(ctx, func(tx *Tx) (TaskTransition, error) {
				if _, err := tx.Append(task.Conversation, Entry{Kind: "scenario.after-run", Data: label}); err != nil {
					return TaskTransition{}, err
				}
				return TaskTransition{Status: "terminal", Outcome: &Outcome{Status: OutcomeCompleted, Result: label}}, nil
			})
		}}
		if err := registry.Install(Extension{Name: "after-run", Tasks: []TaskDefinition{task}, Hooks: Hooks{
			AfterRun: func(_ context.Context, run RunSummary, _ *TaskRuntime) ([]RunEndTask, error) {
				askedMu.Lock()
				asked = append(asked, run)
				n := len(asked)
				askedMu.Unlock()
				input, err := json.Marshal(map[string]string{"label": fmt.Sprintf("run %d", n)})
				return []RunEndTask{{Kind: "scenario.after-run", Input: input}}, err
			}}}); err != nil {
			t.Fatal(err)
		}
	}
	// Declarative extensions: static sections and hook rules.
	for _, spec := range sc.Extensions {
		spec := spec
		extension := Extension{Name: spec.Name}
		for _, section := range spec.Sections {
			text := section.Text
			extension.Sections = append(extension.Sections, Section{Name: section.Key, Untagged: section.Tag != nil && !*section.Tag,
				Render: func(context.Context, PromptInput) (string, error) { return text, nil }})
		}
		if len(spec.BeforeTool) > 0 {
			extension.Hooks.BeforeTool = func(_ context.Context, call *ToolCall, _ *ToolRuntime) error {
				for _, rule := range spec.BeforeTool {
					if rule.Tool != call.Name {
						continue
					}
					if rule.Block != nil {
						return errors.New(*rule.Block)
					}
					if len(rule.Args) > 0 {
						call.Arguments = rule.Args
						return nil
					}
				}
				return nil
			}
		}
		if len(spec.AfterTool) > 0 {
			extension.Hooks.AfterTool = func(_ context.Context, result *ToolResult, rt *ToolRuntime) error {
				for _, rule := range spec.AfterTool {
					if rule.Tool == rt.Call.Name {
						result.Content = rule.Text
						return nil
					}
				}
				return nil
			}
		}
		if rule := spec.BeforeRequest; rule != nil && rule.AppendUser != nil {
			extension.Hooks.BeforeRequest = func(_ context.Context, request *ModelRequest, _ *TaskRuntime) error {
				request.Messages = append(request.Messages, Message{Role: "user", Content: *rule.AppendUser})
				return nil
			}
		}
		if rule := spec.AfterResponse; rule != nil && rule.ErrorMessage != nil {
			extension.Hooks.AfterResponse = func(_ context.Context, response *ModelResponse, _ *TaskRuntime) error {
				if response.StopReason == "error" {
					response.ErrorMessage = *rule.ErrorMessage
				}
				return nil
			}
		}
		if rule := spec.BeforeCompact; rule != nil {
			extension.Hooks.BeforeCompact = func(context.Context, CompactionRequest, *TaskRuntime) (CompactionDecision, error) {
				if rule.Decline {
					return CompactionDecision{Decline: true}, nil
				}
				if rule.Summary != nil {
					return CompactionDecision{Summary: *rule.Summary}, nil
				}
				return CompactionDecision{}, nil
			}
		}
		if len(spec.OnYield) > 0 {
			var yields sync.Mutex
			queue := append(spec.OnYield[:0:0], spec.OnYield...)
			extension.Hooks.OnYield = func(context.Context, ModelResponse, *TaskRuntime) (string, error) {
				yields.Lock()
				defer yields.Unlock()
				if len(queue) == 0 {
					return "", nil
				}
				next := queue[0]
				queue = queue[1:]
				if next.Continue == nil {
					return "", nil
				}
				return *next.Continue, nil
			}
		}
		if err := registry.Install(extension); err != nil {
			t.Fatal(err)
		}
	}
	settings := Settings{ToolExecution: sc.Settings.ToolExecution, SteeringMode: sc.Settings.SteeringMode, FollowUpMode: sc.Settings.FollowUpMode, AfterFailedRun: sc.Settings.AfterFailedRun}
	if sc.Settings.Compaction != nil {
		settings.Compaction = *sc.Settings.Compaction
	}
	if r := sc.Settings.Retry; r != nil {
		settings.Retry = RetrySettings{Enabled: r.Enabled, MaxRetries: r.MaxRetries, BaseDelayMs: r.BaseDelayMs, MaxAgentDelayMs: r.MaxAgentDelayMs}
	}
	// Storage that outlives its Harness, for reopen steps.
	store := reopenable{NewMemoryStorage()}
	var h *Harness
	var root *Conversation
	open := func() {
		var err error
		h, err = Open(ctx, store, HarnessOptions{Model: harnessModel, Registry: registry, Settings: settings})
		if err != nil {
			t.Fatal(err)
		}
		root, err = h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "faux", ID: "faux-1"}, MaxTurns: sc.Agent.MaxTurns, Instructions: sc.Agent.Instructions, Tools: sc.Agent.Tools}})
		if err != nil {
			t.Fatal(err)
		}
	}
	open()
	defer func() { _ = h.Close(ctx) }()
	handle := func(id SubmissionID) *Submission {
		submission, err := h.Submission(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return submission
	}
	type submitted struct {
		submission *Submission
		refused    string
	}
	var submissions []submitted
	var forks []*Conversation
	current := root
	var lives []any
	// Conversation documents, version 1, starting empty.
	docs := map[string]DocumentDefinition{}
	var kinds []string
	for _, d := range sc.Documents {
		docs[d.Kind] = DocumentDefinition{Kind: d.Kind, Version: 1, Scope: "conversation", History: d.History, Fork: d.Fork}
		kinds = append(kinds, d.Kind)
	}
	for _, step := range sc.Steps {
		switch {
		case step.Reopen:
			if len(forks) > 0 {
				t.Fatal("reopening with forks is not supported")
			}
			if err := h.Close(ctx); err != nil {
				t.Fatal(err)
			}
			open()
			current = root
		case step.Fork != nil:
			// At an entry of the current conversation, by index (negative:
			// from the end); the fork becomes current.
			entries, err := current.Entries(ctx)
			if err != nil {
				t.Fatal(err)
			}
			index := step.Fork.At
			if index < 0 {
				index += len(entries)
			}
			fork, err := current.Fork(ctx, entries[index].ID, ConversationOptions{})
			if err != nil {
				t.Fatal(err)
			}
			forks = append(forks, fork)
			current = fork
		case step.Live:
			live, err := ReadDocument[LiveState](ctx, h, liveDocument, uint64(current.ID()))
			if err != nil && !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
			lives = append(lives, scenarioLive(live))
		case step.Doc != nil:
			def, value := docs[step.Doc.Kind], step.Doc.Value
			if err := h.Commit(ctx, func(tx *Tx) error { return tx.PutDocument(def, uint64(current.ID()), value) }); err != nil {
				t.Fatal(err)
			}
		case step.RetireDoc != "":
			def := docs[step.RetireDoc]
			if err := h.Commit(ctx, func(tx *Tx) error { return tx.RetireDocument(def, uint64(current.ID())) }); err != nil {
				t.Fatal(err)
			}
		case len(step.Use) > 0:
			var which any
			if err := json.Unmarshal(step.Use, &which); err != nil {
				t.Fatal(err)
			}
			if n, ok := which.(float64); ok {
				current = forks[int(n)]
			} else {
				current = root
			}
		case step.Submit != nil:
			request := SubmissionRequest{Type: step.Submit.Type, WhenBusy: step.Submit.WhenBusy, WhenIdle: step.Submit.WhenIdle}
			if raw := step.Submit.Content; len(raw) > 0 {
				var blocks []struct {
					Type     string `json:"type"`
					Text     string `json:"text"`
					Data     string `json:"data"`
					MimeType string `json:"mimeType"`
				}
				if err := json.Unmarshal(raw, &request.Content); err != nil {
					if err := json.Unmarshal(raw, &blocks); err != nil {
						t.Fatal(err)
					}
				}
				// Blocks: the text, and the images placed at their offsets.
				for _, block := range blocks {
					if block.Type == "image" {
						request.Images = append(request.Images, ImageContent{Data: block.Data, MimeType: block.MimeType})
						request.ImageOffsets = append(request.ImageOffsets, len(request.Content))
					} else {
						request.Content += block.Text
					}
				}
			}
			if e := step.Submit.Entry; e != nil {
				request.Entry = &Entry{Kind: e.Kind, Data: e.Data}
			}
			submission, err := current.Submit(ctx, request)
			switch {
			case errors.Is(err, ErrBusy):
				submissions = append(submissions, submitted{refused: "ConversationBusy"})
			case errors.Is(err, ErrIdle):
				submissions = append(submissions, submitted{refused: "ConversationIdle"})
			case err != nil:
				t.Fatalf("submit: %v", err)
			default:
				submissions = append(submissions, submitted{submission: submission})
			}
		case step.Wait == "idle":
			if err := current.WaitForIdle(ctx); err != nil {
				t.Fatal(err)
			}
		case step.Wait == "retrying":
			deadline := time.Now().Add(10 * time.Second)
			for {
				live, err := ReadDocument[LiveState](ctx, h, liveDocument, uint64(current.ID()))
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Fatal(err)
				}
				if live.Generation != nil && live.Generation.Retry != nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("no retry")
				}
				time.Sleep(5 * time.Millisecond)
			}
		case step.Wait == "compactions":
			deadline := time.Now().Add(10 * time.Second)
			for {
				live, err := ReadDocument[LiveState](ctx, h, liveDocument, uint64(current.ID()))
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Fatal(err)
				}
				if len(live.Compactions) == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("compactions never ended")
				}
				time.Sleep(5 * time.Millisecond)
			}
		case step.Wait == "settled":
			for _, s := range submissions {
				if s.submission != nil {
					if _, err := handle(s.submission.ID()).Wait(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
		case step.WaitHeld != "":
			select {
			case <-gates.get(step.WaitHeld).reached:
			case <-time.After(10 * time.Second):
				t.Fatalf("hold %s was never reached", step.WaitHeld)
			}
		case step.Release != "":
			close(gates.get(step.Release).released)
		case len(step.Abort) > 0:
			if err := current.Abort(ctx, string(step.Abort) == `"background"`); err != nil {
				t.Fatal(err)
			}
		case step.Configure != nil:
			change := AgentChange{Instructions: step.Configure.Instructions, MaxTurns: step.Configure.MaxTurns, ThinkingLevel: step.Configure.ThinkingLevel}
			if step.Configure.Model != nil {
				change.Model = &ModelRef{Provider: "faux", ID: *step.Configure.Model}
			}
			if err := current.ChangeAgent(ctx, change); err != nil {
				t.Fatal(err)
			}
		case len(step.Compact) > 0:
			var instructions string
			_ = json.Unmarshal(step.Compact, &instructions)
			if _, err := current.Compact(ctx, instructions); err != nil {
				t.Fatal(err)
			}
		case len(step.Reset) > 0:
			var note string
			_ = json.Unmarshal(step.Reset, &note)
			if _, err := current.Reset(ctx, note); err != nil {
				t.Fatal(err)
			}
		}
	}
	entriesOf := func(c *Conversation) []Entry {
		if err := c.WaitForIdle(ctx); err != nil {
			t.Fatal(err)
		}
		entries, err := c.Entries(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return entries
	}
	// Entries as kinds and model messages, and the data of non-pi entries.
	transcriptOf := func(c *Conversation) []any {
		transcript := []any{}
		for _, e := range entriesOf(c) {
			messages := []any{}
			if e.Message != nil {
				messages = append(messages, scenarioMessage(*e.Message))
			}
			entry := map[string]any{"kind": e.Kind, "messages": messages}
			if !strings.HasPrefix(e.Kind, "pi.") && len(e.Data) > 0 {
				entry["data"] = e.Data
			}
			transcript = append(transcript, entry)
		}
		return transcript
	}
	transcript := transcriptOf(root)
	// AfterRun summaries, their entries as root transcript positions.
	rootEntries := entriesOf(root)
	position := func(id EntryID) int {
		for i, e := range rootEntries {
			if e.ID == id {
				return i
			}
		}
		return -1
	}
	afterRun := []any{}
	askedMu.Lock()
	for _, run := range asked {
		afterRun = append(afterRun, map[string]any{"reason": run.Reason, "first": position(run.First), "answer": position(run.Answer), "last": position(run.Last)})
	}
	askedMu.Unlock()
	// The root's usage ledger: tokens per model and per tool.
	ledger, err := ReadDocument[UsageState](ctx, h, usageDocument, uint64(root.ID()))
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	tokens := func(bucket map[string]Usage) map[string]int64 {
		out := map[string]int64{}
		for key, u := range bucket {
			out[key] = u.InputTokens + u.OutputTokens + u.CacheReadTokens + u.CacheWriteTokens
		}
		return out
	}
	usage := map[string]any{"models": tokens(ledger.Models), "tools": tokens(ledger.Tools)}
	// Each conversation's documents, the root first; null when absent.
	var documents []any
	for _, c := range append([]*Conversation{root}, forks...) {
		values := map[string]any{}
		for _, kind := range kinds {
			value, err := ReadDocument[json.RawMessage](ctx, h, docs[kind], uint64(c.ID()))
			switch {
			case errors.Is(err, ErrNotFound):
				values[kind] = nil
			case err != nil:
				t.Fatal(err)
			default:
				values[kind] = value
			}
		}
		documents = append(documents, values)
	}
	var forked []any
	for _, fork := range forks {
		forked = append(forked, transcriptOf(fork))
	}
	settled := []any{}
	for _, s := range submissions {
		if s.submission == nil {
			settled = append(settled, map[string]any{"refused": s.refused})
			continue
		}
		record, err := handle(s.submission.ID()).Record(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]any{"type": record.Request.Type, "status": record.Status}
		if record.Reason != "" {
			out["reason"] = record.Reason
		}
		settled = append(settled, out)
	}
	mu.Lock()
	defer mu.Unlock()
	report := map[string]any{"requests": requests, "summaryRequests": summaryRequests, "transcript": transcript, "usage": usage, "submissions": settled}
	if len(forks) > 0 {
		report["forks"] = forked
	}
	if len(docs) > 0 {
		report["documents"] = documents
	}
	if len(lives) > 0 {
		report["live"] = lives
	}
	if sc.AfterRun != nil {
		report["afterRun"] = afterRun
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestCrosscheckScenarios(t *testing.T) {
	var names []string
	crosscheck.Cases(t, "scenarios/index.json", &names)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			var raw json.RawMessage
			crosscheck.Cases(t, "scenarios/"+name+".json", &raw)
			var sc scenarioSpec
			if err := json.Unmarshal(raw, &sc); err != nil {
				t.Fatal(err)
			}
			var want json.RawMessage
			crosscheck.Run(t, "scenario", raw, &want)
			got := runScenario(t, sc)
			got, want = roundsInCallOrder(t, got), roundsInCallOrder(t, want)
			if !crosscheck.Same(got, want) {
				t.Errorf("differs from upstream:\n%s", crosscheck.Diff(want, got))
			}
		})
	}
}

// roundsInCallOrder puts each run of tool result entries in the order of the
// calls before it: parallel calls append their results as they finish, in
// either runtime, and the model sees them in call order anyway.
func roundsInCallOrder(t *testing.T, raw json.RawMessage) json.RawMessage {
	t.Helper()
	// Every field is kept; the transcripts' rounds are sorted.
	var all map[string]json.RawMessage
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatal(err)
	}
	all["transcript"] = sortedRounds(t, all["transcript"])
	if forks, ok := all["forks"]; ok {
		var each []json.RawMessage
		if err := json.Unmarshal(forks, &each); err != nil {
			t.Fatal(err)
		}
		for i := range each {
			each[i] = sortedRounds(t, each[i])
		}
		data, err := json.Marshal(each)
		if err != nil {
			t.Fatal(err)
		}
		all["forks"] = data
	}
	data, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// sortedRounds puts each run of tool-result entries in a transcript in its
// assistant's call order.
func sortedRounds(t *testing.T, raw json.RawMessage) json.RawMessage {
	t.Helper()
	var transcript []struct {
		Kind     string            `json:"kind"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(raw, &transcript); err != nil {
		t.Fatal(err)
	}
	position := map[string]int{}
	for i := 0; i < len(transcript); i++ {
		entry := transcript[i]
		if entry.Kind == "pi.assistant" && len(entry.Messages) > 0 {
			var assistant struct {
				Calls []struct {
					ID string `json:"id"`
				} `json:"calls"`
			}
			_ = json.Unmarshal(entry.Messages[0], &assistant)
			position = map[string]int{}
			for p, call := range assistant.Calls {
				position[call.ID] = p
			}
			continue
		}
		if entry.Kind != "pi.tool-result" {
			continue
		}
		end := i
		for end < len(transcript) && transcript[end].Kind == "pi.tool-result" {
			end++
		}
		id := func(k int) int {
			var result struct {
				ID string `json:"id"`
			}
			if len(transcript[k].Messages) > 0 {
				_ = json.Unmarshal(transcript[k].Messages[0], &result)
			}
			return position[result.ID]
		}
		run := transcript[i:end]
		sort.SliceStable(run, func(a, b int) bool { return id(i+a) < id(i+b) })
		i = end - 1
	}
	data, err := json.Marshal(transcript)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// scenarioModel is a scripted model with a context window, for threshold
// compaction.
type scenarioModel struct {
	generate taskModelFunc
	windows  map[string]int
}

func (m scenarioModel) Generate(ctx context.Context, request ModelRequest, delta func(string) error) (ModelResponse, error) {
	return m.generate(ctx, request, delta)
}

func (m scenarioModel) ContextWindow(ref ModelRef) int { return m.windows[ref.ID] }

// HasModel: the faux provider registers only its models.
func (m scenarioModel) HasModel(ref ModelRef) bool {
	_, ok := m.windows[ref.ID]
	return ref.Provider == "faux" && ok
}

// fauxUsage is the usage pi-ai's faux provider reports for a request and its
// answer (withUsageEstimate): the prompt's characters, each message as
// "role:text" joined by blank lines, and the answer's, over four. Only the
// total matters to context estimates.
func fauxUsage(messages []Message, response ModelResponse) Usage {
	quarter := func(n int) int64 { return int64((n + 3) / 4) }
	length := 0
	for i, m := range messages {
		role := m.Role
		if role == "tool" {
			role = "toolResult"
		}
		length += utf16Len(role + ":" + fauxText(m))
		if i > 0 {
			length += 2
		}
	}
	answer := Message{Role: "assistant", Thinking: response.Thinking, Content: response.Content, ToolCalls: response.ToolCalls}
	return Usage{InputTokens: quarter(length), OutputTokens: quarter(utf16Len(fauxText(answer)))}
}

// fauxText is a message as the faux provider writes it into its prompt.
func fauxText(m Message) string {
	blocks := func() []string {
		var out []string
		for _, part := range OrderedContent(m.Content, m.Images, m.ImageOffsets) {
			if part.Image != nil {
				out = append(out, fmt.Sprintf("[image:%s:%d]", part.Image.MimeType, len(part.Image.Data)))
			} else if part.Text != "" {
				out = append(out, part.Text)
			}
		}
		return out
	}
	switch m.Role {
	case "system":
		parts := []string{SystemText(m)}
		for _, name := range m.ToolsRemoved {
			parts = append(parts, "tool-:"+jsStringifyRaw(json.RawMessage(fmt.Sprintf(`{"name":%q}`, name))))
		}
		for _, tool := range m.ToolsAdded {
			decl, _ := json.Marshal(map[string]any{"name": tool.Name, "description": tool.Description, "parameters": json.RawMessage(tool.Parameters)})
			parts = append(parts, "tool+:"+jsStringifyRaw(decl))
		}
		var kept []string
		for _, part := range parts {
			if part != "" {
				kept = append(kept, part)
			}
		}
		return strings.Join(kept, "\n")
	case "user":
		return strings.Join(blocks(), "\n")
	case "assistant":
		var parts []string
		if m.Thinking != "" {
			parts = append(parts, m.Thinking)
		}
		if m.Content != "" {
			parts = append(parts, m.Content)
		}
		for _, call := range m.ToolCalls {
			parts = append(parts, call.Name+":"+jsStringifyRaw(call.Arguments))
		}
		return strings.Join(parts, "\n")
	case "tool":
		return strings.Join(append([]string{m.ToolName}, blocks()...), "\n")
	}
	return m.Content
}

// scenarioLive is pi.live without IDs and times, as the upstream runner's
// normalizeLive gives it: counts and presence instead.
func scenarioLive(live LiveState) map[string]any {
	out := map[string]any{}
	slot := func(callID, name, status, output string, droppedBytes, droppedLines int, diagnostics []ToolDiagnostic) map[string]any {
		s := map[string]any{"callId": callID, "name": name, "status": status}
		if output != "" {
			s["output"] = output
		}
		if droppedBytes > 0 {
			s["droppedBytes"], s["droppedLines"] = droppedBytes, droppedLines
		}
		if len(diagnostics) > 0 {
			var notes []string
			for _, d := range diagnostics {
				notes = append(notes, "["+d.Severity+"] "+d.Message)
			}
			s["diagnostics"] = notes
		}
		return s
	}
	if r := live.Run; r != nil {
		out["run"] = map[string]any{"inputs": len(r.Inputs), "turns": r.Turns, "first": r.First != 0, "afterRun": r.AfterRun != nil}
	}
	if g := live.Generation; g != nil {
		gen := map[string]any{"attempt": g.Attempt}
		if g.Message != nil {
			gen["message"] = scenarioMessage(*g.Message)
		}
		if g.Retry != nil {
			gen["retry"] = g.Retry.Error
		}
		if g.Deferred != nil {
			gen["deferred"] = true
		}
		out["generation"] = gen
	}
	if len(live.Tools) > 0 {
		var tools []any
		for _, t := range live.Tools {
			s := slot(t.CallID, t.Name, t.Status, t.Output, t.DroppedBytes, t.DroppedLines, t.Diagnostics)
			s["task"], s["entry"] = t.TaskID != 0, t.Entry != 0
			if t.Unoffered {
				s["unoffered"] = true
			}
			tools = append(tools, s)
		}
		out["tools"] = tools
	}
	if len(live.NestedTools) > 0 {
		var nested []any
		for _, t := range live.NestedTools {
			s := slot(t.CallID, t.Name, t.Status, t.Output, t.DroppedBytes, t.DroppedLines, t.Diagnostics)
			s["parentCallId"], s["arguments"] = t.ParentCallID, t.Arguments
			if t.Summary != nil {
				summary := map[string]any{"isError": t.Summary.IsError}
				if t.Summary.Error != "" {
					summary["error"] = t.Summary.Error
				}
				s["summary"] = summary
			}
			nested = append(nested, s)
		}
		out["nestedTools"] = nested
	}
	if len(live.Compactions) > 0 {
		var compactions []any
		for _, c := range live.Compactions {
			compactions = append(compactions, map[string]any{"trigger": c.Trigger, "blocking": c.Blocking, "attempt": c.Attempt, "retry": c.Retry != nil})
		}
		out["compactions"] = compactions
	}
	if f := live.CompactionFailure; f != nil {
		out["compactionFailure"] = map[string]any{"trigger": f.Trigger, "error": f.Error}
	}
	if f := live.RunFailure; f != nil {
		failure := map[string]any{"inputs": len(f.Inputs), "reason": f.Reason, "first": f.First != 0}
		if f.Detail != "" {
			failure["detail"] = f.Detail
		}
		out["runFailure"] = failure
	}
	return out
}
