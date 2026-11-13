//go:build crosscheck

package durable

import (
	"context"
	"encoding/json"
	"errors"
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
	Model         []struct {
		Text     string `json:"text,omitempty"`
		Thinking string `json:"thinking,omitempty"`
		Stop     string `json:"stop,omitempty"`
		Error    string `json:"error,omitempty"`
		Hold     string `json:"hold,omitempty"`
		Calls    []struct {
			ID   string          `json:"id"`
			Name string          `json:"name"`
			Args json.RawMessage `json:"args,omitempty"`
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
		} `json:"results,omitempty"`
	} `json:"tools,omitempty"`
	Settings struct {
		Retry *struct {
			Enabled         *bool    `json:"enabled,omitempty"`
			MaxRetries      *float64 `json:"maxRetries,omitempty"`
			BaseDelayMs     *float64 `json:"baseDelayMs,omitempty"`
			MaxAgentDelayMs *float64 `json:"maxAgentDelayMs,omitempty"`
		} `json:"retry,omitempty"`
		ToolExecution string `json:"toolExecution,omitempty"`
		SteeringMode  string `json:"steeringMode,omitempty"`
		FollowUpMode  string `json:"followUpMode,omitempty"`
	} `json:"settings"`
	Agent struct {
		MaxTurns     int     `json:"maxTurns,omitempty"`
		Instructions *string `json:"instructions,omitempty"`
	} `json:"agent"`
	Steps []struct {
		Submit *struct {
			Type     string `json:"type"`
			Content  string `json:"content,omitempty"`
			WhenBusy string `json:"whenBusy,omitempty"`
			WhenIdle string `json:"whenIdle,omitempty"`
		} `json:"submit,omitempty"`
		Wait     string          `json:"wait,omitempty"`
		WaitHeld string          `json:"waitHeld,omitempty"`
		Release  string          `json:"release,omitempty"`
		Abort    json.RawMessage `json:"abort,omitempty"`
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
		return map[string]any{"role": "tool", "id": m.ToolCallID, "name": m.ToolName, "text": text(), "isError": m.IsError}
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
	requests := []any{}
	calls := 0
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		i := calls
		calls++
		messages := []any{}
		for _, m := range request.Messages {
			messages = append(messages, scenarioMessage(m))
		}
		requests = append(requests, messages)
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
			return ModelResponse{}, errors.New(step.Error)
		}
		response := ModelResponse{Content: step.Text, Thinking: step.Thinking, StopReason: step.Stop}
		for _, call := range step.Calls {
			args := call.Args
			if len(args) == 0 {
				args = json.RawMessage(`{}`)
			}
			response.ToolCalls = append(response.ToolCalls, ToolCall{ID: call.ID, Name: call.Name, Arguments: args})
		}
		if response.StopReason == "" {
			response.StopReason = "stop"
			if len(response.ToolCalls) > 0 {
				response.StopReason = "toolUse"
			}
		}
		return response, nil
	})
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
		definition := ToolDefinition{ToolSpec: ToolSpec{Name: spec.Name, Description: description, Parameters: parameters}, ReplaySafe: spec.ReplaySafe,
			Execute: func(ctx context.Context, args json.RawMessage, _ *ToolRuntime) (ToolResult, error) {
				toolMu.Lock()
				i := toolCalls
				toolCalls++
				toolMu.Unlock()
				if len(spec.Results) == 0 {
					return ToolResult{Content: string(args)}, nil
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
				out := ToolResult{IsError: result.IsError}
				if result.Text != nil {
					out.Content = *result.Text
				}
				return out, nil
			}}
		if spec.Sequential {
			definition.ExecutionMode = "sequential"
		}
		tools = append(tools, definition)
	}
	if len(tools) > 0 {
		if err := registry.Install(Extension{Name: "scenario", Tools: tools}); err != nil {
			t.Fatal(err)
		}
	}
	settings := Settings{ToolExecution: sc.Settings.ToolExecution, SteeringMode: sc.Settings.SteeringMode, FollowUpMode: sc.Settings.FollowUpMode}
	if r := sc.Settings.Retry; r != nil {
		settings.Retry = RetrySettings{Enabled: r.Enabled, MaxRetries: r.MaxRetries, BaseDelayMs: r.BaseDelayMs, MaxAgentDelayMs: r.MaxAgentDelayMs}
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{Model: model, Registry: registry, Settings: settings})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(ctx)
	root, err := h.Root(ctx, ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "faux", ID: "faux-1"}, MaxTurns: sc.Agent.MaxTurns, Instructions: sc.Agent.Instructions}})
	if err != nil {
		t.Fatal(err)
	}
	type submitted struct {
		submission *Submission
		refused    string
	}
	var submissions []submitted
	for _, step := range sc.Steps {
		switch {
		case step.Submit != nil:
			submission, err := root.Submit(ctx, SubmissionRequest{Type: step.Submit.Type, Content: step.Submit.Content, WhenBusy: step.Submit.WhenBusy, WhenIdle: step.Submit.WhenIdle})
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
			if err := root.WaitForIdle(ctx); err != nil {
				t.Fatal(err)
			}
		case step.Wait == "settled":
			for _, s := range submissions {
				if s.submission != nil {
					if _, err := s.submission.Wait(ctx); err != nil {
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
			if err := root.Abort(ctx, string(step.Abort) == `"background"`); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := root.WaitForIdle(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	transcript := []any{}
	for _, e := range entries {
		messages := []any{}
		if e.Message != nil {
			messages = append(messages, scenarioMessage(*e.Message))
		}
		transcript = append(transcript, map[string]any{"kind": e.Kind, "messages": messages})
	}
	settled := []any{}
	for _, s := range submissions {
		if s.submission == nil {
			settled = append(settled, map[string]any{"refused": s.refused})
			continue
		}
		record, err := s.submission.Record(ctx)
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
	data, err := json.Marshal(map[string]any{"requests": requests, "transcript": transcript, "submissions": settled})
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
	var out struct {
		Requests    json.RawMessage `json:"requests"`
		Submissions json.RawMessage `json:"submissions"`
		Transcript  []struct {
			Kind     string            `json:"kind"`
			Messages []json.RawMessage `json:"messages"`
		} `json:"transcript"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	position := map[string]int{}
	for i := 0; i < len(out.Transcript); i++ {
		entry := out.Transcript[i]
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
		for end < len(out.Transcript) && out.Transcript[end].Kind == "pi.tool-result" {
			end++
		}
		id := func(k int) int {
			var result struct {
				ID string `json:"id"`
			}
			if len(out.Transcript[k].Messages) > 0 {
				_ = json.Unmarshal(out.Transcript[k].Messages[0], &result)
			}
			return position[result.ID]
		}
		run := out.Transcript[i:end]
		sort.SliceStable(run, func(a, b int) bool { return id(i+a) < id(i+b) })
		i = end - 1
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
