package durable

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// Expectations below are what pi-schema 1.5.2's parse() and pi-ai's
// validateToolArguments return for the same schema and arguments.

func TestToolArgumentsAreCoercedAsPiAi(t *testing.T) {
	for _, c := range []struct{ schema, args, want string }{
		{`{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":"42"}`, `{"n":42}`},
		{`{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":"4.7"}`, `{"n":4}`},
		{`{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":true}`, `{"n":1}`},
		{`{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":"0x10"}`, `{"n":16}`},
		{`{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":" 7 "}`, `{"n":7}`},
		{`{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}}}`, `{"tags":"a"}`, `{"tags":["a"]}`},
		{`{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}}}`, `{"tags":["a",1,true,null]}`, `{"tags":["a","1","true","null"]}`},
		{`{"type":"object","properties":{"f":{"type":"boolean"}}}`, `{"f":"0"}`, `{"f":false}`},
		{`{"type":"object","properties":{"opt":{"type":"string"},"req":{"type":"string"}},"required":["req"]}`, `{"opt":null,"req":"x"}`, `{"req":"x"}`},
		{`{"type":"object","properties":{"mode":{"enum":["fast","slow",1,true]}}}`, `{"mode":"1"}`, `{"mode":1}`},
		{`{"type":"object","properties":{"c":{"const":3}}}`, `{"c":"3.0"}`, `{"c":3}`},
		{`{"type":"object","properties":{"v":{"anyOf":[{"type":"number"},{"type":"array","items":{"type":"number"}}]}}}`, `{"v":["1","2"]}`, `{"v":[1,2]}`},
		{`{"type":"object","properties":{"v":{"type":["integer","null"]}}}`, `{"v":""}`, `{"v":null}`},
		{`{"$defs":{"item":{"type":"object","properties":{"id":{"type":"integer"}}}},"type":"object","properties":{"items":{"type":"array","items":{"$ref":"#/$defs/item"}}}}`, `{"items":{"id":"6"}}`, `{"items":[{"id":6}]}`},
		{`{"type":"object","properties":{"s":{"type":"string"}}}`, `{"s":1e21}`, `{"s":"1e+21"}`},
		{`{"type":"object","properties":{"s":{"type":"string"}}}`, `{"s":1e20}`, `{"s":"100000000000000000000"}`},
		{`{"type":"object","properties":{"s":{"type":"string"}}}`, `{"s":0.000001}`, `{"s":"0.000001"}`},
		{`{"type":"object","properties":{"s":{"type":"string"}}}`, `{"s":1e-7}`, `{"s":"1e-7"}`},
		{`{"type":"object","properties":{"t":{"type":"array","prefixItems":[{"type":"integer"},{"type":"string"}],"items":{"type":"boolean"}}}}`, `{"t":["1",2,"true"]}`, `{"t":[1,"2",true]}`},
		{`{"type":"object","patternProperties":{"^x_":{"type":"integer"}},"additionalProperties":{"type":"boolean"}}`, `{"x_a":"2","other":"true"}`, `{"x_a":2,"other":true}`},
		{`{"$defs":{"node":{"type":"object","properties":{"value":{"type":"integer"},"children":{"type":"array","items":{"$ref":"#/$defs/node"}}}}},"$ref":"#/$defs/node"}`, `{"value":"1","children":[{"value":"2","children":{"value":"3"}}]}`, `{"value":1,"children":[{"value":2,"children":[{"value":3}]}]}`},
		{`{"type":"object","properties":{"v":{"type":["string","number"]}}}`, `{"v":true}`, `{"v":"true"}`},
		{`{"type":"object","properties":{"n":{"type":"number"}}}`, `{"n":"9007199254740993"}`, `{"n":9007199254740992}`},
		// A $ref that names nothing rejects only values at its place.
		{`{"type":"object","properties":{"a":{"$ref":"#/$defs/missing"},"b":{"type":"string"}}}`, `{"b":1}`, `{"b":"1"}`},
	} {
		got, err := validateToolArguments(ToolSpec{Name: "t", Parameters: json.RawMessage(c.schema)}, json.RawMessage(c.args))
		if err != nil {
			t.Errorf("%s with %s: %v", c.schema, c.args, err)
			continue
		}
		var a, b any
		_ = json.Unmarshal(got, &a)
		_ = json.Unmarshal([]byte(c.want), &b)
		if !reflect.DeepEqual(a, b) {
			t.Errorf("%s with %s: got %s, want %s", c.schema, c.args, got, c.want)
		}
	}
	for _, c := range []struct{ schema, args string }{
		{`{"type":"object","properties":{"n":{"type":"number"}}}`, `{"n":"-0x10"}`},
		{`{"type":"object","properties":{"n":{"type":"number"}}}`, `{"n":"1_000"}`},
		{`{"type":"object","properties":{"a":{"$ref":"#/$defs/missing"}}}`, `{"a":1}`},
		{`{"type":"object","properties":{"list":{"type":"array","items":false}}}`, `{"list":[1]}`},
	} {
		if got, err := validateToolArguments(ToolSpec{Name: "t", Parameters: json.RawMessage(c.schema)}, json.RawMessage(c.args)); err == nil {
			t.Errorf("%s accepted %s as %s", c.schema, c.args, got)
		}
	}
	// Unchanged arguments keep their bytes, key order included.
	same := json.RawMessage(`{"b":"x","a":1}`)
	if got, err := validateToolArguments(ToolSpec{Name: "t", Parameters: json.RawMessage(`{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}`)}, same); err != nil || string(got) != string(same) {
		t.Fatalf("got %s error=%v", got, err)
	}
}

func TestToolArgumentFailuresAreWordedAsPiAi(t *testing.T) {
	shape := `{"type":"object","properties":{"shape":{"oneOf":[{"type":"object","properties":{"kind":{"const":"circle"},"r":{"type":"number"}},"required":["kind","r"]},{"type":"object","properties":{"kind":{"const":"square"},"side":{"type":"number"}},"required":["kind","side"]}]}}}`
	count := `{"type":"object","properties":{"n":{"type":"integer","minimum":1},"tags":{"type":"array","items":{"type":"string"}}},"required":["n"],"additionalProperties":false}`
	for _, c := range []struct{ schema, args, want string }{
		{count, `{"n":0}`, "Validation failed for tool \"t\":\n  - n: expected >= 1\n\nReceived arguments:\n{\n  \"n\": 0\n}"},
		{count, `{"tags":["a"]}`, "  - n: missing"},
		{count, `{"n":2,"other":true}`, "  - other: unknown field"},
		{count, `{"n":2,"tags":[{"x":1}]}`, "  - tags.0: expected string, got object"},
		{`{"type":"object","properties":{"m":{"enum":["a","b"]}}}`, `{"m":"c"}`, `  - m: expected one of ["a","b"]`},
		{`{"type":"object","properties":{"u":{"anyOf":[{"type":"string"},{"type":"integer"}]}}}`, `{"u":[1]}`, "  - u: expected string, got array"},
		{`{"type":"object","properties":{"o":{"oneOf":[{"type":"integer"},{"type":"number"}]}}}`, `{"o":5}`, "  - o: matches more than one alternative"},
		{`{"type":"object","properties":{"t":{"type":["string","null"]}}}`, `{"t":{"a":1}}`, "  - t: expected string or null, got object"},
		{`{"type":"object","properties":{"n":{"type":"number","maximum":2.5,"multipleOf":0.5}}}`, `{"n":3.2}`, "  - n: expected <= 2.5\n  - n: expected a multiple of 0.5"},
		{shape, `{"shape":{"kind":"square","side":"big"}}`, "  - shape.side: expected number, got string"},
		{shape, `{"shape":{"kind":"triangle"}}`, `  - shape.kind: expected one of ["circle","square"]`},
		{`{"type":"object","properties":{"v":{"anyOf":[{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"]},{"type":"object","properties":{"c":{"type":"string"}},"required":["c"]}]}}}`, `{"v":{"a":1,"b":"x"}}`, "  - v.b: expected integer, got string"},
	} {
		_, err := validateToolArguments(ToolSpec{Name: "t", Parameters: json.RawMessage(c.schema)}, json.RawMessage(c.args))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s with %s: error=%v, want %q", c.schema, c.args, err, c.want)
		}
	}
}

func TestToolsRunWithRepairedAndCoercedArguments(t *testing.T) {
	var mu sync.Mutex
	var received []string
	tool := ToolDefinition{
		ToolSpec: ToolSpec{Name: "sum", Parameters: json.RawMessage(`{"type":"object","properties":{"values":{"type":"array","items":{"type":"integer"}}},"required":["values"]}`)},
		// A JSON string where the array belongs.
		PrepareArguments: func(args json.RawMessage) (json.RawMessage, error) {
			var raw map[string]any
			if err := json.Unmarshal(args, &raw); err != nil {
				return nil, err
			}
			if text, ok := raw["values"].(string); ok {
				if text == "broken" {
					return nil, errors.New("cannot repair values")
				}
				var values any
				if err := json.Unmarshal([]byte(text), &values); err == nil {
					raw["values"] = values
				}
			}
			return json.Marshal(raw)
		},
		Execute: func(_ context.Context, args json.RawMessage, _ *ToolRuntime) (ToolResult, error) {
			mu.Lock()
			received = append(received, string(args))
			mu.Unlock()
			return ToolResult{Content: "ok"}, nil
		},
	}
	for _, c := range []struct{ arguments, executed, error string }{
		{`{"values":"[\"1\",2]"}`, `{"values":[1,2]}`, ""},
		{`{"values":"broken"}`, "", "cannot repair values"},
		{`{"values":["x"]}`, "", "values.0: expected integer, got string"},
	} {
		mu.Lock()
		received = nil
		mu.Unlock()
		h, root := toolHarnessFor(t, tool, json.RawMessage(c.arguments))
		waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
		mu.Lock()
		got := append([]string(nil), received...)
		mu.Unlock()
		result := lastToolResult(t, h, root.ID())
		if c.executed != "" {
			if len(got) != 1 || got[0] != c.executed {
				t.Fatalf("%s: executed with %v", c.arguments, got)
			}
			continue
		}
		if len(got) != 0 || !strings.Contains(result.Content, c.error) {
			t.Fatalf("%s: executed=%v result=%q", c.arguments, got, result.Content)
		}
	}
}

// toolHarnessFor runs a Harness whose model calls tool once with arguments,
// then answers.
func toolHarnessFor(t *testing.T, tool ToolDefinition, arguments json.RawMessage) (*Harness, *Conversation) {
	t.Helper()
	model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
		if !hasToolResult(request) {
			return ModelResponse{ToolCalls: []ToolCall{{ID: "call-1", Name: tool.Name, Arguments: arguments}}}, nil
		}
		return ModelResponse{Content: "done"}, nil
	})
	return runHarness(t, model, Settings{}, Extension{Name: "tools", Tools: []ToolDefinition{tool}})
}

// lastToolResult is a conversation's newest tool result message.
func lastToolResult(t *testing.T, h *Harness, cid ConversationID) Message {
	t.Helper()
	state, err := h.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var newest *Entry
	for _, e := range state.Entries {
		if e.Conversation == cid && e.Kind == "pi.tool-result" && (newest == nil || e.ID > newest.ID) {
			newest = e
		}
	}
	if newest == nil || newest.Message == nil {
		t.Fatal("no tool result")
	}
	return *newest.Message
}

func TestModelCallsWithBrokenStructuredOutputStand(t *testing.T) {
	reports := make(chan Report, 8)
	for _, c := range []struct {
		schema json.RawMessage
		output json.RawMessage
		want   string
	}{
		{json.RawMessage(`{"type":"object","properties":{"n":{"type":"number"}},"required":["n"]}`), json.RawMessage(`{"n":"seven"}`), "Tool typed returned structuredOutput that does not match its schema: n: expected number, got string"},
		{json.RawMessage(`{"type":"object"}`), nil, "Tool typed returned no structuredOutput"},
		{nil, json.RawMessage(`{"n":1}`), "Tool typed returned structuredOutput but declares no structuredOutputSchema"},
	} {
		tool := ToolDefinition{ToolSpec: ToolSpec{Name: "typed", Parameters: json.RawMessage(`{"type":"object"}`)}, StructuredOutputSchema: c.schema,
			Execute: func(context.Context, json.RawMessage, *ToolRuntime) (ToolResult, error) {
				return ToolResult{Content: "typed", StructuredOutput: c.output}, nil
			}}
		model := taskModelFunc(func(ctx context.Context, request ModelRequest, _ func(string) error) (ModelResponse, error) {
			if !hasToolResult(request) {
				return ModelResponse{ToolCalls: []ToolCall{{ID: "call-1", Name: "typed", Arguments: json.RawMessage(`{}`)}}}, nil
			}
			return ModelResponse{Content: "done"}, nil
		})
		registry := NewRegistry()
		if err := registry.Install(Extension{Name: "typed-tools", Tools: []ToolDefinition{tool}}); err != nil {
			t.Fatal(err)
		}
		h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, OnReport: func(report Report) { reports <- report }})
		root := testRoot(t, h)
		if err := root.Configure(context.Background(), Agent{Model: ModelRef{Provider: "fake", ID: "m"}}); err != nil {
			t.Fatal(err)
		}
		waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "go"}))
		// The model never sees structured output: its call stands.
		if result := lastToolResult(t, h, root.ID()); result.IsError || result.Content != "typed" {
			t.Fatalf("%s: result=%+v", c.want, result)
		}
		select {
		case report := <-reports:
			if report.Kind != ReportExtensionError || report.Extension != "typed-tools" || report.Err == nil || report.Err.Error() != c.want {
				t.Fatalf("report=%+v error=%v, want %q", report, report.Err, c.want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: no report", c.want)
		}
	}
}
