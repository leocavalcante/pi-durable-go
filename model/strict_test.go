package model

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

func TestMakeStrictSchemaLikePiAI(t *testing.T) {
	parameters := json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"},"offset":{"type":"number"},` +
		`"metadata":{"type":"object","properties":{"enabled":{"type":"boolean"}}},` +
		`"nullable":{"anyOf":[{"type":"string"},{"type":"null"}]}},"required":["path","metadata"]}`)
	strict, err := makeStrictSchema(parameters)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"object","properties":{"path":{"type":"string"},"offset":{"anyOf":[{"type":"number"},{"type":"null"}]},` +
		`"metadata":{"type":"object","properties":{"enabled":{"anyOf":[{"type":"boolean"},{"type":"null"}]}},"required":["enabled"],"additionalProperties":false},` +
		`"nullable":{"anyOf":[{"type":"string"},{"type":"null"}]}},"required":["path","offset","metadata","nullable"],"additionalProperties":false}`
	if string(strict) != want {
		t.Fatalf("strict=%s\nwant  %s", strict, want)
	}
	for schema, reason := range map[string]string{
		`{"type":"object","properties":{"metadata":{"type":"object","additionalProperties":{"type":"string"}}}}`: "additionalProperties is unsupported",
		`{"allOf":[{"type":"object"}]}`: "allOf schemas are unsupported",
		`{"type":"object","properties":{"value":{"anyOf":[{"type":"object","properties":{"nested":{"type":"string"}}},{"type":"null"}]}}}`: "object and array unions are unsupported",
		`{"type":"object","properties":{"child":{"$ref":"https://example.com/child.json"}},"required":["child"]}`:                          "$ref schemas are unsupported",
	} {
		if _, err := makeStrictSchema(json.RawMessage(schema)); err == nil || !strings.Contains(err.Error(), reason) {
			t.Fatalf("%s: error=%v, want %q", schema, err, reason)
		}
		tool := durable.ToolSpec{Name: "sample_tool", Parameters: json.RawMessage(schema), ConstrainedSampling: json.RawMessage(`{"type":"json_schema","strict":"prefer"}`)}
		if strict, err := strictSampling(tool, true); err != nil || strict != nil {
			t.Fatalf("prefer: strict=%v error=%v", strict, err)
		}
		tool.ConstrainedSampling = json.RawMessage(`{"type":"json_schema","strict":"require"}`)
		if _, err := strictSampling(tool, true); err == nil || !strings.Contains(err.Error(), `Tool "sample_tool" requires JSON-schema constrained sampling, but `) ||
			!strings.Contains(err.Error(), reason) {
			t.Fatalf("require: error=%v", err)
		}
	}
}

func TestOpenAISendsStrictTools(t *testing.T) {
	var body struct {
		Tools []struct {
			Function struct {
				Name       string          `json:"name"`
				Parameters json.RawMessage `json:"parameters"`
				Strict     *bool           `json:"strict"`
			} `json:"function"`
		} `json:"tools"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	req := durable.ModelRequest{Model: durable.ModelRef{ID: "m"}, Tools: []durable.ToolSpec{
		{Name: "strict", Parameters: json.RawMessage(`{"type":"object","properties":{"a":{"type":"string"}}}`), ConstrainedSampling: json.RawMessage(`{"type":"json_schema","strict":"prefer"}`)},
		{Name: "plain", Parameters: json.RawMessage(`{"type":"object"}`)},
	}}
	if _, err := (OpenAI{BaseURL: server.URL}).Generate(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if len(body.Tools) != 2 || body.Tools[0].Function.Strict == nil || !*body.Tools[0].Function.Strict ||
		string(body.Tools[0].Function.Parameters) != `{"type":"object","properties":{"a":{"anyOf":[{"type":"string"},{"type":"null"}]}},"required":["a"],"additionalProperties":false}` ||
		body.Tools[1].Function.Strict == nil || *body.Tools[1].Function.Strict {
		t.Fatalf("tools=%+v", body.Tools)
	}
}
