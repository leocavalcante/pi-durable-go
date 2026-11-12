package durable

import (
	"encoding/json"
	"testing"
)

func TestToolJSONSchemaValidation(t *testing.T) {
	spec := ToolSpec{Name: "count", Parameters: json.RawMessage(`{"type":"object","properties":{"n":{"type":"integer","minimum":1}},"required":["n"],"additionalProperties":false}`)}
	for _, input := range []string{`{}`, `{"n":0}`, `{"n":1,"other":true}`, `[]`, `null`, `broken`} {
		if _, err := validateToolArguments(spec, json.RawMessage(input)); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
	if got, err := validateToolArguments(spec, json.RawMessage(`{"n":1}`)); err != nil || string(got) != `{"n":1}` {
		t.Fatalf("got %s error=%v", got, err)
	}
	// Coerced as pi-ai's validation does: 1.5 truncates for an integer.
	if got, err := validateToolArguments(spec, json.RawMessage(`{"n":1.5}`)); err != nil || string(got) != `{"n":1}` {
		t.Fatalf("got %s error=%v", got, err)
	}
}
func TestToolSchemaCannotLoadExternalResources(t *testing.T) {
	spec := ToolSpec{Name: "bad", Parameters: json.RawMessage(`{"$ref":"https://127.0.0.1/secret"}`)}
	if _, err := validateToolArguments(spec, json.RawMessage(`{}`)); err == nil {
		t.Fatal("accepted external reference")
	}
}
