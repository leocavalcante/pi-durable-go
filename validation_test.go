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

// Validation lists every failure in pi-schema's order: missing properties,
// the declared properties in the schema's order, the other keys in the
// arguments' order, then the object's other constraints; a tagged union is
// told by its first tag in the schema's order. Expectations are upstream's
// messages (TestCrosscheckToolArgumentErrors).
func TestValidationListsFailuresInPiSchemasOrder(t *testing.T) {
	for _, c := range []struct{ schema, args, want string }{
		{"{\"type\":\"object\",\"properties\":{\"u\":{\"anyOf\":[{\"type\":\"object\",\"properties\":{\"z\":{\"const\":\"a\"},\"k\":{\"const\":\"x\"},\"v\":{\"type\":\"number\"}},\"required\":[\"z\",\"k\",\"v\"]},{\"type\":\"object\",\"properties\":{\"z\":{\"const\":\"b\"},\"k\":{\"const\":\"y\"},\"v\":{\"type\":\"string\"}},\"required\":[\"z\",\"k\",\"v\"]}]}}}", "{\"u\":{\"z\":\"a\",\"k\":\"y\",\"v\":true}}", "Validation failed for tool \"t\":\n  - u.k: expected \"x\"\n  - u.v: expected number, got boolean\n\nReceived arguments:\n{\n  \"u\": {\n    \"z\": \"a\",\n    \"k\": \"y\",\n    \"v\": true\n  }\n}"},
		{"{\"type\":\"object\",\"properties\":{\"u\":{\"oneOf\":[{\"type\":\"object\",\"properties\":{\"z\":{\"const\":\"a\"},\"k\":{\"const\":\"x\"},\"v\":{\"type\":\"number\"}},\"required\":[\"z\",\"k\",\"v\"]},{\"type\":\"object\",\"properties\":{\"z\":{\"const\":\"b\"},\"k\":{\"const\":\"y\"},\"v\":{\"type\":\"string\"}},\"required\":[\"z\",\"k\",\"v\"]}]}}}", "{\"u\":{\"z\":\"b\",\"k\":\"x\",\"v\":true}}", "Validation failed for tool \"t\":\n  - u.k: expected \"y\"\n  - u.v: expected string, got boolean\n\nReceived arguments:\n{\n  \"u\": {\n    \"z\": \"b\",\n    \"k\": \"x\",\n    \"v\": true\n  }\n}"},
		{"{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}},\"additionalProperties\":false}", "{\"q\":1,\"b\":2,\"z\":3,\"m\":4,\"2\":5,\"1\":6}", "Validation failed for tool \"t\":\n  - 1: unknown field\n  - 2: unknown field\n  - q: unknown field\n  - b: unknown field\n  - z: unknown field\n  - m: unknown field\n\nReceived arguments:\n{\n  \"1\": 6,\n  \"2\": 5,\n  \"q\": 1,\n  \"b\": 2,\n  \"z\": 3,\n  \"m\": 4\n}"},
		{"{\"type\":\"object\",\"properties\":{\"z\":{\"type\":\"string\"},\"b\":{\"type\":\"number\"},\"m\":{\"type\":\"boolean\"},\"a\":{\"type\":\"string\"}}}", "{\"a\":1,\"b\":\"x\",\"m\":\"y\",\"z\":2}", "Validation failed for tool \"t\":\n  - b: expected number, got string\n  - m: expected boolean, got string\n\nReceived arguments:\n{\n  \"a\": 1,\n  \"b\": \"x\",\n  \"m\": \"y\",\n  \"z\": 2\n}"},
		{"{\"type\":\"object\",\"properties\":{\"z\":{\"type\":\"string\"},\"b\":{\"type\":\"number\"}},\"required\":[\"z\",\"b\",\"a\"]}", "{}", "Validation failed for tool \"t\":\n  - z: missing\n  - b: missing\n  - a: missing\n\nReceived arguments:\n{}"},
		{"{\"type\":\"object\",\"properties\":{\"n\":{\"type\":\"object\",\"properties\":{\"y\":{\"type\":\"string\"},\"x\":{\"type\":\"number\"}},\"additionalProperties\":false}},\"required\":[\"n\"]}", "{\"n\":{\"x\":\"s\",\"y\":1,\"w\":0,\"c\":0}}", "Validation failed for tool \"t\":\n  - n.x: expected number, got string\n  - n.w: unknown field\n  - n.c: unknown field\n\nReceived arguments:\n{\n  \"n\": {\n    \"x\": \"s\",\n    \"y\": 1,\n    \"w\": 0,\n    \"c\": 0\n  }\n}"},
		{"{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"r\"],\"properties\":{\"b\":{\"type\":\"number\"},\"a\":{\"type\":\"string\",\"minLength\":3}},\"minProperties\":9}", "{\"zz\":1,\"b\":\"x\",\"a\":\"q\"}", "Validation failed for tool \"t\":\n  - r: missing\n  - b: expected number, got string\n  - a: expected at least 3 characters\n  - zz: unknown field\n  - root: expected at least 9 properties\n\nReceived arguments:\n{\n  \"zz\": 1,\n  \"b\": \"x\",\n  \"a\": \"q\"\n}"},
		{"{\"required\":[\"r\"],\"properties\":{\"b\":{\"type\":\"number\"}},\"type\":\"object\",\"additionalProperties\":false}", "{\"zz\":1,\"b\":\"x\"}", "Validation failed for tool \"t\":\n  - r: missing\n  - b: expected number, got string\n  - zz: unknown field\n\nReceived arguments:\n{\n  \"zz\": 1,\n  \"b\": \"x\"\n}"},
		{"{\"type\":\"object\",\"properties\":{\"p\":{\"type\":\"object\",\"patternProperties\":{\"^x\":{\"type\":\"number\"}},\"properties\":{\"xa\":{\"type\":\"number\"},\"b\":{\"type\":\"string\"}},\"additionalProperties\":false}}}", "{\"p\":{\"q\":1,\"xb\":\"s\",\"xa\":\"s\",\"b\":2}}", "Validation failed for tool \"t\":\n  - p.xa: expected number, got string\n  - p.q: unknown field\n  - p.xb: expected number, got string\n  - p.xa: expected number, got string\n\nReceived arguments:\n{\n  \"p\": {\n    \"q\": 1,\n    \"xb\": \"s\",\n    \"xa\": \"s\",\n    \"b\": 2\n  }\n}"},
		{"{\"type\":\"object\",\"patternProperties\":{\"b$\":{\"type\":\"number\"},\"^a\":{\"type\":\"boolean\"}}}", "{\"zb\":\"x\",\"ab\":\"y\",\"aa\":\"z\"}", "Validation failed for tool \"t\":\n  - zb: expected number, got string\n  - ab: expected number, got string\n  - ab: expected boolean, got string\n  - aa: expected boolean, got string\n\nReceived arguments:\n{\n  \"zb\": \"x\",\n  \"ab\": \"y\",\n  \"aa\": \"z\"\n}"},
	} {
		for range 5 { // the checker's own order varies
			_, err := validateToolArguments(ToolSpec{Name: "t", Parameters: json.RawMessage(c.schema)}, json.RawMessage(c.args))
			if err == nil || err.Error() != c.want {
				t.Fatalf("%s with %s:\n got %v\nwant %s", c.schema, c.args, err, c.want)
			}
		}
	}
}

// Coerced arguments keep the model's key order, as pi-schema coerces them
// in place.
func TestCoercedArgumentsKeepTheirKeyOrder(t *testing.T) {
	got, err := validateToolArguments(ToolSpec{Name: "t", Parameters: json.RawMessage(`{"type":"object","properties":{"b":{"type":"number"},"n":{"type":"object","properties":{"y":{"type":"boolean"}}}}}`)},
		json.RawMessage(`{"z":1,"b":"2","n":{"y":"true","x":[{"k":1,"a":2}]},"a":null}`))
	if err != nil || string(got) != `{"z":1,"b":2,"n":{"y":true,"x":[{"k":1,"a":2}]},"a":null}` {
		t.Fatalf("got %s, %v", got, err)
	}
}
