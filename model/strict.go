package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	durable "github.com/leocavalcante/pi-durable-go"
)

// A port of pi-ai api/constrained-sampling.ts for JSON-schema tools: a tool
// whose constrainedSampling is {type: "json_schema"} is sent in OpenAI strict
// mode when its schema converts to the strict subset.

// unsupportedStrict says why a schema cannot be made strict.
type unsupportedStrict struct{ reason string }

func (e *unsupportedStrict) Error() string { return e.reason }

func unsupported(format string, args ...any) error {
	return &unsupportedStrict{reason: fmt.Sprintf(format, args...)}
}

var unsupportedStrictKeys = []string{"$ref", "$defs", "definitions", "allOf", "oneOf", "patternProperties",
	"dependentSchemas", "dependencies", "unevaluatedProperties", "propertyNames", "contains", "prefixItems",
	"not", "if", "then", "else"}

// object is a JSON object that keeps its key order.
type object struct {
	keys   []string
	values map[string]any
}

func (o *object) get(key string) (any, bool) {
	v, ok := o.values[key]
	return v, ok
}

func (o *object) set(key string, value any) {
	if _, ok := o.values[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

// decodeOrdered decodes JSON keeping object key order.
func decodeOrdered(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	value, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("trailing data")
	}
	return value, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	token, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := token.(type) {
	case json.Delim:
		switch t {
		case '{':
			o := &object{values: map[string]any{}}
			for dec.More() {
				keyToken, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyToken.(string)
				value, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				o.set(key, value)
			}
			_, err := dec.Token() // }
			return o, err
		case '[':
			var list []any
			for dec.More() {
				value, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				list = append(list, value)
			}
			_, err := dec.Token() // ]
			if list == nil {
				list = []any{}
			}
			return list, err
		}
	}
	return token, nil
}

// encodeOrdered encodes a decoded value in its key order.
func encodeOrdered(value any) ([]byte, error) {
	var out bytes.Buffer
	if err := encodeValue(&out, value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func encodeValue(out *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case *object:
		out.WriteByte('{')
		for i, key := range v.keys {
			if i > 0 {
				out.WriteByte(',')
			}
			name, _ := json.Marshal(key)
			out.Write(name)
			out.WriteByte(':')
			if err := encodeValue(out, v.values[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := encodeValue(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		out.Write(data)
	}
	return nil
}

func typesOf(schema *object) []string {
	switch t := schema.values["type"].(type) {
	case string:
		return []string{t}
	case []any:
		var out []string
		for _, v := range t {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func isStructured(schema any) bool {
	o, ok := schema.(*object)
	if !ok {
		return false
	}
	types := typesOf(o)
	_, properties := o.get("properties")
	_, items := o.get("items")
	return slices.Contains(types, "object") || slices.Contains(types, "array") || properties || items
}

func allowsNull(schema any) bool {
	o, ok := schema.(*object)
	if !ok {
		return false
	}
	if slices.Contains(typesOf(o), "null") {
		return true
	}
	if value, ok := o.get("const"); ok && value == nil {
		return true
	}
	if values, ok := o.values["enum"].([]any); ok && slices.Contains(values, nil) {
		return true
	}
	if variants, ok := o.values["anyOf"].([]any); ok {
		for _, variant := range variants {
			if allowsNull(variant) {
				return true
			}
		}
	}
	return false
}

func makeStrictNode(schema any) error {
	o, ok := schema.(*object)
	if !ok {
		return unsupported("boolean schemas are unsupported")
	}
	for _, key := range unsupportedStrictKeys {
		if _, ok := o.get(key); ok {
			return unsupported("%s schemas are unsupported", key)
		}
	}
	if raw, ok := o.get("anyOf"); ok {
		variants, ok := raw.([]any)
		if !ok || len(variants) == 0 {
			return unsupported("anyOf must contain at least one schema")
		}
		for _, variant := range variants {
			if isStructured(variant) {
				return unsupported("object and array unions are unsupported")
			}
			if err := makeStrictNode(variant); err != nil {
				return err
			}
		}
	}
	if items, ok := o.get("items"); ok {
		if _, tuple := items.([]any); tuple {
			return unsupported("tuple schemas are unsupported")
		}
		if err := makeStrictNode(items); err != nil {
			return err
		}
	}
	isObject := o.values["type"] == "object"
	if _, ok := o.get("properties"); ok && !isObject {
		return unsupported("properties require type object")
	}
	if !isObject {
		return nil
	}
	if additional, ok := o.get("additionalProperties"); ok && additional != false {
		return unsupported("schema-valued or true additionalProperties is unsupported")
	}
	properties := &object{values: map[string]any{}}
	if raw, ok := o.get("properties"); ok {
		if properties, ok = raw.(*object); !ok {
			return unsupported("object properties must be a schema map")
		}
	}
	required := map[string]bool{}
	if raw, ok := o.get("required"); ok {
		list, ok := raw.([]any)
		if !ok {
			return unsupported("object required must be a string array")
		}
		for _, key := range list {
			name, ok := key.(string)
			if !ok {
				return unsupported("object required must be a string array")
			}
			required[name] = true
		}
		for name := range required {
			if _, known := properties.get(name); !known {
				return unsupported("required contains an unknown property")
			}
		}
	}
	names := make([]any, 0, len(properties.keys))
	for _, name := range properties.keys {
		property := properties.values[name]
		if err := makeStrictNode(property); err != nil {
			return err
		}
		if !required[name] && !allowsNull(property) {
			wrapped := &object{values: map[string]any{}}
			wrapped.set("anyOf", []any{property, &object{keys: []string{"type"}, values: map[string]any{"type": "null"}}})
			properties.values[name] = wrapped
		}
		names = append(names, name)
	}
	o.set("required", names)
	o.set("additionalProperties", false)
	return nil
}

// makeStrictSchema converts a tool's parameters to the strict subset
// (pi-ai makeStrictJsonSchema).
func makeStrictSchema(parameters json.RawMessage) (json.RawMessage, error) {
	schema, err := decodeOrdered(parameters)
	if err != nil {
		return nil, unsupported("root schema must have type object")
	}
	root, ok := schema.(*object)
	if !ok {
		return nil, unsupported("root schema must have type object")
	}
	if err := makeStrictNode(root); err != nil {
		return nil, err
	}
	if root.values["type"] != "object" {
		return nil, unsupported("root schema must have type object")
	}
	return encodeOrdered(root)
}

type constrainedSampling struct {
	Type   string `json:"type"`
	Strict string `json:"strict"`
}

// strictSampling decides whether a JSON-schema tool is sent in strict mode
// (pi-ai resolveJsonSchemaStrictSampling): true when its schema converts, nil
// for no decision; a tool that requires strict sampling and cannot get it
// fails the request.
func strictSampling(tool durable.ToolSpec, supportsStrict bool) (*bool, error) {
	var config constrainedSampling
	if len(tool.ConstrainedSampling) == 0 || json.Unmarshal(tool.ConstrainedSampling, &config) != nil || config.Type != "json_schema" {
		return nil, nil
	}
	if supportsStrict {
		if _, err := makeStrictSchema(tool.Parameters); err != nil {
			var reason *unsupportedStrict
			if !errors.As(err, &reason) {
				return nil, err
			}
			if config.Strict != "require" {
				return nil, nil
			}
			return nil, fmt.Errorf("Tool %q requires JSON-schema constrained sampling, but %s.", tool.Name, reason.reason)
		}
		strict := true
		return &strict, nil
	}
	if config.Strict == "require" {
		return nil, fmt.Errorf("Tool %q requires JSON-schema constrained sampling, but strict tools are unsupported.", tool.Name)
	}
	return nil, nil
}
