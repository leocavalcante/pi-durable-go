package durable

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type noSchemaNetwork struct{}

func (noSchemaNetwork) Load(url string) (any, error) {
	return nil, fmt.Errorf("durable: external schema references are disabled")
}

// Tool schemas are local resources. Validating untrusted model arguments must
// never cause schema-controlled filesystem or network requests.
func validateToolArguments(spec ToolSpec, args json.RawMessage) error {
	schema := spec.Parameters
	if len(schema) == 0 {
		schema = json.RawMessage(`{"type":"object"}`)
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	if err != nil {
		return fmt.Errorf("invalid tool schema: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noSchemaNetwork{})
	const location = "urn:pi-durable-go:tool"
	if err := compiler.AddResource(location, document); err != nil {
		return err
	}
	compiled, err := compiler.Compile(location)
	if err != nil {
		return fmt.Errorf("invalid tool schema: %w", err)
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(args))
	if err != nil {
		return fmt.Errorf("invalid tool arguments: %w", err)
	}
	if err := compiled.Validate(value); err != nil {
		return fmt.Errorf("invalid tool arguments: %w", err)
	}
	return nil
}
