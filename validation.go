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
// validateAgainst validates a JSON value against a JSON Schema.
func validateAgainst(schema, value json.RawMessage) error {
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	if err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noSchemaNetwork{})
	compiler.UseRegexpEngine(jsRegexpEngine)
	const location = "urn:pi-durable-go:structured-output"
	if err := compiler.AddResource(location, document); err != nil {
		return err
	}
	compiled, err := compiler.Compile(location)
	if err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(value))
	if err != nil {
		return err
	}
	return compiled.Validate(instance)
}
