package codemode

import (
	"encoding/json"

	durable "github.com/leocavalcante/pi-durable-go"
)

// durableTool is a tool declaration for tests.
type durableTool struct {
	name, description  string
	parameters, output json.RawMessage
}

func definitions(tools []durableTool) []durable.ToolDefinition {
	var out []durable.ToolDefinition
	for _, tool := range tools {
		definition := durable.ToolDefinition{ToolSpec: durable.ToolSpec{Name: tool.name, Description: tool.description, Parameters: tool.parameters}}
		if len(tool.output) > 0 {
			definition.StructuredOutputSchema = tool.output
		}
		out = append(out, definition)
	}
	return out
}
