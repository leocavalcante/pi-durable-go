package codemode

import (
	"fmt"
	"strings"
)

// InternalPrefix starts the reserved names of code mode's internal tools;
// they are never listed to scripts or the model.
const InternalPrefix = "pi.codemode."

const (
	// ClassifyTool and GenerateImagesTool run models.classify() and
	// models.generateImages() as nested calls.
	ClassifyTool       = InternalPrefix + "models.classify"
	GenerateImagesTool = InternalPrefix + "models.generateImages"
)

// modelCalls maps the global a script calls to its internal tool.
var modelCalls = map[string]string{
	"models.classify":       ClassifyTool,
	"models.generateImages": GenerateImagesTool,
}

// catalogGlobals are the catalog functions, host globals of the models
// namespace.
var catalogGlobals = []string{"models.getModelsOfType", "models.getAvailableOfType", "models.getModelOfType"}

// MaxConcurrentModelCalls is how many models.classify() and
// models.generateImages() calls one script may have in flight; the rest
// wait.
const MaxConcurrentModelCalls = 4

// ModelsDescription documents the models globals in the tool's description.
var ModelsDescription = strings.Join([]string{
	"`models` (all functions return promises):",
	"- `models.getModelsOfType(type, provider?)`, `models.getAvailableOfType(type, provider?)` (configured auth only), `models.getModelOfType(type, provider, id)`: the catalog; `type` is \"chat\", \"image\", or \"classifier\".",
	"- `models.classify(model, { state: { ... }, images?: [{ type: \"image\", data, mimeType }], questions: { <id>: { type: \"choice\", instructions, criteria: { <label>: <meaning> } } | { type: \"score\", instructions, criteria: [<lowest level>, ..., <highest level>] } | { type: \"bool\", instructions, criteria: { true: <meaning>, false: <meaning> } } } })` resolves to `{ answers: { <id>: ... }, stopReason, errorMessage?, usage? }`.",
	"- `models.generateImages(model, { input: [{ type: \"text\", text: <prompt> }, ...optional { type: \"image\", data, mimeType }] })` resolves to `{ output: [image blocks], stopReason, errorMessage?, usage? }`; show each image with `image(block)`.",
	fmt.Sprintf("At most %d classify/generateImages calls run at once; more wait.", MaxConcurrentModelCalls),
}, "\n")
