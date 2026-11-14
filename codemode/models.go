package codemode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/js"
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

// Models is the model catalog and the classifier and image models scripts
// reach through models.* (upstream pi-ai's Models). Models are JSON objects
// with at least provider and id, as pi-ai's catalog entries. Go has no pi-ai
// catalog of its own: a host provides one.
type Models interface {
	// ModelsOfType lists the catalog's models of a type (chat, image, or
	// classifier), of one provider when provider is not empty.
	ModelsOfType(ctx context.Context, modelType, provider string) ([]json.RawMessage, error)
	// AvailableOfType lists those with configured authentication.
	AvailableOfType(ctx context.Context, modelType, provider string) ([]json.RawMessage, error)
	// ModelOfType is one model, nil when the catalog has none.
	ModelOfType(ctx context.Context, modelType, provider, id string) (json.RawMessage, error)
	// Classify and GenerateImages call a model with a context the script
	// gave, checked for its shape, and return pi-ai's result object, which
	// the script gets: { answers } or { output }, with stopReason ("stop" is
	// a success), errorMessage, and usage, which the Harness counts.
	Classify(ctx context.Context, model, context json.RawMessage) (json.RawMessage, error)
	GenerateImages(ctx context.Context, model, context json.RawMessage) (json.RawMessage, error)
}

// modelOutcome is what a model call's result says of itself.
type modelOutcome struct {
	StopReason   string `json:"stopReason"`
	ErrorMessage string `json:"errorMessage"`
	Usage        *struct {
		Input      int64 `json:"input"`
		Output     int64 `json:"output"`
		CacheRead  int64 `json:"cacheRead"`
		CacheWrite int64 `json:"cacheWrite"`
		Cost       struct {
			Total float64 `json:"total"`
		} `json:"cost"`
	} `json:"usage"`
}

var modelTypes = []string{"chat", "image", "classifier"}

// describeValue is a script value in an error message: undefined, a string,
// an array, or its keys ({ prompt }).
func describeValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "undefined"
	}
	value, err := js.Parse(raw)
	if err != nil {
		return "undefined"
	}
	switch v := value.(type) {
	case nil:
		return "null"
	case []any:
		if len(v) == 0 {
			return "an empty array"
		}
		return "an array"
	case *js.Object:
		if len(v.Keys) == 0 {
			return "{}"
		}
		keys := v.Keys
		more := ""
		if len(keys) > 6 {
			keys, more = keys[:6], ", ..."
		}
		return "{ " + strings.Join(keys, ", ") + more + " }"
	case string:
		return "a string"
	case float64:
		return "a number"
	case bool:
		return "a boolean"
	}
	return "undefined"
}

// withArticle is "an image", "a classifier".
func withArticle(word string) string {
	if word != "" && strings.ContainsRune("aeiou", rune(word[0])) {
		return "an " + word
	}
	return "a " + word
}

func toModelType(raw json.RawMessage) (string, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil && slices.Contains(modelTypes, text) {
		return text, nil
	}
	shown := "undefined"
	if len(raw) > 0 {
		shown = js.StringifyRaw(raw)
	}
	return "", fmt.Errorf("Unknown model type %s. Use \"chat\", \"image\", or \"classifier\".", shown)
}

func toProvider(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return "", errors.New("provider must be a string")
	}
	return text, nil
}

// toModelInfo is a catalog entry for scripts, without headers: configured
// headers can carry credentials.
func toModelInfo(model json.RawMessage) json.RawMessage {
	value, err := js.Parse(model)
	if err != nil {
		return model
	}
	if object, ok := value.(*js.Object); ok {
		if _, has := object.Values["headers"]; has {
			delete(object.Values, "headers")
			object.Keys = slices.DeleteFunc(slices.Clone(object.Keys), func(key string) bool { return key == "headers" })
		}
	}
	return json.RawMessage(js.Stringify(value))
}

func modelList(models []json.RawMessage) (json.RawMessage, error) {
	infos := make([]json.RawMessage, len(models))
	for i, model := range models {
		infos[i] = toModelInfo(model)
	}
	return json.Marshal(infos)
}

// catalogCall answers a script's call of a catalog function with its
// argument list.
func catalogCall(ctx context.Context, name string, args json.RawMessage, models Models) (json.RawMessage, error) {
	var list []json.RawMessage
	_ = json.Unmarshal(args, &list)
	argument := func(i int) json.RawMessage {
		if i < len(list) {
			return list[i]
		}
		return nil
	}
	switch name {
	case "models.getModelsOfType", "models.getAvailableOfType":
		modelType, err := toModelType(argument(0))
		if err != nil {
			return nil, err
		}
		provider, err := toProvider(argument(1))
		if err != nil {
			return nil, err
		}
		var found []json.RawMessage
		if name == "models.getModelsOfType" {
			found, err = models.ModelsOfType(ctx, modelType, provider)
		} else {
			found, err = models.AvailableOfType(ctx, modelType, provider)
		}
		if err != nil {
			return nil, err
		}
		return modelList(found)
	}
	stringOf := func(raw json.RawMessage) (string, bool) {
		value, err := js.Parse(raw)
		text, ok := value.(string)
		return text, err == nil && ok
	}
	provider, providerOK := stringOf(argument(1))
	id, idOK := stringOf(argument(2))
	if !providerOK || !idOK {
		described := make([]string, len(list))
		for i, value := range list {
			described[i] = describeValue(value)
		}
		return nil, fmt.Errorf("models.getModelOfType(type, provider, id) expects three strings, got (%s). The provider and the id are separate arguments, for example models.getModelOfType(\"classifier\", \"typesafe\", \"jev-latest\").", strings.Join(described, ", "))
	}
	modelType, err := toModelType(argument(0))
	if err != nil {
		return nil, err
	}
	model, err := models.ModelOfType(ctx, modelType, provider, id)
	if err != nil || model == nil {
		return nil, err
	}
	return toModelInfo(model), nil
}

const classifierContextShape = `{ state: { ... }, images?: [{ type: "image", data: <base64>, mimeType }], questions: { <id>: { type: "choice", instructions, criteria: { <label>: <meaning> } } | { type: "score", instructions, criteria: [<lowest level>, ..., <highest level>] } | { type: "bool", instructions, criteria: { true: <meaning>, false: <meaning> } } } }`

func isImageBlock(value any) bool {
	block, ok := value.(*js.Object)
	if !ok || block.Values["type"] != "image" {
		return false
	}
	_, data := block.Values["data"].(string)
	_, mime := block.Values["mimeType"].(string)
	return data && mime
}

// describeParsed is describeValue of a parsed value.
func describeParsed(value any) string {
	if value == nil {
		return "null"
	}
	return describeValue(json.RawMessage(js.Stringify(value)))
}

// describeField is describeValue of an object's field, which may be absent.
func describeField(object *js.Object, key string) string {
	value, ok := object.Values[key]
	if !ok {
		return "undefined"
	}
	return describeParsed(value)
}

// checkClassifierContext checks a script's classifier context, so mistakes
// fail with the expected shape instead of a provider error.
func checkClassifierContext(raw json.RawMessage) error {
	fail := func(problem string) error {
		return fmt.Errorf("models.classify() %s. Expected context: %s.", problem, classifierContextShape)
	}
	value, _ := js.Parse(raw)
	context, ok := value.(*js.Object)
	if !ok {
		return fail("expects a context object as its second argument, got " + describeValue(raw))
	}
	if _, ok := context.Values["state"].(*js.Object); !ok {
		return fail("context.state must be an object, got " + describeField(context, "state"))
	}
	if images, present := context.Values["images"]; present {
		list, ok := images.([]any)
		if !ok {
			return fail("context.images must be an array, got " + describeParsed(images))
		}
		for i, image := range list {
			if !isImageBlock(image) {
				return fail(fmt.Sprintf("context.images[%d] must be an image block, got %s", i, describeParsed(image)))
			}
		}
	}
	questions, ok := context.Values["questions"].(*js.Object)
	if !ok || len(questions.Keys) == 0 {
		return fail("context.questions must map question IDs to questions, got " + describeField(context, "questions"))
	}
	strings := func(values []any) bool {
		if len(values) == 0 {
			return false
		}
		for _, value := range values {
			if _, ok := value.(string); !ok {
				return false
			}
		}
		return true
	}
	for _, id := range questions.Keys {
		at := "context.questions." + id
		question, ok := questions.Values[id].(*js.Object)
		if !ok {
			return fail(at + " must be a question object, got " + describeParsed(questions.Values[id]))
		}
		if _, ok := question.Values["instructions"].(string); !ok {
			return fail(at + ".instructions must be a string")
		}
		criteria := question.Values["criteria"]
		switch question.Values["type"] {
		case "choice":
			object, ok := criteria.(*js.Object)
			var values []any
			if ok {
				for _, key := range object.Keys {
					values = append(values, object.Values[key])
				}
			}
			if !ok || !strings(values) {
				return fail(at + ` is a "choice" question, so criteria must map each label to its meaning`)
			}
		case "score":
			list, ok := criteria.([]any)
			if !ok || !strings(list) {
				return fail(at + ` is a "score" question, so criteria must list the levels as strings, lowest first`)
			}
		case "bool":
			object, ok := criteria.(*js.Object)
			if ok {
				_, t := object.Values["true"].(string)
				_, f := object.Values["false"].(string)
				ok = t && f
			}
			if !ok {
				return fail(at + ` is a "bool" question, so criteria must be { true: string, false: string }`)
			}
		default:
			shown := "undefined"
			if t, present := question.Values["type"]; present {
				shown = js.Stringify(t)
			}
			return fail(at + `.type must be "choice", "score", or "bool", got ` + shown)
		}
	}
	return nil
}

// checkImagesContext checks a script's image context, so mistakes such as
// { prompt } fail with the expected shape.
func checkImagesContext(raw json.RawMessage) error {
	fail := func(problem string) error {
		return fmt.Errorf(`models.generateImages() %s. Expected context: { input: [{ type: "text", text: <prompt> }, ...optional { type: "image", data: <base64>, mimeType } references] }.`, problem)
	}
	value, _ := js.Parse(raw)
	context, ok := value.(*js.Object)
	if !ok {
		return fail("expects a context object as its second argument, got " + describeValue(raw))
	}
	input, ok := context.Values["input"].([]any)
	if !ok || len(input) == 0 {
		return fail("context.input must be a non-empty array of blocks, got " + describeField(context, "input"))
	}
	for i, block := range input {
		if object, ok := block.(*js.Object); ok && object.Values["type"] == "text" {
			if _, ok := object.Values["text"].(string); ok {
				continue
			}
		}
		if isImageBlock(block) {
			continue
		}
		return fail(fmt.Sprintf("context.input[%d] must be a text or image block, got %s", i, describeParsed(block)))
	}
	return nil
}

// resolveModel resolves the script's model by provider and id only, so a
// script-supplied baseUrl or headers never receive the credentials.
func resolveModel(ctx context.Context, name, modelType string, raw json.RawMessage, models Models) (json.RawMessage, string, error) {
	listHint := fmt.Sprintf("List the %s models you can use with models.getAvailableOfType(%q).", modelType, modelType)
	value, _ := js.Parse(raw)
	object, ok := value.(*js.Object)
	var provider, id string
	if ok {
		provider, _ = object.Values["provider"].(string)
		id, _ = object.Values["id"].(string)
		_, hasProvider := object.Values["provider"].(string)
		_, hasID := object.Values["id"].(string)
		ok = hasProvider && hasID
	}
	if !ok {
		undefinedHint := ""
		if len(raw) == 0 || string(raw) == "null" {
			undefinedHint = " models.getModelOfType() returns undefined for an unknown provider or id."
		}
		return nil, "", fmt.Errorf("%s() expects %s model as its first argument, got %s.%s %s", name, withArticle(modelType), describeValue(raw), undefinedHint, listHint)
	}
	resolved, err := models.ModelOfType(ctx, modelType, provider, id)
	if err != nil {
		return nil, "", err
	}
	if resolved != nil {
		return resolved, provider + "/" + id, nil
	}
	for _, other := range modelTypes {
		if other == modelType {
			continue
		}
		if found, err := models.ModelOfType(ctx, other, provider, id); err == nil && found != nil {
			return nil, "", fmt.Errorf("%q is %s model, not %s model. %s", provider+"/"+id, withArticle(other), withArticle(modelType), listHint)
		}
	}
	return nil, "", fmt.Errorf("Unknown %s model %q. %s", modelType, provider+"/"+id, listHint)
}

var modelCallParameters = json.RawMessage(`{"type":"object","properties":{"model":{},"context":{}},"required":["model","context"]}`)

// modelResult is a model call's result as the tool returns it: the whole
// result for the script, usage counted by the Harness.
func modelResult(label string, value json.RawMessage) durable.ToolResult {
	if len(value) == 0 {
		value = json.RawMessage("{}")
	}
	var outcome modelOutcome
	_ = json.Unmarshal(value, &outcome)
	ok := outcome.StopReason == "stop"
	text := label + " done"
	if !ok {
		text = label + " " + outcome.StopReason + ": " + outcome.ErrorMessage
	}
	result := durable.ToolResult{Content: text, StructuredOutput: json.RawMessage(js.StringifyRaw(value)), IsError: !ok}
	if usage := outcome.Usage; usage != nil {
		result.Usage = durable.Usage{InputTokens: usage.Input, OutputTokens: usage.Output, CacheReadTokens: usage.CacheRead,
			CacheWriteTokens: usage.CacheWrite, Cost: usage.Cost.Total}
	}
	return result
}

// modelTools are the internal tools behind models.classify() and
// models.generateImages(): nested durable calls with their own task, live
// row, abort, usage accounting, and replay policy.
func modelTools(models Models) []durable.ToolDefinition {
	call := func(name, global, modelType string, check func(json.RawMessage) error,
		run func(context.Context, json.RawMessage, json.RawMessage) (json.RawMessage, error)) durable.ToolDefinition {
		return durable.ToolDefinition{
			ToolSpec:               durable.ToolSpec{Name: name, Description: global + "() of code mode scripts.", Parameters: modelCallParameters},
			StructuredOutputSchema: json.RawMessage(`{}`),
			Callers:                []string{"tools"},
			Execute: func(ctx context.Context, args json.RawMessage, r *durable.ToolRuntime) (durable.ToolResult, error) {
				var input struct {
					Model   json.RawMessage `json:"model"`
					Context json.RawMessage `json:"context"`
				}
				_ = json.Unmarshal(args, &input)
				model, label, err := resolveModel(ctx, global, modelType, input.Model, models)
				if err != nil {
					return durable.ToolResult{}, err
				}
				if err := check(input.Context); err != nil {
					return durable.ToolResult{}, err
				}
				result, err := run(ctx, model, input.Context)
				if err != nil {
					return durable.ToolResult{}, err
				}
				verb := strings.TrimPrefix(global, "models.")
				return modelResult(verb+" "+label, result), nil
			},
		}
	}
	return []durable.ToolDefinition{
		call(ClassifyTool, "models.classify", "classifier", checkClassifierContext, models.Classify),
		call(GenerateImagesTool, "models.generateImages", "image", checkImagesContext, models.GenerateImages),
	}
}
