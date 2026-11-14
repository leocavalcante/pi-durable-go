package codemode

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/leocavalcante/pi-durable-go/internal/js"
	"github.com/leocavalcante/pi-durable-go/internal/jsnum"
)

// The TypeScript declarations scripts see, ported from pi-codemode's
// declarations.ts.

var identifierPattern = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)

const (
	indent = "  "
	// DefaultInputSchemaMaxChars bounds a rendered input type, in
	// characters, before it becomes unknown.
	DefaultInputSchemaMaxChars = 16_000
	// maxRefExpansions bounds local $ref expansions per rendered schema.
	maxRefExpansions = 32
	// maxTagChars is the longest property tag; longer ones are left out.
	maxTagChars = 80
)

// tagKeywords have a number or string value shown as "@keyword value".
var tagKeywords = []string{"minimum", "exclusiveMinimum", "maximum", "exclusiveMaximum", "multipleOf",
	"minLength", "maxLength", "pattern", "format", "minItems", "maxItems"}

var (
	lowerBounds = map[string]bool{"minimum": true, "exclusiveMinimum": true, "minLength": true, "minItems": true}
	upperBounds = map[string]bool{"maximum": true, "exclusiveMaximum": true, "maxLength": true, "maxItems": true}
)

// ToolDeclaration is what a script sees of a tool: its name, description,
// and input and output schemas (nil for none).
type ToolDeclaration struct {
	Name         string
	Description  string
	InputSchema  json.RawMessage
	OutputSchema json.RawMessage
}

// RenderToolSample is a tool's description followed by its declaration
// (upstream renderToolSample).
func RenderToolSample(tool ToolDeclaration) string {
	declaration := "declare const tools: { " + RenderToolSignature(tool) + " };"
	return js.Trim(tool.Description) + "\n\ncodemode tool declaration:\n```ts\n" + declaration + "\n```"
}

// RenderToolSignature is the tool as a member of the tools object:
// "name(args: T): Promise<R>;". An input type longer than 16000 characters
// renders as unknown.
func RenderToolSignature(tool ToolDeclaration) string {
	input := "unknown"
	if tool.InputSchema != nil {
		input = SchemaToType(tool.InputSchema, DefaultInputSchemaMaxChars)
	}
	output := "unknown"
	if tool.OutputSchema != nil {
		output = SchemaToType(tool.OutputSchema, 0)
	}
	return Identifier(tool.Name) + "(args: " + input + "): Promise<" + output + ">;"
}

// SchemaToType converts a JSON Schema to a TypeScript type expression
// (upstream schemaToType). A result longer than maxChars (when positive) is
// rendered again without constraint tags, and is unknown if still too long.
func SchemaToType(schema json.RawMessage, maxChars int) string {
	root, err := js.Parse(schema)
	if err != nil {
		return "unknown"
	}
	render := func(tags bool) string {
		return toType(root, &typeContext{root: root, resolving: map[string]bool{}, tags: tags})
	}
	rendered := render(true)
	if maxChars <= 0 || js.Len(rendered) <= maxChars {
		return rendered
	}
	untagged := render(false)
	if js.Len(untagged) <= maxChars {
		return untagged
	}
	return "unknown"
}

type typeContext struct {
	root       any
	resolving  map[string]bool
	expansions int
	tags       bool
}

// resolveSchemaRef is the subschema a local $ref names in root, or nil.
func resolveSchemaRef(ref string, root any) any {
	if ref != "#" && !strings.HasPrefix(ref, "#/") {
		return nil
	}
	current := root
	rest := ""
	if len(ref) > 2 {
		rest = ref[2:]
	}
	for _, segment := range strings.Split(rest, "/") {
		if segment == "" {
			continue
		}
		decoded, ok := decodeURIComponent(segment)
		if !ok {
			return nil
		}
		key := strings.ReplaceAll(strings.ReplaceAll(decoded, "~1", "/"), "~0", "~")
		object, isObject := current.(*js.Object)
		if !isObject {
			return nil
		}
		value, found := object.Values[key]
		if !found {
			return nil
		}
		current = value
	}
	switch current.(type) {
	case bool, *js.Object:
		return current
	}
	return nil
}

// decodeURIComponent decodes %XX escapes as UTF-8; false where JavaScript's
// decodeURIComponent throws.
func decodeURIComponent(s string) (string, bool) {
	if !strings.Contains(s, "%") {
		return s, true
	}
	var bytes []byte
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			bytes = append(bytes, s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", false
		}
		decoded, err := url.PathUnescape(s[i : i+3])
		if err != nil {
			return "", false
		}
		bytes = append(bytes, decoded[0])
		i += 2
	}
	if !utf8.Valid(bytes) {
		return "", false
	}
	return string(bytes), true
}

func union(types []string) string {
	var unique []string
	for _, t := range types {
		if !slices.Contains(unique, t) {
			unique = append(unique, t)
		}
	}
	if slices.Contains(unique, "unknown") {
		return "unknown"
	}
	if len(unique) == 0 {
		return "never"
	}
	return strings.Join(unique, " | ")
}

func toType(schema any, context *typeContext) string {
	switch schema {
	case true:
		return "unknown"
	case false:
		return "never"
	}
	object, ok := schema.(*js.Object)
	if !ok {
		return "unknown"
	}
	values := object.Values
	if ref, ok := values["$ref"].(string); ok {
		if context.resolving[ref] || context.expansions >= maxRefExpansions {
			return "unknown"
		}
		target := resolveSchemaRef(ref, context.root)
		if target == nil {
			return "unknown"
		}
		context.expansions++
		context.resolving[ref] = true
		defer delete(context.resolving, ref)
		return toType(target, context)
	}
	if value, ok := values["const"]; ok {
		return js.Stringify(value)
	}
	if enum, ok := values["enum"].([]any); ok {
		types := make([]string, len(enum))
		for i, value := range enum {
			types[i] = js.Stringify(value)
		}
		return union(types)
	}
	variants, ok := values["anyOf"].([]any)
	if !ok {
		variants, ok = values["oneOf"].([]any)
	}
	if ok {
		types := make([]string, len(variants))
		for i, variant := range variants {
			types[i] = toType(variant, context)
		}
		return union(types)
	}
	if parts, ok := values["allOf"].([]any); ok {
		var rendered []string
		for _, part := range parts {
			if t := toType(part, context); t != "unknown" {
				if strings.Contains(t, " | ") {
					t = "(" + t + ")"
				}
				rendered = append(rendered, t)
			}
		}
		if len(rendered) == 0 {
			return "unknown"
		}
		return strings.Join(rendered, " & ")
	}
	kind, present := values["type"]
	if entries, ok := kind.([]any); ok {
		types := make([]string, len(entries))
		for i, entry := range entries {
			types[i] = toType(withValue(object, "type", entry), context)
		}
		return union(types)
	}
	if !present {
		if has(object, "properties") || has(object, "additionalProperties") || has(object, "required") {
			return objectType(object, context)
		}
		if has(object, "items") || has(object, "prefixItems") {
			return arrayType(object, context)
		}
		return "unknown"
	}
	switch kind {
	case "string":
		return "string"
	case "number", "integer":
		return "number"
	case "boolean":
		return "boolean"
	case "null":
		return "null"
	case "array":
		return arrayType(object, context)
	case "object":
		return objectType(object, context)
	}
	return "unknown"
}

func has(object *js.Object, key string) bool {
	_, ok := object.Values[key]
	return ok
}

// withValue is {...object, key: value}.
func withValue(object *js.Object, key string, value any) *js.Object {
	copied := &js.Object{Keys: slices.Clone(object.Keys), Values: make(map[string]any, len(object.Values)+1)}
	for k, v := range object.Values {
		copied.Values[k] = v
	}
	if _, ok := copied.Values[key]; !ok {
		copied.Keys = append(copied.Keys, key)
	}
	copied.Values[key] = value
	return copied
}

func arrayType(schema *js.Object, context *typeContext) string {
	items, present := schema.Values["items"]
	if _, isArray := items.([]any); present && !isArray {
		return "Array<" + toType(items, context) + ">"
	}
	tuple, ok := schema.Values["prefixItems"].([]any)
	if !ok {
		tuple, _ = items.([]any)
	}
	if len(tuple) > 0 {
		types := make([]string, len(tuple))
		for i, item := range tuple {
			types[i] = toType(item, context)
		}
		return "[" + strings.Join(types, ", ") + "]"
	}
	return "unknown[]"
}

func descriptionOf(property any) string {
	if object, ok := property.(*js.Object); ok {
		if description, ok := object.Values["description"].(string); ok {
			return js.Trim(description)
		}
	}
	return ""
}

// lineTerminators end a // comment.
var lineTerminators = regexp.MustCompile("[\r\n\u2028\u2029]")

// constraintTags are a property's constraints its TypeScript type cannot
// express, as JSDoc-style tags: "@integer @minimum 1 @maximum 100 @default 20".
func constraintTags(schema any) string {
	property, ok := schema.(*js.Object)
	if !ok {
		return ""
	}
	// Optional fields such as Pydantic's int | None keep their constraints
	// in the non-null branch, which applies together with the property.
	variants, isArray := property.Values["anyOf"].([]any)
	if !isArray {
		variants, isArray = property.Values["oneOf"].([]any)
	}
	var nonNull []any
	if isArray {
		for _, variant := range variants {
			if object, ok := variant.(*js.Object); ok && object.Values["type"] == "null" {
				continue
			}
			nonNull = append(nonNull, variant)
		}
	}
	branches := []*js.Object{property}
	if len(nonNull) == 1 {
		if branch, ok := nonNull[0].(*js.Object); ok {
			branches = append(branches, branch)
		}
	}
	// Draft-04 and OpenAPI 3.0 make minimum and maximum exclusive with a
	// boolean exclusiveMinimum or exclusiveMaximum.
	schemas := make([]map[string]any, len(branches))
	for i, branch := range branches {
		values := make(map[string]any, len(branch.Values))
		for k, v := range branch.Values {
			values[k] = v
		}
		if branch.Values["exclusiveMinimum"] == true {
			values["exclusiveMinimum"] = branch.Values["minimum"]
			delete(values, "minimum")
		}
		if branch.Values["exclusiveMaximum"] == true {
			values["exclusiveMaximum"] = branch.Values["maximum"]
			delete(values, "maximum")
		}
		schemas[i] = values
	}
	var tags []string
	isInteger := func(t any) bool {
		if t == "integer" {
			return true
		}
		list, ok := t.([]any)
		return ok && slices.Contains(list, any("integer"))
	}
	for _, values := range schemas {
		if isInteger(values["type"]) {
			tags = append(tags, "@integer")
			break
		}
	}
	for _, keyword := range tagKeywords {
		if lowerBounds[keyword] || upperBounds[keyword] {
			var numbers []float64
			for _, values := range schemas {
				if n, ok := values[keyword].(float64); ok {
					numbers = append(numbers, n)
				}
			}
			if len(numbers) == 0 {
				continue
			}
			bound := numbers[0]
			for _, n := range numbers[1:] {
				if lowerBounds[keyword] {
					bound = math.Max(bound, n)
				} else {
					bound = math.Min(bound, n)
				}
			}
			tags = append(tags, "@"+keyword+" "+jsnum.String(bound))
			continue
		}
		var seen []any
		for _, values := range schemas {
			value := values[keyword]
			if slices.ContainsFunc(seen, func(other any) bool { return sameZero(other, value) }) {
				continue
			}
			seen = append(seen, value)
			switch v := value.(type) {
			case float64:
				tags = append(tags, "@"+keyword+" "+jsnum.String(v))
			case string:
				tags = append(tags, "@"+keyword+" "+v)
			}
		}
	}
	for _, values := range schemas {
		if values["uniqueItems"] == true {
			tags = append(tags, "@uniqueItems")
			break
		}
	}
	// Annotations, unlike constraints, come from the property itself.
	if value, ok := property.Values["default"]; ok && value != nil {
		tags = append(tags, "@default "+js.Stringify(value))
	}
	if property.Values["deprecated"] == true {
		tags = append(tags, "@deprecated")
	}
	var kept []string
	for _, tag := range tags {
		if js.Len(tag) > maxTagChars {
			continue
		}
		kept = append(kept, lineTerminators.ReplaceAllStringFunc(tag, func(char string) string {
			r, _ := utf8.DecodeRuneInString(char)
			return fmt.Sprintf(`\u%04x`, r)
		}))
	}
	return strings.Join(kept, " ")
}

// sameZero is SameValueZero for parsed JSON values: primitives by value,
// objects and arrays by identity.
func sameZero(a, b any) bool {
	switch x := a.(type) {
	case *js.Object:
		y, ok := b.(*js.Object)
		return ok && x == y
	case []any:
		y, ok := b.([]any)
		return ok && len(x) == len(y) && (len(x) == 0 || &x[0] == &y[0])
	}
	switch b.(type) {
	case *js.Object, []any:
		return false
	}
	return a == b
}

func propertyKey(name string) string {
	if identifierPattern.MatchString(name) {
		return name
	}
	return js.Stringify(name)
}

// lessUTF16 orders strings as JavaScript's default sort does: by UTF-16 code
// units.
func lessUTF16(a, b string) bool {
	x, y := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(x) && i < len(y); i++ {
		if x[i] != y[i] {
			return x[i] < y[i]
		}
	}
	return len(x) < len(y)
}

func objectType(schema *js.Object, context *typeContext) string {
	properties, ok := schema.Values["properties"].(*js.Object)
	if !ok {
		properties = &js.Object{Values: map[string]any{}}
	}
	required := map[string]bool{}
	if list, ok := schema.Values["required"].([]any); ok {
		for _, name := range list {
			if name, ok := name.(string); ok {
				required[name] = true
			}
		}
	}
	names := slices.Clone(properties.Keys)
	slices.SortStableFunc(names, func(a, b string) int {
		switch {
		case lessUTF16(a, b):
			return -1
		case lessUTF16(b, a):
			return 1
		}
		return 0
	})
	members := make([]string, 0, len(names)+1)
	for _, name := range names {
		optional := "?"
		if required[name] {
			optional = ""
		}
		members = append(members, propertyKey(name)+optional+": "+toType(properties.Values[name], context)+";")
	}
	additional, present := schema.Values["additionalProperties"]
	switch {
	case present && additional != false:
		t := "unknown"
		if additional != true {
			t = toType(additional, context)
		}
		members = append(members, "[key: string]: "+t+";")
	case !present && len(names) == 0:
		members = append(members, "[key: string]: unknown;")
	}
	if len(members) == 0 {
		return "{}"
	}
	comments := make([][]string, len(names))
	any := false
	for i, name := range names {
		var lines []string
		for _, line := range lineTerminators.Split(descriptionOf(properties.Values[name]), -1) {
			if line = js.Trim(line); line != "" {
				lines = append(lines, line)
			}
		}
		if context.tags {
			if tags := constraintTags(properties.Values[name]); tags != "" {
				lines = append(lines, tags)
			}
		}
		comments[i] = lines
		any = any || len(lines) > 0
	}
	if !any {
		return "{ " + strings.Join(members, " ") + " }"
	}
	lines := []string{"{"}
	for i, comment := range comments {
		for _, line := range comment {
			lines = append(lines, indent+"// "+line)
		}
		lines = append(lines, indent+strings.ReplaceAll(members[i], "\n", "\n"+indent))
	}
	for _, member := range members[len(names):] {
		lines = append(lines, indent+member)
	}
	lines = append(lines, "}")
	return strings.Join(lines, "\n")
}
