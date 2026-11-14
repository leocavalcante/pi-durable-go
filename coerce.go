package durable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/leocavalcante/pi-durable-go/internal/jsnum"
)

// Tool arguments are coerced before they are checked, as pi-ai's
// validateToolArguments does with pi-schema's parse() (compile with
// coerce: true): models often send "42" for a number, a single value for an
// array, or null for an omitted optional property. Three passes, in order
// (pi-schema coerce.ts):
//  1. Optional nulls: a null in an optional property whose schema rejects
//     null is dropped, as if omitted.
//  2. Conversion rules from TypeBox's Value.Convert, keyed on JSON Schema
//     structure ($ref, const, enum, anyOf/oneOf, allOf, type, items or
//     prefixItems, properties or patternProperties).
//  3. Primitive coercion by type (also type arrays), as Ajv's coerceTypes.
//
// A value that cannot be coerced is left as it is; the check then reports
// it. Unchanged arguments keep their bytes.

// toolValidator checks and coerces values for one tool schema. Sub-schema
// checks compile on first use.
type toolValidator struct {
	root     any
	schema   *jsonschema.Schema
	mu       sync.Mutex
	compiler *jsonschema.Compiler
	checks   map[string]*jsonschema.Schema
	// keys are the schema's objects' keys in JavaScript's order (integer keys
	// first, then as written), by JSON pointer: pi-schema walks pattern
	// properties and finds a union's tag in that order.
	keys map[string][]string
}

// keyOrders records each object of a schema's text by JSON pointer, with
// its keys in JavaScript's order.
func keyOrders(raw json.RawMessage) map[string][]string {
	value, err := parseJS(raw)
	if err != nil {
		return nil
	}
	orders := map[string][]string{}
	var walk func(value any, pointer string)
	walk = func(value any, pointer string) {
		switch node := value.(type) {
		case *jsObject:
			orders[pointer] = node.Keys
			for _, key := range node.Keys {
				walk(node.Values[key], pointerTo(pointer, key))
			}
		case []any:
			for i, item := range node {
				walk(item, pointerTo(pointer, strconv.Itoa(i)))
			}
		}
	}
	walk(value, "")
	return orders
}

// orderedKeys are m's keys in the order the schema's text has them at
// pointer, then keys it does not have (a lenient reading added them),
// sorted.
func (v *toolValidator) orderedKeys(m map[string]any, pointer string) []string {
	order, ok := v.keys[pointer]
	if !ok {
		return sortedKeys(m)
	}
	keys := make([]string, 0, len(m))
	known := make(map[string]bool, len(order))
	for _, key := range order {
		if _, present := m[key]; present && !known[key] {
			keys = append(keys, key)
		}
		known[key] = true
	}
	var extra []string
	for key := range m {
		if !known[key] {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	return append(keys, extra...)
}

const toolSchemaLocation = "urn:pi-durable-go:tool"

// schemaPrinter words the failures pi-schema has no wording for.
var schemaPrinter = message.NewPrinter(language.English)

// toolValidators caches validators by schema, as pi-ai caches compiled
// validators per schema object (a WeakMap that lets them go with their
// tools): at most maxToolValidators, the oldest leaving first.
var toolValidators = struct {
	sync.Mutex
	byKey map[string]*toolValidator
	order []string
}{byKey: map[string]*toolValidator{}}

const maxToolValidators = 1024

func toolValidatorFor(schema json.RawMessage) (*toolValidator, error) {
	if len(schema) == 0 {
		schema = json.RawMessage(`{"type":"object"}`)
	}
	toolValidators.Lock()
	cached := toolValidators.byKey[string(schema)]
	toolValidators.Unlock()
	if cached != nil {
		return cached, nil
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	if err != nil {
		return nil, fmt.Errorf("invalid tool schema: %w", err)
	}
	// A $ref that names nothing, or names something outside the schema,
	// rejects only values at its place (pi-ai: unresolvedRefs "reject"),
	// instead of making every call fail; nothing is ever loaded.
	document = rejectUnresolvedRefs(document, document)
	// pi-schema compiles any schema it can read and ignores keyword values
	// of the wrong shape; the Go compiler checks schemas against their
	// metaschema, so they are made to pass it first.
	document = lenientSchema(document, !olderDraft(document))
	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(noSchemaNetwork{})
	compiler.UseRegexpEngine(jsRegexpEngine)
	if err := compiler.AddResource(toolSchemaLocation, document); err != nil {
		return nil, fmt.Errorf("invalid tool schema: %w", err)
	}
	compiled, err := compiler.Compile(toolSchemaLocation)
	if err != nil {
		return nil, fmt.Errorf("invalid tool schema: %w", err)
	}
	v := &toolValidator{root: document, schema: compiled, compiler: compiler, checks: map[string]*jsonschema.Schema{"": compiled}, keys: keyOrders(schema)}
	toolValidators.Lock()
	defer toolValidators.Unlock()
	if existing := toolValidators.byKey[string(schema)]; existing != nil {
		return existing, nil
	}
	if len(toolValidators.order) >= maxToolValidators {
		delete(toolValidators.byKey, toolValidators.order[0])
		toolValidators.order = toolValidators.order[1:]
	}
	toolValidators.byKey[string(schema)] = v
	toolValidators.order = append(toolValidators.order, string(schema))
	return v, nil
}

// rejectUnresolvedRefs replaces each $ref that is not a JSON pointer into
// root, or points at nothing, with a schema that rejects every value.
func rejectUnresolvedRefs(node, root any) any {
	switch n := node.(type) {
	case map[string]any:
		out := make(map[string]any, len(n))
		for key, value := range n {
			if key == "$ref" {
				continue
			}
			if key == "const" || key == "enum" || key == "default" || key == "examples" {
				out[key] = value // data, not schemas
				continue
			}
			out[key] = rejectUnresolvedRefs(value, root)
		}
		if ref, ok := n["$ref"].(string); ok {
			if _, resolved := resolvePointer(root, ref); resolved || strings.HasPrefix(ref, "#") && !strings.HasPrefix(ref, "#/") && ref != "#" {
				out["$ref"] = ref // a resolvable pointer, or an anchor the compiler resolves
			} else {
				all, _ := out["allOf"].([]any)
				out["allOf"] = append(append([]any(nil), all...), false)
			}
		}
		return out
	case []any:
		out := make([]any, len(n))
		for i, value := range n {
			out[i] = rejectUnresolvedRefs(value, root)
		}
		return out
	}
	return node
}

// resolvePointer is the node a local "#/..." reference names.
func resolvePointer(root any, ref string) (any, bool) {
	if ref == "#" {
		return root, true
	}
	if !strings.HasPrefix(ref, "#/") {
		return nil, false
	}
	node := root
	for _, token := range strings.Split(ref[2:], "/") {
		// A URI fragment: percent escapes first, then the pointer's own.
		if unescaped, err := url.PathUnescape(token); err == nil {
			token = unescaped
		}
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch n := node.(type) {
		case map[string]any:
			next, ok := n[token]
			if !ok {
				return nil, false
			}
			node = next
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(n) {
				return nil, false
			}
			node = n[i]
		default:
			return nil, false
		}
	}
	return node, true
}

// decodedPointer is a "#/..." reference as a plain JSON pointer.
func decodedPointer(ref string) string {
	pointer := strings.TrimPrefix(ref, "#")
	if pointer == "" {
		return ""
	}
	tokens := strings.Split(pointer[1:], "/")
	for i, token := range tokens {
		if unescaped, err := url.PathUnescape(token); err == nil {
			tokens[i] = unescaped
		}
	}
	return "/" + strings.Join(tokens, "/")
}

// fragmentOf is a JSON pointer as a URI fragment: each token escaped.
func fragmentOf(pointer string) string {
	if pointer == "" {
		return ""
	}
	tokens := strings.Split(pointer[1:], "/")
	for i, token := range tokens {
		tokens[i] = url.PathEscape(token)
	}
	return "/" + strings.Join(tokens, "/")
}

// passes reports whether value passes the schema at pointer, and false for
// ok when that schema cannot be compiled.
func (v *toolValidator) passes(pointer string, value any) (passes, ok bool) {
	v.mu.Lock()
	compiled, cached := v.checks[pointer]
	if !cached {
		var err error
		compiled, err = v.compiler.Compile(toolSchemaLocation + "#" + fragmentOf(pointer))
		if err != nil {
			compiled = nil
		}
		v.checks[pointer] = compiled
	}
	v.mu.Unlock()
	if compiled == nil {
		return false, false
	}
	return compiled.Validate(value) == nil, true
}

// validateToolArguments coerces a call's arguments for its tool's schema and
// checks them (pi-ai validateToolArguments): the arguments to execute with,
// the same bytes when coercion changes nothing.
func validateToolArguments(spec ToolSpec, args json.RawMessage) (json.RawMessage, error) {
	v, err := toolValidatorFor(spec.Parameters)
	if err != nil {
		return nil, err
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(args))
	if err != nil {
		return nil, fmt.Errorf("invalid tool arguments: %w", err)
	}
	coerced, changed := v.coerce(value)
	if err := v.schema.Validate(coerced); err != nil {
		return nil, v.validationFailure(spec.Name, err, coerced, args)
	}
	if !changed {
		return args, nil
	}
	out, err := json.Marshal(coerced)
	if err != nil {
		return nil, fmt.Errorf("invalid tool arguments: %w", err)
	}
	return out, nil
}

// validationFailure is pi-ai's message: each failure at its dotted path in
// pi-schema's words, then the arguments received.
func (v *toolValidator) validationFailure(name string, err error, checked any, received json.RawMessage) error {
	var lines []string
	if failure, ok := err.(*jsonschema.ValidationError); ok {
		// Every issue, a repeated one too, as pi-ai lists them.
		for _, issue := range v.explain(failure, checked, keyOrders(received)) {
			lines = append(lines, fmt.Sprintf("  - %s: %s", validationPath(issue.pointer), issue.message))
		}
	}
	if len(lines) == 0 {
		lines = append(lines, "  - root: "+err.Error())
	}
	// JSON.stringify(arguments, null, 2): their own key order.
	shown := string(received)
	if parsed, err := parseJS(received); err == nil {
		shown = jsStringifyIndent(parsed)
	}
	return fmt.Errorf("Validation failed for tool %q:\n%s\n\nReceived arguments:\n%s", name, strings.Join(lines, "\n"), shown)
}

// issue is one failure as pi-schema's explain words it.
type issue struct{ pointer, message string }

func pointerOf(location []string) string {
	return pointerTo("", location...)
}

// schemaAt is the schema node an error's schema location names.
func (v *toolValidator) schemaAt(schemaURL string) (any, bool) {
	_, fragment, found := strings.Cut(schemaURL, "#")
	if !found {
		return nil, false
	}
	return resolvePointer(v.root, "#"+fragment)
}

// explain words a validation error as pi-schema's explain does: missing and
// unknown properties at their own paths, a union by the branch it was most
// likely meant for, and the rest by keyword.
//
// Issues come in pi-schema's order (orderIssues); argumentKeys are the
// arguments' objects' keys as received, by JSON pointer.
func (v *toolValidator) explain(e *jsonschema.ValidationError, instance any, argumentKeys map[string][]string) []issue {
	var out []issue
	location := e.InstanceLocation
	at := func(message string) { out = append(out, issue{pointerOf(location), message}) }
	switch k := e.ErrorKind.(type) {
	case *kind.AnyOf:
		return v.explainUnion(e, instance, false, argumentKeys)
	case *kind.OneOf:
		if len(k.Subschemas) > 1 {
			return []issue{{pointerOf(location), "matches more than one alternative"}}
		}
		return v.explainUnion(e, instance, true, argumentKeys)
	case *kind.Required:
		for _, missing := range k.Missing {
			out = append(out, issue{pointerTo(pointerOf(location), missing), "missing"})
		}
	case *kind.AdditionalProperties:
		for _, property := range k.Properties {
			out = append(out, issue{pointerTo(pointerOf(location), property), "unknown field"})
		}
	case *kind.Type:
		want := k.Want
		if node, ok := v.schemaAt(e.SchemaURL); ok {
			if schema, isObject := node.(map[string]any); isObject {
				if list := typesOf(schema); len(list) > 0 {
					want = list
				}
			}
		}
		at(fmt.Sprintf("expected %s, got %s", strings.Join(want, " or "), typeName(valueAt(instance, location))))
	case *kind.Enum:
		at("expected one of " + listValues(k.Want))
	case *kind.Const:
		want, _ := json.Marshal(k.Want)
		at("expected " + string(want))
	case *kind.MinLength:
		at(fmt.Sprintf("expected at least %d characters", k.Want))
	case *kind.MaxLength:
		at(fmt.Sprintf("expected at most %d characters", k.Want))
	case *kind.Pattern:
		at("expected to match " + k.Want)
	case *kind.Format:
		at("expected format " + k.Want)
	case *kind.Minimum:
		at("expected >= " + ratString(k.Want))
	case *kind.Maximum:
		at("expected <= " + ratString(k.Want))
	case *kind.ExclusiveMinimum:
		at("expected > " + ratString(k.Want))
	case *kind.ExclusiveMaximum:
		at("expected < " + ratString(k.Want))
	case *kind.MultipleOf:
		at("expected a multiple of " + ratString(k.Want))
	case *kind.MinProperties:
		at(fmt.Sprintf("expected at least %d properties", k.Want))
	case *kind.MaxProperties:
		at(fmt.Sprintf("expected at most %d properties", k.Want))
	case *kind.MinItems:
		at(fmt.Sprintf("expected at least %d items", k.Want))
	case *kind.MaxItems:
		at(fmt.Sprintf("expected at most %d items", k.Want))
	case *kind.UniqueItems:
		at("expected unique items")
	case *kind.Not:
		at("must not match the schema in not")
	case *kind.FalseSchema:
		at("no value is allowed here")
	case *kind.AdditionalItems:
		at("unexpected item")
	case *kind.Contains:
		at("expected at least 1 matching item")
	case *kind.MinContains:
		at(containsMessage(k.Want, -1))
	case *kind.MaxContains:
		at(containsMessage(0, k.Want))
	case *kind.Group, *kind.AllOf, *kind.Reference, *kind.Schema:
	default:
		if len(e.Causes) == 0 {
			at(e.ErrorKind.LocalizedString(schemaPrinter))
		}
	}
	return append(out, v.orderIssues(e, instance, argumentKeys)...)
}

// orderIssues explains an error's causes in the order pi-schema walks an
// object: missing properties, then the declared properties in the schema's
// order, then each other key in the arguments' order (its pattern failures
// in the schema's order, or unknown), then the object's other constraints.
// The checker reports failures flat, so each issue is ranked by its whole
// path through the schema; ties keep the checker's order.
func (v *toolValidator) orderIssues(e *jsonschema.ValidationError, instance any, argumentKeys map[string][]string) []issue {
	type ranked struct {
		issue issue
		rank  []int
	}
	var issues []ranked
	for _, cause := range e.Causes {
		for _, found := range v.explain(cause, instance, argumentKeys) {
			issues = append(issues, ranked{found, v.issueRank(cause, found, argumentKeys)})
		}
	}
	sort.SliceStable(issues, func(i, j int) bool { return slices.Compare(issues[i].rank, issues[j].rank) < 0 })
	out := make([]issue, len(issues))
	for i, r := range issues {
		out[i] = r.issue
	}
	return out
}

// issueRank is an issue's place in pi-schema's walk: at each object on the
// way, (0) for its missing properties, (1, index) for a declared property
// in the schema's order, (2, key index, pattern index) for another key in
// the arguments' order, and (3) for the object's other constraints.
func (v *toolValidator) issueRank(cause *jsonschema.ValidationError, found issue, argumentKeys map[string][]string) []int {
	indexOf := func(order []string, key string) int {
		for i, candidate := range order {
			if candidate == key {
				return i
			}
		}
		return len(order)
	}
	_, fragment, _ := strings.Cut(cause.SchemaURL, "#")
	var tokens []string
	for _, token := range strings.Split(fragment, "/")[1:] {
		if unescaped, err := url.PathUnescape(token); err == nil {
			token = unescaped
		}
		tokens = append(tokens, strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~"))
	}
	location := cause.InstanceLocation
	var rank []int
	schema, depth := "", 0
	instanceAt := func() string { return pointerTo("", location[:min(depth, len(location))]...) }
	keyAt := func() string {
		if depth < len(location) {
			return location[depth]
		}
		return ""
	}
	for i := 0; i < len(tokens); i++ {
		switch token := tokens[i]; {
		case token == "properties" && i+1 < len(tokens):
			rank = append(rank, 1, indexOf(v.keys[pointerTo(schema, "properties")], tokens[i+1]), 0)
			schema, depth, i = pointerTo(schema, "properties", tokens[i+1]), depth+1, i+1
		case token == "patternProperties" && i+1 < len(tokens):
			rank = append(rank, 2, indexOf(argumentKeys[instanceAt()], keyAt()), indexOf(v.keys[pointerTo(schema, "patternProperties")], tokens[i+1]))
			schema, depth, i = pointerTo(schema, "patternProperties", tokens[i+1]), depth+1, i+1
		case token == "additionalProperties":
			rank = append(rank, 2, indexOf(argumentKeys[instanceAt()], keyAt()), -1)
			schema, depth = pointerTo(schema, "additionalProperties"), depth+1
		case (token == "items" || token == "prefixItems" || token == "additionalItems") && depth < len(location):
			index, _ := strconv.Atoi(location[depth])
			rank = append(rank, 1, index, 0)
			schema, depth = pointerTo(schema, token), depth+1
			if i+1 < len(tokens) && token != "additionalItems" {
				if _, err := strconv.Atoi(tokens[i+1]); err == nil {
					schema, i = pointerTo(schema, tokens[i+1]), i+1
				}
			}
		default:
			schema = pointerTo(schema, token)
		}
	}
	switch cause.ErrorKind.(type) {
	case *kind.Required:
		rank = append(rank, 0, 0, 0)
	case *kind.AdditionalProperties:
		key := ""
		if rest, ok := strings.CutPrefix(found.pointer, pointerOf(location)+"/"); ok {
			key, _, _ = strings.Cut(rest, "/")
			key = strings.ReplaceAll(strings.ReplaceAll(key, "~1", "/"), "~0", "~")
		}
		rank = append(rank, 2, indexOf(argumentKeys[pointerOf(location)], key), -1)
	case *kind.MinProperties, *kind.MaxProperties, *kind.PropertyNames, *kind.DependentRequired, *kind.Dependency:
		rank = append(rank, 3, 0, 0)
	}
	return rank
}

// explainUnion is pi-schema's walkUnion for a union nothing matched: a
// tagged union reports the branch its tag selects; otherwise an object is
// blamed on the branch that declares most of its properties, then on the
// branch whose first error is deepest.
func (v *toolValidator) explainUnion(e *jsonschema.ValidationError, instance any, exactlyOne bool, argumentKeys map[string][]string) []issue {
	path := pointerOf(e.InstanceLocation)
	none := []issue{{path, "matches no alternative"}}
	node, _ := v.schemaAt(e.SchemaURL)
	schema, _ := node.(map[string]any)
	keyword := "anyOf"
	if exactlyOne {
		keyword = "oneOf"
	}
	branches, isList := schema[keyword].([]any)
	if !isList || len(e.Causes) != len(branches) {
		return none
	}
	value := valueAt(instance, e.InstanceLocation)
	resolved := make([]map[string]any, len(branches))
	_, unionPointer, _ := strings.Cut(e.SchemaURL, "#")
	firstPointer := pointerTo(unionPointer, keyword, "0")
	for i, branch := range branches {
		resolved[i], _ = branch.(map[string]any)
		if ref, isRef := resolved[i]["$ref"].(string); isRef {
			target, _ := resolvePointer(v.root, ref)
			resolved[i], _ = target.(map[string]any)
			if i == 0 {
				firstPointer = strings.TrimPrefix(ref, "#")
			}
		}
	}
	object, isObject := value.(map[string]any)
	if key, values, tagged := v.tagOf(resolved, firstPointer); tagged && isObject {
		if got, present := object[key]; present {
			for i, tag := range values {
				if jsonEqualValues(tag, got) {
					return v.explain(e.Causes[i], instance, argumentKeys)
				}
			}
		}
		return []issue{{pointerTo(path, key), "expected one of " + listValues(values)}}
	}
	var best []issue
	bestDeclared := -1
	for i, cause := range e.Causes {
		issues := v.explain(cause, instance, argumentKeys)
		if len(issues) == 0 {
			continue
		}
		declared := 0
		if isObject {
			properties, _ := resolved[i]["properties"].(map[string]any)
			for key := range object {
				if _, ok := properties[key]; ok {
					declared++
				}
			}
		}
		if best == nil || declared > bestDeclared || declared == bestDeclared && len(issues[0].pointer) > len(best[0].pointer) {
			best, bestDeclared = issues, declared
		}
	}
	if best == nil {
		return none
	}
	return best
}

// tagOf is a union's discriminator: the first property, in the first
// branch's order (it is at firstPointer), that every branch declares with a
// distinct primitive const.
func (v *toolValidator) tagOf(branches []map[string]any, firstPointer string) (string, []any, bool) {
	if len(branches) == 0 {
		return "", nil, false
	}
	first, ok := branches[0]["properties"].(map[string]any)
	if !ok {
		return "", nil, false
	}
	for _, key := range v.orderedKeys(first, pointerTo(firstPointer, "properties")) {
		values := make([]any, len(branches))
		distinct := true
		for i, branch := range branches {
			properties, _ := branch["properties"].(map[string]any)
			tag, _ := properties[key].(map[string]any)
			value, has := tag["const"]
			switch value.(type) {
			case map[string]any, []any:
				has = false
			}
			if !has {
				distinct = false
				break
			}
			for _, earlier := range values[:i] {
				if jsonEqualValues(earlier, value) {
					distinct = false
				}
			}
			values[i] = value
		}
		if distinct {
			return key, values, true
		}
	}
	return "", nil, false
}

// valueAt is the instance's value at a location.
func valueAt(instance any, location []string) any {
	value := instance
	for _, token := range location {
		switch v := value.(type) {
		case map[string]any:
			value = v[token]
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(v) {
				return nil
			}
			value = v[i]
		default:
			return nil
		}
	}
	return value
}

// typeName is pi-schema's name of a value's type: integral numbers are
// integer.
func typeName(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	case string:
		return "string"
	case bool:
		return "boolean"
	}
	if f, ok := numberOf(value); ok {
		switch {
		case math.IsInf(f, 1):
			return "Infinity"
		case math.IsInf(f, -1):
			return "-Infinity"
		case f == math.Trunc(f):
			return "integer"
		}
		return "number"
	}
	return fmt.Sprintf("%T", value)
}

// listValues is pi-schema's list of allowed values: at most 20.
func listValues(values []any) string {
	const listed = 20
	data, _ := json.Marshal(values)
	if len(values) <= listed {
		return string(data)
	}
	head, _ := json.Marshal(values[:listed])
	return fmt.Sprintf("%s, ... and %d more]", head[:len(head)-1], len(values)-listed)
}

func containsMessage(min, max int) string {
	switch {
	case max < 0:
		if min == 1 {
			return "expected at least 1 matching item"
		}
		return fmt.Sprintf("expected at least %d matching items", min)
	case min == 0:
		if max == 1 {
			return "expected at most 1 matching item"
		}
		return fmt.Sprintf("expected at most %d matching items", max)
	}
	return fmt.Sprintf("expected between %d and %d matching items", min, max)
}

// ratString is a schema bound as JavaScript prints the number.
func ratString(r *big.Rat) string {
	if r == nil {
		return ""
	}
	if r.IsInt() {
		return r.Num().String()
	}
	f, _ := r.Float64()
	return jsNumberString(f)
}

// validationPath is a JSON pointer as the dotted path the model sees:
// /edits/0/oldText is edits.0.oldText, the empty pointer is root.
func validationPath(pointer string) string {
	if pointer == "" {
		return "root"
	}
	tokens := strings.Split(pointer[1:], "/")
	for i, token := range tokens {
		tokens[i] = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
	}
	return strings.Join(tokens, ".")
}

// maxCoercionSteps bounds the schema nodes one call converts.
const maxCoercionSteps = 100000

// coercion is one coerce call's state: the (node, value) pairs pass 2 is
// converting through a re-entry point, and memoized $ref results.
type coercion struct {
	v       *toolValidator
	entered []enteredPair
	cuts    int
	// steps bounds a call's conversion work: a recursive union whose
	// branches all fail can convert the same values exponentially often
	// (pi-schema memoizes only results computed without a cut). Past the
	// budget, values are left as they are for the check to judge.
	steps int
	memo  map[string]map[any]memoEntry
}

// memoEntry keeps the value it is keyed by alive, so its address is not
// reused by another value during the call (a JavaScript Map holds its keys).
type memoEntry struct {
	value, result any
}

type enteredPair struct {
	pointer  string
	identity any
}

// coerce runs the three passes; changed says whether anything changed.
func (v *toolValidator) coerce(value any) (any, bool) {
	c := &coercion{v: v}
	withoutNulls := c.dropOptionalNulls(value, v.root, "")
	converted := c.convert(v.root, "", withoutNulls)
	c.memo, c.cuts = nil, 0
	out := c.coerceTypes(converted, v.root, "")
	return out, !same(out, value)
}

// identity is a value's identity: maps and slices by reference (pass 2
// copies on write), the rest by value.
func identity(value any) any {
	switch value.(type) {
	case map[string]any, []any:
		rv := reflect.ValueOf(value)
		return [2]uintptr{uintptr(rv.UnsafePointer()), uintptr(rv.Len())}
	}
	return value
}

// same is JavaScript's ===, for values copied on write.
func same(a, b any) bool { return identity(a) == identity(b) }

func pointerTo(pointer string, tokens ...string) string {
	for _, token := range tokens {
		pointer += "/" + strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
	}
	return pointer
}

type pattern struct {
	source string
	re     *jsRegexp
	schema any
}

// patternsOf are the node's pattern properties in the schema's order.
func (v *toolValidator) patternsOf(node map[string]any, pointer string) []pattern {
	properties, ok := node["patternProperties"].(map[string]any)
	if !ok {
		return nil
	}
	var out []pattern
	for _, source := range v.orderedKeys(properties, pointerTo(pointer, "patternProperties")) {
		if re, err := compileJSRegexp(source); err == nil {
			out = append(out, pattern{source: source, re: re, schema: properties[source]})
		}
	}
	return out
}

func copyObject(value map[string]any) map[string]any {
	out := make(map[string]any, len(value))
	for key, member := range value {
		out[key] = member
	}
	return out
}

// mapItems maps items, copying the slice only when an item changes.
func mapItems(items []any, mapItem func(any, int) any) []any {
	var out []any
	for i, item := range items {
		next := mapItem(item, i)
		if same(next, item) {
			continue
		}
		if out == nil {
			out = append([]any(nil), items...)
		}
		out[i] = next
	}
	if out == nil {
		return items
	}
	return out
}

// itemSchemaAt is the schema of an array's item at index: a tuple's for its
// prefix (prefixItems, or a draft-7 items array), items after prefixItems, or
// items for every item; nil when nothing applies.
func itemSchemaAt(schema map[string]any, pointer string, index int) (any, string) {
	if items, ok := schema["items"].([]any); ok {
		if index < len(items) {
			return items[index], pointerTo(pointer, "items", strconv.Itoa(index))
		}
		return nil, ""
	}
	if prefix, ok := schema["prefixItems"].([]any); ok && index < len(prefix) {
		return prefix[index], pointerTo(pointer, "prefixItems", strconv.Itoa(index))
	}
	if schema[uncoercedRest] == true {
		return nil, ""
	}
	if items, ok := schema["items"]; ok {
		return items, pointerTo(pointer, "items")
	}
	return nil, ""
}

// Pass 1: optional nulls.

func (c *coercion) dropOptionalNulls(value any, schema any, pointer string) any {
	node, ok := schema.(map[string]any)
	if !ok {
		return value
	}
	if items, isArray := value.([]any); isArray {
		return mapItems(items, func(item any, i int) any {
			itemSchema, itemPointer := itemSchemaAt(node, pointer, i)
			if itemSchema == nil {
				return item
			}
			return c.dropOptionalNulls(item, itemSchema, itemPointer)
		})
	}
	object, isObject := value.(map[string]any)
	if !isObject {
		return value
	}
	properties, hasProperties := node["properties"].(map[string]any)
	patterns := c.v.patternsOf(node, pointer)
	additional, hasAdditional := node["additionalProperties"].(map[string]any)
	if len(patterns) == 0 && !hasAdditional && !hasProperties {
		return value
	}
	required := map[string]bool{}
	if list, ok := node["required"].([]any); ok {
		for _, name := range list {
			if text, ok := name.(string); ok {
				required[text] = true
			}
		}
	}
	var out map[string]any
	current := func() map[string]any {
		if out != nil {
			return out
		}
		return object
	}
	set := func(key string, next any) {
		if same(next, current()[key]) {
			return
		}
		if out == nil {
			out = copyObject(object)
		}
		out[key] = next
	}
	for _, key := range sortedKeys(properties) {
		property := properties[key]
		member, present := object[key]
		if !present {
			continue
		}
		propertySchema, _ := property.(map[string]any)
		_, isRef := propertySchema["$ref"].(string)
		propertyPointer := pointerTo(pointer, "properties", key)
		if member == nil && !required[key] && !isRef {
			if passes, compiled := c.v.passes(propertyPointer, nil); compiled && !passes {
				if out == nil {
					out = copyObject(object)
				}
				delete(out, key)
				continue
			}
		}
		set(key, c.dropOptionalNulls(member, property, propertyPointer))
	}
	if len(patterns) > 0 || hasAdditional {
		for key := range object {
			_, matched := properties[key]
			for _, p := range patterns {
				if !p.re.MatchString(key) {
					continue
				}
				matched = true
				if member, present := current()[key]; present {
					set(key, c.dropOptionalNulls(member, p.schema, pointerTo(pointer, "patternProperties", p.source)))
				}
			}
			if !matched && hasAdditional {
				if member, present := object[key]; present {
					set(key, c.dropOptionalNulls(member, additional, pointerTo(pointer, "additionalProperties")))
				}
			}
		}
	}
	if out == nil {
		return value
	}
	return out
}

// Pass 2: TypeBox's conversion rules.

func (c *coercion) convert(schema any, pointer string, value any) any {
	node, ok := schema.(map[string]any)
	if !ok {
		return value
	}
	if c.steps++; c.steps > maxCoercionSteps {
		return value
	}
	if ref, isRef := node["$ref"].(string); isRef {
		target, resolved := resolvePointer(c.v.root, ref)
		if !resolved {
			return value
		}
		if _, isObject := target.(map[string]any); !isObject {
			return value
		}
		return c.reenter(target, decodedPointer(ref), value, true)
	}
	return c.convertNode(node, pointer, value)
}

// reenter converts node for value at a re-entry point: unchanged when that
// pair is already being converted.
func (c *coercion) reenter(node any, pointer string, value any, memoize bool) any {
	id := identity(value)
	for _, pair := range c.entered {
		if pair.pointer == pointer && pair.identity == id {
			c.cuts++
			return value
		}
	}
	if memoize {
		if entry, ok := c.memo[pointer][id]; ok {
			return entry.result
		}
	}
	cuts := c.cuts
	c.entered = append(c.entered, enteredPair{pointer: pointer, identity: id})
	result := c.convert(node, pointer, value)
	c.entered = c.entered[:len(c.entered)-1]
	if memoize && c.cuts == cuts {
		if c.memo == nil {
			c.memo = map[string]map[any]memoEntry{}
		}
		if c.memo[pointer] == nil {
			c.memo[pointer] = map[any]memoEntry{}
		}
		c.memo[pointer][id] = memoEntry{value: value, result: result}
	}
	return result
}

// convertNode is one schema node's conversion: its first keyword among
// const, enum, the unions, allOf, and type.
func (c *coercion) convertNode(node map[string]any, pointer string, value any) any {
	if literal, ok := node["const"]; ok {
		return convertLiteral(literal, value)
	}
	if values, ok := node["enum"].([]any); ok {
		return c.convertEnum(pointer, values, value)
	}
	if branches, ok := node["anyOf"].([]any); ok {
		return c.convertUnion(node, pointer, "anyOf", branches, value)
	}
	if branches, ok := node["oneOf"].([]any); ok {
		return c.convertUnion(node, pointer, "oneOf", branches, value)
	}
	if members, ok := node["allOf"].([]any); ok {
		next := value
		for i, member := range members {
			next = c.convert(member, pointerTo(pointer, "allOf", strconv.Itoa(i)), next)
		}
		return c.convertMembers(node, pointer, next)
	}
	kind, hasType := node["type"]
	if !hasType {
		_, properties := node["properties"]
		_, patterns := node["patternProperties"]
		if properties || patterns {
			return c.convertObject(node, pointer, value)
		}
		_, items := node["items"]
		_, prefix := node["prefixItems"]
		if items || prefix {
			return c.convertArray(node, pointer, value)
		}
		return value
	}
	switch kind {
	case "string":
		if converted, ok := tryString(value); ok {
			return converted
		}
	case "number":
		if converted, ok := tryNumber(value); ok {
			return converted
		}
	case "integer":
		if converted, ok := tryNumber(value); ok {
			return truncated(converted)
		}
	case "boolean":
		if converted, ok := tryBoolean(value); ok {
			return converted
		}
	case "null":
		if converted, ok := tryNull(value); ok {
			return converted
		}
	case "array":
		return c.convertArray(node, pointer, value)
	case "object":
		return c.convertObject(node, pointer, value)
	}
	return value
}

// convertEnum: a member stays, else the first value the input converts to.
func (c *coercion) convertEnum(pointer string, values []any, value any) any {
	for _, member := range values {
		if jsonEqualValues(member, value) {
			return value
		}
	}
	for _, member := range values {
		candidate := convertLiteral(member, value)
		if !same(candidate, value) {
			if passes, _ := c.v.passes(pointer, candidate); passes {
				return candidate
			}
		}
	}
	return value
}

// convertUnion: a value one branch accepts stays; else the first branch's
// conversion the whole union accepts.
func (c *coercion) convertUnion(node map[string]any, pointer, keyword string, branches []any, value any) any {
	for i := range branches {
		if passes, _ := c.v.passes(pointerTo(pointer, keyword, strconv.Itoa(i)), value); passes {
			return c.convertMembers(node, pointer, value)
		}
	}
	for i, branch := range branches {
		candidate := c.convertMembers(node, pointer, c.convert(branch, pointerTo(pointer, keyword, strconv.Itoa(i)), value))
		if passes, _ := c.v.passes(pointer, candidate); passes {
			return candidate
		}
	}
	return c.convertMembers(node, pointer, value)
}

// convertMembers converts the object members a union or intersection node
// declares, when it declares any.
func (c *coercion) convertMembers(node map[string]any, pointer string, value any) any {
	_, properties := node["properties"]
	_, patterns := node["patternProperties"]
	_, additional := node["additionalProperties"]
	if properties || patterns || additional {
		return c.convertObject(node, pointer, value)
	}
	return value
}

func (c *coercion) convertArray(node map[string]any, pointer string, value any) any {
	var prefix []any
	prefixKeyword := ""
	if items, ok := node["items"].([]any); ok {
		prefix, prefixKeyword = items, "items"
	} else if items, ok := node["prefixItems"].([]any); ok {
		prefix, prefixKeyword = items, "prefixItems"
	}
	if prefixKeyword != "" {
		// A tuple converts its items and never wraps a single value.
		items, isArray := value.([]any)
		if !isArray {
			return value
		}
		rest, hasRest := node["items"]
		if prefixKeyword == "items" || node[uncoercedRest] == true {
			hasRest = false
		}
		return mapItems(items, func(item any, i int) any {
			if i < len(prefix) {
				return c.convert(prefix[i], pointerTo(pointer, prefixKeyword, strconv.Itoa(i)), item)
			}
			if hasRest {
				return c.convert(rest, pointerTo(pointer, "items"), item)
			}
			return item
		})
	}
	itemSchema, hasItems := node["items"]
	if items, isArray := value.([]any); isArray {
		if !hasItems {
			return value
		}
		return mapItems(items, func(item any, _ int) any {
			return c.convert(itemSchema, pointerTo(pointer, "items"), item)
		})
	}
	// The wrapped single value is converted by the item schema with the
	// same value: a re-entry point.
	if _, isObject := itemSchema.(map[string]any); isObject {
		return []any{c.reenter(itemSchema, pointerTo(pointer, "items"), value, false)}
	}
	return []any{value}
}

func (c *coercion) convertObject(node map[string]any, pointer string, value any) any {
	object, ok := value.(map[string]any)
	if !ok {
		return value
	}
	properties, _ := node["properties"].(map[string]any)
	patterns := c.v.patternsOf(node, pointer)
	additional, hasAdditional := node["additionalProperties"].(map[string]any)
	var out map[string]any
	for key, member := range object {
		next := member
		property, matched := properties[key]
		if matched {
			next = c.convert(property, pointerTo(pointer, "properties", key), next)
		}
		for _, p := range patterns {
			if !p.re.MatchString(key) {
				continue
			}
			matched = true
			next = c.convert(p.schema, pointerTo(pointer, "patternProperties", p.source), next)
		}
		if !matched && hasAdditional {
			next = c.convert(additional, pointerTo(pointer, "additionalProperties"), next)
		}
		if !same(next, member) {
			if out == nil {
				out = copyObject(object)
			}
			out[key] = next
		}
	}
	if out == nil {
		return value
	}
	return out
}

// Pass 3: primitive coercion by type.

func (c *coercion) coerceTypes(value any, schema any, pointer string) any {
	node, ok := schema.(map[string]any)
	if !ok {
		return value
	}
	next := value
	if members, ok := node["allOf"].([]any); ok {
		for i, member := range members {
			next = c.coerceTypes(next, member, pointerTo(pointer, "allOf", strconv.Itoa(i)))
		}
	}
	if branches, ok := node["anyOf"].([]any); ok {
		next = c.coerceUnion(next, pointer, "anyOf", branches)
	}
	if branches, ok := node["oneOf"].([]any); ok {
		next = c.coerceUnion(next, pointer, "oneOf", branches)
	}
	types := typesOf(node)
	matchesMember := false
	if len(types) > 1 {
		for _, kind := range types {
			if matchesType(next, kind) {
				matchesMember = true
				break
			}
		}
	}
	if len(types) > 0 && !matchesMember {
		for _, kind := range types {
			if candidate := coercePrimitive(next, kind); !same(candidate, next) {
				next = candidate
				break
			}
		}
	}
	if contains(types, "object") {
		if object, ok := next.(map[string]any); ok {
			next = c.coerceProperties(object, node, pointer)
		}
	}
	if contains(types, "array") {
		if items, ok := next.([]any); ok {
			next = mapItems(items, func(item any, i int) any {
				itemSchema, itemPointer := itemSchemaAt(node, pointer, i)
				if _, isObject := itemSchema.(map[string]any); !isObject {
					return item
				}
				return c.coerceTypes(item, itemSchema, itemPointer)
			})
		}
	}
	return next
}

func (c *coercion) coerceUnion(value any, pointer, keyword string, branches []any) any {
	for i := range branches {
		if passes, _ := c.v.passes(pointerTo(pointer, keyword, strconv.Itoa(i)), value); passes {
			return value
		}
	}
	for i, branch := range branches {
		branchPointer := pointerTo(pointer, keyword, strconv.Itoa(i))
		coerced := c.coerceTypes(value, branch, branchPointer)
		if passes, _ := c.v.passes(branchPointer, coerced); passes {
			return coerced
		}
	}
	return value
}

func (c *coercion) coerceProperties(object map[string]any, node map[string]any, pointer string) any {
	properties, _ := node["properties"].(map[string]any)
	patterns := c.v.patternsOf(node, pointer)
	additional, hasAdditional := node["additionalProperties"].(map[string]any)
	var out map[string]any
	current := func() map[string]any {
		if out != nil {
			return out
		}
		return object
	}
	set := func(key string, next any) {
		if same(next, current()[key]) {
			return
		}
		if out == nil {
			out = copyObject(object)
		}
		out[key] = next
	}
	for _, key := range sortedKeys(properties) {
		if member, present := object[key]; present {
			set(key, c.coerceTypes(member, properties[key], pointerTo(pointer, "properties", key)))
		}
	}
	for key := range object {
		_, matched := properties[key]
		for _, p := range patterns {
			if !p.re.MatchString(key) {
				continue
			}
			matched = true
			set(key, c.coerceTypes(current()[key], p.schema, pointerTo(pointer, "patternProperties", p.source)))
		}
		if !matched && hasAdditional {
			set(key, c.coerceTypes(object[key], additional, pointerTo(pointer, "additionalProperties")))
		}
	}
	if out == nil {
		return object
	}
	return out
}

func typesOf(node map[string]any) []string {
	switch kind := node["type"].(type) {
	case string:
		return []string{kind}
	case []any:
		var out []string
		for _, member := range kind {
			if text, ok := member.(string); ok {
				out = append(out, text)
			}
		}
		return out
	}
	return nil
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func matchesType(value any, kind string) bool {
	switch kind {
	case "number":
		f, ok := numberOf(value)
		return ok && !math.IsInf(f, 0) && !math.IsNaN(f)
	case "integer":
		f, ok := numberOf(value)
		return ok && f == math.Trunc(f) && !math.IsInf(f, 0)
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "null":
		return value == nil
	case "array":
		_, ok := value.([]any)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	}
	return false
}

func coercePrimitive(value any, kind string) any {
	switch kind {
	case "number", "integer":
		if value == nil {
			return float64(0)
		}
		if text, ok := value.(string); ok && jsTrim(text) != "" {
			if parsed, ok := jsNumber(text); ok && !math.IsInf(parsed, 0) && (kind == "number" || parsed == math.Trunc(parsed)) {
				return parsed
			}
		}
		if b, ok := value.(bool); ok {
			if b {
				return float64(1)
			}
			return float64(0)
		}
	case "boolean":
		if value == nil {
			return false
		}
		if text, ok := value.(string); ok {
			switch text {
			case "true":
				return true
			case "false":
				return false
			}
		}
		if f, ok := numberOf(value); ok {
			switch f {
			case 1:
				return true
			case 0:
				return false
			}
		}
	case "string":
		if value == nil {
			return ""
		}
		if f, ok := numberOf(value); ok {
			return jsNumberString(f)
		}
		if b, ok := value.(bool); ok {
			return strconv.FormatBool(b)
		}
	case "null":
		if text, ok := value.(string); ok && text == "" {
			return nil
		}
		if f, ok := numberOf(value); ok && f == 0 {
			return nil
		}
		if b, ok := value.(bool); ok && !b {
			return nil
		}
	}
	return value
}

// Literal conversions (TypeBox's Try* functions).

// numberOf is a JSON number's value.
func numberOf(value any) (float64, bool) {
	switch n := value.(type) {
	case json.Number:
		f, err := strconv.ParseFloat(string(n), 64)
		if err != nil && !math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	case float64:
		return n, true
	}
	return 0, false
}

func finiteNumber(value any) (float64, bool) {
	f, ok := numberOf(value)
	return f, ok && !math.IsInf(f, 0) && !math.IsNaN(f)
}

// convertLiteral is a const or enum value taken from a value of another type
// that converts to exactly it.
func convertLiteral(literal, value any) any {
	if jsonEqualValues(literal, value) {
		return value
	}
	var converted any
	var ok bool
	switch literal.(type) {
	case string:
		converted, ok = tryString(value)
	case json.Number, float64:
		converted, ok = tryNumber(value)
	case bool:
		converted, ok = tryBoolean(value)
	}
	if ok && jsonEqualValues(converted, literal) {
		return converted
	}
	return value
}

// jsonEqualValues is === for JSON primitives: numbers by value.
func jsonEqualValues(a, b any) bool {
	fa, aNumber := numberOf(a)
	fb, bNumber := numberOf(b)
	if aNumber || bNumber {
		return aNumber && bNumber && fa == fb
	}
	switch a.(type) {
	case map[string]any, []any:
		return same(a, b)
	}
	switch b.(type) {
	case map[string]any, []any:
		return false
	}
	return a == b
}

func tryString(value any) (any, bool) {
	if f, ok := finiteNumber(value); ok {
		return jsNumberString(f), true
	}
	switch v := value.(type) {
	case bool:
		return strconv.FormatBool(v), true
	case nil:
		return "null", true
	case string:
		return v, true
	}
	return nil, false
}

func tryNumber(value any) (any, bool) {
	if _, ok := finiteNumber(value); ok {
		return value, true
	}
	switch v := value.(type) {
	case bool:
		if v {
			return float64(1), true
		}
		return float64(0), true
	case nil:
		return float64(0), true
	case string:
		if f, ok := jsNumber(v); ok && !math.IsInf(f, 0) {
			return f, true
		}
		switch strings.ToLower(v) {
		case "false":
			return float64(0), true
		case "true":
			return float64(1), true
		}
		if n, ok := bigIntOfString(v); ok && n.IsInt64() && math.Abs(float64(n.Int64())) <= 1<<53-1 {
			return float64(n.Int64()), true
		}
	}
	return nil, false
}

// truncated is Math.trunc of a number, keeping an integer's representation.
func truncated(value any) any {
	f, _ := numberOf(value)
	if f == math.Trunc(f) {
		return value
	}
	return math.Trunc(f)
}

var (
	bigIntSuffix  = regexp.MustCompile(`^-?(0|[1-9]\d*)n$`)
	bigIntDecimal = regexp.MustCompile(`^-?(0|[1-9]\d*)\.\d+$`)
	bigIntPlain   = regexp.MustCompile(`^-?(0|[1-9]\d*)$`)
)

// bigIntOfString is TypeBox's TryBigInt of a string: 12n, 12.5 (truncated),
// or 12.
func bigIntOfString(value string) (*big.Int, bool) {
	var digits string
	switch {
	case bigIntSuffix.MatchString(value):
		digits = value[:len(value)-1]
	case bigIntDecimal.MatchString(value):
		digits = value[:strings.IndexByte(value, '.')]
	case bigIntPlain.MatchString(value):
		digits = value
	default:
		return nil, false
	}
	n, ok := new(big.Int).SetString(digits, 10)
	return n, ok
}

func tryBoolean(value any) (any, bool) {
	if f, ok := finiteNumber(value); ok {
		switch f {
		case 0:
			return false, true
		case 1:
			return true, true
		}
		return nil, false
	}
	switch v := value.(type) {
	case bool:
		return v, true
	case nil:
		return false, true
	case string:
		lower := strings.ToLower(v)
		if lower == "false" || v == "0" {
			return false, true
		}
		if lower == "true" || v == "1" {
			return true, true
		}
	}
	return nil, false
}

func tryNull(value any) (any, bool) {
	if f, ok := finiteNumber(value); ok {
		if f == 0 {
			return nil, true
		}
		return nil, false
	}
	switch v := value.(type) {
	case bool:
		return nil, !v
	case nil:
		return nil, true
	case string:
		lower := strings.ToLower(v)
		return nil, lower == "undefined" || lower == "null" || v == "" || v == "0"
	}
	return nil, false
}

// JavaScript number semantics.

var jsDecimal = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)

// jsNumber is Number(string): false for NaN; ±Inf for out-of-range and
// "Infinity".
func jsNumber(s string) (float64, bool) {
	t := jsTrim(s)
	if t == "" {
		return 0, true
	}
	if len(t) > 2 && t[0] == '0' {
		base := 0
		switch t[1] {
		case 'x', 'X':
			base = 16
		case 'o', 'O':
			base = 8
		case 'b', 'B':
			base = 2
		}
		if base != 0 {
			digits := t[2:]
			if digits == "" || digits[0] == '+' || digits[0] == '-' || strings.Contains(digits, "_") {
				return 0, false
			}
			n, ok := new(big.Int).SetString(digits, base)
			if !ok {
				return 0, false
			}
			f, _ := new(big.Float).SetInt(n).Float64()
			return f, true
		}
	}
	switch t {
	case "Infinity", "+Infinity":
		return math.Inf(1), true
	case "-Infinity":
		return math.Inf(-1), true
	}
	if !jsDecimal.MatchString(t) {
		return 0, false
	}
	f, err := strconv.ParseFloat(t, 64)
	if err != nil && !math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// jsNumberString is Number.prototype.toString().
func jsNumberString(f float64) string { return jsnum.String(f) }

// prepareArguments runs a tool's PrepareArguments on a copy of the
// arguments; a failure makes them invalid.
func prepareArguments(def *ToolDefinition, args json.RawMessage) (json.RawMessage, error) {
	if def.PrepareArguments == nil {
		return args, nil
	}
	var prepared json.RawMessage
	if err := containPanic(func() error {
		var err error
		prepared, err = def.PrepareArguments(cloneRaw(args))
		return err
	}); err != nil {
		return nil, err
	}
	if !json.Valid(prepared) {
		return nil, fmt.Errorf("%w: prepared arguments are not JSON", ErrInvalid)
	}
	return prepared, nil
}

// firstSchemaFailure checks a value against a schema without coercion and
// words its first failure as pi-schema does; ok when it passes.
func firstSchemaFailure(schema, raw json.RawMessage) (path, message string, ok bool) {
	v, err := toolValidatorFor(schema)
	if err != nil {
		return "root", err.Error(), false
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return "root", "invalid", false
	}
	failure := v.schema.Validate(value)
	if failure == nil {
		return "", "", true
	}
	if validation, isValidation := failure.(*jsonschema.ValidationError); isValidation {
		if issues := v.explain(validation, value, keyOrders(raw)); len(issues) > 0 {
			return validationPath(issues[0].pointer), issues[0].message, false
		}
	}
	return "root", "invalid", false
}

// olderDraft reports whether a schema declares draft 4, 6, or 7, which the
// compiler then reads as such (draft-7 tuples included).
func olderDraft(document any) bool {
	root, ok := document.(map[string]any)
	if !ok {
		return false
	}
	declared, _ := root["$schema"].(string)
	return strings.Contains(declared, "draft-04") || strings.Contains(declared, "draft-06") || strings.Contains(declared, "draft-07")
}

// uncoercedRest marks a tuple whose rest items came from draft 7's
// additionalItems: they are checked but not coerced.
const uncoercedRest = "x-pi-durable-uncoerced-rest"

var schemaTypes = map[string]bool{"null": true, "boolean": true, "object": true, "array": true, "number": true, "integer": true, "string": true}

// lenientSchema is a schema as pi-schema reads it, in a form the 2020-12
// metaschema accepts: draft-7 tuples become prefixItems (when tuples is
// set), keyword values of the wrong shape are dropped as pi-schema ignores
// them, and an invalid type or an empty union matches nothing, as there.
func lenientSchema(node any, tuples bool) any {
	object, ok := node.(map[string]any)
	if !ok {
		return node
	}
	out := make(map[string]any, len(object))
	never := false
	schema := func(value any) (any, bool) {
		switch value.(type) {
		case bool, map[string]any:
			return lenientSchema(value, tuples), true
		}
		return nil, false
	}
	schemas := func(value any) ([]any, bool) {
		list, ok := value.([]any)
		if !ok {
			return nil, false
		}
		out := make([]any, 0, len(list))
		for _, member := range list {
			if converted, ok := schema(member); ok {
				out = append(out, converted)
			}
		}
		return out, true
	}
	schemaMap := func(value any) (map[string]any, bool) {
		members, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		out := make(map[string]any, len(members))
		for name, member := range members {
			if converted, ok := schema(member); ok {
				out[name] = converted
			}
		}
		return out, true
	}
	count := func(value any) bool {
		f, ok := numberOf(value)
		return ok && f >= 0 && f == math.Trunc(f)
	}
	for key, value := range object {
		switch key {
		case "type":
			switch v := value.(type) {
			case string:
				if schemaTypes[v] {
					out[key] = v
				} else {
					never = true
				}
			case []any:
				var valid []any
				for _, member := range v {
					if name, ok := member.(string); ok && schemaTypes[name] {
						valid = append(valid, name)
					}
				}
				if len(valid) == 0 {
					never = true
				} else {
					out[key] = valid
				}
			default:
				never = true
			}
		case "properties", "patternProperties", "$defs", "definitions", "dependentSchemas":
			if converted, ok := schemaMap(value); ok {
				out[key] = converted
			}
		case "items":
			if list, ok := value.([]any); ok && tuples {
				if converted, ok := schemas(list); ok {
					out["prefixItems"] = converted
				}
				continue
			}
			if converted, ok := schema(value); ok {
				out[key] = converted
			} else if converted, ok := schemas(value); ok && !tuples {
				out[key] = converted
			}
		case "additionalItems":
			if converted, ok := schema(value); ok {
				if _, tuple := object["items"].([]any); tuple && tuples {
					// Checked like draft 7's additionalItems, which pi-schema
					// never coerces.
					out["items"] = converted
					out[uncoercedRest] = true
				} else if !tuples {
					out[key] = converted
				}
			}
		case "prefixItems":
			if converted, ok := schemas(value); ok {
				out[key] = converted
			}
		case "additionalProperties", "not", "if", "then", "else", "contains", "propertyNames", "unevaluatedProperties", "unevaluatedItems", "contentSchema":
			if converted, ok := schema(value); ok {
				out[key] = converted
			}
		case "anyOf", "oneOf":
			if converted, ok := schemas(value); ok {
				if len(converted) == 0 {
					never = true
				} else {
					out[key] = converted
				}
			}
		case "allOf":
			if converted, ok := schemas(value); ok && len(converted) > 0 {
				out[key] = converted
			}
		case "required":
			if list, ok := value.([]any); ok {
				var names []any
				seen := map[string]bool{}
				for _, member := range list {
					if name, ok := member.(string); ok && !seen[name] {
						seen[name] = true
						names = append(names, name)
					}
				}
				out[key] = append([]any{}, names...)
			}
		case "enum":
			if list, ok := value.([]any); ok {
				out[key] = list
			}
		case "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "minContains", "maxContains":
			if count(value) {
				out[key] = value
			}
		case "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum":
			if _, ok := numberOf(value); ok {
				out[key] = value
			} else if b, isBool := value.(bool); isBool && !tuples {
				out[key] = b // draft 4's boolean exclusive bounds
			}
		case "multipleOf":
			if f, ok := numberOf(value); ok && f > 0 {
				out[key] = value
			}
		case "pattern", "format", "$ref", "$id", "$anchor", "$schema", "$comment", "title", "description":
			if _, ok := value.(string); ok {
				out[key] = value
			}
		case "uniqueItems":
			if _, ok := value.(bool); ok {
				out[key] = value
			}
		default:
			out[key] = value
		}
	}
	if never {
		all, _ := out["allOf"].([]any)
		out["allOf"] = append(append([]any(nil), all...), false)
	}
	return out
}
