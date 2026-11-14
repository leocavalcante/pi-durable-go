package codemode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// Identifier is the name a script uses for a tool (upstream
// toCodemodeIdentifier): characters that are not valid in a JavaScript
// identifier become "_". mcp__docs__search stays as is, my-tool becomes
// my_tool.
func Identifier(name string) string {
	var out strings.Builder
	for _, r := range name {
		valid := r == '_' || r == '$' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || out.Len() > 0 && r >= '0' && r <= '9'
		if valid {
			out.WriteRune(r)
		} else {
			out.WriteByte('_')
		}
	}
	if out.Len() == 0 {
		return "_"
	}
	return out.String()
}

// OptionsPrefix starts a script's optional first line of options.
const OptionsPrefix = "// @options:"

// SourceGrammar is the Lark grammar of the code mode tool's input, for
// providers with grammar-constrained tool input. It fixes only the shape of
// the options line; ParseSource checks the options and the code.
const SourceGrammar = `
start: options_source | plain_source
options_source: OPTIONS_LINE NEWLINE SOURCE
plain_source: SOURCE

OPTIONS_LINE: /[ \t]*\/\/ @options:[^\r\n]*/
NEWLINE: /\r?\n/
SOURCE: /[\s\S]+/
`

const (
	supportedFieldsText = "`max_output_tokens` and `timeout_ms`"
	// maxTimeoutMs is the largest delay setTimeout supports.
	maxTimeoutMs = 2_147_483_647
)

// SourceOptions are a script's options line: max_output_tokens and
// timeout_ms, nil when absent.
type SourceOptions struct {
	MaxOutputTokens *float64
	TimeoutMs       *float64
}

// SourceError is a script's input that is not code mode source.
type SourceError struct{ Message string }

func (e *SourceError) Error() string { return e.Message }

// ParseSource splits an optional first line "// @options: {...}" from the
// script (upstream parseCodemodeSource). It fails for empty input, invalid
// options, and an options line that is not the first line, which would
// otherwise be ignored without notice.
func ParseSource(input string) (code string, options SourceOptions, err error) {
	if js.Trim(input) == "" {
		return "", SourceOptions{}, &SourceError{"Expected JavaScript source text (non-empty). Provide JS only, optionally with a first line `// @options: {\"max_output_tokens\": 1000}`."}
	}
	lines := strings.Split(input, "\n")
	for index, line := range lines {
		if index > 0 && strings.HasPrefix(js.TrimStart(line), OptionsPrefix) {
			return "", SourceOptions{}, &SourceError{fmt.Sprintf("The %s line must be the first line of the input; found one on line %d", OptionsPrefix, index+1)}
		}
	}
	firstLine := strings.TrimSuffix(lines[0], "\r")
	trimmed := js.TrimStart(firstLine)
	if !strings.HasPrefix(trimmed, OptionsPrefix) {
		return input, SourceOptions{}, nil
	}
	if newline := strings.IndexByte(input, '\n'); newline != -1 {
		code = input[newline:]
	}
	if js.Trim(code) == "" {
		return "", SourceOptions{}, &SourceError{"The @options line must be followed by JavaScript source on subsequent lines"}
	}
	options, err = parseOptions(js.Trim(trimmed[len(OptionsPrefix):]))
	if err != nil {
		return "", SourceOptions{}, err
	}
	return code, options, nil
}

func parseOptions(directive string) (SourceOptions, error) {
	notObject := &SourceError{"@options must be a JSON object with supported fields " + supportedFieldsText}
	if directive == "" {
		return SourceOptions{}, notObject
	}
	if message := js.ParseError(directive); message != "" {
		return SourceOptions{}, &SourceError{"@options must be valid JSON with supported fields " + supportedFieldsText + ": " + message}
	}
	value, err := js.Parse([]byte(directive))
	if err != nil {
		return SourceOptions{}, &SourceError{"@options must be valid JSON with supported fields " + supportedFieldsText + ": " + err.Error()}
	}
	fields, ok := value.(*js.Object)
	if !ok {
		return SourceOptions{}, notObject
	}
	for _, key := range fields.Keys {
		if key != "max_output_tokens" && key != "timeout_ms" {
			return SourceOptions{}, &SourceError{fmt.Sprintf("@options only supports %s; got `%s`", supportedFieldsText, key)}
		}
	}
	var options SourceOptions
	if raw, ok := fields.Values["max_output_tokens"]; ok {
		n, ok := safeInteger(raw)
		if !ok {
			return SourceOptions{}, &SourceError{"@options field `max_output_tokens` must be a non-negative safe integer"}
		}
		options.MaxOutputTokens = &n
	}
	if raw, ok := fields.Values["timeout_ms"]; ok {
		n, ok := safeInteger(raw)
		if !ok || n == 0 || n > maxTimeoutMs {
			return SourceOptions{}, &SourceError{fmt.Sprintf("@options field `timeout_ms` must be a positive integer up to %d", maxTimeoutMs)}
		}
		options.TimeoutMs = &n
	}
	return options, nil
}

// safeInteger is a number that Number.isSafeInteger accepts and that is not
// negative.
func safeInteger(value any) (float64, bool) {
	n, ok := value.(float64)
	if !ok || n != math.Trunc(n) || math.Abs(n) > 1<<53-1 || n < 0 {
		return 0, false
	}
	return n, true
}

// sameJSON is structural JSON equality; key order does not matter.
func sameJSON(a, b json.RawMessage) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == 0 && len(b) == 0
	}
	if bytes.Equal(a, b) {
		return true
	}
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return sameValue(x, y)
}

func sameValue(a, b any) bool {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for key, value := range x {
			other, ok := y[key]
			if !ok || !sameValue(value, other) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !sameValue(x[i], y[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
