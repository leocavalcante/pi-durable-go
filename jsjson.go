package durable

import (
	"encoding/json"
	"strings"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// JavaScript's JSON (internal/js): values as JSON.parse leaves them and
// JSON.stringify writes them, for text upstream builds from parsed JSON (tool
// call arguments in a summarized transcript, and their lengths in estimates).

// jsObject is a parsed JSON object in JavaScript's property order.
type jsObject = js.Object

// parseJS parses JSON into nil, bool, float64, string, []any, or *jsObject.
func parseJS(raw []byte) (any, error) {
	value, err := js.Parse(raw)
	if err != nil {
		return nil, ErrInvalid
	}
	return value, nil
}

func jsStringify(value any) string                 { return js.Stringify(value) }
func writeJSString(out *strings.Builder, s string) { js.WriteString(out, s) }
func jsStringifyRaw(raw json.RawMessage) string    { return js.StringifyRaw(raw) }
func jsStringifyIndent(value any) string           { return js.StringifyIndent(value) }
func wtf8String(units []uint16) string             { return js.WTF8(units) }
func v8JSONParseError(text string) string          { return js.ParseError(text) }
func jsTrim(s string) string                       { return js.Trim(s) }
func utf16Len(text string) int                     { return js.Len(text) }
func isHexDigit(c uint16) bool                     { return js.IsHexDigit(c) }
