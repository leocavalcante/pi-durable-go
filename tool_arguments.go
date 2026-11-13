package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ParseToolCallArguments parses a finished tool call's complete arguments
// text as pi-ai's parseToolCallArguments does, for model adapters: JSON, or
// JSON whose string literals pi-ai's repair fixes, that is an object gives
// the arguments; other text gives empty arguments and an argumentsError
// (ToolCall.ArgumentsError) that names the problem and includes the text,
// which tool validation reports to the model. Empty text is no arguments.
func ParseToolCallArguments(text string) (json.RawMessage, string) {
	if strings.TrimFunc(text, isJSWhitespace) == "" {
		return json.RawMessage(`{}`), ""
	}
	parsed, err := parseJSONWithRepair(text)
	if err == nil && (len(parsed) == 0 || parsed[0] != '{') {
		err = errors.New("Tool arguments must be a JSON object")
	}
	if err != nil {
		return json.RawMessage(`{}`), fmt.Sprintf("%s\n\nReceived arguments:\n%s", err.Error(), text)
	}
	return parsed, ""
}

// parseJSONWithRepair is pi-ai's parseJsonWithRepair: the text, or its
// repair when the text is not JSON and the repair changes it; the error is
// V8's message for the text the last parse read. The value is trimmed of
// surrounding whitespace.
func parseJSONWithRepair(text string) (json.RawMessage, error) {
	message := v8JSONParseError(text)
	if message == "" {
		return json.RawMessage(strings.Trim(text, " \t\n\r")), nil
	}
	repaired := repairJSON(text)
	if repaired == text {
		return nil, errors.New(message)
	}
	if message := v8JSONParseError(repaired); message != "" {
		return nil, errors.New(message)
	}
	return json.RawMessage(strings.Trim(repaired, " \t\n\r")), nil
}

// repairJSON is pi-ai's repairJson: inside string literals it escapes raw
// control characters and doubles backslashes before characters that cannot
// be escaped; other text is unchanged.
func repairJSON(text string) string {
	var b strings.Builder
	inString := false
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRuneInString(text[i:])
		if !inString {
			b.WriteString(text[i : i+size])
			inString = r == '"'
			i += size
			continue
		}
		switch {
		case r == '"':
			b.WriteByte('"')
			inString = false
		case r == '\\':
			if i+1 >= len(text) {
				b.WriteString(`\\`)
				break
			}
			next := text[i+1]
			if next == 'u' && i+6 <= len(text) && isHexString(text[i+2:i+6]) {
				b.WriteString(text[i : i+6])
				i += 6
				continue
			}
			if strings.IndexByte(`"\/bfnrt`, next) >= 0 {
				b.WriteString(text[i : i+2])
				i += 2
				continue
			}
			b.WriteString(`\\`)
		case r < 0x20:
			b.WriteString(jsonControlEscape(r))
		default:
			b.WriteString(text[i : i+size])
		}
		i += size
	}
	return b.String()
}

func isHexString(s string) bool {
	for i := 0; i < len(s); i++ {
		if !isHexDigit(uint16(s[i])) {
			return false
		}
	}
	return true
}

// jsonControlEscape is pi-ai's escapeControlCharacter.
func jsonControlEscape(r rune) string {
	switch r {
	case '\b':
		return `\b`
	case '\f':
		return `\f`
	case '\n':
		return `\n`
	case '\r':
		return `\r`
	case '\t':
		return `\t`
	}
	return fmt.Sprintf(`\u%04x`, r)
}

// isJSWhitespace is what String.prototype.trim removes: ECMAScript's
// WhiteSpace and LineTerminator characters.
func isJSWhitespace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xA0, 0x1680, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF:
		return true
	}
	return r >= 0x2000 && r <= 0x200A
}
