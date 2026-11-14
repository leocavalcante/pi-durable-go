package js

import (
	"fmt"
	"unicode/utf16"
)

// ParseError is the message V8's JSON.parse throws for text, or ""
// when text is JSON. pi-ai passes these messages to the model in a tool
// call's argumentsError, so they are ported from V8's json-parser.cc: the
// expected-token messages at the cursor with its line and column, the end of
// input where a value is due, and other unexpected characters quoted with
// the source around them. Positions count UTF-16 code units, as JavaScript
// strings do.
func ParseError(text string) string {
	p := &v8JSONParser{s: utf16.Encode([]rune(text))}
	p.skipWhitespace()
	if p.parseValue() {
		p.skipWhitespace()
		if p.pos < len(p.s) {
			p.fail("Unexpected non-whitespace character after JSON")
		}
	}
	return p.err
}

type v8JSONParser struct {
	s   []uint16
	pos int
	err string
}

const (
	// V8's kMaxContextCharacters, and kMinOriginalSourceLengthForContext.
	v8ContextCharacters = 10
	v8ContextMinLength  = 2*v8ContextCharacters + 1
)

func (p *v8JSONParser) atEnd() bool { return p.pos >= len(p.s) }

func (p *v8JSONParser) skipWhitespace() {
	for p.pos < len(p.s) {
		switch p.s[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

// fail reports an explicit message at the cursor (V8 ReportUnexpectedToken
// with an error message), with its line and column.
func (p *v8JSONParser) fail(message string) {
	line, column := p.location()
	p.err = fmt.Sprintf("%s at position %d (line %d column %d)", message, p.pos, line, column)
}

// location is V8's CalculateFileLocation: \r\n counts as one line break.
func (p *v8JSONParser) location() (int, int) {
	line, lineStart := 1, 0
	for i := 0; i < p.pos; i++ {
		if p.s[i] == '\r' && i < p.pos-1 && p.s[i+1] == '\n' {
			i++
		}
		if p.s[i] == '\r' || p.s[i] == '\n' {
			line++
			lineStart = i + 1
		}
	}
	return line, p.pos - lineStart + 1
}

// unexpected reports the token at the cursor without an explicit message
// (V8 LookUpErrorMessageForJsonToken).
func (p *v8JSONParser) unexpected() {
	if p.atEnd() {
		p.err = "Unexpected end of JSON input"
		return
	}
	c := p.s[p.pos]
	switch {
	case c == '-' || c >= '0' && c <= '9':
		p.fail("Unexpected number in JSON")
		return
	case c == '"':
		p.fail("Unexpected string in JSON")
		return
	}
	source := string(utf16.Decode(p.s))
	switch source {
	case "[object Object]", "undefined", "Infinity", "NaN":
		p.err = fmt.Sprintf("%q is not valid JSON", source)
		return
	}
	token := string(utf16.Decode([]uint16{c}))
	if len(p.s) < v8ContextMinLength {
		p.err = fmt.Sprintf("Unexpected token '%s', \"%s\" is not valid JSON", token, source)
		return
	}
	switch {
	case p.pos < v8ContextCharacters:
		p.err = fmt.Sprintf("Unexpected token '%s', \"%s\"... is not valid JSON", token, string(utf16.Decode(p.s[:p.pos+v8ContextCharacters])))
	case p.pos < len(p.s)-v8ContextCharacters:
		p.err = fmt.Sprintf("Unexpected token '%s', ...\"%s\"... is not valid JSON", token, string(utf16.Decode(p.s[p.pos-v8ContextCharacters:p.pos+v8ContextCharacters])))
	default:
		p.err = fmt.Sprintf("Unexpected token '%s', ...\"%s\" is not valid JSON", token, string(utf16.Decode(p.s[p.pos-v8ContextCharacters:])))
	}
}

func (p *v8JSONParser) parseValue() bool {
	p.skipWhitespace()
	if p.atEnd() {
		p.unexpected()
		return false
	}
	switch c := p.s[p.pos]; {
	case c == '"':
		return p.parseString()
	case c == '{':
		return p.parseObject()
	case c == '[':
		return p.parseArray()
	case c == '-' || c >= '0' && c <= '9':
		return p.parseNumber()
	case c == 't':
		return p.parseLiteral("true")
	case c == 'f':
		return p.parseLiteral("false")
	case c == 'n':
		return p.parseLiteral("null")
	}
	p.unexpected()
	return false
}

// parseLiteral is V8's ScanLiteral: the first mismatch is unexpected, and
// the end of input before the literal ends too.
func (p *v8JSONParser) parseLiteral(literal string) bool {
	p.pos++
	for i := 1; i < len(literal); i++ {
		if p.atEnd() || p.s[p.pos] != uint16(literal[i]) {
			p.unexpected()
			return false
		}
		p.pos++
	}
	return true
}

func (p *v8JSONParser) digit() bool {
	return !p.atEnd() && p.s[p.pos] >= '0' && p.s[p.pos] <= '9'
}

// parseNumber is V8's ParseJsonNumber.
func (p *v8JSONParser) parseNumber() bool {
	if p.s[p.pos] == '-' {
		p.pos++
	}
	if !p.atEnd() && p.s[p.pos] == '0' {
		p.pos++
		// A leading zero is the only digit before a point or an exponent.
		if p.digit() {
			p.unexpected()
			return false
		}
	} else {
		if !p.digit() {
			p.fail("No number after minus sign in JSON")
			return false
		}
		for p.digit() {
			p.pos++
		}
	}
	if !p.atEnd() && p.s[p.pos] == '.' {
		p.pos++
		if !p.digit() {
			p.fail("Unterminated fractional number in JSON")
			return false
		}
		for p.digit() {
			p.pos++
		}
	}
	if !p.atEnd() && (p.s[p.pos] == 'e' || p.s[p.pos] == 'E') {
		p.pos++
		if !p.atEnd() && (p.s[p.pos] == '-' || p.s[p.pos] == '+') {
			p.pos++
		}
		if !p.digit() {
			p.fail("Exponent part is missing a number in JSON")
			return false
		}
		for p.digit() {
			p.pos++
		}
	}
	return true
}

// parseString is V8's ScanJsonString, from the opening quote.
func (p *v8JSONParser) parseString() bool {
	p.pos++
	for {
		if p.atEnd() {
			p.fail("Unterminated string in JSON")
			return false
		}
		c := p.s[p.pos]
		switch {
		case c == '"':
			p.pos++
			return true
		case c == '\\':
			p.pos++
			if p.atEnd() || p.s[p.pos] > 0xFF {
				p.unexpected()
				return false
			}
			switch p.s[p.pos] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				p.pos++
			case 'u':
				for i := 0; i < 4; i++ {
					p.pos++
					if p.atEnd() || !IsHexDigit(p.s[p.pos]) {
						p.fail("Bad Unicode escape in JSON")
						return false
					}
				}
				p.pos++
			default:
				p.fail("Bad escaped character in JSON")
				return false
			}
		case c < 0x20:
			p.fail("Bad control character in string literal in JSON")
			return false
		default:
			p.pos++
		}
	}
}

// IsHexDigit reports whether c is an ASCII hex digit.
func IsHexDigit(c uint16) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// parseObject: a property name or '}' first, then a colon after each name,
// and a comma or '}' after each value, each with V8's message.
func (p *v8JSONParser) parseObject() bool {
	p.pos++
	p.skipWhitespace()
	if !p.atEnd() && p.s[p.pos] == '}' {
		p.pos++
		return true
	}
	expected := "Expected property name or '}' in JSON"
	for {
		p.skipWhitespace()
		if p.atEnd() || p.s[p.pos] != '"' {
			p.fail(expected)
			return false
		}
		if !p.parseString() {
			return false
		}
		p.skipWhitespace()
		if p.atEnd() || p.s[p.pos] != ':' {
			p.fail("Expected ':' after property name in JSON")
			return false
		}
		p.pos++
		if !p.parseValue() {
			return false
		}
		p.skipWhitespace()
		switch {
		case !p.atEnd() && p.s[p.pos] == ',':
			p.pos++
			expected = "Expected double-quoted property name in JSON"
		case !p.atEnd() && p.s[p.pos] == '}':
			p.pos++
			return true
		default:
			p.fail("Expected ',' or '}' after property value in JSON")
			return false
		}
	}
}

// parseArray: values separated by commas, then ']'.
func (p *v8JSONParser) parseArray() bool {
	p.pos++
	p.skipWhitespace()
	if !p.atEnd() && p.s[p.pos] == ']' {
		p.pos++
		return true
	}
	for {
		if !p.parseValue() {
			return false
		}
		p.skipWhitespace()
		switch {
		case !p.atEnd() && p.s[p.pos] == ',':
			p.pos++
		case !p.atEnd() && p.s[p.pos] == ']':
			p.pos++
			return true
		default:
			p.fail("Expected ',' or ']' after array element in JSON")
			return false
		}
	}
}
