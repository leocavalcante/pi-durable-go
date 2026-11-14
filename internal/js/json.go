// Package js ports the JavaScript behaviors upstream text depends on:
// values as JSON.parse leaves them and JSON.stringify writes them, V8's
// JSON.parse messages, string trimming and lengths in UTF-16 code units, and
// Number.prototype.toFixed.
package js

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/leocavalcante/pi-durable-go/internal/jsnum"
)

// errInvalid: the text is not one JSON value.
var errInvalid = errors.New("js: invalid JSON")

// Object is a parsed JSON object in JavaScript's property order: integer
// keys (array indexes) first, ascending, then the others in insertion order;
// a repeated key keeps its first place and its last value.
type Object struct {
	Keys   []string
	Values map[string]any
}

// Parse parses JSON into nil, bool, float64, string, []any, or *Object.
func Parse(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := parseValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errInvalid
	}
	return value, nil
}

func parseValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch t := token.(type) {
	case json.Delim:
		switch t {
		case '{':
			object := &Object{Values: map[string]any{}}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyToken.(string)
				value, err := parseValue(decoder)
				if err != nil {
					return nil, err
				}
				if _, seen := object.Values[key]; !seen {
					object.Keys = append(object.Keys, key)
				}
				object.Values[key] = value
			}
			if _, err := decoder.Token(); err != nil {
				return nil, err
			}
			object.Order()
			return object, nil
		case '[':
			var items []any
			for decoder.More() {
				item, err := parseValue(decoder)
				if err != nil {
					return nil, err
				}
				items = append(items, item)
			}
			if _, err := decoder.Token(); err != nil {
				return nil, err
			}
			if items == nil {
				items = []any{}
			}
			return items, nil
		}
	case json.Number:
		f, err := strconv.ParseFloat(string(t), 64)
		if err != nil && !math.IsInf(f, 0) {
			return nil, err
		}
		return f, nil
	case string, bool, nil:
		return t, nil
	}
	return nil, errInvalid
}

// Order puts integer keys first, ascending, as JavaScript enumerates them.
func (o *Object) Order() {
	index := func(key string) (uint64, bool) {
		if key == "" || len(key) > 1 && key[0] == '0' {
			return 0, false
		}
		n, err := strconv.ParseUint(key, 10, 32)
		return n, err == nil && n < math.MaxUint32
	}
	sort.SliceStable(o.Keys, func(i, j int) bool {
		a, aIndex := index(o.Keys[i])
		b, bIndex := index(o.Keys[j])
		switch {
		case aIndex && bIndex:
			return a < b
		case aIndex != bIndex:
			return aIndex
		}
		return false
	})
}

// Stringify is JSON.stringify of a parsed value.
func Stringify(value any) string {
	var out strings.Builder
	Write(&out, value)
	return out.String()
}

func Write(out *strings.Builder, value any) {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case float64:
		if math.IsInf(v, 0) || math.IsNaN(v) {
			out.WriteString("null")
		} else {
			out.WriteString(jsnum.String(v))
		}
	case string:
		WriteString(out, v)
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			Write(out, item)
		}
		out.WriteByte(']')
	case *Object:
		out.WriteByte('{')
		for i, key := range v.Keys {
			if i > 0 {
				out.WriteByte(',')
			}
			WriteString(out, key)
			out.WriteByte(':')
			Write(out, v.Values[key])
		}
		out.WriteByte('}')
	}
}

// WriteString quotes a string as JSON.stringify does: the two escapes,
// the short control escapes, \u00XX for other control characters, and
// everything else as it is.
func WriteString(out *strings.Builder, s string) {
	out.WriteByte('"')
	for i := 0; i < len(s); {
		// A lone surrogate (WTF8) is escaped, as JSON.stringify does.
		if s[i] == 0xED && i+2 < len(s) && s[i+1] >= 0xA0 && s[i+1] <= 0xBF {
			fmt.Fprintf(out, `\u%04x`, 0xD000|uint16(s[i+1]&0x3F)<<6|uint16(s[i+2]&0x3F))
			i += 3
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if r < 0x20 {
				out.WriteString(`\u00`)
				out.WriteString(strconv.FormatInt(int64(r)>>4, 16))
				out.WriteString(strconv.FormatInt(int64(r)&0xf, 16))
			} else {
				out.WriteRune(r)
			}
		}
	}
	out.WriteByte('"')
}

// StringifyRaw is JSON.stringify(JSON.parse(raw)); raw itself when it
// does not parse.
func StringifyRaw(raw json.RawMessage) string {
	value, err := Parse(raw)
	if err != nil {
		return string(raw)
	}
	return Stringify(value)
}

// StringifyIndent is JSON.stringify(value, null, 2) of a parsed value.
func StringifyIndent(value any) string {
	var out strings.Builder
	writeIndent(&out, value, "")
	return out.String()
}

func writeIndent(out *strings.Builder, value any, indent string) {
	inner := indent + "  "
	switch v := value.(type) {
	case []any:
		if len(v) == 0 {
			out.WriteString("[]")
			return
		}
		out.WriteString("[\n")
		for i, item := range v {
			if i > 0 {
				out.WriteString(",\n")
			}
			out.WriteString(inner)
			writeIndent(out, item, inner)
		}
		out.WriteString("\n" + indent + "]")
	case *Object:
		if len(v.Keys) == 0 {
			out.WriteString("{}")
			return
		}
		out.WriteString("{\n")
		for i, key := range v.Keys {
			if i > 0 {
				out.WriteString(",\n")
			}
			out.WriteString(inner)
			WriteString(out, key)
			out.WriteString(": ")
			writeIndent(out, v.Values[key], inner)
		}
		out.WriteString("\n" + indent + "}")
	default:
		Write(out, value)
	}
}

// WTF8 is JavaScript string text as a Go string: surrogate pairs
// become their characters, and a lone surrogate keeps its three-byte
// encoding (WTF-8), which WriteString escapes as JSON.stringify does.
func WTF8(units []uint16) string {
	var b strings.Builder
	for i := 0; i < len(units); i++ {
		u := units[i]
		switch {
		case utf16.IsSurrogate(rune(u)) && u < 0xDC00 && i+1 < len(units) && units[i+1] >= 0xDC00 && units[i+1] <= 0xDFFF:
			b.WriteRune(utf16.DecodeRune(rune(u), rune(units[i+1])))
			i++
		case utf16.IsSurrogate(rune(u)):
			b.Write([]byte{0xE0 | byte(u>>12), 0x80 | byte(u>>6)&0x3F, 0x80 | byte(u)&0x3F})
		default:
			b.WriteRune(rune(u))
		}
	}
	return b.String()
}
