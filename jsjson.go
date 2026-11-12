package durable

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// JavaScript's JSON: values as JSON.parse leaves them and JSON.stringify
// writes them, for text upstream builds from parsed JSON (tool call
// arguments in a summarized transcript, and their lengths in estimates).

// jsObject is a parsed JSON object in JavaScript's property order: integer
// keys (array indexes) first, ascending, then the others in insertion order;
// a repeated key keeps its first place and its last value.
type jsObject struct {
	keys   []string
	values map[string]any
}

// parseJS parses JSON into nil, bool, float64, string, []any, or *jsObject.
func parseJS(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := parseJSValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, ErrInvalid
	}
	return value, nil
}

func parseJSValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch t := token.(type) {
	case json.Delim:
		switch t {
		case '{':
			object := &jsObject{values: map[string]any{}}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyToken.(string)
				value, err := parseJSValue(decoder)
				if err != nil {
					return nil, err
				}
				if _, seen := object.values[key]; !seen {
					object.keys = append(object.keys, key)
				}
				object.values[key] = value
			}
			if _, err := decoder.Token(); err != nil {
				return nil, err
			}
			object.order()
			return object, nil
		case '[':
			var items []any
			for decoder.More() {
				item, err := parseJSValue(decoder)
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
	return nil, ErrInvalid
}

// order puts integer keys first, ascending, as JavaScript enumerates them.
func (o *jsObject) order() {
	index := func(key string) (uint64, bool) {
		if key == "" || len(key) > 1 && key[0] == '0' {
			return 0, false
		}
		n, err := strconv.ParseUint(key, 10, 32)
		return n, err == nil && n < math.MaxUint32
	}
	sort.SliceStable(o.keys, func(i, j int) bool {
		a, aIndex := index(o.keys[i])
		b, bIndex := index(o.keys[j])
		switch {
		case aIndex && bIndex:
			return a < b
		case aIndex != bIndex:
			return aIndex
		}
		return false
	})
}

// jsStringify is JSON.stringify of a parsed value.
func jsStringify(value any) string {
	var out strings.Builder
	writeJS(&out, value)
	return out.String()
}

func writeJS(out *strings.Builder, value any) {
	switch v := value.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case float64:
		if math.IsInf(v, 0) || math.IsNaN(v) {
			out.WriteString("null")
		} else {
			out.WriteString(jsNumberString(v))
		}
	case string:
		writeJSString(out, v)
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			writeJS(out, item)
		}
		out.WriteByte(']')
	case *jsObject:
		out.WriteByte('{')
		for i, key := range v.keys {
			if i > 0 {
				out.WriteByte(',')
			}
			writeJSString(out, key)
			out.WriteByte(':')
			writeJS(out, v.values[key])
		}
		out.WriteByte('}')
	}
}

// writeJSString quotes a string as JSON.stringify does: the two escapes,
// the short control escapes, \u00XX for other control characters, and
// everything else as it is.
func writeJSString(out *strings.Builder, s string) {
	out.WriteByte('"')
	for i := 0; i < len(s); {
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

// jsStringifyRaw is JSON.stringify(JSON.parse(raw)); raw itself when it
// does not parse.
func jsStringifyRaw(raw json.RawMessage) string {
	value, err := parseJS(raw)
	if err != nil {
		return string(raw)
	}
	return jsStringify(value)
}
