package durable

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strings"
)

// jsonEqual compares JSON values without floating-point rounding. Number
// exponents are normalized symbolically, so even large exponents stay bounded
// by the input's size rather than allocating the expanded number.
func jsonEqual(a, b json.RawMessage) bool {
	decode := func(data json.RawMessage) (any, error) {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		var value any
		err := dec.Decode(&value)
		return value, err
	}
	x, err := decode(a)
	if err != nil {
		return bytes.Equal(a, b)
	}
	y, err := decode(b)
	return err == nil && equalJSONValue(x, y)
}

func equalJSONValue(a, b any) bool {
	switch a := a.(type) {
	case json.Number:
		v, ok := b.(json.Number)
		return ok && normalizedNumber(a.String()) == normalizedNumber(v.String())
	case map[string]any:
		v, ok := b.(map[string]any)
		if !ok || len(a) != len(v) {
			return false
		}
		for key, value := range a {
			other, exists := v[key]
			if !exists || !equalJSONValue(value, other) {
				return false
			}
		}
		return true
	case []any:
		v, ok := b.([]any)
		if !ok || len(a) != len(v) {
			return false
		}
		for i := range a {
			if !equalJSONValue(a[i], v[i]) {
				return false
			}
		}
		return true
	default:
		return a == b // JSON scalars only; maps and arrays handled above.
	}
}

func normalizedNumber(value string) string {
	negative := strings.HasPrefix(value, "-")
	value = strings.TrimPrefix(value, "-")
	var exponent big.Int
	if i := strings.IndexAny(value, "eE"); i >= 0 {
		exponent.SetString(value[i+1:], 10)
		value = value[:i]
	}
	if i := strings.IndexByte(value, '.'); i >= 0 {
		exponent.Sub(&exponent, big.NewInt(int64(len(value)-i-1)))
		value = value[:i] + value[i+1:]
	}
	value = strings.TrimLeft(value, "0")
	if value == "" {
		return "0"
	}
	trimmed := strings.TrimRight(value, "0")
	exponent.Add(&exponent, big.NewInt(int64(len(value)-len(trimmed))))
	if negative {
		trimmed = "-" + trimmed
	}
	return trimmed + "e" + exponent.String()
}
