//go:build crosscheck

package durable

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// A finished call's arguments text as pi-ai reads it (parseToolCallArguments,
// with its repair), and V8's JSON.parse messages, which its errors carry.
func TestCrosscheckFinishedToolArguments(t *testing.T) {
	long := `{"key":"` + strings.Repeat("v", 30) + `",oops"more":"` + strings.Repeat("w", 30) + `"}`
	texts := []string{"", "   ", "\u00a0", "{}", `{"a":1}`, ` {"a":1} `, "[]", `[1]`, `"str"`, "1", "null", "true",
		`{"a":1`, `{"a":1,}`, `{a:1}`, `{"a" 1}`, `{"a":1 "b":2}`, `[1,2`, `{"a":tru}`, "{\"a\":\"x\ny\"}", `{"a":"\q"}`,
		`{"a":-}`, `{"a":1e}`, `{"a":1.}`, `{"a":1} x`, "x", `{"a":"unterminated`, `{"a":"\u12G4"}`, `{"a":[1 2]}`, "nul",
		`{"long":"` + strings.Repeat("x", 40) + `" oops}`, `{"a":01}`, `{"a":1}}`, `"str`, `{"a":NaN}`, `{,}`, `{"a":1,"b"}`,
		`{"a":1,"b":}`, `[,1]`, `{"k":"v"`, `{"k":"v",`, `{"k":"v","`, `{"k":"v","x`, `{"k"`, `{"k":`, `{"a":[`, `{"a":[1,`,
		`{"a":{"b":1}`, "{", "[", "-", "1.", "1e", "1e+", `"\u`, `"\u12`, "undefined", "[object Object]", "NaN", "Infinity",
		"{\"a\":1}\n\nx", "{\r\n\"a\":1,\r\n}", `{"é":1,}`, `{"😀":1,}`, strings.Repeat("x", 30), `{"a":1}` + strings.Repeat(" ", 20) + "x",
		long, `{"a":z` + strings.Repeat("1", 30) + `}`, "t", "tr", "true x", "-a", "0x10", "1.5.2", ".5", "+1", `{"a":1,,}`,
		`{"":1}`, "[1,]", "[]]", "{}}", "\t{\"a\":\t1}x", "\"\x00\"", "\u00a0{}", "{\"a\":\"b\"\u00a0}", `{"a": [1, 2, {"b": tru}]}`,
		"123abc", "{\"a\":1}\u2028", `{"a":-01}`, `{"a":1E5x}`, `{"a":"\uD83D"}`, `[1,2,3,4,5,6,7,8,9,10,11,12,13 14]`, `[[[[`,
		`{"a":{"b":{"c":`, `"abc\`, `{"x":"y"} {"z":1}`, strings.Repeat("y", 20) + "z", strings.Repeat("y", 21), strings.Repeat("y", 22),
		"{\"a\":\"tab\there\"}", "{\"a\":\"line\nbreak\",\"b\":\"\\d\"}", `{"path":"C:\Users\me"}`, `{"a":"\u00e9 \x"}`, `{"a":"trailing\`,
		"{\"a\":\"ctl\x01\"}", `{"a":"ok"}` + "\n", `{"n":12345678901234567890}`, `{"a":"😀\q"}`, "{\"é\":\"\x02\"", `{"a":"\é"}`,
		`tr"`, "tr1", "nu-", `fals"e`, "[tru]", `{"a":t"}`, strings.Repeat("é", 25) + "x" + strings.Repeat("é", 25), `{"a":😀}`,
		strings.Repeat(" ", 15) + "x" + strings.Repeat(" ", 15), `{"a":"b",` + strings.Repeat("\n", 3) + `}`, "{\"a\":1,\n\n\n   x}"}
	var want []struct {
		Arguments      json.RawMessage `json:"arguments"`
		ArgumentsError string          `json:"argumentsError"`
		Parse          string          `json:"parse"`
	}
	crosscheck.Run(t, "toolArguments", texts, &want)
	for i, text := range texts {
		if got := v8JSONParseError(text); got != want[i].Parse {
			t.Errorf("JSON.parse(%q): %q, V8 %q", text, got, want[i].Parse)
		}
		args, argumentsError := ParseToolCallArguments(text)
		if argumentsError != want[i].ArgumentsError || !crosscheck.Same(args, want[i].Arguments) {
			t.Errorf("%q: %s %q, pi-ai %s %q", text, args, argumentsError, want[i].Arguments, want[i].ArgumentsError)
		}
	}
}
