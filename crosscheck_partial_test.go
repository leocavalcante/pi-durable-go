//go:build crosscheck

package durable

import (
	"strings"
	"testing"

	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// More partial texts for pi-ai's streaming reading (ToolCallArgumentsStream):
// unterminated numbers, escapes, malformed text, and odd positions.
func TestCrosscheckStreamedArguments(t *testing.T) {
	partials := []string{`{"a":1.`, `{"b":2,"a":1.`, `{"b":2,"a":1E-`, `{"b":2,"a":-`, `{"a":1e`, `[1.`, `{"a":[1,2.`, `{"a":{"c":1.`,
		`{"b":2,"a":12`, `{"a":1e5`, `{"a":-0`, `{"a":0.5`, `{"a":01`, `{"a":1.5e+`, `{"a":-1.25E-3`, `{"a":` + strings.Repeat("9", 70),
		`{"a":t`, `{"a":tru`, `{"a":true`, `{"a":f`, `{"a":n`, `{"a":nu`, `{"a":x`, `{"a":NaN`, `{"a":Infinity`, `{"a":-I`,
		`{"a":"x\`, `{"a":"x\u`, `{"a":"x\u00e`, `{"a":"x\u00e9`, `{"a":"\d`, `{"a":"\u12G4"`, `{"a":"line` + "\n" + `break`,
		`{"a" 1`, `{"a":1 "b":2`, `[1 2`, `{"a":1,,"b":2`, `{,"a":1`, `{"a":}`, `{"a":]`, `{"a":[1}`, `}{"a":1`, `{"a":1}}`,
		`{"a":1} {"b":2}`, `"str`, `12`, `[{"a":1`, `{"a":{"b":{"c":[1,{"d":"e`, `{"a":"b","a":"c"`, `{"1":"x","b":"y","0":"z"`,
		`{"__proto__":{"x":1}`, `  {  "a"  :  "spaced"  `, `{"a":[]`, `{"a":{}`, `{"a":[[]`, `{"a":"é😀`, `{"k\"q":1`, `{"a":1e+5}`, `{"a":"\ud83d`, `{"\ud83d":1`, `{"a":"\ud83d\ude00`}
	var want []string
	crosscheck.Run(t, "partial", partials, &want)
	for i, text := range partials {
		if got := partialArguments(text); !crosscheck.Same(got, []byte(want[i])) {
			t.Errorf("%q: %s, upstream %s", text, got, want[i])
		}
	}
}
