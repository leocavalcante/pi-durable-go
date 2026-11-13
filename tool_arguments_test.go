package durable

import (
	"encoding/json"
	"testing"
)

// Expectations are pi-ai 1.5.2's parseToolCallArguments results for the same
// texts (TestCrosscheckFinishedToolArguments compares many more): its repair
// of string literals, and V8's JSON.parse messages in its argumentsError.
func TestToolCallArgumentsAreReadAsPiAi(t *testing.T) {
	for _, tc := range []struct{ text, args, argumentsError string }{
		{"", "{}", ""},
		{"{\"a\":1}", "{\"a\":1}", ""},
		{" {\"a\":1} ", "{\"a\":1}", ""},
		{"[1]", "{}", "Tool arguments must be a JSON object\n\nReceived arguments:\n[1]"},
		{"\"str\"", "{}", "Tool arguments must be a JSON object\n\nReceived arguments:\n\"str\""},
		{"{\"a\":1", "{}", "Expected ',' or '}' after property value in JSON at position 6 (line 1 column 7)\n\nReceived arguments:\n{\"a\":1"},
		{"{\"a\":1,}", "{}", "Expected double-quoted property name in JSON at position 7 (line 1 column 8)\n\nReceived arguments:\n{\"a\":1,}"},
		{"{a:1}", "{}", "Expected property name or '}' in JSON at position 1 (line 1 column 2)\n\nReceived arguments:\n{a:1}"},
		{"{\"a\" 1}", "{}", "Expected ':' after property name in JSON at position 5 (line 1 column 6)\n\nReceived arguments:\n{\"a\" 1}"},
		{"{\"a\":tru}", "{}", "Unexpected token '}', \"{\"a\":tru}\" is not valid JSON\n\nReceived arguments:\n{\"a\":tru}"},
		{"{\"a\":\"x\ny\"}", "{\"a\":\"x\\ny\"}", ""},
		{"{\"a\":\"\\q\"}", "{\"a\":\"\\\\q\"}", ""},
		{"{\"a\":-}", "{}", "No number after minus sign in JSON at position 6 (line 1 column 7)\n\nReceived arguments:\n{\"a\":-}"},
		{"{\"a\":1.}", "{}", "Unterminated fractional number in JSON at position 7 (line 1 column 8)\n\nReceived arguments:\n{\"a\":1.}"},
		{"{\"a\":1} x", "{}", "Unexpected non-whitespace character after JSON at position 8 (line 1 column 9)\n\nReceived arguments:\n{\"a\":1} x"},
		{"x", "{}", "Unexpected token 'x', \"x\" is not valid JSON\n\nReceived arguments:\nx"},
		{"{\"a\":\"unterminated", "{}", "Unterminated string in JSON at position 18 (line 1 column 19)\n\nReceived arguments:\n{\"a\":\"unterminated"},
		{"{\"a\":\"\\u12G4\"}", "{\"a\":\"\\\\u12G4\"}", ""},
		{"{\"a\":01}", "{}", "Unexpected number in JSON at position 6 (line 1 column 7)\n\nReceived arguments:\n{\"a\":01}"},
		{"NaN", "{}", "\"NaN\" is not valid JSON\n\nReceived arguments:\nNaN"},
		{"{\r\n\"a\":1,\r\n}", "{}", "Expected double-quoted property name in JSON at position 11 (line 3 column 1)\n\nReceived arguments:\n{\r\n\"a\":1,\r\n}"},
		{"{\"a\":z111111111111111111111111111111}", "{}", "Unexpected token 'z', \"{\"a\":z111111111\"... is not valid JSON\n\nReceived arguments:\n{\"a\":z111111111111111111111111111111}"},
		{"{\"a\": [1, 2, {\"b\": tru}]}", "{}", "Unexpected token '}', ...\" {\"b\": tru}]}\" is not valid JSON\n\nReceived arguments:\n{\"a\": [1, 2, {\"b\": tru}]}"},
		{"\"abc\\", "{}", "Unterminated string in JSON at position 6 (line 1 column 7)\n\nReceived arguments:\n\"abc\\"},
		{"{\"path\":\"C:\\Users\\me\"}", "{\"path\":\"C:\\\\Users\\\\me\"}", ""},
		{"{\"a\":\"tab\there\"}", "{\"a\":\"tab\\there\"}", ""},
		{"tr\"", "{}", "Unexpected string in JSON at position 2 (line 1 column 3)\n\nReceived arguments:\ntr\""},
		{"{\"😀\":1,}", "{}", "Expected double-quoted property name in JSON at position 8 (line 1 column 9)\n\nReceived arguments:\n{\"😀\":1,}"},
	} {
		args, argumentsError := ParseToolCallArguments(tc.text)
		if argumentsError != tc.argumentsError || !jsonEqual(args, json.RawMessage(tc.args)) {
			t.Errorf("%q: %s %q, want %s %q", tc.text, args, argumentsError, tc.args, tc.argumentsError)
		}
	}
}
