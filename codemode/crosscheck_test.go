//go:build crosscheck

package codemode

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// TestCrosscheckFunctions compares identifiers, declarations, source
// parsing, the description, the section, and result formatting with
// upstream's own functions. With PI_DURABLE_CROSSCHECK_RECORD=1 it records
// upstream's answers for TestUpstreamAnswers.
func TestCrosscheckFunctions(t *testing.T) {
	var raw json.RawMessage
	crosscheck.Cases(t, "codemode.json", &raw)
	var cases upstreamCases
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	var upstream any
	crosscheck.Run(t, "codemode", raw, &upstream)
	compareAnswers(t, portAnswers(t, cases), upstream)
	if os.Getenv("PI_DURABLE_CROSSCHECK_RECORD") == "1" {
		data, err := json.MarshalIndent(map[string]any{"cases": raw, "upstream": upstream}, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile("testdata/upstream.json", append(data, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
