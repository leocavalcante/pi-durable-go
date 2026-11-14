//go:build crosscheck

package durable

import (
	"encoding/json"
	"testing"
)

// For the external test package's code mode scenarios: it can import the
// codemode package, which imports this one.

// SetScenarioCodemode sets how a scenario's code mode extension is built.
func SetScenarioCodemode(build func(t *testing.T, options json.RawMessage) []Extension) {
	scenarioCodemode = build
}

// CompareScenario runs a scenario here and in upstream's Harness.
func CompareScenario(t *testing.T, raw json.RawMessage) { compareScenario(t, raw) }
