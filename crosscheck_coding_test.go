//go:build crosscheck

package durable_test

import (
	"encoding/json"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
	"github.com/leocavalcante/pi-durable-go/tools"
)

// TestCrosscheckCodingScenarios plays the coding scenarios here, with the
// coding tools over a local environment, and in upstream's Harness with its
// coding tools over a Node environment.
func TestCrosscheckCodingScenarios(t *testing.T) {
	if crosscheck.Upstream() == "" {
		t.Skip("no upstream checkout: run make crosscheck, or set PI_DURABLE_CROSSCHECK_UPSTREAM")
	}
	durable.SetScenarioCoding(func(t *testing.T, dir string) (durable.Extension, func(durable.ConversationRecord) (durable.ExecutionEnv, error)) {
		return tools.Coding(), func(durable.ConversationRecord) (durable.ExecutionEnv, error) { return tools.NewLocalEnv(dir) }
	})
	defer durable.SetScenarioCoding(nil)
	durable.SetScenarioToolSearch(tools.ToolSearch)
	defer durable.SetScenarioToolSearch(nil)
	var names []string
	crosscheck.Cases(t, "scenarios/coding/index.json", &names)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			var raw json.RawMessage
			crosscheck.Cases(t, "scenarios/coding/"+name+".json", &raw)
			durable.CompareScenario(t, raw)
		})
	}
}
