//go:build crosscheck

package durable_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
	"github.com/leocavalcante/pi-durable-go/tools"
)

// TestCrosscheckCodingScenarios plays the coding scenarios here, with the
// coding tools over a local environment, and in upstream's Harness with its
// coding tools over a Node environment.
func TestCrosscheckCodingScenarios(t *testing.T) {
	crosscheck.Require(t)
	durable.SetScenarioCoding(func(t *testing.T, dir string, bash json.RawMessage) (durable.Extension, func(durable.ConversationRecord) (durable.ExecutionEnv, error)) {
		options := tools.CodingOptions{}
		if len(bash) > 0 {
			var spec struct {
				CommandPrefix string            `json:"commandPrefix"`
				Env           map[string]string `json:"env"`
				Cwd           string            `json:"cwd"`
			}
			if err := json.Unmarshal(bash, &spec); err != nil {
				t.Fatal(err)
			}
			options.Bash = tools.ShellToolOptions{CommandPrefix: spec.CommandPrefix,
				Prepare: func(_ context.Context, execution *tools.ShellExecution, _ *durable.ToolRuntime) error {
					for name, value := range spec.Env {
						execution.Env[name] = value
					}
					if spec.Cwd != "" {
						execution.Cwd = filepath.Join(execution.Cwd, spec.Cwd)
					}
					return nil
				}}
		}
		return tools.CodingWith(options), func(durable.ConversationRecord) (durable.ExecutionEnv, error) { return tools.NewLocalEnv(dir) }
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
