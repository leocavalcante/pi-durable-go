//go:build crosscheck

package durable_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/codemode"
	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// TestCrosscheckCodemodeScenarios plays the code mode scenarios here and in
// upstream's Harness, both with pi-codemode's sandbox from the prepared
// checkout's dependencies, which the installed Pi provides.
func TestCrosscheckCodemodeScenarios(t *testing.T) {
	upstream := crosscheck.Upstream()
	if upstream == "" {
		t.Skip("no upstream checkout: run make crosscheck, or set PI_DURABLE_CROSSCHECK_UPSTREAM")
	}
	sandbox := &codemode.NodeSandbox{CodemodeDir: filepath.Join(upstream, "packages", "durable", "node_modules", "@earendil-works", "pi-codemode")}
	defer sandbox.Close()
	durable.SetScenarioCodemode(func(t *testing.T, raw json.RawMessage) []durable.Extension {
		var options struct {
			Namespaces []struct {
				Name         string `json:"name"`
				Description  string `json:"description"`
				Instructions string `json:"instructions"`
				Prefix       string `json:"prefix"`
			} `json:"namespaces"`
			InlineBudget *int `json:"inlineBudget"`
		}
		if err := json.Unmarshal(raw, &options); err != nil {
			t.Fatal(err)
		}
		var namespaces []codemode.Namespace
		for _, namespace := range options.Namespaces {
			namespaces = append(namespaces, codemode.Namespace{Name: namespace.Name, Description: namespace.Description,
				Instructions: namespace.Instructions, Prefix: namespace.Prefix})
		}
		return []durable.Extension{codemode.Extension(codemode.Options{Sandbox: sandbox, Namespaces: namespaces, InlineBudget: options.InlineBudget})}
	})
	defer durable.SetScenarioCodemode(nil)
	var names []string
	crosscheck.Cases(t, "scenarios/codemode/index.json", &names)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			var raw json.RawMessage
			crosscheck.Cases(t, "scenarios/codemode/"+name+".json", &raw)
			durable.CompareScenario(t, raw)
		})
	}
}
