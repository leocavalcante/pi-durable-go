//go:build crosscheck

package durable_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/codemode"
	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// TestCrosscheckCodemodeScenarios plays the code mode scenarios here, with
// the Go VM (QuickJSSandbox), and in upstream's Harness with pi-codemode's
// own sandbox. PI_DURABLE_CROSSCHECK_CODEMODE=node plays them here with
// pi-codemode's sandbox too, from the prepared checkout's dependencies.
func TestCrosscheckCodemodeScenarios(t *testing.T) {
	crosscheck.Require(t)
	upstream := crosscheck.Upstream()
	var sandbox interface {
		codemode.Sandbox
		Close() error
	} = &codemode.QuickJSSandbox{}
	if os.Getenv("PI_DURABLE_CROSSCHECK_CODEMODE") == "node" {
		sandbox = &codemode.NodeSandbox{CodemodeDir: filepath.Join(upstream, "packages", "durable", "node_modules", "@earendil-works", "pi-codemode")}
	}
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
