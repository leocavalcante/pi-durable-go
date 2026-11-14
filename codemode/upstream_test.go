package codemode

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// upstreamCases are the cases of internal/crosscheck/testdata/codemode.json,
// which the codemode oracle op answers with upstream's own functions.
type upstreamCases struct {
	Identifiers []string `json:"identifiers"`
	Types       []struct {
		Schema   json.RawMessage `json:"schema"`
		MaxChars int             `json:"maxChars"`
	} `json:"types"`
	Samples []struct {
		Name         string          `json:"name"`
		Description  string          `json:"description"`
		InputSchema  json.RawMessage `json:"inputSchema"`
		OutputSchema json.RawMessage `json:"outputSchema"`
	} `json:"samples"`
	Sources      []string `json:"sources"`
	Descriptions []bool   `json:"descriptions"`
	Sections     []struct {
		Tools []struct {
			Name                   string          `json:"name"`
			Description            string          `json:"description"`
			Parameters             json.RawMessage `json:"parameters"`
			StructuredOutputSchema json.RawMessage `json:"structuredOutputSchema"`
		} `json:"tools"`
		Namespaces []struct {
			Name         string `json:"name"`
			Description  string `json:"description"`
			Instructions string `json:"instructions"`
			Prefix       string `json:"prefix"`
		} `json:"namespaces"`
		Budget int `json:"budget"`
	} `json:"sections"`
	Results []struct {
		Result  Result `json:"result"`
		Options struct {
			GeneratedImages int          `json:"generatedImages"`
			WallMs          float64      `json:"wallMs"`
			MaxOutputTokens float64      `json:"maxOutputTokens"`
			Calls           []NestedCall `json:"calls"`
			Limits          struct {
				MaxBytes int `json:"maxBytes"`
				MaxLines int `json:"maxLines"`
			} `json:"limits"`
		} `json:"options"`
		FailWrites bool `json:"failWrites"`
	} `json:"results"`
}

// portAnswers answers the cases with the port, in the oracle's shape.
func portAnswers(t *testing.T, cases upstreamCases) any {
	t.Helper()
	answers := map[string]any{}
	var identifiers, types, samples, descriptions []any
	for _, name := range cases.Identifiers {
		identifiers = append(identifiers, Identifier(name))
	}
	for _, c := range cases.Types {
		types = append(types, SchemaToType(c.Schema, c.MaxChars))
	}
	for _, c := range cases.Samples {
		tool := ToolDeclaration{Name: c.Name, Description: c.Description}
		if len(c.InputSchema) > 0 {
			tool.InputSchema = c.InputSchema
		}
		if len(c.OutputSchema) > 0 {
			tool.OutputSchema = c.OutputSchema
		}
		samples = append(samples, RenderToolSample(tool))
	}
	var sources []any
	for _, text := range cases.Sources {
		code, options, err := ParseSource(text)
		if err != nil {
			sources = append(sources, map[string]any{"ok": false, "error": err.Error()})
			continue
		}
		parsed := map[string]any{}
		if options.MaxOutputTokens != nil {
			parsed["maxOutputTokens"] = *options.MaxOutputTokens
		}
		if options.TimeoutMs != nil {
			parsed["timeoutMs"] = *options.TimeoutMs
		}
		sources = append(sources, map[string]any{"ok": true, "value": map[string]any{"code": code, "options": parsed}})
	}
	for _, models := range cases.Descriptions {
		descriptions = append(descriptions, Description(models))
	}
	var sections []any
	for _, c := range cases.Sections {
		var tools []durableTool
		for _, tool := range c.Tools {
			tools = append(tools, durableTool{name: tool.Name, description: tool.Description, parameters: tool.Parameters, output: tool.StructuredOutputSchema})
		}
		var namespaces []Namespace
		for _, namespace := range c.Namespaces {
			namespaces = append(namespaces, Namespace{Name: namespace.Name, Description: namespace.Description, Instructions: namespace.Instructions, Prefix: namespace.Prefix})
		}
		if section, ok := Section(definitions(tools), namespaces, c.Budget); ok {
			sections = append(sections, section)
		} else {
			sections = append(sections, nil)
		}
	}
	var results []any
	for _, c := range cases.Results {
		var writes []any
		write := func(name string, data []byte) (string, string) {
			written := map[string]any{"name": name, "size": len(data)}
			if name == "output.txt" {
				written["size"], written["text"] = js.Len(string(data)), string(data)
			}
			writes = append(writes, written)
			if c.FailWrites {
				return "", "disk full"
			}
			return "/out/" + name, ""
		}
		if writes == nil {
			writes = []any{}
		}
		calls := c.Options.Calls
		items, fullOutputPath, err := formatResult(c.Result, formatOptions{generatedImages: c.Options.GeneratedImages, wallMs: c.Options.WallMs,
			maxOutputTokens: c.Options.MaxOutputTokens, calls: calls,
			limits: outputLimits{maxBytes: c.Options.Limits.MaxBytes, maxLines: c.Options.Limits.MaxLines}, write: write})
		if writes == nil {
			writes = []any{}
		}
		if err != nil {
			results = append(results, map[string]any{"ok": false, "error": err.Error(), "writes": writes})
			continue
		}
		var content []any
		for _, it := range items {
			if it.image {
				content = append(content, map[string]any{"type": "image", "data": it.data, "mimeType": it.mimeType})
			} else {
				content = append(content, map[string]any{"type": "text", "text": it.text})
			}
		}
		value := map[string]any{"content": content}
		if fullOutputPath != "" {
			value["fullOutputPath"] = fullOutputPath
		}
		results = append(results, map[string]any{"ok": true, "value": value, "writes": writes})
	}
	answers["identifiers"], answers["types"], answers["samples"], answers["sources"] = identifiers, types, samples, sources
	answers["descriptions"], answers["sections"], answers["results"] = descriptions, sections, results
	return normalize(t, answers)
}

// normalize is v as JSON decodes it, so values compare as upstream's do.
func normalize(t *testing.T, v any) any {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// compareAnswers reports each answer that differs from upstream's.
func compareAnswers(t *testing.T, got, want any) {
	t.Helper()
	g, w := got.(map[string]any), want.(map[string]any)
	for key, wanted := range w {
		gotList, _ := g[key].([]any)
		wantList, _ := wanted.([]any)
		if len(gotList) != len(wantList) {
			t.Errorf("%s: %d answers, upstream %d", key, len(gotList), len(wantList))
			continue
		}
		for i := range wantList {
			if !reflect.DeepEqual(gotList[i], wantList[i]) {
				gotJSON, _ := json.MarshalIndent(gotList[i], "", " ")
				wantJSON, _ := json.MarshalIndent(wantList[i], "", " ")
				t.Errorf("%s[%d]:\n got %s\nwant %s", key, i, gotJSON, wantJSON)
			}
		}
	}
}

// TestUpstreamAnswers compares the port with upstream's answers recorded by
// the cross-check (PI_DURABLE_CROSSCHECK_RECORD=1 make crosscheck).
func TestUpstreamAnswers(t *testing.T) {
	data, err := os.ReadFile("testdata/upstream.json")
	if err != nil {
		t.Fatal(err)
	}
	var recorded struct {
		Cases    upstreamCases `json:"cases"`
		Upstream any           `json:"upstream"`
	}
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatal(err)
	}
	compareAnswers(t, portAnswers(t, recorded.Cases), recorded.Upstream)
}
