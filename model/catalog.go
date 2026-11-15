package model

import (
	_ "embed"
	"encoding/json"
	"slices"
	"sync"

	durable "github.com/leocavalcante/pi-durable-go"
)

// pi-ai's catalog of the models its openai-completions adapter serves,
// generated from the installed pi-ai by scripts/gen-openai-catalog.mjs.
//
//go:embed catalog/openai-completions.json
var openAICatalogJSON []byte

// CatalogModel is a model of pi-ai's catalog that speaks Chat Completions
// (pi-ai Model): what OpenAIModel configures model.OpenAI with.
type CatalogModel struct {
	ID               string             `json:"id"`
	Name             string             `json:"name"`
	Provider         string             `json:"provider"`
	BaseURL          string             `json:"baseUrl"`
	Reasoning        bool               `json:"reasoning"`
	Input            []string           `json:"input"`
	Cost             ModelCost          `json:"cost"`
	ContextWindow    int                `json:"contextWindow"`
	MaxTokens        int                `json:"maxTokens"`
	Compat           json.RawMessage    `json:"compat,omitempty"`
	ThinkingLevelMap map[string]*string `json:"thinkingLevelMap,omitempty"`
	Headers          map[string]string  `json:"headers,omitempty"`
	InputLimits      *struct {
		Images *struct {
			Resize *durable.ImageLimits `json:"resize,omitempty"`
		} `json:"images,omitempty"`
	} `json:"inputLimits,omitempty"`
}

var openAICatalog = sync.OnceValues(func() (struct {
	Source string         `json:"source"`
	Models []CatalogModel `json:"models"`
}, error) {
	var catalog struct {
		Source string         `json:"source"`
		Models []CatalogModel `json:"models"`
	}
	err := json.Unmarshal(openAICatalogJSON, &catalog)
	return catalog, err
})

// OpenAICatalog lists the models of pi-ai's catalog that speak Chat
// Completions, by provider and ID, and names the pi-ai release they come
// from.
func OpenAICatalog() ([]CatalogModel, string) {
	catalog, err := openAICatalog()
	if err != nil {
		panic("model: the embedded catalog is invalid: " + err.Error())
	}
	return slices.Clone(catalog.Models), catalog.Source
}

// OpenAIModel is the model.OpenAI that serves a model of pi-ai's catalog,
// as pi-ai's getModel defines it: its base URL, context window, output
// limit, reasoning and thinking levels, compat (strict mode included),
// price, headers, and image input and limits. Set APIKey (and HTTPClient)
// on it, and send requests for the same provider and ID, which pi-ai's
// provider detection reads. ok is false for a model the catalog lacks.
func OpenAIModel(provider, id string) (OpenAI, bool) {
	models, _ := OpenAICatalog()
	for _, entry := range models {
		if entry.Provider == provider && entry.ID == id {
			return entry.OpenAI(), true
		}
	}
	return OpenAI{}, false
}

// OpenAI is the adapter configured for the model.
func (m CatalogModel) OpenAI() OpenAI {
	adapter := OpenAI{BaseURL: m.BaseURL, Window: m.ContextWindow, MaxTokens: m.MaxTokens, Reasoning: m.Reasoning,
		NoImages: !slices.Contains(m.Input, "image"), ThinkingLevelMap: m.ThinkingLevelMap}
	if len(m.Compat) > 0 {
		_ = json.Unmarshal(m.Compat, &adapter.Compat)
		var strict struct {
			SupportsStrictMode bool `json:"supportsStrictMode"`
		}
		_ = json.Unmarshal(m.Compat, &strict)
		adapter.StrictMode = strict.SupportsStrictMode
	}
	cost := m.Cost
	adapter.Cost = &cost
	if len(m.Headers) > 0 {
		adapter.Headers = make(map[string]string, len(m.Headers))
		for name, value := range m.Headers {
			adapter.Headers[name] = value
		}
	}
	if m.InputLimits != nil && m.InputLimits.Images != nil && m.InputLimits.Images.Resize != nil {
		adapter.InputImageLimits = *m.InputLimits.Images.Resize
	}
	return adapter
}
