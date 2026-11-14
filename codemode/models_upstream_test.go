package codemode

import (
	"context"
	"encoding/json"
	"os"
	"testing"
)

// modelsCases are internal/crosscheck/testdata/codemode-models.json, which
// the codemodeModels oracle op answers with upstream's models.ts.
type modelsCases struct {
	Catalog []struct {
		ModelType string          `json:"modelType"`
		Model     json.RawMessage `json:"model"`
	} `json:"catalog"`
	Calls []struct {
		Name string          `json:"name"`
		Args json.RawMessage `json:"args"`
	} `json:"calls"`
	Resolve []struct {
		Name  string          `json:"name"`
		Type  string          `json:"type"`
		Model json.RawMessage `json:"model"`
	} `json:"resolve"`
	ClassifierContexts []json.RawMessage `json:"classifierContexts"`
	ImagesContexts     []json.RawMessage `json:"imagesContexts"`
}

// caseModels is the cases' catalog.
type caseModels struct {
	noModels
	cases modelsCases
}

func (m caseModels) ModelsOfType(_ context.Context, modelType, provider string) ([]json.RawMessage, error) {
	found := []json.RawMessage{}
	for _, entry := range m.cases.Catalog {
		var model struct{ Provider string }
		_ = json.Unmarshal(entry.Model, &model)
		if entry.ModelType == modelType && (provider == "" || model.Provider == provider) {
			found = append(found, entry.Model)
		}
	}
	return found, nil
}

func (m caseModels) AvailableOfType(ctx context.Context, modelType, provider string) ([]json.RawMessage, error) {
	return m.ModelsOfType(ctx, modelType, provider)
}

func (m caseModels) ModelOfType(_ context.Context, modelType, provider, id string) (json.RawMessage, error) {
	for _, entry := range m.cases.Catalog {
		var model struct{ ID, Provider string }
		_ = json.Unmarshal(entry.Model, &model)
		if entry.ModelType == modelType && model.Provider == provider && model.ID == id {
			return entry.Model, nil
		}
	}
	return nil, nil
}

func modelsAnswers(t *testing.T, cases modelsCases) any {
	t.Helper()
	answer := func(value json.RawMessage, err error) any {
		if err != nil {
			return map[string]any{"ok": false, "error": err.Error()}
		}
		if value == nil {
			return map[string]any{"ok": true}
		}
		return map[string]any{"ok": true, "value": value}
	}
	models := caseModels{cases: cases}
	ctx := context.Background()
	out := map[string][]any{"calls": {}, "resolve": {}, "classifierContexts": {}, "imagesContexts": {}}
	for _, c := range cases.Calls {
		out["calls"] = append(out["calls"], answer(catalogCall(ctx, c.Name, c.Args, models)))
	}
	for _, c := range cases.Resolve {
		_, label, err := resolveModel(ctx, c.Name, c.Type, c.Model, models)
		value, _ := json.Marshal(label)
		out["resolve"] = append(out["resolve"], answer(value, err))
	}
	for _, c := range cases.ClassifierContexts {
		out["classifierContexts"] = append(out["classifierContexts"], answer(nil, checkClassifierContext(c)))
	}
	for _, c := range cases.ImagesContexts {
		out["imagesContexts"] = append(out["imagesContexts"], answer(nil, checkImagesContext(c)))
	}
	return normalize(t, out)
}

// TestUpstreamModelsAnswers compares models.* with upstream's answers
// recorded by the cross-check (PI_DURABLE_CROSSCHECK_RECORD=1 make crosscheck).
func TestUpstreamModelsAnswers(t *testing.T) {
	data, err := os.ReadFile("testdata/models-upstream.json")
	if err != nil {
		t.Fatal(err)
	}
	var recorded struct {
		Cases    modelsCases `json:"cases"`
		Upstream any         `json:"upstream"`
	}
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatal(err)
	}
	compareAnswers(t, modelsAnswers(t, recorded.Cases), recorded.Upstream)
}
