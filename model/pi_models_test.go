package model

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/leocavalcante/pi-durable-go/codemode"
)

// Pi answers code mode's models.* from its SDK's catalog.
var _ codemode.Models = Pi{}

func TestPiServesCodeModeModels(t *testing.T) {
	p := fakePi(t)
	ctx := context.Background()
	classifiers, err := p.ModelsOfType(ctx, "classifier", "")
	if err != nil || len(classifiers) != 1 || string(classifiers[0]) != `{"id":"judge","provider":"test","api":"test-classifier","type":"classifier"}` {
		t.Fatalf("classifiers %s, %v", classifiers, err)
	}
	images, err := p.AvailableOfType(ctx, "image", "test")
	if err != nil || len(images) != 1 || !strings.Contains(string(images[0]), `"painter"`) {
		t.Fatalf("images %s, %v", images, err)
	}
	if none, err := p.ModelsOfType(ctx, "image", "other"); err != nil || len(none) != 0 {
		t.Fatalf("other provider %s, %v", none, err)
	}
	judge, err := p.ModelOfType(ctx, "classifier", "test", "judge")
	if err != nil || strings.Contains(string(judge), "secret") {
		t.Fatalf("judge %s, %v", judge, err)
	}
	if missing, err := p.ModelOfType(ctx, "classifier", "test", "nope"); err != nil || missing != nil {
		t.Fatalf("missing %s, %v", missing, err)
	}
	questions := `"questions":{"good":{"type":"bool","instructions":"?","criteria":{"true":"y","false":"n"}}}`
	// The SDK's own model runs, credentials included, whatever the script
	// passed besides its provider and ID.
	result, err := p.Classify(ctx, json.RawMessage(`{"provider":"test","id":"judge","baseUrl":"https://attacker.invalid"}`), json.RawMessage(`{"state":{},`+questions+`}`))
	if err != nil {
		t.Fatal(err)
	}
	var classified struct {
		Answers    map[string]struct{ Value bool } `json:"answers"`
		StopReason string                          `json:"stopReason"`
		SawHeaders bool                            `json:"sawHeaders"`
		Usage      struct{ Input int }             `json:"usage"`
	}
	if err := json.Unmarshal(result, &classified); err != nil || !classified.Answers["good"].Value || classified.StopReason != "stop" || !classified.SawHeaders || classified.Usage.Input != 7 {
		t.Fatalf("classified %s, %v", result, err)
	}
	// Provider error text never crosses the boundary.
	failed, err := p.Classify(ctx, judge, json.RawMessage(`{"state":{"fail":true},`+questions+`}`))
	if err != nil || strings.Contains(string(failed), "secret") || !strings.Contains(string(failed), `"errorMessage":"pi: provider request failed`) {
		t.Fatalf("failed %s, %v", failed, err)
	}
	generated, err := p.GenerateImages(ctx, json.RawMessage(`{"provider":"test","id":"painter"}`), json.RawMessage(`{"input":[{"type":"text","text":"a cat"}]}`))
	if err != nil || !strings.Contains(string(generated), `"output":[{"type":"image","data":"aGk=","mimeType":"image/png"}]`) {
		t.Fatalf("generated %s, %v", generated, err)
	}
	if _, err := p.Classify(ctx, json.RawMessage(`{"provider":"test","id":"painter"}`), json.RawMessage(`{}`)); err == nil || !strings.Contains(err.Error(), "model not found") {
		t.Fatalf("an image model classifying: %v", err)
	}
	if _, err := p.ModelsOfType(ctx, "text", ""); err == nil || !strings.Contains(err.Error(), "invalid model request") {
		t.Fatalf("unknown type: %v", err)
	}
}
