package model

import (
	"reflect"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

// OpenAIModel configures model.OpenAI as pi-ai's catalog defines a model
// (TestCrosscheckOpenAICatalog compares the requests with pi-ai's).
func TestOpenAIModelFromCatalog(t *testing.T) {
	models, source := OpenAICatalog()
	if len(models) != 728 || source != "@earendil-works/pi-ai 1.5.2" {
		t.Fatalf("%d models from %q", len(models), source)
	}
	str := func(s string) *string { return &s }
	yes, no := true, false
	kimi, ok := OpenAIModel("baseten", "moonshotai/Kimi-K3")
	if !ok {
		t.Fatal("Kimi K3 is missing")
	}
	want := OpenAI{BaseURL: "https://inference.baseten.co/v1", Window: 1048576, MaxTokens: 262144, Reasoning: true, StrictMode: true,
		Compat: OpenAICompat{SupportsStore: &no, SupportsDeveloperRole: &no, SupportsReasoningEffort: &yes, SupportsUsageInStreaming: &yes,
			MaxTokensField: "max_tokens", SendSessionAffinityHeaders: &yes, SupportsLongCacheRetention: &no, ThinkingFormat: "openai"},
		Cost:             &ModelCost{Input: 3, Output: 15, CacheRead: 0.3},
		ThinkingLevelMap: map[string]*string{"off": str("none"), "minimal": nil, "low": str("low"), "medium": nil, "high": str("high"), "xhigh": nil, "max": str("max")},
		InputImageLimits: durable.ImageLimits{MaxWidth: 2000, MaxHeight: 2000, MaxBytes: 4718592}}
	if !reflect.DeepEqual(kimi, want) {
		t.Fatalf("Kimi K3:\n got %+v\nwant %+v", kimi, want)
	}
	if !kimi.AcceptsImages(durable.ModelRef{}) || kimi.ImageLimits(durable.ModelRef{}).MaxBytes != 4718592 {
		t.Fatal("Kimi K3 takes images within its limits")
	}
	copilot, ok := OpenAIModel("github-copilot", "gemini-3.8-flash")
	if !ok || copilot.Headers["Editor-Version"] != "vscode/1.107.0" || copilot.Headers["Copilot-Integration-Id"] != "vscode-chat" {
		t.Fatalf("Copilot's headers: %+v", copilot.Headers)
	}
	tiered, ok := OpenAIModel("openrouter", "anthropic/claude-haiku-5.5:batch")
	if !ok || len(tiered.Cost.Tiers) != 1 || tiered.Cost.Tiers[0] != (ModelCostTier{InputTokensAbove: 100000, Input: 0.25, Output: 1.25, CacheRead: 0.025, CacheWrite: 0.3125}) {
		t.Fatalf("tiers: %+v", tiered.Cost)
	}
	if _, ok := OpenAIModel("openai", "no-such-model"); ok {
		t.Fatal("an unknown model was found")
	}
	// The catalog is a copy each time.
	models[0].ID = "changed"
	if again, _ := OpenAICatalog(); again[0].ID == "changed" {
		t.Fatal("the catalog was changed through a copy")
	}
}

// GitHub Copilot's per-request headers name who started the turn and
// whether images are sent (pi-ai buildCopilotDynamicHeaders).
func TestCopilotHeaders(t *testing.T) {
	user := copilotHeaders([]durable.Message{{Role: "assistant"}, {Role: "user", Content: "hi"}})
	if !reflect.DeepEqual(user, map[string]string{"X-Initiator": "user", "Openai-Intent": "conversation-edits"}) {
		t.Fatalf("user turn: %v", user)
	}
	agent := copilotHeaders([]durable.Message{{Role: "user"}, {Role: "assistant"}, {Role: "tool", Images: []durable.ImageContent{{Data: "x", MimeType: "image/png"}}}})
	if !reflect.DeepEqual(agent, map[string]string{"X-Initiator": "agent", "Openai-Intent": "conversation-edits", "Copilot-Vision-Request": "true"}) {
		t.Fatalf("agent turn with images: %v", agent)
	}
}
