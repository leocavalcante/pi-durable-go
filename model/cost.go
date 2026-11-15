package model

import (
	"encoding/json"
	"math"
	"strings"

	durable "github.com/leocavalcante/pi-durable-go"
)

// ModelCost is a model's price in dollars per million tokens (pi-ai
// ModelCost), with optional tiers that apply above an input size.
type ModelCost struct {
	Input      float64         `json:"input"`
	Output     float64         `json:"output"`
	CacheRead  float64         `json:"cacheRead"`
	CacheWrite float64         `json:"cacheWrite"`
	Tiers      []ModelCostTier `json:"tiers,omitempty"`
}

// ModelCostTier is a price that applies when a request's input tokens,
// cache reads and writes included, are above InputTokensAbove.
type ModelCostTier struct {
	Input            float64 `json:"input"`
	Output           float64 `json:"output"`
	CacheRead        float64 `json:"cacheRead"`
	CacheWrite       float64 `json:"cacheWrite"`
	InputTokensAbove float64 `json:"inputTokensAbove"`
}

// price sets usage's cost from the model's price, as pi-ai's calculateCost
// does: the highest tier the input passes, and one-hour cache writes at
// twice the input price. A nil price prices everything at zero.
func (c *ModelCost) price(usage *durable.Usage) {
	if c == nil {
		c = &ModelCost{}
	}
	input := float64(usage.InputTokens + usage.CacheReadTokens + usage.CacheWriteTokens)
	rates := ModelCostTier{Input: c.Input, Output: c.Output, CacheRead: c.CacheRead, CacheWrite: c.CacheWrite}
	matched := -1.0
	for _, tier := range c.Tiers {
		if input > tier.InputTokensAbove && tier.InputTokensAbove > matched {
			rates, matched = tier, tier.InputTokensAbove
		}
	}
	var longWrite int64
	if usage.CacheWrite1hTokens != nil {
		longWrite = *usage.CacheWrite1hTokens
	}
	shortWrite := usage.CacheWriteTokens - longWrite
	usage.CostInput = rates.Input / 1000000 * float64(usage.InputTokens)
	usage.CostOutput = rates.Output / 1000000 * float64(usage.OutputTokens)
	usage.CostCacheRead = rates.CacheRead / 1000000 * float64(usage.CacheReadTokens)
	usage.CostCacheWrite = (rates.CacheWrite*float64(shortWrite) + rates.Input*2*float64(longWrite)) / 1000000
	usage.Cost = usage.CostInput + usage.CostOutput + usage.CostCacheRead + usage.CostCacheWrite
	usage.CostFromProvider = false
}

// usagePricing is how an answer's usage is priced.
type usagePricing struct {
	cost       *ModelCost
	openRouter bool
}

// openRouter reports an OpenRouter model (pi-ai isOpenRouterModel).
func openRouter(provider, baseURL string) bool {
	return provider == "openrouter" || strings.Contains(baseURL, "openrouter.ai")
}

// applyOpenRouterCost replaces the estimated total with OpenRouter's billed
// cost when the usage carries complete, valid accounting (pi-ai
// applyOpenRouterReportedCost).
func applyOpenRouterCost(usage *durable.Usage, reported json.RawMessage, byok *bool, details *struct {
	UpstreamInferenceCost            json.RawMessage `json:"upstream_inference_cost"`
	UpstreamInferencePromptCost      json.RawMessage `json:"upstream_inference_prompt_cost"`
	UpstreamInferenceCompletionsCost json.RawMessage `json:"upstream_inference_completions_cost"`
}) {
	cost, ok := validCost(reported)
	if !ok || byok == nil {
		return
	}
	if !*byok {
		usage.Cost, usage.CostFromProvider = cost, true
		return
	}
	if details == nil {
		return
	}
	upstream, ok := validCost(details.UpstreamInferenceCost)
	if strings.TrimSpace(string(details.UpstreamInferenceCost)) == "null" {
		prompt, promptOK := validCost(details.UpstreamInferencePromptCost)
		completions, completionsOK := validCost(details.UpstreamInferenceCompletionsCost)
		if !promptOK || !completionsOK {
			return
		}
		upstream, ok = prompt+completions, true
	}
	if !ok {
		return
	}
	usage.Cost, usage.CostFromProvider = cost+upstream, true
}

// validCost is a JSON number that is finite and not negative.
func validCost(raw json.RawMessage) (float64, bool) {
	var value any
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return 0, false
	}
	number, ok := value.(float64)
	return number, ok && !math.IsInf(number, 0) && !math.IsNaN(number) && number >= 0
}
