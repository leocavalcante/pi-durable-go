package model

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

// Usage is reported and priced as pi-ai's openai-completions reports it;
// the expectations are pi-ai's answers (TestCrosscheckOpenAIUsage).
func TestOpenAIUsageAsPiAI(t *testing.T) {
	answer := `data: {"choices":[{"delta":{"content":"x"},"finish_reason":"stop"}]}`
	price := &ModelCost{Input: 3, Output: 15, CacheRead: 0.3, CacheWrite: 3.75}
	tiered := &ModelCost{Input: 3, Output: 15, CacheRead: 0.3, CacheWrite: 3.75, Tiers: []ModelCostTier{{InputTokensAbove: 100, Input: 6, Output: 30, CacheRead: 0.6, CacheWrite: 7.5}}}
	for _, c := range []struct {
		usage    string
		provider string
		cost     *ModelCost
		want     string
	}{
		{`{"prompt_tokens":150,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":30}}`, "", tiered,
			"120 20 30 0 none 0 170 0.00072 0.0006000000000000001 1.8e-05 0 0.001338 false"},
		{`{"prompt_tokens":100,"completion_tokens":20,"prompt_tokens_details":{"cached_tokens":40,"cache_write_tokens":10},"completion_tokens_details":{"reasoning_tokens":5}}`, "", price,
			"50 20 40 10 none 5 120 0.00015000000000000001 0.00030000000000000003 1.2e-05 3.75e-05 0.0004995 false"},
		{`{"prompt_tokens":10,"completion_tokens":2,"cost":0.01,"is_byok":true,"cost_details":{"upstream_inference_cost":null,"upstream_inference_prompt_cost":0.2,"upstream_inference_completions_cost":0.3}}`, "openrouter", price,
			"10 2 0 0 none 0 12 3e-05 3e-05 0 0 0.51 true"},
		{"", "", price, "0 0 0 0 none none 0 0 0 0 0 0 false"},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, answer+"\n\n")
			if c.usage != "" {
				fmt.Fprint(w, `data: {"choices":[],"usage":`+c.usage+"}\n\n")
			}
			fmt.Fprint(w, "data: [DONE]\n\n")
		}))
		response, err := OpenAI{BaseURL: server.URL + "/v1", Cost: c.cost}.Generate(context.Background(),
			durable.ModelRequest{Model: durable.ModelRef{Provider: c.provider, ID: "m"}, Messages: []durable.Message{{Role: "user", Content: "hi"}}}, nil)
		server.Close()
		if err != nil {
			t.Fatal(err)
		}
		u := response.Usage
		got := fmt.Sprintf("%d %d %d %d %v %v %d %v %v %v %v %v %v", u.InputTokens, u.OutputTokens, u.CacheReadTokens, u.CacheWriteTokens,
			deref(u.CacheWrite1hTokens), deref(u.ReasoningTokens), u.TotalTokens, u.CostInput, u.CostOutput, u.CostCacheRead, u.CostCacheWrite, u.Cost, u.CostFromProvider)
		if got != c.want {
			t.Errorf("usage %s:\n got %s\nwant %s", c.usage, got, c.want)
		}
	}
}

// deref shows an optional counter: its value, or none.
func deref(n *int64) string {
	if n == nil {
		return "none"
	}
	return fmt.Sprint(*n)
}
