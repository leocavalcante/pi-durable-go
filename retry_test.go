package durable

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

func TestRetryPolicyResolution(t *testing.T) {
	for _, tc := range []struct {
		name     string
		settings RetrySettings
		want     retryPolicy
	}{
		{"default", RetrySettings{}, retryPolicy{true, 3, 2000, 60000}},
		{"enabled-only", RetrySettings{Enabled: ptr(false)}, retryPolicy{false, 3, 2000, 60000}},
		{"max-retries-zero", RetrySettings{MaxRetries: ptr(0.0)}, retryPolicy{true, 0, 2000, 60000}},
		{"base-delay", RetrySettings{BaseDelayMs: ptr(10.0)}, retryPolicy{true, 3, 10, 60000}},
		{"cap-zero", RetrySettings{MaxAgentDelayMs: ptr(0.0)}, retryPolicy{true, 3, 2000, 0}},
		{"all", RetrySettings{Enabled: ptr(true), MaxRetries: ptr(7.0), BaseDelayMs: ptr(1.0), MaxAgentDelayMs: ptr(9.0)}, retryPolicy{true, 7, 1, 9}},
	} {
		if got := tc.settings.resolve(); got != tc.want {
			t.Errorf("%s: resolve()=%+v, want %+v", tc.name, got, tc.want)
		}
	}
	policy := RetrySettings{}.resolve()
	for attempt, want := range map[int]bool{1: true, 3: true, 4: false} {
		if policy.allows(attempt) != want {
			t.Errorf("default allows(%d) != %v", attempt, want)
		}
	}
	for _, tc := range []struct {
		max     float64
		attempt int
		want    bool
	}{{math.Inf(1), 1 << 30, true}, {-1, 1, false}, {math.NaN(), 1, false}, {2.5, 2, true}, {2.5, 3, false}} {
		if got := (retryPolicy{enabled: true, maxRetries: tc.max}).allows(tc.attempt); got != tc.want {
			t.Errorf("maxRetries=%v allows(%d)=%v, want %v", tc.max, tc.attempt, got, tc.want)
		}
	}
	if (retryPolicy{enabled: false, maxRetries: 3}).allows(1) {
		t.Error("disabled policy allowed a retry")
	}
}

func TestRetryPolicyDelay(t *testing.T) {
	policy := RetrySettings{}.resolve()
	for attempt, want := range map[int]float64{0: 2000, 1: 2000, 2: 4000, 3: 8000, 5: 32000, 6: 60000, 1000: 60000} {
		if got := retryDelayMs(policy, attempt); got != want {
			t.Errorf("retryDelayMs(default, %d)=%v, want %v", attempt, got, want)
		}
	}
	// pi-ai retry.test.ts "caps agent retry delay".
	for _, tc := range []struct {
		policy  retryPolicy
		attempt int
		want    float64
	}{
		{retryPolicy{baseDelayMs: 2000, maxAgentDelayMs: 60000}, 6, 60000},
		{retryPolicy{baseDelayMs: 2000, maxAgentDelayMs: 5000}, 5, 5000},
		{retryPolicy{baseDelayMs: 2000, maxAgentDelayMs: 0}, 5, 0},
		// Not a safe integer: replaced by MAX_SAFE_INTEGER, then capped.
		{retryPolicy{baseDelayMs: 1.5, maxAgentDelayMs: 60000}, 1, 60000},
		{retryPolicy{baseDelayMs: math.NaN(), maxAgentDelayMs: 60000}, 1, 60000},
		{retryPolicy{baseDelayMs: math.Inf(1), maxAgentDelayMs: math.Inf(1)}, 1, maxSafeInteger},
		{retryPolicy{baseDelayMs: 2000, maxAgentDelayMs: math.Inf(1)}, 2000, maxSafeInteger},
		{retryPolicy{baseDelayMs: -10, maxAgentDelayMs: 60000}, 2, -20},
	} {
		if got := retryDelayMs(tc.policy, tc.attempt); got != tc.want {
			t.Errorf("retryDelayMs(%+v, %d)=%v, want %v", tc.policy, tc.attempt, got, tc.want)
		}
	}
	if got := retryDelayMs(retryPolicy{baseDelayMs: 1, maxAgentDelayMs: math.NaN()}, 1); !math.IsNaN(got) {
		t.Errorf("NaN cap delay=%v, want NaN as Math.min", got)
	}
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		delay float64
		want  time.Time
	}{
		{2000, now.Add(2 * time.Second)},
		{0.5, now.Add(500 * time.Microsecond)},
		{math.NaN(), now},
		{-5, now},
		{maxSafeInteger, now.Add(time.Duration(math.MaxInt64))},
	} {
		if got := retryDeadline(now, tc.delay); !got.Equal(tc.want) {
			t.Errorf("retryDeadline(%v)=%v, want %v", tc.delay, got, tc.want)
		}
	}
}

type classifiedTestError struct {
	retryable, overflow bool
}

func (classifiedTestError) Error() string           { return "503 prompt is too long" }
func (e classifiedTestError) Retryable() bool       { return e.retryable }
func (e classifiedTestError) ContextOverflow() bool { return e.overflow }

func TestModelErrorClassification(t *testing.T) {
	// Every classification case of pi-ai v1.5.2 test/retry.test.ts.
	retryable := []string{
		"An error occurred while processing your request. You can retry your request, or contact us through our help center at help.openai.com if the error persists. Please include the request ID req_******** in your message.",
		`{"message":"The system encountered an unexpected error during processing. Try your request again."}`,
		"ResourceExhausted: Worker local total request limit reached (288/48)",
		"The socket connection was closed unexpectedly. For more information, pass `verbose: true` in the second argument to fetch()",
		"Error: exceeded request buffer limit while retrying upstream",
		"The pending stream has been canceled (caused by: getaddrinfo ENOTFOUND bedrock-runtime.us-east-1.amazonaws.com)",
		"connect ENOTFOUND api.example.com",
		"EAI_AGAIN api.example.com",
		"getaddrinfo failed for api.example.com",
		"The pending stream has been canceled",
		"The pending stream has been canceled (caused by: socket closed)",
		"The service is temporarily unavailable. Please retry later.",
		"Service unavailable: The service is temporarily unavailable.",
		"request to https://oauth2.googleapis.com/token failed, reason:",
		"request to https://oauth2.googleapis.com/token failed, reason: connect ECONNREFUSED 127.0.0.1:9",
		"unknown: Unable to forward upstream stream",
		"upstream_error: Upstream request failed",
		"upstream_stream_read_error: Upstream response stream was interrupted",
		"Upstream HTTP/2 stream failed",
		"Claude Code's stream from Anthropic was cut short after the reply had begun",
		"OpenAI Responses stream ended before a terminal response event",
		"The system is currently experiencing high demand and cannot process your request. Your request exceeds the maximum usage size allowed during peak load. For improved capacity reliability, consider switching to Provisioned Throughput.",
		"subscription_sharing_usage_unavailable: Usage cannot be checked.",
		"subscription_sharing_user_unavailable: User cannot be loaded.",
		"overloaded_error",
		"520 status code (no body)",
		"524 status code (no body)",
		// RE2 rewrite of the status-code lookarounds: boundaries at either end.
		"503", "HTTP 503.", "status=429;", "(502)", "terminated",
	}
	notRetryable := []string{
		"429 quota exceeded",
		`OpenAI API error (429): {"code":"subscription_sharing_usage_limit_exceeded","message":"Usage limit reached."}`,
		`OpenAI API error (429): {"code":"subscription_sharing_workplace_individual_usage_limit_exceeded","message":"Usage limit reached."}`,
		`401 {"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"},"request_id":"req_011CU503xyz"}`,
		"400 image exceeds 5 MB maximum: 5316784 bytes > 5242880 bytes",
		"", "not an error", "invalid x-api-key", "A503", "503B", "5030", "1429",
		// The non-retryable 1308 rewrite, and an unrelated longer code.
		`429 {"code":"1308","message":"plan usage limit"}`, `429 {"code": 1308}`,
		"429 insufficient_quota", "billing hard limit", "Monthly usage limit reached",
	}
	for _, message := range retryable {
		if !IsRetryableModelError(errors.New(message)) {
			t.Errorf("not retryable: %q", message)
		}
	}
	for _, message := range notRetryable {
		if IsRetryableModelError(errors.New(message)) {
			t.Errorf("retryable: %q", message)
		}
	}
	if !IsRetryableModelError(errors.New(`429 {"code":"13080"}`)) {
		t.Error("1308 must not match inside a longer code")
	}

	// Every error-text case of pi-ai v1.5.2 test/overflow.test.ts.
	overflow := []struct{ message, provider string }{
		{`github-copilot API error (413): {"message":"failed to parse request","code":""}`, "ollama"},
		{"custom-provider API error (413): Payload Too Large", "ollama"},
		{`413 {"error":{"message":"failed to parse request","code":""}}`, "ollama"},
		{"413", "ollama"},
		{"400 `prompt too long; exceeded max context length by 100918 tokens`", "ollama"},
		{`400 {"code":"1261","message":"Prompt too long"}`, "zai"},
		{`400 {"code":"1261","message":"Prompt exceeds max length"}`, "zai"},
		{"400 The input (516368 tokens) is longer than the model's context length (262144 tokens).", "ollama"},
		{"Error: 503 litellm.ServiceUnavailableError: litellm.MidStreamFallbackError: litellm.APIConnectionError: APIConnectionError: OpenAIException - Requested token count exceeds the model's maximum context length of 131072 tokens.", "ollama"},
		{"400: {\"message\":\"litellm.ContextWindowExceededError: litellm.BadRequestError: litellm._pre_call_checks: Context Window exceeded for given call. No models have context window large enough for this call.\nModel=Qwen3.8-Flash-Next, Max Input Tokens=262144, Got=265262\nmodel=Qwen3.8-Flash-Next. context_window_fallbacks=None. fallbacks=[].\"}", "ollama"},
		{"Error: 400 Input length (265330) exceeds model's maximum context length (262144).", "ollama"},
		{"Provider returned error: Input length 131393 exceeds the maximum allowed input length of 131040 tokens.", "ollama"},
		{"400 Prompt has 256468 tokens, but the configured context size is 256000 tokens", "ollama"},
		{"Prompt has 5,958,968 tokens, but the configured context size is 256,000 tokens", "ollama"},
		{"400 status code (no body)", "cerebras"},
		{"413 status code (no body)", "cerebras"},
		{"prompt is too long: 213462 tokens > 200000 maximum", ""},
		{"413\u00a0Payload Too Large", ""}, // JavaScript \s includes no-break space
	}
	notOverflow := []struct{ message, provider string }{
		{`github-copilot API error (400): {"message":"failed to parse request","code":""}`, "ollama"},
		{"custom-provider API error (429): Too many requests", "ollama"},
		{`github-copilot API error (400): {"message":"failed to parse request: invalid value 413"}`, "ollama"},
		{"custom-provider API error (400): upstream mentioned OpenAI API error (413): invalid input", "ollama"},
		{"custom-provider API error (4130): invalid input", "ollama"},
		{"4130 failed to parse request", "ollama"},
		{"500 `model runner crashed unexpectedly`", "ollama"},
		{"400 status code (no body)", "opencode-go"},
		{"Throttling error: Too many tokens, please wait before trying again.", "bedrock"},
		{"Service unavailable: The service is temporarily unavailable.", "bedrock"},
		{"Rate limit exceeded, please retry after 30 seconds.", ""},
		{"Too many requests. Please slow down.", ""},
		{"", "cerebras"},
	}
	for _, tc := range overflow {
		if !IsContextOverflowError(errors.New(tc.message), tc.provider) {
			t.Errorf("not overflow (%s): %q", tc.provider, tc.message)
		}
	}
	for _, tc := range notOverflow {
		if IsContextOverflowError(errors.New(tc.message), tc.provider) {
			t.Errorf("overflow (%s): %q", tc.provider, tc.message)
		}
	}

	// Optional methods answer for themselves, through wrapping.
	for _, tc := range []classifiedTestError{{false, false}, {true, false}, {false, true}, {true, true}} {
		err := fmt.Errorf("adapter: %w", tc)
		if IsRetryableModelError(err) != tc.retryable || IsContextOverflowError(err, "") != tc.overflow {
			t.Errorf("optional methods ignored: %+v", tc)
		}
	}
	if IsRetryableModelError(nil) || IsContextOverflowError(nil, "cerebras") {
		t.Error("nil error classified")
	}
}

// pi-ai's patterns are JavaScript regular expressions with the i flag and
// without u: only ASCII letters fold, a dot stops at line terminators, and a
// lone dot matches one UTF-16 unit. Expectations are upstream's answers
// (TestCrosscheckErrorClassification).
func TestRetryPatternsMatchAsJavaScriptDoes(t *testing.T) {
	for text, want := range map[string]bool{
		"\u212a429 error":               true, // the Kelvin sign is not [a-z] to JavaScript
		"a429b":                         false,
		"rate\rlimit":                   false,
		"rate\u2028limit":               false,
		"rate\U0001F600limit":           false,
		"rate limit":                    true,
		"\u017fervice unavailable":      false, // nor is the long s an s
		"SERVICE UNAVAILABLE":           true,
		"\u212aotal: too many requests": true,
	} {
		if got := IsRetryableModelError(errors.New(text)); got != want {
			t.Errorf("%q: retryable %v, want %v", text, got, want)
		}
	}
	if !IsContextOverflowError(errors.New("input token count\U0001F600 exceeds the maximum"), "openai") {
		t.Error("a dot repeated matches an astral character")
	}
}
