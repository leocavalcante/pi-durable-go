package durable

import (
	"errors"
	"math"
	"regexp"
	"strings"
	"time"
)

// RetrySettings is Pi Durable's HarnessSettings.retry. Each set field
// overrides the default policy: Enabled true, MaxRetries 3, BaseDelayMs 2000,
// and MaxAgentDelayMs 60000. A nil field keeps the default, while an explicit
// false or zero is honored. Numbers follow JavaScript's: math.Inf(1) retries
// without bound, and a negative or NaN MaxRetries never retries.
type RetrySettings struct {
	Enabled         *bool
	MaxRetries      *float64 // retries after the first attempt
	BaseDelayMs     *float64 // the delay before retry n is BaseDelayMs × 2^(n-1)
	MaxAgentDelayMs *float64 // cap on each delay
}

// retryPolicy is a resolved RetrySettings (pi-ai's RetryPolicy).
type retryPolicy struct {
	enabled         bool
	maxRetries      float64
	baseDelayMs     float64
	maxAgentDelayMs float64
}

// defaultMaxAgentDelayMs is pi-ai's DEFAULT_MAX_AGENT_RETRY_DELAY_MS.
const defaultMaxAgentDelayMs = 60_000

// resolve merges the set fields over Pi Durable's DEFAULT_RETRY_POLICY.
func (s RetrySettings) resolve() retryPolicy {
	policy := retryPolicy{enabled: true, maxRetries: 3, baseDelayMs: 2000, maxAgentDelayMs: defaultMaxAgentDelayMs}
	if s.Enabled != nil {
		policy.enabled = *s.Enabled
	}
	if s.MaxRetries != nil {
		policy.maxRetries = *s.MaxRetries
	}
	if s.BaseDelayMs != nil {
		policy.baseDelayMs = *s.BaseDelayMs
	}
	if s.MaxAgentDelayMs != nil {
		policy.maxAgentDelayMs = *s.MaxAgentDelayMs
	}
	return policy
}

// allows reports whether a failed 1-based attempt may be retried.
func (p retryPolicy) allows(attempt int) bool {
	return p.enabled && float64(attempt) <= p.maxRetries
}

// maxSafeInteger is JavaScript's Number.MAX_SAFE_INTEGER.
const maxSafeInteger = 1<<53 - 1

// retryDelayMs ports pi-ai's retryDelayMs: BaseDelayMs × 2^max(0, attempt−1),
// replaced by Number.MAX_SAFE_INTEGER when not a safe integer, then capped.
func retryDelayMs(policy retryPolicy, attempt int) float64 {
	delay := policy.baseDelayMs * math.Pow(2, math.Max(0, float64(attempt-1)))
	if !(delay == math.Trunc(delay) && math.Abs(delay) <= maxSafeInteger) { // NaN and ±Inf fail too
		delay = maxSafeInteger
	}
	return math.Min(delay, policy.maxAgentDelayMs) // NaN propagates, as Math.min's does
}

// retryDeadline returns now + delayMs. A NaN or negative delay is due at once,
// as JavaScript compares NaN deadlines; a huge one saturates.
func retryDeadline(now time.Time, delayMs float64) time.Time {
	if !(delayMs > 0) {
		return now
	}
	if delayMs >= float64(math.MaxInt64/int64(time.Millisecond)) {
		return now.Add(time.Duration(math.MaxInt64))
	}
	return now.Add(time.Duration(delayMs * float64(time.Millisecond)))
}

// RetryableError is implemented by model errors that know whether their
// failure is transient. Adapters that sanitize provider text implement it,
// so classification never needs the raw text.
type RetryableError interface {
	error
	Retryable() bool
}

// ContextOverflowError is implemented by model errors that know whether the
// request exceeded the model's context window.
type ContextOverflowError interface {
	error
	ContextOverflow() bool
}

// IsRetryableModelError ports pi-ai's isRetryableAssistantError for a Go model
// error. An error implementing Retryable() answers itself (found with
// errors.As); otherwise its message is classified with pi-ai v1.5.2's
// patterns: transient load, status, server, gateway, transport, and stream
// failures retry, while quota, billing, and subscription limits do not.
func IsRetryableModelError(err error) bool {
	if err == nil {
		return false
	}
	var classified RetryableError
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	message := err.Error()
	if message == "" || nonRetryableProviderLimitPattern.MatchString(message) {
		return false
	}
	return retryableProviderErrorPattern.MatchString(message)
}

// IsContextOverflowError ports the error-text part of pi-ai's
// isContextOverflow. An error implementing ContextOverflow() answers itself;
// otherwise its message is matched against pi-ai v1.5.2's overflow patterns
// and exclusions. provider is the request's provider, needed for Cerebras'
// bodyless 400/413 responses.
func IsContextOverflowError(err error, provider string) bool {
	if err == nil {
		return false
	}
	var classified ContextOverflowError
	if errors.As(err, &classified) {
		return classified.ContextOverflow()
	}
	message := err.Error()
	if message == "" {
		return false
	}
	for _, pattern := range nonOverflowPatterns {
		if pattern.MatchString(message) {
			return false
		}
	}
	for _, pattern := range overflowPatterns {
		if pattern.MatchString(message) {
			return true
		}
	}
	return provider == "cerebras" && cerebrasBodylessOverflowPattern.MatchString(message)
}

// JavaScript's \s and \S. RE2's \s is ASCII-only and lacks \v.
const (
	jsSpace    = `[\t\n\v\f\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]`
	jsNonSpace = `[^\t\n\v\f\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}]`
)

// jsCaseless translates a pi-ai pattern, an ASCII JavaScript regular
// expression with the i flag and without u, to RE2 matching the same
// strings. RE2's (?i) folds Unicode (k matches the Kelvin sign), so ASCII
// letters become classes of both cases instead and nothing else folds. A dot
// stops at JavaScript's line terminators, and alone (.?) it matches one
// UTF-16 unit, which no astral character is. \s and \S are JavaScript's.
// Lookarounds are rewritten by hand where they occur.
func jsCaseless(pattern string) string {
	isLetter := func(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
	both := func(c byte) string { return strings.ToLower(string(c)) + strings.ToUpper(string(c)) }
	const unit, units = `[^\n\r\x{2028}\x{2029}\x{10000}-\x{10ffff}]`, `[^\n\r\x{2028}\x{2029}]`
	var out strings.Builder
	inClass := false
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch {
		case c == '\\' && i+1 < len(pattern):
			i++
			switch next := pattern[i]; {
			case next == 's' && !inClass:
				out.WriteString(jsSpace)
			case next == 'S' && !inClass:
				out.WriteString(jsNonSpace)
			case (next == 's' || next == 'S') && inClass:
				panic("jsCaseless: \\s in a class")
			default:
				out.WriteByte('\\')
				out.WriteByte(next)
			}
		case inClass:
			switch {
			case c == ']':
				inClass = false
				out.WriteByte(c)
			case isLetter(c) && i+2 < len(pattern) && pattern[i+1] == '-' && isLetter(pattern[i+2]):
				lo, hi := pattern[i], pattern[i+2]
				out.WriteString(strings.ToLower(string(lo)) + "-" + strings.ToLower(string(hi)) + strings.ToUpper(string(lo)) + "-" + strings.ToUpper(string(hi)))
				i += 2
			case isLetter(c):
				out.WriteString(both(c))
			default:
				out.WriteByte(c)
			}
		case c == '[':
			inClass = true
			out.WriteByte(c)
			if i+1 < len(pattern) && pattern[i+1] == '^' {
				out.WriteByte('^')
				i++
			}
		case c == '.':
			if i+1 < len(pattern) && pattern[i+1] == '?' {
				out.WriteString(unit)
			} else {
				out.WriteString(units)
			}
		case isLetter(c):
			out.WriteString("[" + both(c) + "]")
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

func providerErrorPattern(patterns ...string) *regexp.Regexp {
	return regexp.MustCompile(jsCaseless(strings.Join(patterns, "|")))
}

// Verbatim from pi-ai v1.5.2 src/utils/retry.ts, in order. The two lookaround
// patterns are rewritten for RE2; the rewrites match the same strings.
var nonRetryableProviderLimitPattern = providerErrorPattern(
	"GoUsageLimitError",
	"FreeUsageLimitError",
	"Monthly usage limit reached",
	"available balance",
	"insufficient_quota",
	"out of budget",
	"quota exceeded",
	"billing",
	"subscription_sharing_usage_limit_exceeded",
	"subscription_sharing_workplace_individual_usage_limit_exceeded",
	// pi-ai: "code"\s*:\s*"?1308(?![0-9])
	`"code"\s*:\s*"?1308(?:[^0-9]|$)`,
)

var retryableProviderErrorPattern = providerErrorPattern(
	"overloaded",
	"server_busy",
	"servers are currently busy",
	"currently experiencing high demand",
	"model is at capacity",
	"rate.?limit",
	"too many requests",
	// pi-ai: (?<![a-z0-9])(?:429|500|502|503|504|520|524)(?![a-z0-9])
	"(?:^|[^a-z0-9])(?:429|500|502|503|504|520|524)(?:[^a-z0-9]|$)",
	"service.?(?:is.?)?(?:temporarily.?)?unavailable",
	"server.?error",
	"internal.?error",
	"provider.?returned.?(?:an.?)?error",
	"exceeded request buffer limit while retrying upstream",
	"unable to forward upstream",
	"upstream.?request.?failed",
	"upstream_stream_read_error",
	"stream was interrupted",
	"http/?2 stream failed",
	"network.?error",
	"connection.?error",
	"connection.?refused",
	"connection.?lost",
	"other side closed",
	"fetch failed",
	`request to \S+ failed, reason:`,
	"getaddrinfo",
	"ENOTFOUND",
	"EAI_AGAIN",
	"upstream.?connect",
	"reset before headers",
	"socket hang up",
	"socket connection was closed",
	"timed? out",
	"timeout",
	"terminated",
	"websocket.?closed",
	"websocket.?error",
	"ended without",
	"stream ended before message_stop",
	"stream ended before a terminal response event",
	"http2 request did not get a response",
	"stream disconnected before completion",
	"cut short after the reply had begun",
	"pending stream has been canceled",
	"retry delay",
	"you can retry your request",
	"try your request again",
	"please retry your request",
	"ResourceExhausted",
	"subscription_sharing_usage_unavailable",
	"subscription_sharing_user_unavailable",
)

func caseless(pattern string) *regexp.Regexp {
	return regexp.MustCompile(jsCaseless(pattern))
}

// Verbatim from pi-ai v1.5.2 src/utils/overflow.ts, in order.
var overflowPatterns = []*regexp.Regexp{
	caseless(`prompt (?:is )?too long`),
	caseless(`prompt exceeds max length`),
	caseless(`request_too_large`),
	caseless(`input is too long for requested model`),
	caseless(`exceeds the context window`),
	caseless(`exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))`),
	caseless(`context window exceeded`),
	caseless(`input token count.*exceeds the maximum`),
	caseless(`maximum prompt length is \d+`),
	caseless(`reduce the length of the messages`),
	caseless(`maximum context length is \d+ tokens`),
	caseless(`exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?`),
	caseless(`input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)`),
	caseless(`exceeds the limit of \d+`),
	caseless(`exceeds the available context size`),
	caseless(`greater than the context length`),
	caseless(`context window exceeds limit`),
	caseless(`exceeded model token limit`),
	caseless(`too large for model with \d+ maximum context length`),
	caseless(`prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?`),
	caseless(`model_context_window_exceeded`),
	caseless(`prompt too long; exceeded (?:max )?context length`),
	caseless(`range of input length should be`),
	caseless(`context[_ ]length[_ ]exceeded`),
	caseless(`too many tokens`),
	caseless(`token limit exceeded`),
	caseless(`^(?:[\w -]+ API error \(413\):|413(?:\s|:|$))`),
}

var cerebrasBodylessOverflowPattern = caseless(`^4(?:00|13)\s*(?:status code)?\s*\(no body\)`)

var nonOverflowPatterns = []*regexp.Regexp{
	caseless(`^(Throttling error|Service unavailable):`),
	caseless(`rate limit`),
	caseless(`too many requests`),
}
