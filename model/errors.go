package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

	durable "github.com/leocavalcante/pi-durable-go"
)

// providerError is a sanitized adapter error. Its message never contains
// provider text: Retryable and ContextOverflow carry the classification the
// adapter computed from that text, with Pi Durable's pi-ai patterns, before
// discarding it.
type providerError struct {
	message   string
	retryable bool
	overflow  bool
}

func (e *providerError) Error() string         { return e.message }
func (e *providerError) Retryable() bool       { return e.retryable }
func (e *providerError) ContextOverflow() bool { return e.overflow }

var (
	_ durable.RetryableError       = (*providerError)(nil)
	_ durable.ContextOverflowError = (*providerError)(nil)
)

// classifiedError returns message, classified by the private provider text.
func classifiedError(message, text, provider string) error {
	cause := errors.New(text)
	return &providerError{message: message, retryable: durable.IsRetryableModelError(cause),
		overflow: durable.IsContextOverflowError(cause, provider)}
}

// sanitizedError is an adapter error whose classification is known.
func sanitizedError(message string, retryable bool) error {
	return &providerError{message: message, retryable: retryable}
}

// maxProviderErrorChars is pi-ai's MAX_PROVIDER_ERROR_BODY_CHARS.
const maxProviderErrorChars = 4000

// maxErrorBodyBytes bounds how much of an error response is read.
const maxErrorBodyBytes = 64 << 10

func truncateErrorText(text string) string {
	if len(text) <= maxProviderErrorChars {
		return text
	}
	cut := maxProviderErrorChars
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "... [truncated]"
}

func compactJSON(raw json.RawMessage) string {
	var out bytes.Buffer
	if json.Compact(&out, raw) != nil {
		return string(raw)
	}
	return out.String()
}

func truthyJSON(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case float64:
		return v != 0
	case string:
		return v != ""
	}
	return true
}

// openAIErrorText reproduces the message pi-ai reports for a failed HTTP
// response from the openai SDK: the SDK's status message, or, when the body
// has an `error` object, "<status>: <that object>" as pi-ai's
// formatProviderError composes it. It is only classified, never returned.
func openAIErrorText(status int, body []byte) string {
	noBody := fmt.Sprintf("%d status code (no body)", status)
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		var value any
		if json.Unmarshal(body, &value) == nil && truthyJSON(value) {
			return noBody // JSON without an error field
		}
		if len(body) == 0 {
			return noBody
		}
		return fmt.Sprintf("%d %s", status, truncateErrorText(string(body)))
	}
	raw, ok := fields["error"]
	if !ok {
		return noBody
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) == nil && len(object) > 0 {
		return fmt.Sprintf("%d: %s", status, truncateErrorText(compactJSON(raw)))
	}
	var value any
	if json.Unmarshal(raw, &value) != nil || !truthyJSON(value) {
		return noBody
	}
	return fmt.Sprintf("%d %s", status, truncateErrorText(compactJSON(raw)))
}

// streamErrorText is the message of an SSE error event, as the openai SDK
// raises it: the error's message, or the error itself as JSON.
func streamErrorText(raw json.RawMessage) string {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) == nil {
		var value any
		if message, ok := object["message"]; ok && json.Unmarshal(message, &value) == nil && truthyJSON(value) {
			if text, ok := value.(string); ok {
				return truncateErrorText(text)
			}
			return truncateErrorText(compactJSON(message))
		}
	}
	return truncateErrorText(compactJSON(raw))
}
