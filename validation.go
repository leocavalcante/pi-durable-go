package durable

import "errors"

// noSchemaNetwork refuses every external schema reference: tool schemas are
// local resources, and validating untrusted model arguments must never cause
// schema-controlled filesystem or network requests.
type noSchemaNetwork struct{}

func (noSchemaNetwork) Load(url string) (any, error) {
	return nil, errors.New("durable: external schema references are disabled")
}
