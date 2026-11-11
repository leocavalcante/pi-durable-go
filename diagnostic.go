package durable

import (
	"fmt"
	"runtime/debug"
	"strings"
	"unicode/utf8"
)

const maxDiagnosticBytes = 4 * 1024

// diagnostic bounds persisted runtime diagnostics, not task-authored data.
func diagnostic(message string) string {
	message = strings.ToValidUTF8(message, "\uFFFD")
	if message == "" {
		message = "task failed without an error message"
	}
	if len(message) <= maxDiagnosticBytes {
		return message
	}
	const marker = " [truncated]"
	end := maxDiagnosticBytes - len(marker)
	for !utf8.RuneStart(message[end]) {
		end--
	}
	return message[:end] + marker
}

// panicError retains the stack only in the volatile report, never the outcome.
type panicError struct {
	value any
	stack []byte
}

func (e *panicError) Error() string { return fmt.Sprintf("panic: %v", e.value) }
func (e *panicError) Stack() []byte { return append([]byte(nil), e.stack...) }

func containPanic(fn func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &panicError{value: value, stack: debug.Stack()}
		}
	}()
	return fn()
}
