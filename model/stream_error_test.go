package model

import (
	"context"
	"strings"
	"testing"
)

func TestSSEErrorEventIsNotSuccessfulEmptyAnswer(t *testing.T) {
	_, err := parseSSE(context.Background(), strings.NewReader("data: {\"error\":{\"message\":\"synthetic private detail\"}}\n\ndata: [DONE]\n\n"), nil)
	if err == nil {
		t.Fatal("stream error reported success")
	}
	if strings.Contains(err.Error(), "private detail") {
		t.Fatal("provider error body disclosed")
	}
}
