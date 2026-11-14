package model

import (
	"context"
	"strings"
	"testing"

	durable "github.com/leocavalcante/pi-durable-go"
)

func TestSSEErrorEventIsNotSuccessfulEmptyAnswer(t *testing.T) {
	_, err := parseSSE(context.Background(), strings.NewReader("data: {\"error\":{\"message\":\"synthetic private detail\"}}\n\ndata: [DONE]\n\n"), nil, durable.ModelRef{}, true, nil)
	if err == nil {
		t.Fatal("stream error reported success")
	}
	if strings.Contains(err.Error(), "private detail") {
		t.Fatal("provider error body disclosed")
	}
}
