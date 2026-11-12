package durable

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSectionWrapsApplyInExtensionOrder(t *testing.T) {
	var mu sync.Mutex
	var requests []ModelRequest
	model := taskModelFunc(func(_ context.Context, req ModelRequest, _ func(string) error) (ModelResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, req)
		return ModelResponse{Content: "ok"}, nil
	})
	text := func(s string) func(context.Context, PromptInput) (string, error) {
		return func(context.Context, PromptInput) (string, error) { return s, nil }
	}
	reports := make(chan Report, 4)
	registry := NewRegistry()
	for _, extension := range []Extension{
		{Name: "base", Sections: []Section{{Name: "rules", Render: text("be kind")}, {Name: "dropped", Render: text("gone")}, {Name: "broken", Render: text("kept?")}}},
		{Name: "outer", WrapSections: map[string]func(Section) Section{
			"rules": func(inner Section) Section {
				return Section{Name: "rules", Render: func(ctx context.Context, input PromptInput) (string, error) {
					text, err := inner.Render(ctx, input)
					return strings.ToUpper(text), err
				}}
			},
			"dropped": func(Section) Section { return Section{Name: "renamed"} },
			"broken":  func(Section) Section { panic("wrap failed") },
			"missing": func(s Section) Section { return s },
		}},
	} {
		if err := registry.Install(extension); err != nil {
			t.Fatal(err)
		}
	}
	h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, OnReport: func(r Report) { reports <- r }})
	root, err := h.Root(context.Background(), ConversationOptions{Agent: Agent{Model: ModelRef{Provider: "fake", ID: "m"}}})
	if err != nil {
		t.Fatal(err)
	}
	waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	mu.Lock()
	prompt := promptText(requests[0].Messages)
	mu.Unlock()
	// The wrapped section renders through its wrap; a renamed or panicking
	// wrap drops its target.
	if prompt != "<rules>\nBE KIND\n</rules>" {
		t.Fatalf("prompt=%q", prompt)
	}
	// Both are reported as the wrapping extension's errors (upstream).
	var messages []string
	for len(messages) < 2 {
		select {
		case report := <-reports:
			if report.Kind != ReportExtensionError || report.Extension != "outer" {
				t.Fatalf("report=%+v", report)
			}
			messages = append(messages, report.Err.Error())
		case <-time.After(5 * time.Second):
			t.Fatalf("reports=%q", messages)
		}
	}
	joined := strings.Join(messages, "\n")
	if !strings.Contains(joined, "Wrapper renamed dropped to renamed") || !strings.Contains(joined, "wrap panicked: wrap failed") {
		t.Fatalf("reports=%q", messages)
	}
}
