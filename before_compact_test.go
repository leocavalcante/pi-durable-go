package durable

import (
	"context"
	"strings"
	"testing"
)

func TestBeforeCompactHooksDeclineOrSupplySummaries(t *testing.T) {
	for _, mode := range []string{"decline", "summary"} {
		t.Run(mode, func(t *testing.T) {
			var seen CompactionRequest
			model := &windowModel{}
			registry := NewRegistry()
			if err := registry.Install(Extension{Name: "compactor", Hooks: Hooks{BeforeCompact: func(_ context.Context, request CompactionRequest, _ *TaskRuntime) (CompactionDecision, error) {
				seen = request
				if mode == "decline" {
					return CompactionDecision{Decline: true}, nil
				}
				return CompactionDecision{Summary: "hook summary"}, nil
			}}}); err != nil {
				t.Fatal(err)
			}
			h := taskTestHarness(t, HarnessOptions{Model: model, Registry: registry, Settings: Settings{Compaction: compactEverything}})
			c, _ := compactionConversation(t, h)
			id, err := c.Compact(context.Background(), "focus")
			if err != nil {
				t.Fatal(err)
			}
			if record, err := h.WaitTask(context.Background(), id); err != nil || record.Outcome.Status != OutcomeCompleted {
				t.Fatalf("record=%+v error=%v", record, err)
			}
			if seen.Reason != "manual" || seen.Instructions != "focus" || len(seen.Messages) == 0 || seen.FirstKept == 0 || model.summaries() != 0 {
				t.Fatalf("hook saw %+v; summaries=%d", seen, model.summaries())
			}
			messages, _ := c.Context(context.Background())
			summarized := strings.Contains(messages[0].Content, "hook summary")
			if summarized != (mode == "summary") {
				t.Fatalf("context=%+v", messages)
			}
		})
	}
}
