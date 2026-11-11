package durable

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestSettingsFuncIsReadAtEachUse(t *testing.T) {
	var disabled atomic.Bool
	var reads atomic.Int32
	model := taskModelFunc(func(context.Context, ModelRequest, func(string) error) (ModelResponse, error) {
		return ModelResponse{}, errors.New("503 overloaded")
	})
	h := taskTestHarness(t, HarnessOptions{Model: model, SettingsFunc: func() Settings {
		reads.Add(1)
		if disabled.Load() {
			return Settings{Retry: RetrySettings{Enabled: ptr(false)}}
		}
		return Settings{}
	}})
	root := testRoot(t, h)
	// Retries are disabled by the time the failure is classified.
	disabled.Store(true)
	record := waitSettled(t, h, submit(t, root, SubmissionRequest{Content: "hi"}))
	if record.Reason != RunModelError || reads.Load() == 0 {
		t.Fatalf("record=%+v reads=%d", record, reads.Load())
	}
}
