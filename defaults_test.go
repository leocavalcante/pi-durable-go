package durable

import (
	"testing"
	"time"
)

func TestExportedDefaultsAreTheFallbacks(t *testing.T) {
	if got, want := DefaultRetrySettings().resolve(), (RetrySettings{}).resolve(); got != want {
		t.Fatalf("retry defaults=%+v, fallback %+v", got, want)
	}
	model := ModelRef{Provider: "p", ID: "m"}
	if got, want := resolveCompaction(DefaultCompactionSettings(), nil, model), resolveCompaction(CompactionSettings{}, nil, model); got != want {
		t.Fatalf("compaction defaults=%+v, fallback %+v", got, want)
	}
	progress := DefaultProgressSettings()
	if time.Duration(*progress.PartialIntervalMs)*time.Millisecond != defaultPartialInterval || *progress.OutputIntervalMs != 100 || *progress.PersistWithinMs != 1000 {
		t.Fatalf("progress defaults=%+v", progress)
	}
	// Fresh values: changing one does not change the next.
	*DefaultRetrySettings().MaxRetries = 9
	if *DefaultRetrySettings().MaxRetries != 3 {
		t.Fatal("a default was shared")
	}
	if DefaultRestartLimit != 5 {
		t.Fatalf("restart limit=%d", DefaultRestartLimit)
	}
}
