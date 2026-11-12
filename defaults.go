package durable

// The defaults Settings fall back to, as upstream exports them
// (DEFAULT_RETRY_POLICY, DEFAULT_COMPACTION_POLICY, DEFAULT_PROGRESS_POLICY,
// DEFAULT_RESTART_LIMIT). Each call returns fresh values.

// DefaultRetrySettings is the retry policy: enabled, 3 retries, a 2000 ms
// base delay that doubles, capped at 60000 ms.
func DefaultRetrySettings() RetrySettings {
	return RetrySettings{Enabled: boolPtr(true), MaxRetries: floatPtr(3), BaseDelayMs: floatPtr(2000), MaxAgentDelayMs: floatPtr(defaultMaxAgentDelayMs)}
}

// DefaultCompactionSettings is the compaction policy: enabled, 16384 tokens
// reserved, 20000 recent tokens kept, background compaction from 32768.
func DefaultCompactionSettings() CompactionSettings {
	return CompactionSettings{Enabled: boolPtr(true), ReserveTokens: floatPtr(16384), KeepRecentTokens: floatPtr(20000), BackgroundTokens: floatPtr(32768)}
}

// DefaultProgressSettings is the progress policy: partials and tool output
// committed at most every 100 ms, stored within 1000 ms.
func DefaultProgressSettings() ProgressSettings {
	return ProgressSettings{PartialIntervalMs: floatPtr(100), OutputIntervalMs: floatPtr(100), PersistWithinMs: floatPtr(1000)}
}

// DefaultRestartLimit is how many times in a row a crash may cut a task off
// before it ends instead of running again.
const DefaultRestartLimit = defaultRestartLimit

func boolPtr(v bool) *bool        { return &v }
func floatPtr(v float64) *float64 { return &v }
