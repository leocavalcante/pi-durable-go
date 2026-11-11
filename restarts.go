package durable

import (
	"encoding/json"
	"fmt"
	"math"
)

// defaultRestartLimit is Pi Durable's DEFAULT_RESTART_LIMIT.
const defaultRestartLimit = 5

// Detail reasons for outcomes of tasks that crashes ended (Pi Durable §5.4).
const (
	reasonRestartLimit = "restart_limit"
	reasonInterrupted  = "interrupted"
)

var (
	restartLimitDetail = json.RawMessage(`{"reason":"restart_limit"}`)
	interruptedDetail  = json.RawMessage(`{"reason":"interrupted"}`)
)

// normalizeRestartLimit follows upstream restartLimit(): a number of 1 or more
// is rounded down, +Inf never ends a task, and anything else (including NaN,
// zero, and negative values) selects the default.
func normalizeRestartLimit(limit float64) float64 {
	if limit >= 1 { // NaN compares false
		return math.Floor(limit)
	}
	return defaultRestartLimit
}

func (h *Harness) restartLimit() float64 {
	return normalizeRestartLimit(h.settings().RestartLimit)
}

// cutOffTooOften reports whether a task was cut off the restart limit times in
// a row, so its next phase ends it instead of running again.
func cutOffTooOften(restarts int, limit float64) bool {
	return restarts > 0 && float64(restarts) >= limit
}

// salvageable reports whether a task at the limit may still do the cleanup it
// does when it ends, such as converting a partial: only at the limit. Past it,
// that cleanup may be what keeps cutting the task off.
func salvageable(restarts int, limit float64) bool {
	return float64(restarts) <= limit
}

// cutOffCount describes how often a task was cut off: "once" or "3 times in a row".
func cutOffCount(restarts int) string {
	if restarts == 1 {
		return "once"
	}
	return fmt.Sprintf("%d times in a row", restarts)
}

// limitsRestarts names the built-in kinds whose phases end themselves at the
// limit; the scheduler faults a run of any other kind instead.
func limitsRestarts(kind string) bool {
	switch kind {
	case "pi.generation", "pi.tool", "pi.compaction":
		return true
	}
	return false
}

// detailReason returns a JSON object's string "reason" field, or "" when the
// detail is not an object or has no string reason.
func detailReason(detail json.RawMessage) string {
	if len(detail) == 0 {
		return ""
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(detail, &fields) != nil || fields == nil {
		return ""
	}
	var reason string
	if json.Unmarshal(fields["reason"], &reason) != nil {
		return ""
	}
	return reason
}

// interruptionOf reports whether a failed outcome says crashes ended its task:
// restart_limit or interrupted in ErrorDetail's reason.
func interruptionOf(outcome *Outcome) string {
	if outcome == nil || outcome.Status != OutcomeFailed {
		return ""
	}
	switch reason := detailReason(outcome.ErrorDetail); reason {
	case reasonRestartLimit, reasonInterrupted:
		return reason
	}
	return ""
}
