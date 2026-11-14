package codemode

// NestedCall is one tool call a script made, as the code mode call's details
// show it (upstream CodemodeNestedCall). Its nested call ID is
// "<callId>/c<ID>".
type NestedCall struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	// Args is the compact JSON of the arguments, cut for display.
	Args string `json:"args"`
	// Status is running, ok, error, or cancelled.
	Status     string `json:"status"`
	DurationMs *int64 `json:"durationMs,omitempty"`
	// Error is the error text, cut for display.
	Error string `json:"error,omitempty"`
}

// ToolDetails are the code mode call's details, in its result: its tool
// calls in the order it made them, with their status (upstream
// CodemodeToolDetails). Calls to globals are not listed.
type ToolDetails struct {
	Calls []NestedCall `json:"calls"`
	// FullOutputPath is the temp file with the complete text output, when
	// the output was cut to its token budget.
	FullOutputPath string `json:"fullOutputPath,omitempty"`
}
