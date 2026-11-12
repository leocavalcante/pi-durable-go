package durable

// TelemetryAttribute describes one attribute of the durable telemetry
// vocabulary: Type is "string", "number", or "boolean"; Values, when set,
// are the only values it takes; Cardinality "high" marks record IDs.
type TelemetryAttribute struct {
	Type        string   `json:"type"`
	Required    bool     `json:"required,omitempty"`
	Description string   `json:"description"`
	Values      []string `json:"values,omitempty"`
	Cardinality string   `json:"cardinality,omitempty"`
}

// TelemetryEventSchema describes a span event.
type TelemetryEventSchema struct {
	Description string                        `json:"description"`
	Attributes  map[string]TelemetryAttribute `json:"attributes"`
}

// TelemetrySpanSchema describes a span: its attributes at start and end, its
// events, and when it ends with an error.
type TelemetrySpanSchema struct {
	Description     string                          `json:"description"`
	Parents         string                          `json:"parents"`
	StartAttributes map[string]TelemetryAttribute   `json:"startAttributes"`
	EndAttributes   map[string]TelemetryAttribute   `json:"endAttributes"`
	Events          map[string]TelemetryEventSchema `json:"events"`
	StatusDefault   string                          `json:"statusDefault"`
	ErrorWhen       string                          `json:"errorWhen"`
}

// TelemetryMetricSchema describes a metric: Kind is "counter",
// "upDownCounter", or "histogram".
type TelemetryMetricSchema struct {
	Kind        string                        `json:"kind"`
	Unit        string                        `json:"unit,omitempty"`
	Description string                        `json:"description"`
	Attributes  map[string]TelemetryAttribute `json:"attributes"`
}

// TelemetrySchema is the content-free span, event, and metric vocabulary a
// Harness emits (upstream's DURABLE_TELEMETRY_SCHEMA): IDs address durable
// records, never content.
type TelemetrySchema struct {
	Version int                              `json:"version"`
	Spans   map[string]TelemetrySpanSchema   `json:"spans"`
	Metrics map[string]TelemetryMetricSchema `json:"metrics"`
}

// DurableTelemetrySchema returns the vocabulary as upstream v1.5.2 defines
// it, a fresh value each call.
func DurableTelemetrySchema() TelemetrySchema {
	entry := TelemetryEventSchema{
		Description: "A transcript entry was committed; content remains in the transcript.",
		Attributes: map[string]TelemetryAttribute{
			"entry.id":   {Type: "number", Required: true, Description: "Transcript entry ID.", Cardinality: "high"},
			"entry.kind": {Type: "string", Required: true, Description: "Entry kind."},
		},
	}
	reportType := TelemetryAttribute{
		Type:        "string",
		Required:    true,
		Description: "Report kind (`Report.kind`).",
		Values:      []string{"storage_failed", "task_faulted", "task_interrupted", "migration_failed", "task_outdated", "task_error", "extension_error", "host_error", "listener_error"},
	}
	reportAttributes := func() map[string]TelemetryAttribute {
		return map[string]TelemetryAttribute{
			"type":      reportType,
			"reason":    {Type: "string", Description: "Fixed report reason, when present.", Values: []string{"threw", "restart_limit", "interrupted", "missing_task", "task_too_old", "migration_failed"}},
			"extension": {Type: "string", Description: "Registered extension name, when present."},
			"task.id":   {Type: "number", Description: "Task ID, when present.", Cardinality: "high"},
		}
	}
	report := func() TelemetryEventSchema {
		return TelemetryEventSchema{Description: "A report was raised; no error payload is recorded.", Attributes: reportAttributes()}
	}
	submission := func() TelemetryEventSchema {
		return TelemetryEventSchema{
			Description: "A submission was settled.",
			Attributes: map[string]TelemetryAttribute{
				"submission.id":   {Type: "number", Required: true, Description: "Submission ID.", Cardinality: "high"},
				"status":          {Type: "string", Required: true, Values: []string{"done", "unanswered"}, Description: "Settlement."},
				"answer.entry_id": {Type: "number", Description: "Answer entry ID.", Cardinality: "high"},
			},
		}
	}
	return TelemetrySchema{
		Version: 1,
		Spans: map[string]TelemetrySpanSchema{
			"pi.durable.submit": {
				Description: "Public submission admission: with its commit for submit(draft, context); the admission only for submit(tx, draft), whose result is provisional (a later throw or storage failure of the caller's commit leaves no submission) and whose commit events go to the caller's commit context. Inputs and continues start root traces linked to their callers; writes are children. Internal admissions emit phase events only.",
				Parents:     "any",
				StartAttributes: map[string]TelemetryAttribute{
					"submission.type": {Type: "string", Required: true, Values: []string{"input", "write", "continue"}, Description: "Submission type."},
					"conversation.id": {Type: "number", Required: true, Description: "Conversation ID.", Cardinality: "high"},
				},
				EndAttributes: map[string]TelemetryAttribute{
					"submission.id": {Type: "number", Description: "Admitted submission ID.", Cardinality: "high"},
					"result":        {Type: "string", Values: []string{"queued", "placed", "done"}, Description: "Admission result; stale writes report done."},
				},
				Events:        map[string]TelemetryEventSchema{"pi.durable.entry": entry, "pi.durable.submission": submission(), "pi.durable.report": report()},
				StatusDefault: "ok",
				ErrorWhen:     "Admission failed.",
			},
			"pi.durable.phase": {
				Description: "One task phase invocation, including its decision commit, never a wait or restart.",
				Parents:     "any",
				StartAttributes: map[string]TelemetryAttribute{
					"task.id":   {Type: "number", Required: true, Description: "Task ID.", Cardinality: "high"},
					"task.name": {Type: "string", Required: true, Description: "Registered task name."},
					"phase":     {Type: "string", Required: true, Description: "Invoked checkpoint phase."},
					"kind":      {Type: "string", Required: true, Values: []string{"run", "abort"}, Description: "Invocation kind."},
					"attempt":   {Type: "number", Description: "Checkpoint attempt, when present."},
					"resumed":   {Type: "boolean", Required: true, Description: "First invocation of a task loaded in this process."},
				},
				EndAttributes: map[string]TelemetryAttribute{
					"outcome":      {Type: "string", Description: "next=<phase>, waiting, sleeping, done, faulted, or aborted."},
					"tool.name":    {Type: "string", Description: "Called tool name."},
					"tool.call_id": {Type: "string", Description: "Transcript tool call ID.", Cardinality: "high"},
				},
				Events: map[string]TelemetryEventSchema{
					"pi.durable.entry":      entry,
					"pi.durable.submission": submission(),
					"pi.durable.report":     report(),
					"pi.durable.checkpoint": {
						Description: "The running checkpoint's phase changed while the invocation continued.",
						Attributes:  map[string]TelemetryAttribute{"phase": {Type: "string", Required: true, Description: "Committed checkpoint phase."}},
					},
					"pi.durable.replay": {
						Description: "A nested tool call was answered from its stored result.",
						Attributes:  map[string]TelemetryAttribute{"tool.name": {Type: "string", Required: true, Description: "Called tool name."}},
					},
				},
				StatusDefault: "ok",
				ErrorWhen:     "Faulted, failed, and orphaned outcomes use task_faulted, task_failed, and task_orphaned with a diagnostic capped at 500 characters. Aborted outcomes keep status ok.",
			},
		},
		Metrics: map[string]TelemetryMetricSchema{
			"pi.durable.reports": {
				Kind:        "counter",
				Description: "Reports raised, grouped by their fixed kind.",
				Attributes:  map[string]TelemetryAttribute{"type": reportType},
			},
			"pi.durable.phase.duration": {
				Kind:        "histogram",
				Unit:        "ms",
				Description: "Duration of a phase invocation and its decision commit.",
				Attributes: map[string]TelemetryAttribute{
					"task":    {Type: "string", Required: true, Description: "Registered task name."},
					"phase":   {Type: "string", Required: true, Description: "Invoked checkpoint phase."},
					"outcome": {Type: "string", Required: true, Description: "Phase outcome."},
				},
			},
			"pi.durable.tasks.active": {
				Kind:        "upDownCounter",
				Description: "Live tasks held by this Harness; decremented at terminal settlement or close.",
				Attributes:  map[string]TelemetryAttribute{"task": {Type: "string", Required: true, Description: "Registered task name."}},
			},
		},
	}
}
