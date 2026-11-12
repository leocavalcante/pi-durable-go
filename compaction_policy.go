package durable

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"unicode/utf16"
)

// CompactionSettings is Pi Durable's compaction policy (settings.compaction,
// and per conversation Agent.Compaction). Nil fields keep the next layer's
// value; the defaults are enabled, ReserveTokens 16384, KeepRecentTokens
// 20000, and BackgroundTokens 32768. ModelOverrides, keyed "provider/id",
// override fields for one model.
type CompactionSettings struct {
	Enabled          *bool                         `json:"enabled,omitempty"`
	ReserveTokens    *float64                      `json:"reserveTokens,omitempty"`
	KeepRecentTokens *float64                      `json:"keepRecentTokens,omitempty"`
	BackgroundTokens *float64                      `json:"backgroundTokens,omitempty"`
	ModelOverrides   map[string]CompactionSettings `json:"modelOverrides,omitempty"`
}

// compactionPolicy is a resolved CompactionSettings.
type compactionPolicy struct {
	enabled                                           bool
	reserveTokens, keepRecentTokens, backgroundTokens float64
}

// ContextWindowModel is implemented by models that know a model's context
// window in tokens; zero means unknown, which disables threshold compaction.
type ContextWindowModel interface {
	ContextWindow(ModelRef) int
}

// ImageInputModel is optionally implemented by a Model that knows which
// models take images (pi-ai's model input). Without it, every model is taken
// to accept them, as upstream assumes for a model it cannot resolve.
type ImageInputModel interface {
	AcceptsImages(ModelRef) bool
}

// ImageLimits bound the images a model takes: pixels per side and base64
// bytes (pi-ai's inputLimits.images.resize). Zero fields keep the defaults.
type ImageLimits struct {
	MaxWidth  int `json:"maxWidth,omitempty"`
	MaxHeight int `json:"maxHeight,omitempty"`
	MaxBytes  int `json:"maxBytes,omitempty"`
}

// ImageLimitsModel is optionally implemented by a Model that knows a model's
// image limits from its catalog.
type ImageLimitsModel interface {
	ImageLimits(ModelRef) ImageLimits
}

// resolveCompaction resolves the policy for a model in a conversation, field
// by field: the conversation's model override, the Harness's model override,
// the conversation's own fields, then the Harness-wide policy over the
// defaults. The conversation's own Enabled false also beats the Harness's
// model override.
func resolveCompaction(harness CompactionSettings, conversation *CompactionSettings, model ModelRef) compactionPolicy {
	key := model.Provider + "/" + model.ID
	var layers []*CompactionSettings
	var conversationModel *CompactionSettings
	if conversation != nil {
		if override, ok := conversation.ModelOverrides[key]; ok {
			conversationModel = &override
			layers = append(layers, conversationModel)
		}
	}
	if override, ok := harness.ModelOverrides[key]; ok {
		layers = append(layers, &override)
	}
	if conversation != nil {
		layers = append(layers, conversation)
	}
	layers = append(layers, &harness)
	number := func(get func(*CompactionSettings) *float64, fallback float64) float64 {
		for _, layer := range layers {
			if v := get(layer); v != nil {
				return *v
			}
		}
		return fallback
	}
	policy := compactionPolicy{
		enabled:          true,
		reserveTokens:    number(func(c *CompactionSettings) *float64 { return c.ReserveTokens }, 16384),
		keepRecentTokens: number(func(c *CompactionSettings) *float64 { return c.KeepRecentTokens }, 20000),
		backgroundTokens: number(func(c *CompactionSettings) *float64 { return c.BackgroundTokens }, 32768),
	}
	switch {
	case conversationModel != nil && conversationModel.Enabled != nil:
		policy.enabled = *conversationModel.Enabled
	case conversation != nil && conversation.Enabled != nil && !*conversation.Enabled:
		policy.enabled = false
	default:
		for _, layer := range layers {
			if layer.Enabled != nil {
				policy.enabled = *layer.Enabled
				break
			}
		}
	}
	return policy
}

// Upstream's summary message around a compaction summary.
const (
	summaryPrefix = "The conversation history before this point was compacted into the following summary:\n\n<summary>\n"
	summarySuffix = "\n</summary>"
)

// compactionSummary is a summary entry's model message and cut. Summaries
// written before the upstream format carry only the summary text.
func compactionSummary(e Entry) (Message, EntryID, error) {
	var comp struct {
		Summary string  `json:"summary"`
		Cut     EntryID `json:"cut"`
		Text    string  `json:"text"`
	}
	if err := json.Unmarshal(e.Data, &comp); err != nil {
		return Message{}, 0, err
	}
	text := comp.Text
	if text == "" {
		text = "Conversation summary:\n" + comp.Summary
	}
	return Message{Role: "user", Content: text}, comp.Cut, nil
}

// estimateMessageTokens ports pi-ai's estimate: 3.5 characters per token.
// estimatedImageChars is what pi-ai counts for an image (ESTIMATED_IMAGE_CHARS).
const estimatedImageChars = 4800

func estimateMessageTokens(m Message) int {
	if m.Role == "system" {
		// pi-ai: the rendered prompt text, plus the JSON of the tool changes.
		tokens := estimateTextTokens(SystemText(m))
		if len(m.ToolsAdded) > 0 {
			tokens += estimateTextTokens(stringifyJSON(m.ToolsAdded))
		}
		if len(m.ToolsRemoved) > 0 {
			removed := make([]map[string]string, 0, len(m.ToolsRemoved))
			for _, name := range m.ToolsRemoved {
				removed = append(removed, map[string]string{"name": name})
			}
			tokens += estimateTextTokens(stringifyJSON(removed))
		}
		return tokens
	}
	// Lengths are JavaScript's, UTF-16 code units, as pi-ai counts.
	chars := utf16Len(m.Content) + estimatedImageChars*len(m.Images)
	if m.Role == "assistant" {
		chars += utf16Len(m.Thinking)
		for _, call := range m.ToolCalls {
			chars += utf16Len(call.Name) + utf16Len(jsStringifyRaw(call.Arguments))
		}
	}
	return int(math.Ceil(float64(chars) / 3.5))
}

func estimateTextTokens(text string) int {
	return int(math.Ceil(float64(utf16Len(text)) / 3.5))
}

// stringifyJSON encodes v as JavaScript's JSON.stringify does for estimates:
// compact, without Go's HTML escapes of <, >, and &.
func stringifyJSON(v any) string {
	var out strings.Builder
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(v) != nil {
		return ""
	}
	return strings.TrimSuffix(out.String(), "\n")
}

// utf16Len is a string's JavaScript length: UTF-16 code units.
func utf16Len(text string) int {
	n := 0
	for _, r := range text {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

func usageTokens(u Usage) int {
	return int(u.InputTokens + u.OutputTokens + u.CacheReadTokens + u.CacheWriteTokens)
}

// estimateContext is the size of a request over the view followed by extra:
// the usage of the newest assistant after the head marker, whose request
// included everything before it, plus estimates of the messages after it.
func estimateContext(view contextView, extra []Message) int {
	measured := -1
	tokens := 0
	for i := len(view.entries) - 1; i >= 0 && measured < 0; i-- {
		if view.head != nil && view.entries[i].ID <= view.head.ID {
			continue
		}
		for j := len(view.contributions[i]) - 1; j >= 0; j-- {
			if m := view.contributions[i][j]; m.Role == "assistant" && usageTokens(m.Usage) > 0 {
				measured, tokens = i, usageTokens(m.Usage)
				break
			}
		}
	}
	for i := measured + 1; i < len(view.contributions); i++ {
		for _, m := range view.contributions[i] {
			tokens += estimateMessageTokens(m)
		}
	}
	for _, m := range extra {
		tokens += estimateMessageTokens(m)
	}
	return tokens
}

// selectCut returns the index in view.entries of the first entry a summary
// keeps, or -1 when there is nothing to compact (spec §8.7): walk back from
// the tail until keepRecentTokens are kept, then cut at the first candidate
// at or after that entry: an entry whose contribution starts with a user or
// assistant message, never a tool result, and never a user entry that a
// result of the preceding assistant's calls still follows.
func selectCut(view contextView, keepRecentTokens float64) int {
	start := 0
	if view.head != nil {
		start = 1
	}
	var candidates []int
	for i := start; i < len(view.contributions); i++ {
		if isCutCandidate(view.contributions, i) {
			candidates = append(candidates, i)
		}
	}
	kept := 0.0
	cut := -1
	for i := len(view.contributions) - 1; i >= start; i-- {
		for _, m := range view.contributions[i] {
			kept += float64(estimateMessageTokens(m))
		}
		if kept < keepRecentTokens {
			continue
		}
		for _, candidate := range candidates {
			if candidate >= i {
				cut = candidate
				break
			}
		}
		if cut < 0 && len(candidates) > 0 {
			cut = candidates[len(candidates)-1]
		}
		break
	}
	if cut < 0 {
		return -1
	}
	for i := start; i < cut; i++ {
		if len(view.contributions[i]) > 0 {
			return cut
		}
	}
	return -1
}

func isCutCandidate(contributions [][]Message, index int) bool {
	if len(contributions[index]) == 0 {
		return false
	}
	first := contributions[index][0]
	if first.Role == "assistant" {
		return true
	}
	if first.Role != "user" {
		return false
	}
	calls := map[string]bool{}
	for before := index - 1; before >= 0; before-- {
		found := false
		for j := len(contributions[before]) - 1; j >= 0; j-- {
			if m := contributions[before][j]; m.Role == "assistant" {
				for _, call := range m.ToolCalls {
					calls[call.ID] = true
				}
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if len(calls) == 0 {
		return true
	}
	for after := index; after < len(contributions); after++ {
		for position, m := range contributions[after] {
			if m.Role == "assistant" && (after > index || position > 0) {
				return true
			}
			if m.Role == "tool" && calls[m.ToolCallID] {
				return false
			}
		}
	}
	return true
}

const summarizationSystemPrompt = `You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.

Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.`

const summarizationPrompt = `The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.

Use this EXACT format:

## Goal
[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

## Constraints & Preferences
- [Any constraints, preferences, or requirements mentioned by user]
- [Or "(none)" if none were mentioned]

## Progress
### Done
- [x] [Completed tasks/changes]

### In Progress
- [ ] [Current work]

### Blocked
- [Issues preventing progress, if any]

## Key Decisions
- **[Decision]**: [Brief rationale]

## Next Steps
1. [Ordered list of what should happen next]

## Critical Context
- [Any data, examples, or references needed to continue]
- [Or "(none)" if not applicable]

Keep each section concise. Preserve exact file paths, function names, and error messages.`

// toolResultMaxChars is the longest tool result text a summary source keeps.
const toolResultMaxChars = 2000

// serializeConversation renders messages as plain text, so the summarizer
// reads a transcript instead of continuing it. System messages and thinking
// are omitted.
func serializeConversation(messages []Message) string {
	var parts []string
	for _, m := range messages {
		switch m.Role {
		case "user":
			if text := contentText(m); text != "" {
				parts = append(parts, "[User]: "+text)
			}
		case "assistant":
			if m.Content != "" {
				parts = append(parts, "[Assistant]: "+m.Content)
			}
			var calls []string
			for _, call := range m.ToolCalls {
				// Object.entries and JSON.stringify of the parsed arguments.
				var rendered []string
				if parsed, err := parseJS(call.Arguments); err == nil {
					if object, ok := parsed.(*jsObject); ok {
						for _, key := range object.keys {
							rendered = append(rendered, key+"="+jsStringify(object.values[key]))
						}
					}
				}
				calls = append(calls, call.Name+"("+strings.Join(rendered, ", ")+")")
			}
			if len(calls) > 0 {
				parts = append(parts, "[Assistant tool calls]: "+strings.Join(calls, "; "))
			}
		case "tool":
			if text := contentText(m); text != "" {
				// JavaScript lengths and slicing: UTF-16 code units.
				if units := utf16.Encode([]rune(text)); len(units) > toolResultMaxChars {
					text = fmt.Sprintf("%s\n\n[... %d more characters truncated]", string(utf16.Decode(units[:toolResultMaxChars])), len(units)-toolResultMaxChars)
				}
				parts = append(parts, "[Tool result]: "+text)
			}
		}
	}
	return strings.Join(parts, "\n\n")
}

func summaryPrompt(messages []Message, instructions string) string {
	focus := ""
	if instructions != "" {
		focus = "\n\nAdditional focus: " + instructions
	}
	return "<conversation>\n" + serializeConversation(messages) + "\n</conversation>\n\n" + summarizationPrompt + focus
}

// contentText is a message's text blocks joined by newlines, as upstream
// joins the text around its images.
func contentText(m Message) string {
	if len(m.Images) == 0 || len(m.ImageOffsets) != len(m.Images) {
		return m.Content
	}
	var texts []string
	for _, part := range OrderedContent(m.Content, m.Images, m.ImageOffsets) {
		if part.Image == nil {
			texts = append(texts, part.Text)
		}
	}
	return strings.Join(texts, "\n")
}
