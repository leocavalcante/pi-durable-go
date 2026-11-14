package model

import (
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf16"

	durable "github.com/leocavalcante/pi-durable-go"
)

// sameOpenAIModel reports whether model.OpenAI gave answer m for model
// (its ProviderData), as pi-ai compares an answer's provider, API, and model.
func sameOpenAIModel(m durable.Message, model durable.ModelRef) (openAIReplay, bool) {
	var replay openAIReplay
	same := json.Unmarshal(m.ProviderData, &replay) == nil && replay.Format == openAIReplayFormat &&
		replay.Provider == model.Provider && replay.Model == model.ID
	return replay, same
}

// transformMessages is pi-ai's transformMessages for openai-completions: an
// answer from another model gets its tool call IDs normalized (and its tool
// results follow), a failed or aborted answer is not replayed, and a tool
// call left without a result gets pi-ai's synthetic error result before the
// next answer or user message.
func transformMessages(messages []durable.Message, model durable.ModelRef) []durable.Message {
	renamed := map[string]string{}
	var out []durable.Message
	var pending []durable.ToolCall
	answered := map[string]bool{}
	close := func() {
		for _, call := range pending {
			if !answered[call.ID] {
				out = append(out, durable.Message{Role: "tool", ToolCallID: call.ID, ToolName: call.Name, Content: "No result provided", IsError: true})
			}
		}
		pending, answered = nil, map[string]bool{}
	}
	for _, m := range messages {
		switch m.Role {
		case "assistant":
			if _, same := sameOpenAIModel(m, model); !same && len(m.ToolCalls) > 0 {
				calls := make([]durable.ToolCall, len(m.ToolCalls))
				for i, call := range m.ToolCalls {
					if id := normalizeToolCallID(call.ID, model.Provider); id != call.ID {
						renamed[call.ID] = id
						call.ID = id
					}
					calls[i] = call
				}
				m.ToolCalls = calls
			}
			close()
			if m.StopReason == "error" || m.StopReason == "aborted" || m.Aborted {
				continue
			}
			if len(m.ToolCalls) > 0 {
				pending, answered = m.ToolCalls, map[string]bool{}
			}
			out = append(out, m)
		case "tool":
			if id, ok := renamed[m.ToolCallID]; ok {
				m.ToolCallID = id
			}
			answered[m.ToolCallID] = true
			out = append(out, m)
		case "user":
			close()
			out = append(out, m)
		default:
			out = append(out, m)
		}
	}
	close()
	return out
}

// normalizeToolCallID is pi-ai's normalizeToolCallId for Chat Completions:
// a pipe-separated ID (OpenAI Responses' call_id|item_id) becomes
// call_id_item_id in [a-zA-Z0-9_-], within 40 characters by a hash; OpenAI's
// own IDs are cut to 40.
func normalizeToolCallID(id, provider string) string {
	units := utf16.Encode([]rune(id))
	pipe := -1
	for i, u := range units {
		if u == '|' {
			pipe = i
			break
		}
	}
	if pipe >= 0 {
		sanitize := func(part []uint16) string {
			var b strings.Builder
			for _, u := range part {
				if u < 128 && (u >= 'a' && u <= 'z' || u >= 'A' && u <= 'Z' || u >= '0' && u <= '9' || u == '_' || u == '-') {
					b.WriteByte(byte(u))
				} else {
					b.WriteByte('_')
				}
			}
			return b.String()
		}
		callID, itemID := sanitize(units[:pipe]), sanitize(units[pipe+1:])
		combined := callID
		if itemID != "" {
			combined += "_" + itemID
		}
		if len(combined) <= 40 {
			return combined
		}
		hash := shortHash(id)
		if len(hash) > 8 {
			hash = hash[:8]
		}
		prefix := callID[:min(len(callID), max(1, 40-len(hash)-1))]
		return prefix + "_" + hash
	}
	if provider == "openai" && len(units) > 40 {
		return string(utf16.Decode(units[:40]))
	}
	return id
}

// shortHash is pi-ai's shortHash: two 32-bit multiplicative hashes of the
// UTF-16 code units, in base 36.
func shortHash(s string) string {
	h1, h2 := uint32(0xdeadbeef), uint32(0x41c6ce57)
	for _, ch := range utf16.Encode([]rune(s)) {
		h1 = (h1 ^ uint32(ch)) * 2654435761
		h2 = (h2 ^ uint32(ch)) * 1597334677
	}
	h1 = (h1^(h1>>16))*2246822507 ^ (h2^(h2>>13))*3266489909
	h2 = (h2^(h2>>16))*2246822507 ^ (h1^(h1>>13))*3266489909
	return strconv.FormatUint(uint64(h2), 36) + strconv.FormatUint(uint64(h1), 36)
}
