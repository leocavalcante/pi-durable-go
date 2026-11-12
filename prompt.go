package durable

import (
	"bytes"
	"encoding/json"
	"strings"
)

// Positional prompt and tool changes (upstream harness/prompt.ts and pi-ai
// utils/transcript.ts, spec §7.4): generation records the sections and tools
// it offers as pi.system entries at their transcript positions, patching what
// the earlier ones set, and replays them to know what the model saw.

// orderedText is a section map that keeps insertion order: setting an
// existing name keeps its place, and a name removed and set again moves to
// the end.
type orderedText struct {
	names []string
	text  map[string]string
}

func (o *orderedText) set(name, text string) {
	if o.text == nil {
		o.text = map[string]string{}
	}
	if _, ok := o.text[name]; !ok {
		o.names = append(o.names, name)
	}
	o.text[name] = text
}

func (o *orderedText) remove(name string) {
	if _, ok := o.text[name]; !ok {
		return
	}
	delete(o.text, name)
	for i, n := range o.names {
		if n == name {
			o.names = append(o.names[:i:i], o.names[i+1:]...)
			break
		}
	}
}

func (o *orderedText) sections() []SystemSection {
	out := make([]SystemSection, 0, len(o.names))
	for _, name := range o.names {
		text := o.text[name]
		out = append(out, SystemSection{Name: name, Text: &text})
	}
	return out
}

// replaySections gives the sections in effect after replaying the system
// messages in order.
func replaySections(messages []Message) orderedText {
	var shown orderedText
	for _, m := range messages {
		if m.Role != "system" {
			continue
		}
		for _, section := range m.Sections {
			if section.Text == nil {
				shown.remove(section.Name)
			} else {
				shown.set(section.Name, *section.Text)
			}
		}
	}
	return shown
}

// CurrentTools gives the tools on offer after replaying every system
// message's tool changes in order (pi-ai getCurrentTools).
func CurrentTools(messages []Message) []ToolSpec {
	var names []string
	tools := map[string]ToolSpec{}
	for _, m := range messages {
		if m.Role != "system" {
			continue
		}
		for _, name := range m.ToolsRemoved {
			if _, ok := tools[name]; ok {
				delete(tools, name)
				for i, n := range names {
					if n == name {
						names = append(names[:i:i], names[i+1:]...)
						break
					}
				}
			}
		}
		for _, tool := range m.ToolsAdded {
			if _, ok := tools[tool.Name]; !ok {
				names = append(names, tool.Name)
			}
			tools[tool.Name] = tool
		}
	}
	out := make([]ToolSpec, 0, len(names))
	for _, name := range names {
		out = append(out, tools[name])
	}
	return out
}

// HasToolChanges reports whether any system message changes the tools, so
// the messages, not a separate tool list, declare them.
func HasToolChanges(messages []Message) bool {
	for _, m := range messages {
		if m.Role == "system" && (len(m.ToolsAdded) > 0 || len(m.ToolsRemoved) > 0) {
			return true
		}
	}
	return false
}

// SystemText renders a system message's prompt as one text: its content,
// then its sections in order, separated by blank lines (pi-ai
// getSystemMessageText).
func SystemText(m Message) string {
	var parts []string
	if m.Content != "" {
		parts = append(parts, m.Content)
	}
	for _, section := range m.Sections {
		if section.Text != nil && *section.Text != "" {
			parts = append(parts, *section.Text)
		}
	}
	return strings.Join(parts, "\n\n")
}

// CollapseSystemMessages folds every system message into one leading
// message holding the replayed prompt and tools, for a model that takes no
// system messages mid-conversation (pi-ai collapseSystemMessages): later
// content is appended to the first, sections are patched by name, and tools
// are replayed. Other messages keep their order.
func CollapseSystemMessages(messages []Message) []Message {
	var content []string
	var others []Message
	seen := false
	for _, m := range messages {
		if m.Role != "system" {
			others = append(others, m)
			continue
		}
		seen = true
		if m.Content != "" {
			content = append(content, m.Content)
		}
	}
	tools := CurrentTools(messages)
	if !seen && len(tools) == 0 {
		return others
	}
	sections := replaySections(messages)
	head := Message{Role: "system", Content: strings.Join(content, "\n\n"), Sections: sections.sections(), ToolsAdded: tools}
	return append([]Message{head}, others...)
}

// declarationsEqual reports whether two tools declare the same interface to
// the model: name, description, and parameters (pi-ai declarationsEqual).
func declarationsEqual(a, b ToolSpec) bool {
	if a.Name != b.Name || a.Description != b.Description {
		return false
	}
	return bytes.Equal(compactJSON(a.Parameters), compactJSON(b.Parameters)) &&
		bytes.Equal(compactJSON(a.ConstrainedSampling), compactJSON(b.ConstrainedSampling))
}

func compactJSON(raw json.RawMessage) []byte {
	var out bytes.Buffer
	if json.Compact(&out, raw) != nil {
		return raw
	}
	return out.Bytes()
}

// toolDeclaration is the part of a tool that enters the transcript.
func toolDeclaration(tool ToolSpec) ToolSpec {
	return ToolSpec{Name: tool.Name, Description: tool.Description, Parameters: cloneRaw(tool.Parameters), ConstrainedSampling: cloneRaw(tool.ConstrainedSampling)}
}

// planSystemEntries plans the pi.system entries that make the replayed
// sections and tools of view equal desired and tools, in values and order:
//
//   - after a head marker with no later pi.system entry, one complete
//     baseline that omits every retained earlier pi.system entry, written
//     even when it restates the replayed values;
//   - otherwise, when a minimal section patch would leave another order, one
//     entry removing every shown section and one re-adding every desired
//     section in order;
//   - otherwise the minimal patch, or nothing.
//
// Tool changes ride on the last planned entry, or on one of their own.
func planSystemEntries(view contextView, desired orderedText, tools []ToolSpec) []Entry {
	head := view.head
	if head != nil {
		later := false
		for _, e := range view.entries {
			if isSystemEntry(e) && e.ID > head.ID {
				later = true
				break
			}
		}
		if !later {
			var edits []ContextEdit
			for _, e := range view.entries {
				if isSystemEntry(e) {
					edits = append(edits, ContextEdit{Target: e.ID, Action: "omit"})
				}
			}
			added := make([]ToolSpec, 0, len(tools))
			for _, tool := range tools {
				added = append(added, toolDeclaration(tool))
			}
			baseline := systemEntry(desired.sections(), nil, added)
			baseline.Edits = edits
			return []Entry{baseline}
		}
	}
	patches := planSections(replaySections(view.messages), desired)
	removed, added := planTools(CurrentTools(view.messages), tools)
	if len(removed) == 0 && len(added) == 0 {
		entries := make([]Entry, 0, len(patches))
		for _, patch := range patches {
			entries = append(entries, systemEntry(patch, nil, nil))
		}
		return entries
	}
	if len(patches) == 0 {
		return []Entry{systemEntry(nil, removed, added)}
	}
	entries := make([]Entry, 0, len(patches))
	for i, patch := range patches {
		if i == len(patches)-1 {
			entries = append(entries, systemEntry(patch, removed, added))
		} else {
			entries = append(entries, systemEntry(patch, nil, nil))
		}
	}
	return entries
}

// planTools plans the tool changes from offered to desired. A changed
// declaration is removed and re-added. Replay keeps retained tools in place
// and appends additions; when that would not yield the desired order, every
// offered tool is removed and every desired tool re-added in order.
func planTools(offered, desired []ToolSpec) ([]string, []ToolSpec) {
	wanted := map[string]ToolSpec{}
	for _, tool := range desired {
		wanted[tool.Name] = tool
	}
	keptNames := map[string]bool{}
	var replayed []string
	for _, tool := range offered {
		if next, ok := wanted[tool.Name]; ok && declarationsEqual(tool, next) {
			keptNames[tool.Name] = true
			replayed = append(replayed, tool.Name)
		}
	}
	var added []ToolSpec
	for _, tool := range desired {
		if !keptNames[tool.Name] {
			added = append(added, toolDeclaration(tool))
			replayed = append(replayed, tool.Name)
		}
	}
	for i, name := range replayed {
		if name != desired[i].Name {
			var removed []string
			for _, tool := range offered {
				removed = append(removed, tool.Name)
			}
			all := make([]ToolSpec, 0, len(desired))
			for _, tool := range desired {
				all = append(all, toolDeclaration(tool))
			}
			return removed, all
		}
	}
	var removed []string
	for _, tool := range offered {
		if !keptNames[tool.Name] {
			removed = append(removed, tool.Name)
		}
	}
	return removed, added
}

// planSections plans the section patches: none, the minimal patch, or a
// remove-all and re-add-all pair when the minimal patch would leave another
// order.
func planSections(shown, desired orderedText) [][]SystemSection {
	var patchedOrder []string
	for _, name := range shown.names {
		if _, ok := desired.text[name]; ok {
			patchedOrder = append(patchedOrder, name)
		}
	}
	for _, name := range desired.names {
		if _, ok := shown.text[name]; !ok {
			patchedOrder = append(patchedOrder, name)
		}
	}
	for i, name := range desired.names {
		if patchedOrder[i] != name {
			removeAll := make([]SystemSection, 0, len(shown.names))
			for _, name := range shown.names {
				removeAll = append(removeAll, SystemSection{Name: name})
			}
			return [][]SystemSection{removeAll, desired.sections()}
		}
	}
	var patch []SystemSection
	for _, name := range shown.names {
		value := shown.text[name]
		if next, ok := desired.text[name]; !ok {
			patch = append(patch, SystemSection{Name: name})
		} else if next != value {
			text := next
			patch = append(patch, SystemSection{Name: name, Text: &text})
		}
	}
	for _, name := range desired.names {
		if _, ok := shown.text[name]; !ok {
			text := desired.text[name]
			patch = append(patch, SystemSection{Name: name, Text: &text})
		}
	}
	if len(patch) == 0 {
		return nil
	}
	return [][]SystemSection{patch}
}

// systemEntry is a pi.system entry: a system message with empty content.
func systemEntry(sections []SystemSection, removed []string, added []ToolSpec) Entry {
	return Entry{Kind: "pi.system", Message: &Message{Role: "system", Sections: sections, ToolsRemoved: removed, ToolsAdded: added}}
}
