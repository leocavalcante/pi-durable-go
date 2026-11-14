package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
)

// StreamEvent is one streamed change of an answer, as pi-ai's
// AssistantMessageEvent names them: text_delta and thinking_delta append
// Delta to the text or thinking of content block Index; toolcall_start opens
// a tool call (ID, Name) at Index, toolcall_delta appends raw argument JSON
// text to it, and toolcall_end closes it.
type StreamEvent struct {
	Type  string `json:"type"`
	Index int    `json:"contentIndex"`
	Delta string `json:"delta,omitempty"`
	ID    string `json:"id,omitempty"`
	Name  string `json:"name,omitempty"`
}

// EventModel is a Model that streams thinking and tool-call arguments too, so
// the partial answer in pi.live shows them (upstream partial messages). The
// Harness uses GenerateEvents when a model has it, and Generate's text
// deltas otherwise.
type EventModel interface {
	Model
	GenerateEvents(ctx context.Context, request ModelRequest, onEvent func(StreamEvent) error) (ModelResponse, error)
}

// partialMessage builds the partial assistant message of a streamed answer:
// its text, its thinking, and its tool calls with the arguments read so far.
type partialMessage struct {
	text     strings.Builder
	thinking strings.Builder
	calls    []partialCall
	byIndex  map[int]int
}

type partialCall struct {
	id, name string
	raw      strings.Builder
}

func (p *partialMessage) observe(event StreamEvent) {
	switch event.Type {
	case "text_delta":
		p.text.WriteString(event.Delta)
	case "thinking_delta":
		p.thinking.WriteString(event.Delta)
	case "toolcall_start":
		if p.byIndex == nil {
			p.byIndex = map[int]int{}
		}
		p.byIndex[event.Index] = len(p.calls)
		p.calls = append(p.calls, partialCall{id: event.ID, name: event.Name})
	case "toolcall_delta":
		if i, ok := p.byIndex[event.Index]; ok {
			p.calls[i].raw.WriteString(event.Delta)
		}
	}
}

// message is the partial message as pi.live holds it.
func (p *partialMessage) message() *Message {
	message := &Message{Role: "assistant", Content: p.text.String(), Thinking: p.thinking.String()}
	for i := range p.calls {
		call := &p.calls[i]
		message.ToolCalls = append(message.ToolCalls, ToolCall{ID: call.id, Name: call.name, Arguments: partialArguments(call.raw.String())})
	}
	return message
}

func (p *partialMessage) empty() bool {
	return p.text.Len() == 0 && p.thinking.Len() == 0 && len(p.calls) == 0
}

// partialArguments are a streamed tool call's arguments so far: an object,
// {} until one shows (pi-ai's ToolCallArgumentsStream).
func partialArguments(raw string) json.RawMessage {
	value, ok := parsePartialJSON(raw)
	if !ok || !bytes.HasPrefix(value, []byte("{")) {
		return json.RawMessage(`{}`)
	}
	return value
}

// parsePartialJSON is the value of JSON text read so far as pi-ai's
// StreamingJsonParser reads it, written as JSON.stringify writes it; false
// before a value starts. An unterminated string shows what was read (an
// unterminated escape held back), an unterminated container the members and
// elements read, and a member whose key or value has not started is omitted.
// An unterminated literal shows once it is a prefix of true, false, or null,
// and an unterminated number once its characters form a number, an exponent
// without digits ignored, unless it is longer than 64 characters. Malformed
// text is read leniently: missing commas and colons are tolerated, an invalid
// escape stays literal text, values in invalid positions and unknown words
// are dropped, a stray closer is ignored, and text after the root value is.
// Unlike upstream's parser, which copies its open containers for each
// snapshot within a work budget, every reading here is fresh.
func parsePartialJSON(text string) (json.RawMessage, bool) {
	p := &streamingJSON{}
	units := utf16.Encode([]rune(text))
	for i := 0; i < len(units) && !p.done; {
		switch {
		case p.str != nil:
			i = p.readString(units, i)
		case len(p.scalar) > 0:
			i = p.readScalar(units, i)
		default:
			p.structural(units[i])
			i++
		}
	}
	if open := p.str; open != nil && !open.key {
		p.place(open.slot, wtf8String(open.text))
	}
	if p.scalarSlot != nil && len(p.scalar) > 0 && len(p.scalar) <= maxPendingScalarLength {
		if value, ok := pendingScalarValue(string(utf16.Decode(p.scalar))); ok {
			p.place(p.scalarSlot, value)
		}
	}
	if !p.hasRoot {
		return nil, false
	}
	return json.RawMessage(jsStringify(settledStreamValue(p.root))), true
}

// An unterminated number longer than this does not show (upstream
// MAX_PENDING_SCALAR_LENGTH).
const maxPendingScalarLength = 64

var (
	streamNumber                     = regexp.MustCompile(`^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$`)
	streamNumberWithoutExponentDigit = regexp.MustCompile(`^(-?(?:0|[1-9]\d*)(?:\.\d+)?)[eE][+-]?$`)
)

// streamingJSON is pi-ai's StreamingJsonParser state, reading the whole text
// at once.
type streamingJSON struct {
	root       any
	hasRoot    bool
	done       bool
	stack      []*streamFrame
	str        *streamString
	scalar     []uint16
	scalarSlot *streamSlot
}

// streamArray is an open array, whose elements are set in place.
type streamArray struct{ items []any }

type streamFrame struct {
	object *jsObject
	array  *streamArray
	slot   *streamSlot
	key    string
	hasKey bool
	phase  string // key, colon, value, or comma
}

// streamSlot is where a value goes: the root, an element, or a member.
type streamSlot struct {
	kind  byte // 'r', 'e', or 'm'
	key   string
	index int
}

type streamString struct {
	key    bool
	slot   *streamSlot
	text   []uint16
	escape []uint16
}

func (p *streamingJSON) top() *streamFrame {
	if len(p.stack) == 0 {
		return nil
	}
	return p.stack[len(p.stack)-1]
}

func (p *streamingJSON) structural(c uint16) {
	switch c {
	case ' ', '\t', '\n', '\r':
	case '"':
		p.startString()
	case '{', '[':
		p.openContainer(c == '{')
	case '}', ']':
		// A stray closer outside any container is ignored.
		if frame := p.top(); frame != nil {
			p.stack = p.stack[:len(p.stack)-1]
			p.valueEnded(p.top())
		}
	case ':':
		if frame := p.top(); frame != nil && frame.object != nil && frame.phase == "colon" {
			frame.phase = "value"
		}
	case ',':
		if frame := p.top(); frame != nil {
			frame.phase = "key"
			if frame.array != nil {
				frame.phase = "value"
			}
		}
	default:
		p.scalar = []uint16{c}
		p.scalarSlot = p.valueSlot()
	}
}

// valueSlot is where a value starting now goes; nil for a position that
// takes none. A missing colon is tolerated: {"a" 1} reads as {"a": 1}.
func (p *streamingJSON) valueSlot() *streamSlot {
	frame := p.top()
	switch {
	case frame == nil:
		if p.hasRoot {
			return nil
		}
		return &streamSlot{kind: 'r'}
	case frame.array != nil:
		return &streamSlot{kind: 'e', index: len(frame.array.items)}
	case (frame.phase == "value" || frame.phase == "colon") && frame.hasKey:
		return &streamSlot{kind: 'm', key: frame.key}
	}
	return nil
}

func (p *streamingJSON) startString() {
	// A missing comma is tolerated: {"a": 1 "b": 2} reads the second member.
	if frame := p.top(); frame != nil && frame.object != nil && (frame.phase == "key" || frame.phase == "comma") {
		p.str = &streamString{key: true}
		return
	}
	p.str = &streamString{slot: p.valueSlot()}
	p.place(p.str.slot, "")
}

func (p *streamingJSON) readString(units []uint16, start int) int {
	open := p.str
	run := start
	for i := start; i < len(units); i++ {
		c := units[i]
		if len(open.escape) > 0 {
			if p.continueEscape(open, c) {
				run = i + 1
				continue
			}
			// Not part of the escape: read as a string character.
			run = i
		}
		switch c {
		case '"':
			open.text = append(open.text, units[run:i]...)
			p.endString(open)
			return i + 1
		case '\\':
			open.text = append(open.text, units[run:i]...)
			open.escape = []uint16{'\\'}
			run = i + 1
		}
	}
	open.text = append(open.text, units[run:]...)
	return len(units)
}

var streamEscapes = map[uint16]uint16{'"': '"', '\\': '\\', '/': '/', 'b': '\b', 'f': '\f', 'n': '\n', 'r': '\r', 't': '\t'}

// continueEscape continues an escape with c; false when c is not part of
// it. An invalid escape, or \u without four hex digits, stays text, as
// pi-ai's repairJson keeps it.
func (p *streamingJSON) continueEscape(open *streamString, c uint16) bool {
	if len(open.escape) == 1 {
		if decoded, ok := streamEscapes[c]; ok {
			open.escape = nil
			open.text = append(open.text, decoded)
			return true
		}
		if c == 'u' {
			open.escape = append(open.escape, 'u')
			return true
		}
		open.escape = nil
		open.text = append(open.text, '\\')
		return false
	}
	if isHexDigit(c) {
		open.escape = append(open.escape, c)
		if len(open.escape) < 6 {
			return true
		}
		value, _ := strconv.ParseUint(string(utf16.Decode(open.escape[2:])), 16, 16)
		open.escape = nil
		open.text = append(open.text, uint16(value))
		return true
	}
	open.text = append(open.text, open.escape...)
	open.escape = nil
	return false
}

func (p *streamingJSON) endString(open *streamString) {
	p.str = nil
	frame := p.top()
	if open.key {
		if frame != nil {
			frame.key, frame.hasKey, frame.phase = wtf8String(open.text), true, "colon"
		}
		return
	}
	p.place(open.slot, wtf8String(open.text))
	p.valueEnded(frame)
}

func isStreamDelimiter(c uint16) bool {
	switch c {
	case ' ', '\t', '\n', '\r', ',', ':', '{', '}', '[', ']', '"':
		return true
	}
	return false
}

// readScalar reads number or literal characters up to the next delimiter.
func (p *streamingJSON) readScalar(units []uint16, start int) int {
	end := start
	for end < len(units) && !isStreamDelimiter(units[end]) {
		end++
	}
	p.scalar = append(p.scalar, units[start:end]...)
	if end < len(units) {
		token, slot := string(utf16.Decode(p.scalar)), p.scalarSlot
		p.scalar, p.scalarSlot = nil, nil
		if value, ok := scalarValue(token); ok {
			p.place(slot, value)
		}
		p.valueEnded(p.top())
	}
	return end
}

// scalarValue is the value of a complete number or literal token; other
// tokens are dropped.
func scalarValue(token string) (any, bool) {
	switch token {
	case "true":
		return true, true
	case "false":
		return false, true
	case "null":
		return nil, true
	}
	if streamNumber.MatchString(token) {
		f, _ := strconv.ParseFloat(token, 64)
		return f, true
	}
	return nil, false
}

// pendingScalarValue is the value an unterminated token shows.
func pendingScalarValue(token string) (any, bool) {
	for _, literal := range []struct {
		text  string
		value any
	}{{"true", true}, {"false", false}, {"null", nil}} {
		if strings.HasPrefix(literal.text, token) {
			return literal.value, true
		}
	}
	if value, ok := scalarValue(token); ok {
		return value, true
	}
	if m := streamNumberWithoutExponentDigit.FindStringSubmatch(token); m != nil {
		f, _ := strconv.ParseFloat(m[1], 64)
		return f, true
	}
	return nil, false
}

func (p *streamingJSON) openContainer(object bool) {
	slot := p.valueSlot()
	frame := &streamFrame{slot: slot, phase: "value"}
	var container any
	if object {
		frame.object, frame.phase = &jsObject{Values: map[string]any{}}, "key"
		container = frame.object
	} else {
		frame.array = &streamArray{}
		container = frame.array
	}
	p.place(slot, container)
	p.stack = append(p.stack, frame)
}

// valueEnded: a value in frame, or the root value without one, ended; text
// after the root value is ignored.
func (p *streamingJSON) valueEnded(frame *streamFrame) {
	if frame == nil {
		if p.hasRoot {
			p.done = true
		}
		return
	}
	frame.phase = "comma"
}

// place sets the value at slot in the innermost open container; nothing
// for a value in an invalid position.
func (p *streamingJSON) place(slot *streamSlot, value any) {
	if slot == nil {
		return
	}
	switch slot.kind {
	case 'r':
		p.root, p.hasRoot = value, true
	case 'e':
		items := &p.top().array.items
		if slot.index == len(*items) {
			*items = append(*items, value)
		} else {
			(*items)[slot.index] = value
		}
	case 'm':
		members := p.top().object
		if _, ok := members.Values[slot.key]; !ok {
			members.Keys = append(members.Keys, slot.key)
		}
		members.Values[slot.key] = value
	}
}

// settledStreamValue turns open arrays into slices and puts object keys in
// JavaScript's order.
func settledStreamValue(value any) any {
	switch v := value.(type) {
	case *streamArray:
		items := make([]any, len(v.items))
		for i, item := range v.items {
			items[i] = settledStreamValue(item)
		}
		return items
	case *jsObject:
		for key, item := range v.Values {
			v.Values[key] = settledStreamValue(item)
		}
		v.Order()
		return v
	}
	return value
}
