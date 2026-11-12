package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
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
// {} until one shows (pi-ai's parseStreamingJson rules).
func partialArguments(raw string) json.RawMessage {
	value, ok := parsePartialJSON(raw)
	if !ok || !bytes.HasPrefix(value, []byte("{")) {
		return json.RawMessage(`{}`)
	}
	return value
}

// parsePartialJSON renders the value of JSON text read so far, following
// pi-ai's streaming JSON rules: an unterminated string shows what was read
// (an unterminated escape held back), an unterminated container the members
// and elements read, a member whose key or value has not started is omitted,
// an unterminated literal shows once it is a prefix of true, false, or null,
// and an unterminated number once its digits form a number. Malformed text
// is read leniently: missing commas and colons are tolerated, an invalid
// escape stays literal text, and text after the root value is ignored.
func parsePartialJSON(text string) (json.RawMessage, bool) {
	p := &partialParser{s: text}
	var out bytes.Buffer
	if !p.value(&out) {
		return nil, false
	}
	return out.Bytes(), true
}

type partialParser struct {
	s   string
	pos int
}

func (p *partialParser) eof() bool { return p.pos >= len(p.s) }

func (p *partialParser) skipSpace() {
	for !p.eof() {
		switch p.s[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

// value writes the value starting here; false when none has started (or the
// text there is no value).
func (p *partialParser) value(out *bytes.Buffer) bool {
	p.skipSpace()
	if p.eof() {
		return false
	}
	switch c := p.s[p.pos]; {
	case c == '{':
		p.pos++
		p.object(out)
		return true
	case c == '[':
		p.pos++
		p.array(out)
		return true
	case c == '"':
		p.pos++
		writeJSONString(out, p.str())
		return true
	case c == 't' || c == 'f' || c == 'n':
		return p.literal(out)
	case c == '-' || c >= '0' && c <= '9':
		return p.number(out)
	default:
		p.pos++ // a value in an invalid position is dropped
		return false
	}
}

func (p *partialParser) object(out *bytes.Buffer) {
	out.WriteByte('{')
	members := 0
	for {
		p.skipSpace()
		if p.eof() {
			break
		}
		c := p.s[p.pos]
		if c == '}' {
			p.pos++
			break
		}
		if c == ',' {
			p.pos++
			continue
		}
		if c != '"' {
			p.pos++ // an unknown word is dropped
			continue
		}
		p.pos++
		key, closed := p.strClosed()
		if !closed {
			p.pos = len(p.s) // a key still streaming: the member has not started
			break
		}
		p.skipSpace()
		if !p.eof() && p.s[p.pos] == ':' {
			p.pos++
		}
		var value bytes.Buffer
		if !p.value(&value) {
			if p.eof() {
				break // its value has not started
			}
			continue
		}
		if members > 0 {
			out.WriteByte(',')
		}
		writeJSONString(out, key)
		out.WriteByte(':')
		out.Write(value.Bytes())
		members++
	}
	out.WriteByte('}')
}

func (p *partialParser) array(out *bytes.Buffer) {
	out.WriteByte('[')
	elements := 0
	for {
		p.skipSpace()
		if p.eof() {
			break
		}
		c := p.s[p.pos]
		if c == ']' {
			p.pos++
			break
		}
		if c == ',' {
			p.pos++
			continue
		}
		var value bytes.Buffer
		if !p.value(&value) {
			if p.eof() {
				break
			}
			continue
		}
		if elements > 0 {
			out.WriteByte(',')
		}
		out.Write(value.Bytes())
		elements++
	}
	out.WriteByte(']')
}

// str reads a string's characters after its opening quote.
func (p *partialParser) str() string {
	s, _ := p.strClosed()
	return s
}

// strClosed reads a string after its opening quote and says whether its
// closing quote was read. An escape still streaming is held back; an
// invalid one stays literal text.
func (p *partialParser) strClosed() (string, bool) {
	var b strings.Builder
	for !p.eof() {
		c := p.s[p.pos]
		switch {
		case c == '"':
			p.pos++
			return b.String(), true
		case c == '\\':
			if p.pos+1 >= len(p.s) {
				p.pos = len(p.s)
				return b.String(), false
			}
			e := p.s[p.pos+1]
			switch e {
			case '"', '\\', '/':
				b.WriteByte(e)
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'u':
				if p.pos+6 > len(p.s) {
					p.pos = len(p.s)
					return b.String(), false
				}
				code, err := strconv.ParseUint(p.s[p.pos+2:p.pos+6], 16, 32)
				if err != nil {
					b.WriteString(p.s[p.pos : p.pos+2])
					p.pos += 2
					continue
				}
				r := rune(code)
				if utf16.IsSurrogate(r) {
					rest := p.s[p.pos+6:]
					if len(rest) < 6 && strings.HasPrefix(`\u`, rest[:min(len(rest), 2)]) && (len(rest) <= 2 || isHex(rest[2:])) {
						p.pos = len(p.s) // its low surrogate is still streaming
						return b.String(), false
					}
					if len(rest) >= 6 && rest[0] == '\\' && rest[1] == 'u' {
						if low, err := strconv.ParseUint(rest[2:6], 16, 32); err == nil {
							if pair := utf16.DecodeRune(r, rune(low)); pair != utf8.RuneError {
								b.WriteRune(pair)
								p.pos += 12
								continue
							}
						}
					}
					r = utf8.RuneError
				}
				b.WriteRune(r)
				p.pos += 6
				continue
			default:
				b.WriteByte('\\') // an invalid escape stays literal
				b.WriteByte(e)
			}
			p.pos += 2
		default:
			b.WriteByte(c)
			p.pos++
		}
	}
	return b.String(), false
}

func (p *partialParser) literal(out *bytes.Buffer) bool {
	start := p.pos
	for !p.eof() && p.s[p.pos] >= 'a' && p.s[p.pos] <= 'z' {
		p.pos++
	}
	word := p.s[start:p.pos]
	for _, literal := range []string{"true", "false", "null"} {
		if word == literal || p.eof() && word != "" && strings.HasPrefix(literal, word) {
			out.WriteString(literal)
			return true
		}
	}
	return false // an unknown word is dropped
}

func (p *partialParser) number(out *bytes.Buffer) bool {
	start := p.pos
	for !p.eof() && strings.IndexByte("+-0123456789.eE", p.s[p.pos]) >= 0 {
		p.pos++
	}
	text := p.s[start:p.pos]
	if json.Valid([]byte(text)) {
		out.WriteString(text)
		return true
	}
	if !p.eof() {
		return false
	}
	// Unterminated: the digits read so far, without a dangling sign,
	// point, or exponent.
	for text != "" && !json.Valid([]byte(text)) {
		text = text[:len(text)-1]
	}
	if text == "" {
		return false
	}
	out.WriteString(text)
	return true
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if !strings.ContainsRune("0123456789abcdefABCDEF", rune(s[i])) {
			return false
		}
	}
	return true
}

// writeJSONString writes s as a JSON string without HTML escaping.
func writeJSONString(out *bytes.Buffer, s string) {
	encoder := json.NewEncoder(out)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(s)
	out.Truncate(out.Len() - 1) // Encode's newline
}
