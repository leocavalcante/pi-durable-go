package durable

import (
	"regexp"
	"strings"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// Bounded output streams, ported from upstream chord's output.ts: a tool
// call's running output is kept in a head or tail window of its output
// limits, sanitized when read, with exact counts of what the limits dropped.

// outputLimitsOf is a tool's output limits with the defaults filled in
// (upstream outputLimits); all defaults without a tool.
func outputLimitsOf(def *ToolDefinition) OutputLimits {
	limits := OutputLimits{}
	if def != nil && def.OutputLimits != nil {
		limits = *def.OutputLimits
	}
	if limits.MaxBytes <= 0 {
		limits.MaxBytes = defaultMaxOutputBytes
	}
	if limits.MaxLines <= 0 {
		limits.MaxLines = defaultMaxOutputLines
	}
	if limits.Retain != "tail" {
		limits.Retain = "head"
	}
	return limits
}

// invalidOutput and escapeSequence are what sanitizeOutput removes: control
// characters other than tab and newline, interlinear annotation marks, and
// whole 7-bit escape sequences (strings up to BEL or ST, CSI, and short
// ESC forms).
var (
	invalidOutput  = regexp.MustCompile("[\x00-\x08\x0b-\x1f\ufff9-\ufffb]")
	escapeSequence = regexp.MustCompile("\x1b[\\]PX^_][^\x07\x1b]*(?:\x07|\x1b\\\\)|\x1b\\[[\x30-\x3f]*[\x20-\x2f]*[\x40-\x7e]|\x1b[\x20-\x2f]*[\x30-\x7e]")
)

// sanitizeOutput removes escape sequences and control characters that break
// display and transcripts; tabs and newlines stay (upstream chord).
func sanitizeOutput(text string) string {
	if !strings.ContainsFunc(text, func(r rune) bool { return r < 0x20 && r != '\t' && r != '\n' || r >= 0xfff9 && r <= 0xfffb }) {
		return text
	}
	return invalidOutput.ReplaceAllString(escapeSequence.ReplaceAllString(text, ""), "")
}

// outputSlice is an exact slice of text within limits and what it left out.
type outputSlice struct {
	text                       string
	bytes                      int
	droppedBytes, droppedLines int
}

// boundSlice bounds text to whole lines within the limits: the first lines
// for head, the last lines for tail; a single line longer than maxBytes is
// cut at the byte limit on a character boundary (upstream chord
// boundOutput).
func boundSlice(text string, limits OutputLimits) outputSlice {
	from, to := 0, 0
	if limits.Retain == "tail" {
		from, to = tailRange(text, limits)
	} else {
		from, to = headRange(text, limits)
	}
	kept := text[from:to]
	return outputSlice{text: kept, bytes: len(kept), droppedBytes: len(text) - len(kept), droppedLines: lineCount(text) - lineCount(kept)}
}

func headRange(text string, limits OutputLimits) (int, int) {
	if limits.MaxLines == 0 || limits.MaxBytes == 0 {
		return 0, 0
	}
	end := len(text)
	lines := 0
	for index := strings.IndexByte(text, '\n'); index != -1; {
		if lines++; lines == limits.MaxLines {
			end = index + 1
			break
		}
		next := strings.IndexByte(text[index+1:], '\n')
		if next == -1 {
			break
		}
		index += 1 + next
	}
	if end > limits.MaxBytes {
		newline := strings.LastIndexByte(text[:limits.MaxBytes], '\n')
		if newline == -1 {
			end = characterEnd(text, limits.MaxBytes)
		} else {
			end = newline + 1
		}
	}
	return 0, end
}

func tailRange(text string, limits OutputLimits) (int, int) {
	if limits.MaxLines == 0 || limits.MaxBytes == 0 {
		return len(text), len(text)
	}
	// A trailing newline ends the last line rather than starting another.
	last := len(text) - 1
	if last >= 0 && text[last] == '\n' {
		last--
	}
	start := 0
	lines := 1
	index := -1
	if last >= 0 {
		index = strings.LastIndexByte(text[:last+1], '\n')
	}
	for index != -1 {
		if lines == limits.MaxLines {
			start = index + 1
			break
		}
		lines++
		if index == 0 {
			index = -1
		} else {
			index = strings.LastIndexByte(text[:index], '\n')
		}
	}
	if len(text)-start > limits.MaxBytes {
		from := len(text) - limits.MaxBytes
		newline := -1
		if at := strings.IndexByte(text[from-1:], '\n'); at != -1 {
			newline = from - 1 + at
		}
		// The first line starting inside the byte window, or a cut of the
		// last line when it alone is too long.
		if newline != -1 && newline+1 < len(text) {
			start = newline + 1
		} else {
			start = characterStart(text, from)
		}
	}
	return start, len(text)
}

// characterEnd is the last character boundary at or before index.
func characterEnd(text string, index int) int {
	end := index
	for end > 0 && end < len(text) && text[end]&0xc0 == 0x80 {
		end--
	}
	return end
}

// characterStart is the first character boundary at or after index.
func characterStart(text string, index int) int {
	start := index
	for start < len(text) && text[start]&0xc0 == 0x80 {
		start++
	}
	return start
}

// lineCount counts lines; a final unterminated line counts.
func lineCount(text string) int {
	if text == "" {
		return 0
	}
	return countLines(strings.Count(text, "\n"), text[len(text)-1] == '\n')
}

// countLines is the lines of text with newlines newlines (upstream lines).
func countLines(newlines int, terminated bool) int {
	if terminated {
		return newlines
	}
	return newlines + 1
}

// outputChunk is a stored chunk of an output stream.
type outputChunk struct {
	text            string
	bytes, newlines int
}

// outputBuffer is the bounded running output of one stream (upstream chord
// OutputBuffer): head retention stops storing once its window is full, and
// tail retention drops stored text the window no longer needs; counts of the
// whole stream keep the dropped totals exact. Chunks are bytes decoded as
// one UTF-8 stream, so a character split across chunks survives; a
// byte-order mark stays, as in upstream's string chunks.
type outputBuffer struct {
	limits          OutputLimits
	decoder         js.UTF8Decoder
	chunks          []outputChunk
	storedBytes     int
	storedNewlines  int
	full            bool
	totalBytes      int
	totalNewlines   int
	endsWithNewline bool
}

func newOutputBuffer(limits OutputLimits) *outputBuffer {
	return &outputBuffer{limits: limits, endsWithNewline: true}
}

// push accepts a chunk; it reports whether anything was accepted.
func (b *outputBuffer) push(chunk string) bool {
	return b.accept(b.decoder.Decode([]byte(chunk), true))
}

// end flushes an incomplete trailing character as U+FFFD; called when the
// stream ends. It reports whether anything was accepted.
func (b *outputBuffer) end() bool {
	return b.accept(b.decoder.Decode(nil, false))
}

func (b *outputBuffer) accept(text string) bool {
	if text == "" {
		return false
	}
	size, newlines := len(text), strings.Count(text, "\n")
	b.totalBytes += size
	b.totalNewlines += newlines
	b.endsWithNewline = strings.HasSuffix(text, "\n")
	if b.full {
		return true
	}
	b.chunks = append(b.chunks, outputChunk{text: text, bytes: size, newlines: newlines})
	b.storedBytes += size
	b.storedNewlines += newlines
	if b.limits.Retain != "tail" {
		// Nothing past a full window is ever needed.
		b.full = b.storedBytes > b.limits.MaxBytes || b.storedNewlines >= b.limits.MaxLines
		return true
	}
	// Drop leading chunks while the rest still holds more than a window, plus
	// one byte or line, so the window's line start can still be found.
	for len(b.chunks) > 1 {
		first := b.chunks[0]
		bytesAfter, newlinesAfter := b.storedBytes-first.bytes, b.storedNewlines-first.newlines
		if bytesAfter <= b.limits.MaxBytes+1 && newlinesAfter <= b.limits.MaxLines+1 {
			break
		}
		b.chunks = b.chunks[1:]
		b.storedBytes, b.storedNewlines = bytesAfter, newlinesAfter
	}
	return true
}

// boundedOutput is retained, sanitized output and what the limits dropped.
type boundedOutput struct {
	text                       string
	droppedBytes, droppedLines int
}

// snapshot is the retained, sanitized output and what the limits dropped
// from the whole stream.
func (b *outputBuffer) snapshot() boundedOutput {
	var stored string
	if len(b.chunks) == 1 {
		stored = b.chunks[0].text
	} else {
		var joined strings.Builder
		for _, chunk := range b.chunks {
			joined.WriteString(chunk.text)
		}
		stored = joined.String()
	}
	kept := boundSlice(stored, b.limits)
	storedLines := countLines(b.storedNewlines, stored == "" || strings.HasSuffix(stored, "\n"))
	keptLines := storedLines - kept.droppedLines
	// Tail windows never reach back before this one, but finding a later
	// window's first line needs what precedes it: keep the shortest suffix
	// longer than the window by a byte or a line.
	if b.limits.Retain == "tail" || len(b.chunks) > 1 {
		text := stored
		if b.limits.Retain == "tail" {
			text = tailMargin(stored, b.limits)
		}
		b.chunks = nil
		if text != "" {
			b.chunks = []outputChunk{{text: text, bytes: len(text), newlines: strings.Count(text, "\n")}}
		}
		b.storedBytes, b.storedNewlines = len(text), strings.Count(text, "\n")
	}
	return boundedOutput{
		text:         sanitizeOutput(kept.text),
		droppedBytes: b.totalBytes - kept.bytes,
		droppedLines: countLines(b.totalNewlines, b.endsWithNewline) - keptLines,
	}
}

// tailMargin is the shortest suffix of text with more than maxBytes bytes or
// more than maxLines newlines, or all of it.
func tailMargin(text string, limits OutputLimits) string {
	byteStart := 0
	if len(text) > limits.MaxBytes {
		byteStart = characterEnd(text, len(text)-limits.MaxBytes-1)
	}
	lineStart := 0
	newlines := 0
	for index := strings.LastIndexByte(text, '\n'); index != -1; index = strings.LastIndexByte(text[:index], '\n') {
		if newlines++; newlines > limits.MaxLines {
			lineStart = index
			break
		}
		if index == 0 {
			break
		}
	}
	start := max(byteStart, lineStart)
	if start == 0 {
		return text
	}
	return text[start:]
}
