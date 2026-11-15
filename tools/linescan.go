package tools

import (
	"bytes"
	"errors"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// LineScan is where lines of a file are (upstream LineScan): the file's
// newlines, the byte range of the selected lines from the start of the
// first to the end of the last (without its newline), where the first
// selected line ends and the last starts, and the UTF-8 sizes of the
// decoded selection and its first line.
type LineScan struct {
	Newlines       int64 `json:"newlines"`
	Start          int64 `json:"start"`
	End            int64 `json:"end"`
	FirstLineEnd   int64 `json:"firstLineEnd"`
	LastLineStart  int64 `json:"lastLineStart"`
	SelectedBytes  int64 `json:"selectedBytes"`
	FirstLineBytes int64 `json:"firstLineBytes"`
}

// lineScanner computes a LineScan from a file's bytes fed in order
// (upstream LineScanner); decoded sizes are those of decoding the whole
// file, a leading byte-order mark dropped. endLine < 0 scans to the end.
type lineScanner struct {
	startLine, endLine int64
	position           int64
	newlines           int64
	lineStart          int64
	start, end         int64
	firstLineEnd       int64
	lastLineStart      int64
	selectedBytes      int64
	firstLineBytes     int64
	selection          *js.UTF8Decoder
	firstLine          *js.UTF8Decoder
	head               []byte // the first bytes, until it is known whether they are a byte-order mark
	headDone           bool
	bom                bool
}

// unset marks a position not found yet.
const unset = -1

func newLineScanner(startLine, endLine int64) (*lineScanner, error) {
	if startLine < 0 || startLine > maxSafeInteger || endLine != 0 && (endLine <= startLine || endLine > maxSafeInteger) {
		return nil, errors.New("Invalid line range")
	}
	if endLine == 0 {
		endLine = -1
	}
	s := &lineScanner{startLine: startLine, endLine: endLine, start: unset, end: unset, firstLineEnd: unset, lastLineStart: unset}
	if startLine == 0 {
		s.begin(0)
	}
	return s, nil
}

func (s *lineScanner) push(chunk []byte) {
	if !s.headDone {
		take := min(3-len(s.head), len(chunk))
		s.head = append(s.head, chunk[:take]...)
		if len(s.head) < 3 {
			return
		}
		s.releaseHead()
		chunk = chunk[take:]
	}
	s.process(chunk)
}

func (s *lineScanner) releaseHead() {
	head := s.head
	s.head, s.headDone = nil, true
	s.bom = len(head) >= 3 && head[0] == 0xef && head[1] == 0xbb && head[2] == 0xbf
	s.process(head)
}

// lastSelected is the index of the last selected line; -1 when the
// selection runs to the end.
func (s *lineScanner) lastSelected() int64 {
	if s.endLine < 0 {
		return -1
	}
	return s.endLine - 1
}

func (s *lineScanner) process(chunk []byte) {
	base := s.position
	from := 0
	for offset := 0; ; {
		index := bytes.IndexByte(chunk[offset:], '\n')
		if index == -1 {
			break
		}
		index += offset
		// The newline ends line s.newlines; it belongs to the selection
		// between selected lines only.
		s.feed(chunk, base, from, index)
		line := s.newlines
		position := base + int64(index)
		if line == s.startLine {
			s.endFirstLine(position)
		}
		if line == s.lastSelected() {
			s.endSelection(position)
		}
		s.feed(chunk, base, index, index+1)
		from = index + 1
		s.newlines++
		s.lineStart = position + 1
		if s.newlines == s.startLine {
			s.begin(s.lineStart)
		}
		if s.newlines == s.lastSelected() {
			s.lastLineStart = s.lineStart
		}
		offset = index + 1
	}
	s.feed(chunk, base, from, len(chunk))
	s.position += int64(len(chunk))
}

func (s *lineScanner) finish() LineScan {
	if !s.headDone {
		s.releaseHead()
	}
	size := s.position
	if s.start == unset {
		return LineScan{Newlines: s.newlines, Start: size, End: size, FirstLineEnd: size, LastLineStart: size}
	}
	if s.firstLineEnd == unset {
		s.endFirstLine(size)
	}
	if s.end == unset {
		s.endSelection(size)
	}
	lastLineStart := s.lastLineStart
	if lastLineStart == unset {
		// A selection that reaches past the last line ends with the last line.
		lastLineStart = s.lineStart
	}
	return LineScan{Newlines: s.newlines, Start: s.start, End: s.end, FirstLineEnd: s.firstLineEnd, LastLineStart: lastLineStart,
		SelectedBytes: s.selectedBytes, FirstLineBytes: s.firstLineBytes}
}

func (s *lineScanner) begin(start int64) {
	s.start = start
	if s.endLine >= 0 && s.startLine == s.endLine-1 {
		s.lastLineStart = start
	}
	s.selection, s.firstLine = &js.UTF8Decoder{}, &js.UTF8Decoder{}
}

func (s *lineScanner) endFirstLine(position int64) {
	s.firstLineEnd = position
	if s.firstLine != nil {
		s.firstLineBytes += int64(len(s.firstLine.Decode(nil, false)))
	}
	s.firstLine = nil
}

func (s *lineScanner) endSelection(position int64) {
	s.end = position
	if s.selection != nil {
		s.selectedBytes += int64(len(s.selection.Decode(nil, false)))
	}
	s.selection = nil
}

// feed feeds chunk[from:to], which starts at file offset base, to the
// decoders of the ranges still open.
func (s *lineScanner) feed(chunk []byte, base int64, from, to int) {
	// Decoding the whole file drops a leading byte-order mark.
	if s.bom && base+int64(from) < 3 {
		from = min(to, int(3-base))
	}
	if to <= from {
		return
	}
	part := chunk[from:to]
	if s.selection != nil {
		s.selectedBytes += int64(len(s.selection.Decode(part, true)))
	}
	if s.firstLine != nil {
		s.firstLineBytes += int64(len(s.firstLine.Decode(part, true)))
	}
}
