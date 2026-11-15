package tools

import (
	"bytes"
	"strings"
	"testing"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// Upstream's StreamDecoder and LineScanner tests (test/env-line-scan.test.ts),
// with its seeded random files.

// lineScanRandom is the test's seeded generator, bit for bit.
func lineScanRandom(seed uint32) func() float64 {
	state := seed
	return func() float64 {
		state += 0x6d2b79f5
		t := state
		t = (t ^ t>>15) * (t | 1)
		t ^= t + (t^t>>7)*(t|61)
		return float64(t^t>>14) / 4294967296
	}
}

// Newlines, ASCII, a byte-order mark, valid multi-byte sequences, and bytes
// that form invalid or truncated sequences.
var lineScanPieces = [][]byte{
	{0x0a}, {0x0a}, {0x61}, {0x62, 0x63}, {0xef, 0xbb, 0xbf}, {0xc3, 0xa9}, {0xe2, 0x82, 0xac},
	{0xf0, 0x9f, 0x98, 0x80}, {0xe2, 0x82}, {0xff}, {0x80}, {0xf0, 0x9f}, {0x0d, 0x0a},
}

func lineScanFile(next func() float64) []byte {
	var out []byte
	pieces := int(next() * 60)
	for i := 0; i < pieces; i++ {
		out = append(out, lineScanPieces[int(next()*float64(len(lineScanPieces)))]...)
	}
	return out
}

// decodeWhole decodes as new TextDecoder().decode does: a leading byte-order
// mark dropped unless keepBOM.
func decodeWhole(data []byte, keepBOM bool) string {
	var decoder js.UTF8Decoder
	text := decoder.Decode(data, false)
	if !keepBOM {
		text = strings.TrimPrefix(text, "\ufeff")
	}
	return text
}

func TestStreamDecoderAsUpstream(t *testing.T) {
	// A U+FEFF that does not start the stream stays.
	data := []byte{0xe2, 0x82, 0xef, 0xbb, 0xbf, 0x61}
	var decoder streamDecoder
	var text strings.Builder
	for _, b := range data {
		text.WriteString(decoder.decode([]byte{b}, false))
	}
	text.WriteString(decoder.decode(nil, true))
	if text.String() != "\ufffd\ufeffa" || text.String() != decodeWhole(data, false) {
		t.Fatalf("got %q", text.String())
	}
	// Any chunking decodes as decoding the whole stream.
	for seed := uint32(1); seed <= 5000; seed++ {
		next := lineScanRandom(seed)
		file := lineScanFile(next)
		var decoder streamDecoder
		var text strings.Builder
		for offset := 0; offset < len(file); {
			size := 1 + int(next()*7)
			text.WriteString(decoder.decode(file[offset:min(offset+size, len(file))], false))
			offset += size
		}
		text.WriteString(decoder.decode(nil, true))
		if want := decodeWhole(file, false); text.String() != want {
			t.Fatalf("seed %d: %q, want %q", seed, text.String(), want)
		}
	}
}

func TestLineScannerAgreesWithDecodingTheWholeFile(t *testing.T) {
	for seed := uint32(1); seed <= 5000; seed++ {
		next := lineScanRandom(seed)
		file := lineScanFile(next)
		lines := strings.Split(decodeWhole(file, false), "\n")
		startLine := int64(next() * float64(len(lines)+2))
		endLine := int64(0)
		if next() >= 0.3 {
			endLine = startLine + 1 + int64(next()*float64(len(lines)+1))
		}
		scanner, err := newLineScanner(startLine, endLine)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		for offset := 0; offset < len(file); {
			size := 1 + int(next()*7)
			scanner.push(file[offset:min(offset+size, len(file))])
			offset += size
		}
		scan := scanner.finish()
		if scan.Newlines != int64(len(lines)-1) {
			t.Fatalf("seed %d: %d newlines, want %d", seed, scan.Newlines, len(lines)-1)
		}
		decode := func(from, to int64) string { return decodeWhole(file[from:to], from > 0) }
		end := int64(len(lines))
		if endLine != 0 {
			end = min(endLine, end)
		}
		var selected []string
		if startLine < int64(len(lines)) {
			selected = lines[startLine:end]
		}
		joined := strings.Join(selected, "\n")
		if decode(scan.Start, scan.End) != joined || scan.SelectedBytes != int64(len(joined)) {
			t.Fatalf("seed %d: selection %q (%d bytes), want %q", seed, decode(scan.Start, scan.End), scan.SelectedBytes, joined)
		}
		if startLine < int64(len(lines)) {
			first := lines[startLine]
			if decode(scan.Start, scan.FirstLineEnd) != first || scan.FirstLineBytes != int64(len(first)) {
				t.Fatalf("seed %d: first line %q, want %q", seed, decode(scan.Start, scan.FirstLineEnd), first)
			}
			lastLine := end - 1
			lastEnd := int64(len(file))
			if lastLine+1 < int64(len(lines)) {
				lastEnd = scan.LastLineStart + int64(bytes.IndexByte(file[scan.LastLineStart:], '\n'))
			}
			if decode(scan.LastLineStart, lastEnd) != lines[lastLine] {
				t.Fatalf("seed %d: last line %q, want %q", seed, decode(scan.LastLineStart, lastEnd), lines[lastLine])
			}
		} else if scan.Start != int64(len(file)) || scan.End != int64(len(file)) || scan.SelectedBytes != 0 {
			t.Fatalf("seed %d: past the end %+v", seed, scan)
		}
	}
}

func TestLineScannerRejectsEmptyOrInvalidRanges(t *testing.T) {
	for _, r := range [][2]int64{{2, 2}, {-1, 0}, {3, 1}} {
		if _, err := newLineScanner(r[0], r[1]); err == nil {
			t.Fatalf("range %v accepted", r)
		}
	}
}
