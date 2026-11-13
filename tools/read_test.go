package tools

import (
	"io"
	"strings"
	"testing"
)

// Sizes are written as upstream's formatSize writes them: toFixed rounds an
// exact half up, where fmt rounds it to even.
func TestSizesAreFormattedAsUpstream(t *testing.T) {
	for n, want := range map[int]string{0: "0B", 1023: "1023B", 1024: "1.0KB", 1280: "1.3KB", 2304: "2.3KB", 1048575: "1024.0KB", 1048576: "1.0MB", 1310720: "1.3MB", 1153433: "1.1MB", 5376: "5.3KB", 123456789: "117.7MB"} {
		if got := formatSize(n); got != want {
			t.Errorf("%d: %s, want %s", n, got, want)
		}
	}
	for x, want := range map[float64]string{1.125: "1.13", 1.5: "1.50", 2: "2.00", 1.005: "1.00", 0.125: "0.13"} {
		if got := jsToFixed(x, 2); got != want {
			t.Errorf("%v: %s, want %s", x, got, want)
		}
	}
}

// Notes are upstream's read tool's (1.5.2) for the same files.
func TestReadNotesAreUpstreams(t *testing.T) {
	for name, c := range map[string]struct {
		content string
		note    string
	}{
		"bytes.txt": {strings.Repeat(strings.Repeat("y", 40)+"\n", 1500), "Showing lines 1-1248 of 1501 (50.0KB limit). Use offset=1249 to continue."},
		"utf8.txt":  {strings.Repeat(strings.Repeat("é", 100)+"\n", 600), "Showing lines 1-254 of 601 (50.0KB limit). Use offset=255 to continue."},
		"wide.txt":  {strings.Repeat("x", 60000) + "\nsecond\n", "Line 1 is 58.6KB, exceeds the 50.0KB limit; showing its first 50.0KB. Use bash: sed -n '1p' wide.txt | tail -c +51201"},
	} {
		result, err := readText(strings.NewReader(c.content), reopenText(c.content), name, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Diagnostics) != 1 || result.Diagnostics[0].Message != c.note {
			t.Errorf("%s: diagnostics=%+v, want %q", name, result.Diagnostics, c.note)
		}
	}
}

// reopenText opens content again.
func reopenText(content string) func() (io.Reader, func(), error) {
	return func() (io.Reader, func(), error) { return strings.NewReader(content), func() {}, nil }
}
