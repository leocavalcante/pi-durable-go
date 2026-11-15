package tools

import "testing"

// Upstream's truncation cases (test/env-truncate.test.ts).
func TestTruncateHeadAsUpstream(t *testing.T) {
	for _, c := range []struct {
		content            string
		maxLines, maxBytes int
		want               headTruncation
	}{
		{"aé🙂\nb", 10, 100, headTruncation{content: "aé🙂\nb", totalLines: 2, outputLines: 2}},
		{"aé🙂\nb", 10, 7, headTruncation{content: "aé🙂", truncated: true, truncatedBy: "bytes", totalLines: 2, outputLines: 1}},
		// A trailing newline is no extra line.
		{"line\nline\nline\n", 3, 100, headTruncation{content: "line\nline\nline\n", totalLines: 3, outputLines: 3}},
		{"one\ntwo\nthree\nfour", 2, 100, headTruncation{content: "one\ntwo", truncated: true, truncatedBy: "lines", totalLines: 4, outputLines: 2}},
		// Only the trailing newline exceeds the bytes, at the line cap.
		{"hello\nworld\n", 2, 11, headTruncation{content: "hello\nworld", truncated: true, truncatedBy: "bytes", totalLines: 2, outputLines: 2}},
		{"éé\nabc", 10, 4, headTruncation{content: "éé", truncated: true, truncatedBy: "bytes", totalLines: 2, outputLines: 1}},
		{"éé\nabc", 10, 3, headTruncation{truncated: true, truncatedBy: "bytes", totalLines: 2, firstLineExceeds: true}},
	} {
		if got := truncateHead(c.content, c.maxLines, c.maxBytes); got != c.want {
			t.Errorf("truncateHead(%q, %d lines, %d bytes) = %+v, want %+v", c.content, c.maxLines, c.maxBytes, got, c.want)
		}
	}
	for n, want := range map[int]string{1023: "1023B", 1536: "1.5KB", 3 * 1024 * 1024: "3.0MB"} {
		if got := formatSize(n); got != want {
			t.Errorf("formatSize(%d) = %q, want %q", n, got, want)
		}
	}
}
