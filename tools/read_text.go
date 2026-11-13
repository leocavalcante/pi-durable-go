package tools

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"

	durable "github.com/leocavalcante/pi-durable-go"

	"github.com/leocavalcante/pi-durable-go/internal/jsnum"
)

// FileOpener is optionally implemented by an ExecutionEnv to stream a file,
// so the read tool holds only one page of it in memory.
type FileOpener interface {
	OpenFile(ctx context.Context, path string) (io.ReadCloser, error)
}

// headTruncation is upstream's TruncationResult of truncateHead.
type headTruncation struct {
	content          string
	truncated        bool
	truncatedBy      string // "lines" or "bytes"
	totalLines       int
	outputLines      int
	firstLineExceeds bool
}

// splitLinesForCounting splits on newlines, a trailing one adding no line.
func splitLinesForCounting(content string) []string {
	if content == "" {
		return nil
	}
	lines := strings.Split(content, "\n")
	if strings.HasSuffix(content, "\n") {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// truncateHeadOf keeps the first whole lines of a text within maxReadLines
// and maxReadBytes, given a prefix of it (the whole text, or more than
// maxReadBytes+1 bytes) and its totals (upstream truncateHeadOf).
func truncateHeadOf(prefix string, totalLines, totalBytes int) headTruncation {
	if totalLines <= maxReadLines && totalBytes <= maxReadBytes {
		return headTruncation{content: prefix, totalLines: totalLines, outputLines: totalLines}
	}
	lines := splitLinesForCounting(prefix)
	if len(lines) > 0 && len(lines[0]) > maxReadBytes {
		return headTruncation{truncated: true, truncatedBy: "bytes", totalLines: totalLines, firstLineExceeds: true}
	}
	var kept []string
	size := 0
	by := "lines"
	for i := 0; i < len(lines) && i < maxReadLines; i++ {
		n := len(lines[i])
		if i > 0 {
			n++
		}
		if size+n > maxReadBytes {
			by = "bytes"
			break
		}
		kept = append(kept, lines[i])
		size += n
	}
	if by != "bytes" {
		if len(kept) < totalLines {
			by = "lines"
		} else {
			by = "bytes"
		}
	}
	return headTruncation{content: strings.Join(kept, "\n"), truncated: true, truncatedBy: by, totalLines: totalLines, outputLines: len(kept)}
}

// formatSize is upstream's human-readable size.
func formatSize(n int) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%dB", n)
	case n < 1024*1024:
		return jsToFixed(float64(n)/1024, 1) + "KB"
	default:
		return jsToFixed(float64(n)/(1024*1024), 1) + "MB"
	}
}

// jsToFixed is Number.prototype.toFixed for a finite number: the decimal
// with digits fraction digits nearest its exact value, the larger on a tie
// (fmt rounds ties to even).
func jsToFixed(x float64, digits int) string {
	negative := x < 0
	exact := new(big.Rat).SetFloat64(math.Abs(x))
	if exact == nil {
		return strconv.FormatFloat(x, 'f', digits, 64)
	}
	scaled := new(big.Rat).Mul(exact, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(digits)), nil)))
	// floor(scaled + 1/2): the nearest integer, the larger on a tie.
	twice := new(big.Int).Mul(scaled.Num(), big.NewInt(2))
	twice.Add(twice, scaled.Denom())
	n := new(big.Int).Quo(twice, new(big.Int).Mul(scaled.Denom(), big.NewInt(2)))
	text := n.String()
	if digits > 0 {
		if len(text) <= digits {
			text = strings.Repeat("0", digits-len(text)+1) + text
		}
		text = text[:len(text)-digits] + "." + text[len(text)-digits:]
	}
	if negative && strings.Trim(text, "0.") != "" {
		text = "-" + text
	}
	return text
}

// lineScan is one pass over a text file: its newlines, and the head,
// bytes, and lines of the lines [start, end) it selects.
type lineScan struct {
	prefix            []byte
	newlines          int
	selectedLines     int
	selectedBytes     int
	firstLineBytes    int
	lastSelectedEmpty bool
}

// scanLines reads r once, keeping only the selection's head; end < 0
// selects to the end. A leading byte order mark is not text.
func scanLines(r io.Reader, startLine, endLine int) (lineScan, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	if head, _ := br.Peek(3); bytes.Equal(head, []byte("\xef\xbb\xbf")) {
		_, _ = br.Discard(3)
	}
	scan := lineScan{lastSelectedEmpty: true}
	capture := func(b []byte) {
		if room := maxReadBytes + 2 - len(scan.prefix); room > 0 {
			if len(b) > room {
				b = b[:room]
			}
			scan.prefix = append(scan.prefix, b...)
		}
	}
	line := 0
	lineStarted := false
	for {
		chunk, err := br.ReadSlice('\n')
		if len(chunk) > 0 || (err == io.EOF && !lineStarted) {
			selected := line >= startLine && (endLine < 0 || line < endLine)
			body := chunk
			ended := len(chunk) > 0 && chunk[len(chunk)-1] == '\n'
			if ended {
				body = chunk[:len(chunk)-1]
			}
			if selected {
				if !lineStarted {
					if scan.selectedLines > 0 {
						scan.selectedBytes++
						capture([]byte{'\n'})
					}
					scan.selectedLines++
					scan.lastSelectedEmpty = true
				}
				if len(body) > 0 {
					scan.lastSelectedEmpty = false
				}
				scan.selectedBytes += len(body)
				capture(body)
				if line == startLine {
					scan.firstLineBytes += len(body)
				}
			}
			lineStarted = !ended
			if ended {
				scan.newlines++
				line++
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err == io.EOF {
			return scan, nil
		}
		if err != nil {
			return scan, err
		}
	}
}

// sliceIndex is how Array.prototype.slice reads an index: truncated, NaN 0.
func sliceIndex(x float64) float64 {
	if math.IsNaN(x) {
		return 0
	}
	return math.Trunc(x)
}

func safeInteger(x float64) bool { return x == math.Trunc(x) && math.Abs(x) <= 1<<53-1 }

// readText reads a text file's lines as upstream's read tool does, from the
// numbers the model sent: a falsy offset starts at the first line, another
// at max(0, offset - 1), and the lines are taken like
// allLines.slice(start, start + limit), which truncates fractions and counts
// a negative end from the end. It equals splitting the whole file on
// newlines and bounding the selection with truncateHead, while holding only
// the selection's head; notes about truncation and continuation are
// diagnostics, their numbers printed as JavaScript prints them. first is the
// file from its start; reopen opens it again for the second scan a negative
// end needs.
func readText(first io.Reader, reopen func() (io.Reader, func(), error), path string, offset, limit *float64) (durable.ToolResult, error) {
	start, offsetText := 0.0, "undefined"
	if offset != nil {
		offsetText = jsnum.String(*offset)
		if *offset != 0 && !math.IsNaN(*offset) {
			start = math.Max(0, *offset-1)
		}
	}
	startDisplay := start + 1
	sliceStart := sliceIndex(start)
	// A start beyond any file is scanned from 0 only to count lines; an
	// empty selection is scanned as one line and then ignored.
	scanStart := 0
	if safeInteger(sliceStart) {
		scanStart = int(sliceStart)
	}
	scanEnd := -1
	if limit != nil {
		if end := math.Max(float64(scanStart)+1, sliceIndex(start+*limit)); safeInteger(end) {
			scanEnd = int(end)
		}
	}
	scan, err := scanLines(first, scanStart, scanEnd)
	if err != nil {
		return durable.ToolResult{}, err
	}
	totalFileLines := scan.newlines + 1
	if start >= float64(totalFileLines) {
		return durable.ToolResult{}, fmt.Errorf("Offset %s is beyond end of file (%d lines total)", offsetText, totalFileLines)
	}
	var userLimitedLines *float64
	selectedLineCount := float64(totalFileLines) - sliceStart
	if limit != nil {
		endLine := math.Min(start+*limit, float64(totalFileLines))
		n := endLine - start
		userLimitedLines = &n
		// slice counts a negative end from the end of the lines, which
		// only the line count tells; scan again for it.
		relativeEnd := sliceIndex(endLine)
		sliceEnd := relativeEnd
		if relativeEnd < 0 {
			sliceEnd = math.Max(float64(totalFileLines)+relativeEnd, 0)
		}
		selectedLineCount = math.Max(0, sliceEnd-sliceStart)
		if selectedLineCount > 0 && relativeEnd < 0 {
			r, done, err := reopen()
			if err != nil {
				return durable.ToolResult{}, err
			}
			scan, err = scanLines(r, scanStart, int(sliceEnd))
			done()
			if err != nil {
				return durable.ToolResult{}, err
			}
		}
	}
	empty := selectedLineCount == 0
	totalLines, totalBytes := 0, 0
	if !empty && scan.selectedBytes > 0 {
		totalLines, totalBytes = scan.selectedLines, scan.selectedBytes
		if scan.lastSelectedEmpty && scan.selectedLines > 1 {
			totalLines-- // a trailing newline adds no line
		}
	}
	head := ""
	if !empty {
		head = strings.ToValidUTF8(string(scan.prefix), "\uFFFD")
	}
	truncation := truncateHeadOf(head, totalLines, totalBytes)
	output := truncation.content
	var diagnostics []durable.ToolDiagnostic
	switch {
	case truncation.firstLineExceeds:
		// Like allLines[startLine], a fractional start line names no line.
		var lineBytes []byte
		lineSize := 0
		if start == math.Trunc(start) {
			lineBytes, lineSize = []byte(strings.SplitN(head, "\n", 2)[0]), scan.firstLineBytes
		}
		// chord's characterEnd: the limit itself past the bytes' end, so a
		// fractional start still says the limit.
		end := maxReadBytes
		for end > 0 && end < len(lineBytes) && !utf8.RuneStart(lineBytes[end]) {
			end--
		}
		output = string(lineBytes[:min(end, len(lineBytes))])
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "warn", Code: "truncated",
			Message: fmt.Sprintf("Line %s is %s, exceeds the %s limit; showing its first %s. Use bash: sed -n '%sp' %s | tail -c +%d",
				jsnum.String(startDisplay), formatSize(lineSize), formatSize(maxReadBytes), formatSize(end), jsnum.String(startDisplay), path, end+1)})
	case truncation.truncated:
		endDisplay := startDisplay + float64(truncation.outputLines) - 1
		limitText := ""
		if truncation.truncatedBy != "lines" {
			limitText = fmt.Sprintf(" (%s limit)", formatSize(maxReadBytes))
		}
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "info", Code: "truncated",
			Message: fmt.Sprintf("Showing lines %s-%s of %d%s. Use offset=%s to continue.", jsnum.String(startDisplay), jsnum.String(endDisplay), totalFileLines, limitText, jsnum.String(endDisplay+1))})
	case userLimitedLines != nil && start+*userLimitedLines < float64(totalFileLines):
		remaining := float64(totalFileLines) - (start + *userLimitedLines)
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "info",
			Message: fmt.Sprintf("%s more lines in file. Use offset=%s to continue.", jsnum.String(remaining), jsnum.String(start+*userLimitedLines+1))})
	}
	structured, err := json.Marshal(output)
	if err != nil {
		return durable.ToolResult{}, err
	}
	return durable.ToolResult{Content: output, StructuredOutput: structured, Diagnostics: diagnostics}, nil
}
