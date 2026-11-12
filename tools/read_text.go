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

// characterEnd is the largest end at most limit that does not split a
// UTF-8 character of b.
func characterEnd(b []byte, limit int) int {
	if limit >= len(b) {
		return len(b)
	}
	end := limit
	for end > 0 && !utf8.RuneStart(b[end]) {
		end--
	}
	return end
}

// readText reads lines [offset-1, offset-1+limit) of a text stream as
// upstream's read tool does: equal to splitting the whole file on newlines
// and bounding the selection with truncateHead, while holding only the
// selection's head. Remarks about truncation and continuation are
// diagnostics.
func readText(r io.Reader, path string, startLine int, offset string, limit *int) (durable.ToolResult, error) {
	endLine := -1
	if limit != nil {
		endLine = startLine + *limit
	}
	br := bufio.NewReaderSize(r, 64*1024)
	if head, _ := br.Peek(3); bytes.Equal(head, []byte("\xef\xbb\xbf")) {
		_, _ = br.Discard(3)
	}
	var prefix []byte
	line, newlines := 0, 0
	selectedLines, selectedBytes, firstLineBytes := 0, 0, 0
	lastSelectedEmpty := true
	capture := func(b []byte) {
		if room := maxReadBytes + 2 - len(prefix); room > 0 {
			if len(b) > room {
				b = b[:room]
			}
			prefix = append(prefix, b...)
		}
	}
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
					if selectedLines > 0 {
						selectedBytes++
						capture([]byte{'\n'})
					}
					selectedLines++
					lastSelectedEmpty = true
				}
				if len(body) > 0 {
					lastSelectedEmpty = false
				}
				selectedBytes += len(body)
				capture(body)
				if line == startLine {
					firstLineBytes += len(body)
				}
			}
			lineStarted = !ended
			if ended {
				newlines++
				line++
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return durable.ToolResult{}, err
		}
	}
	totalFileLines := newlines + 1
	if startLine >= totalFileLines {
		return durable.ToolResult{}, fmt.Errorf("Offset %s is beyond end of file (%d lines total)", offset, totalFileLines)
	}
	var userLimitedLines *int
	if limit != nil {
		end := min(startLine+*limit, totalFileLines)
		n := end - startLine
		userLimitedLines = &n
	}
	totalLines := 0
	if selectedLines > 0 && selectedBytes > 0 {
		totalLines = selectedLines
		if lastSelectedEmpty && selectedLines > 1 {
			totalLines-- // a trailing newline adds no line
		}
	}
	head := strings.ToValidUTF8(string(prefix), "\uFFFD")
	truncation := truncateHeadOf(head, totalLines, selectedBytes)
	output := truncation.content
	var diagnostics []durable.ToolDiagnostic
	startDisplay := startLine + 1
	switch {
	case truncation.firstLineExceeds:
		lineBytes := []byte(strings.SplitN(head, "\n", 2)[0])
		end := characterEnd(lineBytes, maxReadBytes)
		output = string(lineBytes[:end])
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "warn", Code: "truncated",
			Message: fmt.Sprintf("Line %d is %s, exceeds the %s limit; showing its first %s. Use bash: sed -n '%dp' %s | tail -c +%d",
				startDisplay, formatSize(firstLineBytes), formatSize(maxReadBytes), formatSize(end), startDisplay, path, end+1)})
	case truncation.truncated:
		endDisplay := startDisplay + truncation.outputLines - 1
		limitText := ""
		if truncation.truncatedBy != "lines" {
			limitText = fmt.Sprintf(" (%s limit)", formatSize(maxReadBytes))
		}
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "info", Code: "truncated",
			Message: fmt.Sprintf("Showing lines %d-%d of %d%s. Use offset=%d to continue.", startDisplay, endDisplay, totalFileLines, limitText, endDisplay+1)})
	case userLimitedLines != nil && startLine+*userLimitedLines < totalFileLines:
		remaining := totalFileLines - (startLine + *userLimitedLines)
		diagnostics = append(diagnostics, durable.ToolDiagnostic{Severity: "info",
			Message: fmt.Sprintf("%d more lines in file. Use offset=%d to continue.", remaining, startLine+*userLimitedLines+1)})
	}
	structured, err := json.Marshal(output)
	if err != nil {
		return durable.ToolResult{}, err
	}
	return durable.ToolResult{Content: output, StructuredOutput: structured, Diagnostics: diagnostics}, nil
}
