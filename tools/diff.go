package tools

import (
	"fmt"
	"strconv"
	"strings"
)

// diffPart is a run of lines (each with its newline, when it has one) that
// are kept (' '), removed ('-'), or added ('+').
type diffPart struct {
	kind  byte
	lines []string
}

func splitAfterLines(content string) []string {
	if content == "" {
		return nil
	}
	return strings.SplitAfter(content, "\n")[:strings.Count(content, "\n")+boolInt(!strings.HasSuffix(content, "\n"))]
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// diffLines is a Myers line diff, removals before additions in each change.
func diffLines(oldContent, newContent string) []diffPart {
	a, b := splitAfterLines(oldContent), splitAfterLines(newContent)
	prefix := 0
	for prefix < len(a) && prefix < len(b) && a[prefix] == b[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(a)-prefix && suffix < len(b)-prefix && a[len(a)-1-suffix] == b[len(b)-1-suffix] {
		suffix++
	}
	var parts []diffPart
	add := func(kind byte, line string) {
		if n := len(parts); n > 0 && parts[n-1].kind == kind {
			parts[n-1].lines = append(parts[n-1].lines, line)
			return
		}
		parts = append(parts, diffPart{kind: kind, lines: []string{line}})
	}
	for _, line := range a[:prefix] {
		add(' ', line)
	}
	for _, op := range myers(a[prefix:len(a)-suffix], b[prefix:len(b)-suffix]) {
		add(op.kind, op.line)
	}
	for _, line := range a[len(a)-suffix:] {
		add(' ', line)
	}
	// Removals first within each change, as jsdiff reports them.
	for i := 0; i+1 < len(parts); i++ {
		if parts[i].kind == '+' && parts[i+1].kind == '-' {
			parts[i], parts[i+1] = parts[i+1], parts[i]
		}
	}
	return parts
}

type diffOp struct {
	kind byte
	line string
}

func myers(a, b []string) []diffOp {
	n, m := len(a), len(b)
	if n == 0 && m == 0 {
		return nil
	}
	limit := n + m
	v := make([]int, 2*limit+2)
	var trace [][]int
	for d := 0; d <= limit; d++ {
		trace = append(trace, append([]int(nil), v...))
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && v[k-1+limit] < v[k+1+limit]) {
				x = v[k+1+limit]
			} else {
				x = v[k-1+limit] + 1
			}
			y := x - k
			for x < n && y < m && a[x] == b[y] {
				x++
				y++
			}
			v[k+limit] = x
			if x >= n && y >= m {
				return backtrack(trace, a, b, d, limit)
			}
		}
	}
	return nil
}

func backtrack(trace [][]int, a, b []string, d, limit int) []diffOp {
	x, y := len(a), len(b)
	var ops []diffOp
	for ; d > 0; d-- {
		v := trace[d]
		k := x - y
		var prevK int
		if k == -d || (k != d && v[k-1+limit] < v[k+1+limit]) {
			prevK = k + 1
		} else {
			prevK = k - 1
		}
		prevX := v[prevK+limit]
		prevY := prevX - prevK
		for x > prevX && y > prevY {
			ops = append(ops, diffOp{' ', a[x-1]})
			x--
			y--
		}
		if x == prevX {
			ops = append(ops, diffOp{'+', b[y-1]})
			y--
		} else {
			ops = append(ops, diffOp{'-', a[x-1]})
			x--
		}
	}
	for x > 0 && y > 0 {
		ops = append(ops, diffOp{' ', a[x-1]})
		x--
		y--
	}
	for i, j := 0, len(ops)-1; i < j; i, j = i+1, j-1 {
		ops[i], ops[j] = ops[j], ops[i]
	}
	return ops
}

// generateDiffString is upstream's display diff: numbered changed lines with
// contextLines of context, and the first changed line of the new content.
func generateDiffString(oldContent, newContent string, contextLines int) (string, int) {
	parts := diffLines(oldContent, newContent)
	width := len(strconv.Itoa(max(strings.Count(oldContent, "\n")+1, strings.Count(newContent, "\n")+1)))
	num := func(n int) string { return fmt.Sprintf("%*d", width, n) }
	skipped := " " + strings.Repeat(" ", width) + " ..."
	var output []string
	oldLine, newLine, firstChanged := 1, 1, 0
	lastWasChange := false
	for i, part := range parts {
		raw := make([]string, len(part.lines))
		for j, line := range part.lines {
			raw[j] = strings.TrimSuffix(line, "\n")
		}
		if part.kind != ' ' {
			if firstChanged == 0 {
				firstChanged = newLine
			}
			for _, line := range raw {
				if part.kind == '+' {
					output = append(output, "+"+num(newLine)+" "+line)
					newLine++
				} else {
					output = append(output, "-"+num(oldLine)+" "+line)
					oldLine++
				}
			}
			lastWasChange = true
			continue
		}
		show := func(lines []string) {
			for _, line := range lines {
				output = append(output, " "+num(oldLine)+" "+line)
				oldLine++
				newLine++
			}
		}
		skip := func(n int) {
			if n > 0 {
				output = append(output, skipped)
				oldLine += n
				newLine += n
			}
		}
		nextIsChange := i < len(parts)-1 && parts[i+1].kind != ' '
		switch {
		case lastWasChange && nextIsChange:
			if len(raw) <= contextLines*2 {
				show(raw)
			} else {
				show(raw[:contextLines])
				skip(len(raw) - 2*contextLines)
				show(raw[len(raw)-contextLines:])
			}
		case lastWasChange:
			shown := raw[:min(contextLines, len(raw))]
			show(shown)
			skip(len(raw) - len(shown))
		case nextIsChange:
			n := max(0, len(raw)-contextLines)
			skip(n)
			show(raw[n:])
		default:
			oldLine += len(raw)
			newLine += len(raw)
		}
		lastWasChange = false
	}
	return strings.Join(output, "\n"), firstChanged
}

// generateUnifiedPatch is a unified patch with file headers only.
func generateUnifiedPatch(path, oldContent, newContent string, contextLines int) string {
	type line struct {
		kind byte
		text string
	}
	var lines []line
	for _, part := range diffLines(oldContent, newContent) {
		for _, text := range part.lines {
			lines = append(lines, line{part.kind, text})
		}
	}
	var out strings.Builder
	out.WriteString("--- " + path + "\n+++ " + path + "\n")
	oldNo, newNo := 1, 1
	for i := 0; i < len(lines); {
		if lines[i].kind == ' ' {
			oldNo++
			newNo++
			i++
			continue
		}
		// A hunk: context before, changes joined by short context, context after.
		start := max(0, i-contextLines)
		oldStart, newStart := oldNo-(i-start), newNo-(i-start)
		end := i
		for end < len(lines) {
			if lines[end].kind != ' ' {
				end++
				continue
			}
			run := end
			for run < len(lines) && lines[run].kind == ' ' {
				run++
			}
			if run < len(lines) && run-end <= 2*contextLines {
				end = run
				continue
			}
			end = min(run, end+contextLines)
			break
		}
		oldCount, newCount := 0, 0
		var body strings.Builder
		for _, l := range lines[start:end] {
			switch l.kind {
			case ' ':
				oldCount++
				newCount++
			case '-':
				oldCount++
			case '+':
				newCount++
			}
			body.WriteByte(l.kind)
			body.WriteString(l.text)
			if !strings.HasSuffix(l.text, "\n") {
				body.WriteString("\n\\ No newline at end of file\n")
			}
		}
		if oldCount == 0 {
			oldStart--
		}
		if newCount == 0 {
			newStart--
		}
		fmt.Fprintf(&out, "@@ -%d,%d +%d,%d @@\n", oldStart, oldCount, newStart, newCount)
		out.WriteString(body.String())
		for _, l := range lines[i:end] {
			switch l.kind {
			case ' ':
				oldNo++
				newNo++
			case '-':
				oldNo++
			case '+':
				newNo++
			}
		}
		i = end
	}
	return out.String()
}
