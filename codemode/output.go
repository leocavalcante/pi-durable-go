package codemode

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf16"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// A script's result as the model reads it (upstream output.ts): a "Script
// completed/failed" header with the wall time, the script's text items, its
// console lines in one <console_output> block, the returned value, and on
// failure the error with the calls already made. Text past the token budget
// is cut to its start and end, with the complete text in a temp file; images
// are saved to temp files too, with their path next to them.

// DefaultMaxOutputTokens is the default token budget for script output.
const DefaultMaxOutputTokens = 10_000

// lineMargin counts the lines of the result besides the script's text.
const lineMargin = 100

// imageExtensions are the file extensions of the image types image()
// accepts.
var imageExtensions = map[string]string{"image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif", "image/webp": ".webp"}

// item is a content block of the result: text, or an image.
type item struct {
	image    bool
	text     string
	data     string
	mimeType string
}

// outputFile writes a file of the result under a fixed name and returns its
// path, or the error text.
type outputFile func(name string, data []byte) (path string, failure string)

// outputLimits are the tool's limits, which the Harness enforces by cutting
// without a spill file: results stay inside them.
type outputLimits struct{ maxBytes, maxLines int }

// valueText is like the script's text(): strings as is, other values as
// compact JSON.
func valueText(value []byte) string {
	parsed, err := js.Parse(value)
	if err != nil {
		return string(value)
	}
	if text, ok := parsed.(string); ok {
		return text
	}
	return js.Stringify(parsed)
}

// formatItems lays out the script's output: with more than one text item,
// each starts with a "==> text N/M <==" line; console lines follow all other
// output in one <console_output> block.
func formatItems(output []OutputItem) []item {
	total := 0
	for _, out := range output {
		if out.Type == "text" && !out.Console {
			total++
		}
	}
	var items, consoleLines = []item{}, []string{}
	index := 0
	for _, out := range output {
		switch {
		case out.Type == "image":
			items = append(items, item{image: true, data: out.Data, mimeType: out.MimeType})
		case out.Console:
			consoleLines = append(consoleLines, out.Text)
		default:
			index++
			text := out.Text
			if total > 1 {
				text = fmt.Sprintf("==> text %d/%d <==\n%s", index, total, out.Text)
			}
			items = append(items, item{text: text})
		}
	}
	if len(consoleLines) > 0 {
		items = append(items, item{text: "<console_output>\n" + strings.Join(consoleLines, "\n") + "\n</console_output>"})
	}
	return items
}

// joinAdjacentText joins adjacent text items into one, each part starting on
// its own line.
func joinAdjacentText(items []item) []item {
	var joined []item
	var parts strings.Builder
	open, lastPart := false, ""
	flush := func() {
		if open {
			joined = append(joined, item{text: parts.String()})
		}
		parts.Reset()
		open, lastPart = false, ""
	}
	for _, it := range items {
		if it.image {
			flush()
			joined = append(joined, it)
			continue
		}
		if lastPart != "" && !strings.HasSuffix(lastPart, "\n") {
			parts.WriteString("\n")
			lastPart = "\n"
		}
		parts.WriteString(it.text)
		open = true
		if it.text != "" {
			lastPart = it.text
		}
	}
	flush()
	return joined
}

// jsSlice is Array.prototype.slice(start, end) of a list of lines; end < 0
// counts from the end, and math.MaxInt means to the end.
func jsSlice(lines []string, start, end int) []string {
	n := len(lines)
	clamp := func(i int) int {
		if i < 0 {
			i += n
		}
		return max(0, min(i, n))
	}
	from, to := clamp(start), clamp(end)
	if from >= to {
		return nil
	}
	return lines[from:to]
}

// utf16Slice is String.prototype.slice(start, end) over code units, ends
// counting from the end when negative.
func utf16Slice(units []uint16, start, end int) string {
	n := len(units)
	clamp := func(i int) int {
		if i < 0 {
			i += n
		}
		return max(0, min(i, n))
	}
	from, to := clamp(start), clamp(end)
	if from >= to {
		return ""
	}
	return string(utf16.Decode(units[from:to]))
}

// truncate applies the token budget: when the combined text exceeds it, the
// text items become one item that keeps the start and end of the text, and
// images follow it. The complete text is written to a temp file.
func truncate(items []item, maxTokens float64, limits outputLimits, write outputFile) ([]item, string) {
	var texts []string
	for _, it := range items {
		if !it.image {
			texts = append(texts, it.text)
		}
	}
	combined := strings.Join(texts, "\n")
	units := utf16.Encode([]rune(combined))
	// Within the limits too: a UTF-16 unit is at most 3 UTF-8 bytes, and the
	// rest of the result is a few short lines.
	budget := int(math.Min(maxTokens*charsPerToken, math.Floor(float64(limits.maxBytes)/4)))
	lineBudget := int(math.Floor(float64(limits.maxLines-lineMargin) / 2))
	lines := strings.Count(combined, "\n") + 1
	if len(texts) == 0 || len(units) <= budget && lines <= 2*lineBudget {
		return items, ""
	}
	headChars := budget / 2
	tailChars := budget - headChars
	head := strings.Join(jsSlice(strings.Split(utf16Slice(units, 0, headChars), "\n"), 0, lineBudget), "\n")
	tail := ""
	if tailChars > 0 {
		tailLines := strings.Split(utf16Slice(units, -tailChars, len(units)), "\n")
		if lineBudget == 0 {
			tail = strings.Join(tailLines, "\n") // slice(-0) is the whole list
		} else {
			tail = strings.Join(jsSlice(tailLines, -lineBudget, len(tailLines)), "\n")
		}
	}
	removed := len(units) - js.Len(head) - js.Len(tail)
	text := fmt.Sprintf("Warning: truncated output (original token count: %d)\nTotal output lines: %d\n\n%s…%d tokens truncated…%s",
		int(math.Ceil(float64(len(units))/charsPerToken)), lines, head, int(math.Ceil(float64(removed)/charsPerToken)), tail)
	path, failure := write("output.txt", []byte(combined))
	if failure == "" {
		text += "\n\n[Full output: " + path + " (read with offset/limit)]"
	} else {
		text += "\n\n[Could not save the full output: " + failure + "]"
	}
	out := []item{{text: text}}
	for _, it := range items {
		if it.image {
			out = append(out, it)
		}
	}
	if failure != "" {
		path = ""
	}
	return out, path
}

// atob decodes base64 as Node's atob does (WHATWG forgiving-base64), with
// its error messages.
func atob(data string) ([]byte, error) {
	invalid := errors.New("Invalid character")
	var cleaned strings.Builder
	count, equals := 0, 0
	for i := 0; i < len(data); i++ {
		c := data[i]
		switch {
		case c == '\t' || c == '\n' || c == '\f' || c == '\r' || c == ' ':
			continue
		case c == '=':
			count++
			equals++
			if equals > 2 {
				return nil, invalid
			}
		case c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '+' || c == '/':
			if equals > 0 {
				return nil, invalid // "=" only at the end
			}
			count++
			cleaned.WriteByte(c)
		default:
			return nil, invalid
		}
	}
	remainder := count % 4
	if remainder == 0 {
		remainder = (count - equals) % 4
	} else if equals > 0 {
		return nil, invalid
	}
	if remainder == 1 {
		return nil, errors.New("The string to be decoded is not correctly encoded.")
	}
	decoded, err := base64.RawStdEncoding.DecodeString(cleaned.String())
	if err != nil {
		return nil, invalid
	}
	return decoded, nil
}

// saveImages saves each image to a file and puts a text item with its path
// before it; an image shown twice is saved once.
func saveImages(items []item, write outputFile) ([]item, error) {
	labels := map[string]string{}
	var out []item
	for _, it := range items {
		if !it.image {
			out = append(out, it)
			continue
		}
		label, ok := labels[it.data]
		if !ok {
			bytes, err := atob(it.data)
			if err != nil {
				return nil, err
			}
			kind := fmt.Sprintf("%s, %d bytes", it.mimeType, len(bytes))
			extension, known := imageExtensions[it.mimeType]
			if !known {
				extension = ".bin"
			}
			// A failed write must not discard the result of a script whose calls
			// already ran: it becomes part of the label.
			path, failure := write(fmt.Sprintf("image-%d%s", len(labels)+1, extension), bytes)
			if failure == "" {
				label = "[Image saved to " + path + " (" + kind + ")]"
			} else {
				label = "[Image (" + kind + ") could not be saved: " + failure + "]"
			}
			labels[it.data] = label
		}
		out = append(out, item{text: label}, it)
	}
	return out, nil
}

func formatError(result Result, calls []NestedCall) string {
	failure := result.Error
	if failure == nil {
		failure = &Error{Kind: "sandbox", Message: "unknown error"}
	}
	var head string
	switch failure.Kind {
	case "script":
		head = failure.Stack
		if head == "" {
			name := failure.Name
			if name == "" {
				name = "Error"
			}
			head = name + ": " + failure.Message
		}
	case "timeout":
		head = "Script timed out: " + failure.Message
	case "aborted":
		head = "Script aborted: " + failure.Message
	default:
		head = "Script sandbox failed: " + failure.Message
	}
	summary := "No tool calls were made."
	if len(calls) > 0 {
		made := make([]string, len(calls))
		for i, call := range calls {
			made[i] = call.Name + " (" + call.Status + ")"
		}
		summary = "Tool calls made before the failure (they are not undone): " + strings.Join(made, ", ")
	}
	return head + "\n\n" + summary
}

type formatOptions struct {
	// generatedImages counts the images models.generateImages() returned;
	// when the script showed none, the result says so.
	generatedImages int
	wallMs          float64
	maxOutputTokens float64
	calls           []NestedCall
	limits          outputLimits
	write           outputFile
}

// formatResult is the tool result's content for a script's result, and the
// path of the complete text when it was cut.
func formatResult(result Result, options formatOptions) ([]item, string, error) {
	output := append([]OutputItem(nil), result.Output...)
	if result.OK && len(result.Value) > 0 {
		output = append(output, OutputItem{Type: "text", Text: valueText(result.Value)})
	}
	items := formatItems(output)
	if !result.OK {
		items = append(items, item{text: "Script error:\n" + formatError(result, options.calls)})
	}
	if generated := options.generatedImages; generated > 0 {
		shown := false
		for _, it := range items {
			shown = shown || it.image
		}
		if !shown {
			plural := "s"
			if generated == 1 {
				plural = ""
			}
			items = append(items, item{text: fmt.Sprintf("Note: models.generateImages() returned %d image%s that the script did not show. Show each image block of result.output with image(block).", generated, plural)})
		}
	}
	truncated, fullOutputPath := truncate(joinAdjacentText(items), options.maxOutputTokens, options.limits, options.write)
	// After truncation, which joins the text items and moves images after
	// them, so each path stays next to its image.
	saved, err := saveImages(truncated, options.write)
	if err != nil {
		return nil, "", err
	}
	header := "Script completed"
	if !result.OK {
		header = "Script failed"
	}
	header += "\nWall time " + js.ToFixed(options.wallMs/1000, 1) + " seconds\nOutput:\n"
	return append([]item{{text: header}}, joinAdjacentText(saved)...), fullOutputPath, nil
}

// toolResultContent is the result's blocks as a tool result: text blocks
// joined by "\n", as pi-ai joins a tool result's text, with each image where
// it appears.
func toolResultContent(items []item) (string, []durable.ImageContent, []int) {
	var content strings.Builder
	var images []durable.ImageContent
	var offsets []int
	wrote := false
	for _, it := range items {
		if it.image {
			images = append(images, durable.ImageContent{Data: it.data, MimeType: it.mimeType})
			offsets = append(offsets, content.Len())
			continue
		}
		if wrote {
			content.WriteString("\n")
		}
		content.WriteString(it.text)
		wrote = true
	}
	return content.String(), images, offsets
}
