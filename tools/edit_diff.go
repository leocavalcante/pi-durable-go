package tools

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/text/unicode/norm"
)

// A port of upstream edit-diff.ts: line endings, fuzzy matching, and
// replacements that keep unchanged lines byte for byte.

func detectLineEnding(content string) string {
	crlf := strings.Index(content, "\r\n")
	lf := strings.Index(content, "\n")
	if lf == -1 || crlf == -1 {
		return "\n"
	}
	if crlf < lf {
		return "\r\n"
	}
	return "\n"
}

func normalizeToLF(text string) string {
	return strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
}

func restoreLineEndings(text, ending string) string {
	if ending == "\r\n" {
		return strings.ReplaceAll(text, "\n", "\r\n")
	}
	return text
}

var fuzzyReplacer = strings.NewReplacer(
	"\u2018", "'", "\u2019", "'", "\u201A", "'", "\u201B", "'",
	"\u201C", `"`, "\u201D", `"`, "\u201E", `"`, "\u201F", `"`,
	"\u2010", "-", "\u2011", "-", "\u2012", "-", "\u2013", "-", "\u2014", "-", "\u2015", "-", "\u2212", "-",
	"\u00A0", " ", "\u2002", " ", "\u2003", " ", "\u2004", " ", "\u2005", " ", "\u2006", " ", "\u2007", " ",
	"\u2008", " ", "\u2009", " ", "\u200A", " ", "\u202F", " ", "\u205F", " ", "\u3000", " ",
)

// normalizeForFuzzyMatch applies NFKC, strips trailing whitespace from each
// line, and maps smart quotes, Unicode dashes, and special spaces to ASCII.
func normalizeForFuzzyMatch(text string) string {
	lines := strings.Split(norm.NFKC.String(text), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t\v\f\r\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000\ufeff")
	}
	return fuzzyReplacer.Replace(strings.Join(lines, "\n"))
}

var linesWithEndings = regexp.MustCompile(`[^\n]*\n|[^\n]+`)

func splitLinesWithEndings(content string) []string {
	return linesWithEndings.FindAllString(content, -1)
}

type lineSpan struct{ start, end int }

func lineSpans(content string) []lineSpan {
	var spans []lineSpan
	offset := 0
	for _, line := range splitLinesWithEndings(content) {
		spans = append(spans, lineSpan{offset, offset + len(line)})
		offset += len(line)
	}
	return spans
}

type replacement struct {
	editIndex, matchIndex, matchLength int
	newText                            string
}

func replacementLineRange(lines []lineSpan, r replacement) (int, int, error) {
	start, end := r.matchIndex, r.matchIndex+r.matchLength
	startLine := -1
	for i, line := range lines {
		if start >= line.start && start < line.end {
			startLine = i
			break
		}
	}
	if startLine == -1 {
		return 0, 0, errors.New("Replacement range is outside the base content.")
	}
	endLine := startLine
	for endLine < len(lines) && lines[endLine].end < end {
		endLine++
	}
	if endLine >= len(lines) {
		return 0, 0, errors.New("Replacement range is outside the base content.")
	}
	return startLine, endLine + 1, nil
}

func applyReplacements(content string, replacements []replacement, offset int) string {
	result := content
	for i := len(replacements) - 1; i >= 0; i-- {
		r := replacements[i]
		at := r.matchIndex - offset
		result = result[:at] + r.newText + result[at+r.matchLength:]
	}
	return result
}

// applyReplacementsPreservingUnchangedLines applies replacements matched in
// the normalized base to the original, rewriting only the lines they touch.
func applyReplacementsPreservingUnchangedLines(original, base string, replacements []replacement) (string, error) {
	originalLines := splitLinesWithEndings(original)
	baseLines := lineSpans(base)
	if len(originalLines) != len(baseLines) {
		return "", errors.New("Cannot preserve unchanged lines because the base content has a different line count.")
	}
	type group struct {
		start, end   int
		replacements []replacement
	}
	sorted := append([]replacement(nil), replacements...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].matchIndex < sorted[j].matchIndex })
	var groups []*group
	for _, r := range sorted {
		start, end, err := replacementLineRange(baseLines, r)
		if err != nil {
			return "", err
		}
		if n := len(groups); n > 0 && start < groups[n-1].end {
			groups[n-1].end = max(groups[n-1].end, end)
			groups[n-1].replacements = append(groups[n-1].replacements, r)
			continue
		}
		groups = append(groups, &group{start, end, []replacement{r}})
	}
	var result strings.Builder
	next := 0
	for _, g := range groups {
		result.WriteString(strings.Join(originalLines[next:g.start], ""))
		from, to := baseLines[g.start].start, baseLines[g.end-1].end
		result.WriteString(applyReplacements(base[from:to], g.replacements, from))
		next = g.end
	}
	result.WriteString(strings.Join(originalLines[next:], ""))
	return result.String(), nil
}

// fuzzyFindText finds oldText exactly, or else in fuzzy-normalized space.
func fuzzyFindText(content, oldText string) (index, length int, fuzzy, found bool) {
	if at := strings.Index(content, oldText); at != -1 {
		return at, len(oldText), false, true
	}
	fuzzyContent, fuzzyOld := normalizeForFuzzyMatch(content), normalizeForFuzzyMatch(oldText)
	if at := strings.Index(fuzzyContent, fuzzyOld); at != -1 {
		return at, len(fuzzyOld), true, true
	}
	return -1, 0, false, false
}

func countOccurrences(content, oldText string) int {
	return strings.Count(normalizeForFuzzyMatch(content), normalizeForFuzzyMatch(oldText))
}

type textEdit struct{ oldText, newText string }

// applyEditsToNormalizedContent matches every edit against the same
// LF-normalized content and applies them, with upstream's errors.
func applyEditsToNormalizedContent(content string, edits []textEdit, path string) (string, string, error) {
	total := len(edits)
	normalized := make([]textEdit, len(edits))
	for i, edit := range edits {
		normalized[i] = textEdit{normalizeToLF(edit.oldText), normalizeToLF(edit.newText)}
		if normalized[i].oldText == "" {
			if total == 1 {
				return "", "", fmt.Errorf("oldText must not be empty in %s.", path)
			}
			return "", "", fmt.Errorf("edits[%d].oldText must not be empty in %s.", i, path)
		}
	}
	usedFuzzy := false
	for _, edit := range normalized {
		if _, _, fuzzy, _ := fuzzyFindText(content, edit.oldText); fuzzy {
			usedFuzzy = true
		}
	}
	base := content
	if usedFuzzy {
		base = normalizeForFuzzyMatch(content)
	}
	var matched []replacement
	for i, edit := range normalized {
		index, length, _, found := fuzzyFindText(base, edit.oldText)
		if !found {
			if total == 1 {
				return "", "", fmt.Errorf("Could not find the exact text in %s. The old text must match exactly including all whitespace and newlines.", path)
			}
			return "", "", fmt.Errorf("Could not find edits[%d] in %s. The oldText must match exactly including all whitespace and newlines.", i, path)
		}
		if n := countOccurrences(base, edit.oldText); n > 1 {
			if total == 1 {
				return "", "", fmt.Errorf("Found %d occurrences of the text in %s. The text must be unique. Please provide more context to make it unique.", n, path)
			}
			return "", "", fmt.Errorf("Found %d occurrences of edits[%d] in %s. Each oldText must be unique. Please provide more context to make it unique.", n, i, path)
		}
		matched = append(matched, replacement{editIndex: i, matchIndex: index, matchLength: length, newText: edit.newText})
	}
	sort.SliceStable(matched, func(i, j int) bool { return matched[i].matchIndex < matched[j].matchIndex })
	for i := 1; i < len(matched); i++ {
		previous, current := matched[i-1], matched[i]
		if previous.matchIndex+previous.matchLength > current.matchIndex {
			return "", "", fmt.Errorf("edits[%d] and edits[%d] overlap in %s. Merge them into one edit or target disjoint regions.", previous.editIndex, current.editIndex, path)
		}
	}
	var updated string
	if usedFuzzy {
		var err error
		if updated, err = applyReplacementsPreservingUnchangedLines(content, base, matched); err != nil {
			return "", "", err
		}
	} else {
		updated = applyReplacements(base, matched, 0)
	}
	if updated == content {
		if total == 1 {
			return "", "", fmt.Errorf("No changes made to %s. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected.", path)
		}
		return "", "", fmt.Errorf("No changes made to %s. The replacements produced identical content.", path)
	}
	return content, updated, nil
}
