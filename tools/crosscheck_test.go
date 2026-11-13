//go:build crosscheck

package tools

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/leocavalcante/pi-durable-go/internal/crosscheck"
)

// Cross-checks with upstream's own code (make crosscheck).

func TestCrosscheckEditDiff(t *testing.T) {
	var cases []struct {
		Content string `json:"content"`
		Edits   []struct {
			OldText string `json:"oldText"`
			NewText string `json:"newText"`
		} `json:"edits"`
	}
	crosscheck.Cases(t, "edit.json", &cases)
	var want []map[string]any
	crosscheck.Run(t, "editDiff", cases, &want)
	for i, c := range cases {
		got := map[string]any{"fuzzy": normalizeForFuzzyMatch(c.Content)}
		edits := make([]textEdit, len(c.Edits))
		for j, e := range c.Edits {
			edits[j] = textEdit{e.OldText, e.NewText}
		}
		base, next, err := applyEditsToNormalizedContent(normalizeToLF(c.Content), edits, "f.txt")
		if err != nil {
			got["error"] = err.Error()
		} else {
			diff, first := generateDiffString(base, next, 4)
			got["base"], got["next"], got["diff"], got["first"] = base, next, diff, float64(first)
			got["patch"] = generateUnifiedPatch("f.txt", base, next, 4)
		}
		if !reflect.DeepEqual(got, want[i]) {
			t.Errorf("case %d:\n got %q\nwant %q", i, got, want[i])
		}
	}
}

func TestCrosscheckRead(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"empty.txt": "",
		"one.txt":   "one line",
		"three.txt": "a\nb\nc\n",
		"wide.txt":  strings.Repeat("x", 60000) + "\nsecond\n",
		"bytes.txt": strings.Repeat(strings.Repeat("y", 40)+"\n", 1500),
		"crlf.txt":  "r1\r\nr2\r\nr3",
		"bom.txt":   "\ufeffbom first\nnext",
		"utf8.txt":  strings.Repeat(strings.Repeat("é", 100)+"\n", 600),
		"blank.txt": "\n\n\n",
	}
	var many []string
	for i := 1; i <= 2500; i++ {
		many = append(many, fmt.Sprintf("line %d", i))
	}
	files["many.txt"] = strings.Join(many, "\n")
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		path string
		args string
	}{
		{"empty.txt", `{}`}, {"one.txt", `{}`}, {"three.txt", `{}`}, {"three.txt", `{"offset":2}`}, {"three.txt", `{"offset":2,"limit":1}`},
		{"three.txt", `{"offset":9}`}, {"three.txt", `{"offset":4}`}, {"many.txt", `{}`}, {"many.txt", `{"offset":2400}`},
		{"many.txt", `{"offset":10,"limit":5}`}, {"many.txt", `{"limit":3}`}, {"wide.txt", `{}`}, {"wide.txt", `{"offset":2}`},
		{"bytes.txt", `{}`}, {"crlf.txt", `{}`}, {"bom.txt", `{}`}, {"utf8.txt", `{}`}, {"utf8.txt", `{"offset":300,"limit":2}`},
		{"blank.txt", `{}`}, {"three.txt", `{"limit":0}`}, {"three.txt", `{"offset":0}`}, {"three.txt", `{"offset":-2}`},
		// Fractions and negative ends, as Array.prototype.slice reads them.
		{"many.txt", `{"offset":2.5,"limit":3.5}`}, {"many.txt", `{"offset":10,"limit":2.25}`}, {"many.txt", `{"limit":-2}`},
		{"many.txt", `{"offset":3,"limit":-2490}`}, {"many.txt", `{"offset":3,"limit":-2499}`}, {"three.txt", `{"limit":-1}`},
		{"three.txt", `{"offset":1.5}`}, {"three.txt", `{"offset":3.9}`}, {"three.txt", `{"offset":0.5,"limit":1}`},
		{"many.txt", `{"offset":1e21}`}, {"many.txt", `{"offset":2400.75,"limit":50}`}, {"wide.txt", `{"offset":1.5}`},
		{"bytes.txt", `{"offset":0.25}`}, {"many.txt", `{"limit":1.5}`}, {"three.txt", `{"offset":-0.5,"limit":-1.5}`},
	}
	input := map[string]any{"dir": dir}
	var list [][2]any
	for _, c := range cases {
		list = append(list, [2]any{c.path, json.RawMessage(c.args)})
	}
	input["cases"] = list
	var want []struct {
		Text        string   `json:"text"`
		Diagnostics []string `json:"diagnostics"`
		Error       string   `json:"error"`
	}
	crosscheck.Run(t, "read", input, &want)
	for i, c := range cases {
		var a readArgs
		if err := decodeArgs(json.RawMessage(c.args), &a); err != nil {
			t.Fatal(err)
		}
		a.Path = c.path
		file := filepath.Join(dir, c.path)
		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		reopen := func() (io.Reader, func(), error) {
			again, err := os.Open(file)
			if err != nil {
				return nil, nil, err
			}
			return again, func() { _ = again.Close() }, nil
		}
		result, err := readText(f, reopen, c.path, a.Offset, a.Limit)
		f.Close()
		if want[i].Error != "" || err != nil {
			if err == nil || err.Error() != want[i].Error {
				t.Errorf("%s %s: error %v, upstream %q", c.path, c.args, err, want[i].Error)
			}
			continue
		}
		var diagnostics []string
		for _, d := range result.Diagnostics {
			diagnostics = append(diagnostics, d.Severity+"|"+d.Code+"|"+d.Message)
		}
		if result.Content != want[i].Text || strings.Join(diagnostics, "\n") != strings.Join(want[i].Diagnostics, "\n") {
			t.Errorf("%s %s: diagnostics %q, upstream %q (texts equal: %v)", c.path, c.args, diagnostics, want[i].Diagnostics, result.Content == want[i].Text)
		}
	}
}

func TestCrosscheckToolSearchRanking(t *testing.T) {
	docs := []SearchDocument{
		{Name: "mcp__peekaboo__app", Text: "Launch, quit, or focus an application window", NamespaceText: "peekaboo screen automation"},
		{Name: "mcp__peekaboo__see", Text: "Capture a screenshot of the screen or a window and describe it", NamespaceText: "peekaboo screen automation"},
		{Name: "window", Text: "Move and resize windows"},
		{Name: "mcp__github__create_issue", Text: "Create a new issue in a GitHub repository with title and body", NamespaceText: "github"},
		{Name: "mcp__github__list_issues", Text: "List issues in a repository, filtered by state and labels", NamespaceText: "github"},
		{Name: "read_file", Text: "Read a file from disk. path: file path"},
		{Name: "écrire", Text: "Écrire un fichier sur le disque"},
	}
	queries := []string{"screenshot window", "peekaboo app", "app", "github issue", "create issue", "read file", "mcp__github__list_issues",
		"list_issues", "écrire fichier", "ECRIRE", "nothing matches here", "issues labels state"}
	piDocs := make([]map[string]string, len(docs))
	for i, d := range docs {
		piDocs[i] = map[string]string{"name": d.Name, "text": d.Text, "namespaceText": d.NamespaceText}
	}
	var want [][]string
	crosscheck.Run(t, "rank", map[string]any{"docs": piDocs, "queries": queries, "limit": 5}, &want)
	for i, query := range queries {
		got := RankDocuments(query, docs, 5)
		if strings.Join(got, ",") != strings.Join(want[i], ",") {
			t.Errorf("%q: %v, upstream %v", query, got, want[i])
		}
	}
}

func TestCrosscheckCodingToolSpecs(t *testing.T) {
	var want map[string]struct {
		Description  string          `json:"description"`
		Parameters   json.RawMessage `json:"parameters"`
		OutputSchema json.RawMessage `json:"outputSchema"`
		OutputLimits *struct {
			Retain string `json:"retain"`
		} `json:"outputLimits"`
	}
	crosscheck.Run(t, "toolSpecs", nil, &want)
	for _, tool := range append(Coding().Tools, PowerShell().Tools...) {
		upstream, ok := want[tool.Name]
		if !ok {
			t.Errorf("upstream has no %s tool", tool.Name)
			continue
		}
		if tool.Description != upstream.Description {
			t.Errorf("%s description:\n got %q\nwant %q", tool.Name, tool.Description, upstream.Description)
		}
		if !crosscheck.Same(tool.Parameters, upstream.Parameters) {
			t.Errorf("%s parameters:\n got %s\nwant %s", tool.Name, tool.Parameters, upstream.Parameters)
		}
		output := json.RawMessage("null")
		if len(tool.StructuredOutputSchema) > 0 {
			output = tool.StructuredOutputSchema
		}
		if !crosscheck.Same(output, upstream.OutputSchema) {
			t.Errorf("%s output schema:\n got %s\nwant %s", tool.Name, output, upstream.OutputSchema)
		}
		retain := ""
		if tool.OutputLimits != nil {
			retain = tool.OutputLimits.Retain
		}
		wantRetain := ""
		if upstream.OutputLimits != nil {
			wantRetain = upstream.OutputLimits.Retain
		}
		if retain != wantRetain {
			t.Errorf("%s retains %q, upstream %q", tool.Name, retain, wantRetain)
		}
	}
}

func TestCrosscheckNumbers(t *testing.T) {
	sizes := []int{0, 1, 1023, 1024, 1280, 1536, 2304, 5376, 51200, 1048575, 1048576, 1153433, 1310720, 1572864, 123456789}
	fixed := [][2]float64{{1.125, 2}, {1.5, 2}, {2, 2}, {1.005, 2}, {0.125, 2}, {1.25, 1}, {0.05, 1}, {99.95, 1}}
	var want struct {
		Sizes []string `json:"sizes"`
		Fixed []string `json:"fixed"`
	}
	crosscheck.Run(t, "numbers", map[string]any{"sizes": sizes, "fixed": fixed}, &want)
	for i, n := range sizes {
		if got := formatSize(n); got != want.Sizes[i] {
			t.Errorf("formatSize(%d) = %s, upstream %s", n, got, want.Sizes[i])
		}
	}
	for i, c := range fixed {
		if got := jsToFixed(c[0], int(c[1])); got != want.Fixed[i] {
			t.Errorf("(%v).toFixed(%v) = %s, upstream %s", c[0], c[1], got, want.Fixed[i])
		}
	}
}
