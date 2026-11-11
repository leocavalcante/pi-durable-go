package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode"

	durable "github.com/leocavalcante/pi-durable-go"
	"golang.org/x/text/unicode/norm"
)

// ToolSearchName is the tool_search tool's name.
const ToolSearchName = "tool_search"

// DefaultToolSearchLimit is how many tools a search loads at most by default.
const DefaultToolSearchLimit = 8

const toolSearchDescription = "# Tool discovery\n\nSearches over deferred tool metadata with BM25 and exposes matching tools for the next model call.\n\nSome of the tools, such as tools of MCP servers, may not have been provided to you upfront, and you should use this tool (`tool_search`) to search for the required tools. For MCP tool discovery, always use `tool_search`."

// ToolSearch is upstream's tool_search extension: the model searches the
// deferred tools it is not offered yet, and the matches are loaded through the
// result's AddTools, so the next request offers them. Install it and select it
// where the model should find deferred tools.
func ToolSearch() durable.Extension {
	return durable.Extension{Name: "tool-search", Tools: []durable.ToolDefinition{{
		ToolSpec: durable.ToolSpec{Name: ToolSearchName, Description: toolSearchDescription,
			Parameters: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","description":"Search query for deferred tools."},"limit":{"type":"number","description":"Maximum number of tools to return. Defaults to 8."}},"required":["query"]}`)},
		// Searching changes what the model is offered.
		Callers:    []string{"model"},
		ReplaySafe: true,
		Execute: func(ctx context.Context, raw json.RawMessage, r *durable.ToolRuntime) (durable.ToolResult, error) {
			var args struct {
				Query string   `json:"query"`
				Limit *float64 `json:"limit"`
			}
			if err := json.Unmarshal(raw, &args); err != nil {
				return durable.ToolResult{}, err
			}
			if strings.TrimSpace(args.Query) == "" {
				return durable.ToolResult{}, errors.New("query must not be empty")
			}
			limit := DefaultToolSearchLimit
			if args.Limit != nil {
				if *args.Limit <= 0 || *args.Limit != math.Trunc(*args.Limit) {
					return durable.ToolResult{}, errors.New("limit must be a positive integer")
				}
				limit = int(*args.Limit)
			}
			candidates, err := r.DeferredTools(ctx)
			if err != nil {
				return durable.ToolResult{}, err
			}
			documents := make([]SearchDocument, len(candidates))
			descriptions := map[string]string{}
			for i, tool := range candidates {
				documents[i] = SearchDocument{Name: tool.Name, Text: searchText(tool)}
				descriptions[tool.Name] = tool.Description
			}
			loaded := RankDocuments(args.Query, documents, limit)
			text := "No matching tools found."
			if len(loaded) > 0 {
				plural := "s"
				if len(loaded) == 1 {
					plural = ""
				}
				lines := []string{fmt.Sprintf("Loaded %d tool%s. They are available from your next call:", len(loaded), plural)}
				for _, name := range loaded {
					first := strings.SplitN(strings.TrimSpace(descriptions[name]), "\n", 2)[0]
					lines = append(lines, fmt.Sprintf("- %s: %s", name, strings.TrimSuffix(first, "\r")))
				}
				text = strings.Join(lines, "\n")
			}
			details, _ := json.Marshal(map[string][]string{"loaded": append([]string{}, loaded...)})
			return durable.ToolResult{Content: text, Details: details, AddTools: loaded}, nil
		},
	}}}
}

// searchText is a tool's description and its parameters' names and
// descriptions; the ranker reads the name separately.
func searchText(tool durable.ToolDefinition) string {
	parts := []string{tool.Description}
	var schema any
	_ = json.Unmarshal(tool.Parameters, &schema)
	schemaText(schema, schema, &parts, map[any]bool{})
	var kept []string
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			kept = append(kept, part)
		}
	}
	return strings.Join(kept, " ")
}

func schemaText(schema, root any, parts *[]string, seen map[any]bool) {
	object, ok := schema.(map[string]any)
	if !ok {
		return
	}
	if ref, ok := object["$ref"].(string); ok && strings.HasPrefix(ref, "#/") {
		target := root
		for _, segment := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
			if m, ok := target.(map[string]any); ok {
				target = m[strings.ReplaceAll(strings.ReplaceAll(segment, "~1", "/"), "~0", "~")]
			} else {
				target = nil
			}
		}
		if m, ok := target.(map[string]any); ok && !seen[fmt.Sprintf("%p", m)] {
			seen[fmt.Sprintf("%p", m)] = true
			schemaText(m, root, parts, seen)
		}
	}
	if description, ok := object["description"].(string); ok {
		*parts = append(*parts, description)
	}
	if properties, ok := object["properties"].(map[string]any); ok {
		names := make([]string, 0, len(properties))
		for name := range properties {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			*parts = append(*parts, name)
			schemaText(properties[name], root, parts, seen)
		}
	}
	schemaText(object["items"], root, parts, seen)
	for _, key := range []string{"anyOf", "oneOf", "allOf"} {
		if variants, ok := object[key].([]any); ok {
			for _, variant := range variants {
				schemaText(variant, root, parts, seen)
			}
		}
	}
}

// SearchDocument is a tool as the ranker sees it.
type SearchDocument struct {
	Name          string
	Text          string // description and parameter names and descriptions
	NamespaceText string
}

const (
	bm25K1          = 1.2
	bm25B           = 0.75
	nameWeight      = 3
	namespaceWeight = 0.5
)

// RankDocuments ports upstream's Okapi BM25 tool ranking (k1 1.2, b 0.75)
// with the name and namespace as separate fields: tools matching more query
// terms rank first, a term in the name outweighs any number of mentions, a
// whole local name in the query earns a bonus, and a query that is exactly a
// tool's name ranks it first. Ties keep document order.
func RankDocuments(query string, documents []SearchDocument, limit int) []string {
	queryTerms := uniqueTerms(tokenize(query))
	if len(queryTerms) == 0 || len(documents) == 0 || limit <= 0 {
		return nil
	}
	type indexed struct {
		name                 string
		nameTerms, namespace map[string]bool
		local                []string
		counts               map[string]int
		length               int
	}
	docs := make([]indexed, len(documents))
	total := 0
	for i, document := range documents {
		counts := map[string]int{}
		length := 0
		for _, term := range tokenize(document.Text) {
			counts[term]++
			length++
		}
		segments := strings.Split(document.Name, "__")
		docs[i] = indexed{name: document.Name, nameTerms: set(tokenize(document.Name)), namespace: set(tokenize(document.NamespaceText)),
			local: uniqueTerms(tokenize(segments[len(segments)-1])), counts: counts, length: length}
		total += length
	}
	average := float64(total) / float64(len(docs))
	if average == 0 {
		average = 1
	}
	idf := map[string]float64{}
	bonus := 0.0
	for _, term := range queryTerms {
		frequency := 0
		for _, doc := range docs {
			if doc.nameTerms[term] || doc.counts[term] > 0 || doc.namespace[term] {
				frequency++
			}
		}
		idf[term] = math.Log(1 + (float64(len(docs)-frequency)+0.5)/(float64(frequency)+0.5))
		bonus += idf[term]
	}
	exactQuery := normalizeName(query)
	type match struct {
		name    string
		score   float64
		covered int
		exact   bool
		order   int
	}
	var matches []match
	queried := set(queryTerms)
	for order, doc := range docs {
		normalization := bm25K1 * (1 - bm25B + bm25B*float64(doc.length)/average)
		score, covered := 0.0, 0
		for _, term := range queryTerms {
			count := float64(doc.counts[term])
			weight := count * (bm25K1 + 1) / (count + normalization)
			if doc.nameTerms[term] {
				weight += nameWeight
			}
			if doc.namespace[term] {
				weight += namespaceWeight
			}
			if weight > 0 {
				covered++
			}
			score += idf[term] * weight
		}
		if score == 0 {
			continue
		}
		all := len(doc.local) > 0
		for _, term := range doc.local {
			all = all && queried[term]
		}
		if all {
			score += bonus
		}
		normalized := normalizeName(doc.name)
		matches = append(matches, match{name: doc.name, score: score, covered: covered, order: order,
			exact: normalized == exactQuery || strings.HasSuffix(normalized, "__"+exactQuery)})
	}
	sort.SliceStable(matches, func(i, j int) bool {
		a, b := matches[i], matches[j]
		if a.exact != b.exact {
			return a.exact
		}
		if a.covered != b.covered {
			return a.covered > b.covered
		}
		return a.score > b.score
	})
	var out []string
	for i := 0; i < len(matches) && i < limit; i++ {
		out = append(out, matches[i].name)
	}
	return out
}

var stopWords = set([]string{"a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "in", "is", "it", "of", "on", "or", "that", "the", "this", "to", "with"})

var (
	lowerUpper = regexp.MustCompile(`([a-z0-9])([A-Z])`)
	upperRun   = regexp.MustCompile(`([A-Z]+)([A-Z][a-z])`)
	asciiWord  = regexp.MustCompile(`^[a-z0-9]+$`)
	plural     = regexp.MustCompile(`(ches|shes|sses|xes|zes)$`)
)

// tokenize splits identifiers and words, lowercases, drops English stop
// words, and singularizes ASCII words. Text in scripts without spaces, such
// as Chinese, is split per character, approximating Intl.Segmenter.
func tokenize(text string) []string {
	text = norm.NFC.String(text)
	text = lowerUpper.ReplaceAllString(text, "$1 $2")
	text = upperRun.ReplaceAllString(text, "$1 $2")
	text = strings.ToLower(text)
	var words []string
	var word strings.Builder
	flush := func() {
		if word.Len() > 0 {
			words = append(words, word.String())
			word.Reset()
		}
	}
	for _, r := range text {
		switch {
		case unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Thai):
			flush()
			words = append(words, string(r))
		case unicode.IsLetter(r) || unicode.IsMark(r) || unicode.IsNumber(r):
			word.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	var out []string
	for _, w := range words {
		if stopWords[w] {
			continue
		}
		if asciiWord.MatchString(w) {
			w = stem(w)
		}
		out = append(out, w)
	}
	return out
}

func stem(term string) string {
	switch {
	case len(term) > 4 && strings.HasSuffix(term, "ies"):
		return term[:len(term)-3] + "y"
	case len(term) > 4 && plural.MatchString(term):
		return term[:len(term)-2]
	case len(term) > 3 && strings.HasSuffix(term, "s") && !strings.HasSuffix(term, "ss"):
		return term[:len(term)-1]
	}
	return term
}

func normalizeName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '$' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func set(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, item := range items {
		out[item] = true
	}
	return out
}

func uniqueTerms(items []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, item := range items {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}
