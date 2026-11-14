package codemode

import (
	"encoding/json"
	"math"
	"sort"
	"strings"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/js"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// What the model reads about code mode (upstream describe.ts): the tool's
// fixed description, and the codemode prompt section, which lists the tools
// scripts can call as TypeScript declarations, grouped by namespace and
// limited to a token budget.

// Namespace groups tools by a name prefix, such as an MCP server's
// "mcp__github__". The longest matching prefix wins.
type Namespace struct {
	Name         string
	Description  string
	Instructions string
	Prefix       string
}

// DefaultInlineBudget is the default token budget of the tool declarations
// in the codemode section.
const DefaultInlineBudget = 3000

// charsPerToken estimates the cost of a tool section.
const charsPerToken = 4

var textOutputSchema = json.RawMessage(`{"type":"string"}`)

const descriptionIntro = "Run JavaScript that calls other tools. The input is raw JavaScript (not JSON, no code fence), run as an async function body in a QuickJS sandbox: top-level `await` and `return` work. No Node, file system, network, or timers.\n" +
	"- `await tools.<name>({ ...args })` resolves to a string, or an object if the tool's declaration says so, and rejects with an Error on failure. A tool whose declaration returns an object resolves on failure too when it returned that object; check its fields. Calls still running when the script ends are cancelled.\n" +
	"- The script survives a restart: it resumes where it was, without running finished calls again.\n" +
	"- Optional first line: `// @options: {\"max_output_tokens\": 10000, \"timeout_ms\": 60000}`"

// Description is the code mode tool's description: the script environment
// and its globals, the same for every conversation.
func Description(models bool) string {
	globals := strings.Join([]string{
		"Globals:",
		"- `text(value)`, `image(dataUrlOrImageBlock)`, `console.log(...)`, and top-level `return` add output; `exit()` ends the script. With several text items, each starts with a `==> text N/M <==` line, and `console` lines follow the other output in one `<console_output>` block. `image()` also saves the image to a temp file and the result names its path.",
		"- `store(key, value)` and `load(key)` keep JSON values across codemode calls.",
		"- `ALL_TOOLS`, `await searchTools(query, { limit?, namespace? })`, `await describeTool(name)`, `await describeNamespace(name)`: find tools the `codemode` section does not list.",
	}, "\n")
	parts := []string{descriptionIntro, globals}
	if models {
		parts = append(parts, ModelsDescription)
	}
	parts = append(parts, "The `codemode` section of the system prompt lists the tools scripts can call, by namespace, as far as space allows.")
	return strings.Join(parts, "\n\n")
}

// declarationOf is what a script sees of a tool; tools without a structured
// output schema resolve to their text output.
func declarationOf(tool durable.ToolDefinition) ToolDeclaration {
	output := tool.StructuredOutputSchema
	if len(output) == 0 {
		output = textOutputSchema
	}
	var input json.RawMessage
	if len(tool.Parameters) > 0 {
		input = tool.Parameters
	}
	return ToolDeclaration{Name: tool.Name, Description: tool.Description, InputSchema: input, OutputSchema: output}
}

// sampleOf is a tool's description and TypeScript declaration.
func sampleOf(tool durable.ToolDefinition) string { return RenderToolSample(declarationOf(tool)) }

// namespaceOf is the namespace with the longest prefix name starts with.
func namespaceOf(name string, namespaces []Namespace) *Namespace {
	var found *Namespace
	for i := range namespaces {
		namespace := &namespaces[i]
		if strings.HasPrefix(name, namespace.Prefix) && (found == nil || len(namespace.Prefix) > len(found.Prefix)) {
			found = namespace
		}
	}
	return found
}

// renderToolSection is "### `id` (`raw name`)" followed by the tool's
// description and declaration.
func renderToolSection(tool durable.ToolDefinition) string {
	id := Identifier(tool.Name)
	heading := "### `" + id + "`"
	if id != tool.Name {
		heading += " (`" + tool.Name + "`)"
	}
	return heading + "\n" + js.Trim(sampleOf(tool))
}

type catalogEntry struct {
	name    string
	section string
	cost    int
}

type catalogGroup struct {
	namespace *Namespace
	entries   []catalogEntry
}

// selectCatalog picks the tool sections that fit the budget: in each round
// every group places its cheapest remaining tool; a group whose next tool
// does not fit drops out while the others continue.
func selectCatalog(groups []*catalogGroup, budget int) map[string]bool {
	var queues [][]catalogEntry
	for _, group := range groups {
		queue := append([]catalogEntry(nil), group.entries...)
		sort.SliceStable(queue, func(i, j int) bool { return queue[i].cost < queue[j].cost })
		if len(queue) > 0 {
			queues = append(queues, queue)
		}
	}
	shown := map[string]bool{}
	remaining := budget
	for len(queues) > 0 {
		var active [][]catalogEntry
		for _, queue := range queues {
			next := queue[0]
			if next.cost > remaining {
				continue
			}
			remaining -= next.cost
			shown[next.name] = true
			if queue = queue[1:]; len(queue) > 0 {
				active = append(active, queue)
			}
		}
		queues = active
	}
	return shown
}

// localeCompare orders strings as String.prototype.localeCompare does, by
// the root collation.
var localeCompare = collate.New(language.Und)

// Section is the codemode prompt section: the tools scripts can call,
// grouped by namespace, with their declarations as far as the budget allows.
// False without tools.
func Section(tools []durable.ToolDefinition, namespaces []Namespace, budget int) (string, bool) {
	if len(tools) == 0 {
		return "", false
	}
	groups := map[string]*catalogGroup{"": {}}
	order := []*catalogGroup{groups[""]}
	for _, tool := range tools {
		namespace := namespaceOf(tool.Name, namespaces)
		key := ""
		if namespace != nil {
			key = "ns:" + namespace.Name
		}
		group, ok := groups[key]
		if !ok {
			group = &catalogGroup{namespace: namespace}
			groups[key] = group
			order = append(order, group)
		}
		section := renderToolSection(tool)
		group.entries = append(group.entries, catalogEntry{name: tool.Name, section: section,
			cost: int(math.Ceil(float64(js.Len(section)) / charsPerToken))})
	}
	sort.SliceStable(order, func(i, j int) bool {
		a, b := order[i], order[j]
		if a.namespace == nil || b.namespace == nil {
			return a.namespace == nil && b.namespace != nil
		}
		return localeCompare.CompareString(a.namespace.Name, b.namespace.Name) < 0
	})
	shown := selectCatalog(order, budget)
	sections := []string{"Tools scripts can call (`await tools.<name>(args)`):"}
	for _, group := range order {
		if len(group.entries) == 0 {
			continue
		}
		var visible []catalogEntry
		for _, entry := range group.entries {
			if shown[entry.name] {
				visible = append(visible, entry)
			}
		}
		if group.namespace != nil {
			listing := ""
			switch {
			case len(visible) == len(group.entries):
			case len(visible) == 0:
				listing = " (tools not listed)"
			default:
				listing = " (some tools not listed)"
			}
			heading := "## " + group.namespace.Name + listing
			if description := js.Trim(group.namespace.Description); description != "" {
				heading += "\n" + description
			}
			sections = append(sections, heading)
		} else if len(visible) < len(group.entries) {
			sections = append(sections, "Some tools are not listed: find them with `searchTools()`.")
		}
		for _, entry := range visible {
			sections = append(sections, entry.section)
		}
	}
	return strings.Join(sections, "\n\n"), true
}
