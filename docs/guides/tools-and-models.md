# Tools and models

A Harness reaches models through the `durable.Model` interface and runs tools
as durable tasks. This repository brings two model adapters, the coding tools,
a local execution environment, and code mode; anything else plugs in through
the same interfaces.

## Models

### `model.OpenAI`: pure Go

`model.OpenAI` streams OpenAI-compatible Chat Completions with no Node and no
CGO. It follows pi-ai's openai-completions adapter: request fields, thinking
formats, the full `compat` (`OpenAICompat`), headers, message transforms,
OpenRouter's reasoning details, grammar-constrained tools, and usage priced as
pi-ai prices it.

It embeds pi-ai's catalog of the 728 models that adapter serves, so a model
from it is one call away:

```go
llm, ok := model.OpenAIModel("groq", "llama-3.3-70b-versatile")
if !ok {
    return errors.New("not in the catalog")
}
llm.APIKey = os.Getenv("GROQ_API_KEY")
h, err := durable.Open(ctx, store, durable.HarnessOptions{Model: llm, Registry: registry})
```

`OpenAIModel` sets the base URL, context window, output limit, reasoning and
thinking levels, compat, price, headers, and image limits as pi-ai defines
them; for every catalog model, the requests equal pi-ai's. `model.OpenAICatalog`
lists them. Any other OpenAI-compatible endpoint works with a hand-made
`model.OpenAI{BaseURL: ..., APIKey: ...}`.

### `model.Pi`: every pi-ai provider

`model.Pi` reuses an installed Pi coding-agent SDK's authentication and provider
transports, including existing `/login` OAuth credentials and refresh, so every
provider pi-ai supports is available. It needs Node.js 22.19+ and the tested
Pi SDK 1.5.2; no Pi agent or tool loop is started. See
[model/README.md](../../model/README.md) for setup, credential discovery, and limits.

### Your own model

Implement `durable.Model`:

```go
type Model interface {
    Generate(ctx context.Context, req ModelRequest, delta func(string) error) (ModelResponse, error)
}
```

Optional interfaces add what upstream's models report: `EventModel` streams
thinking and tool calls as they arrive, `ContextWindowModel` enables threshold
compaction, `ImageInputModel` and `ImageLimitsModel` describe image input,
`ModelLookup` says which models exist, and `DeferredModel` serves deferred
answers.

### Images and identity

Messages, inputs (`SubmissionRequest.Images`), and tool results carry images,
after the text or interleaved with it at `ImageOffsets` (`OrderedContent` gives
the ordered blocks); both adapters send them in order, and a model without
image input sees pi-ai's placeholders in place. `HarnessOptions.Identity`
(pi-ai's `ClientIdentity`) is merged over `DefaultHarnessIdentity` (Pi's
provider attribution; a JSON null `Attribution` turns it off) and set on every
model request, including those tasks, hooks, and tools make through
`TaskRuntime.Model`.

## Tools

### The coding tools

`tools.Coding()` supplies `read`, `write`, `edit`, and `bash` with upstream's
results: streaming reads with continuation notes and images, fuzzy edits with
diffs, and shell output with timeouts, spill files, and exit codes. Their
parameter schemas are upstream's, checked against 1.5.2. Tool arguments are
coerced and checked as pi-ai does (`"42"` for a number, a single value for an
array), after an optional `ToolDefinition.PrepareArguments` repair. Upstream's
file mutation queue is ported: `write` and `edit` calls on one file (by
canonical path, in a `LocalEnv`) run one at a time in arrival order. Other
files never wait, and it is no lock against `bash` or other processes.

- `tools.CodingWith(CodingOptions{Images: tools.StdImages()})` also resizes,
  rotates, and converts images; `CodingOptions.Bash` takes upstream's
  `CommandPrefix` and `Prepare` hook, which can change each command's script,
  working directory, and environment.
- `tools.PowerShell()` adds `powershell`.
- `tools.ToolSearch()` supplies `tool_search`, which loads deferred tools that
  match a query.

The tools run in the conversation's environment, which
`HarnessOptions.Environment` provides; see the
[execution environment](execution-environment.md) guide.

### Your own tools

A tool is a `durable.ToolDefinition` in an extension:

```go
registry := durable.NewRegistry()
err := registry.Install(durable.Extension{Name: "weather", Tools: []durable.ToolDefinition{{
    ToolSpec: durable.ToolSpec{
        Name:        "forecast",
        Description: "Gets tomorrow's forecast for a city.",
        Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
    },
    // Safe to run again when a crash interrupted it.
    ReplaySafe: true,
    Execute: func(ctx context.Context, args json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
        var in struct{ City string }
        if err := json.Unmarshal(args, &in); err != nil {
            return durable.ToolResult{}, err
        }
        return durable.ToolResult{Content: "Sunny in " + in.City}, nil
    },
}}})
```

A running tool streams progress with `ToolRuntime.Output`, reports
`Diagnostic`s and `Details`, commits its own records with `CommitChange`, and
calls other tools with `ExecuteTool`. `OutputLimits` bound its result,
`StructuredOutputSchema` gives programs a typed value, `Callers` limits who may
call it, and `Deferred` keeps it out of requests until a search loads it. The
[conversations guide](conversations.md#tool-calls) covers how calls end.

### Code mode

`codemode.Extension` adds upstream's `codemode` tool: the model writes
JavaScript that calls the conversation's tools, and the script survives a
restart by deterministic replay. See the [code mode](code-mode.md) guide.

## The chat example

The [chat example](../../examples/chat/main.go) wires these together with
SQLite and defaults to Pi authentication:

```sh
# First sign in using /login in Pi. Use a provider/model from Pi's /model picker.
go run ./examples/chat -provider openai-codex -model YOUR_PI_MODEL_ID -prompt 'Hello'

# Or the pure-Go adapter, configured from pi-ai's catalog:
GROQ_API_KEY=... go run ./examples/chat -adapter openai -provider groq \
  -model llama-3.3-70b-versatile -api-key-env GROQ_API_KEY -prompt 'Hello'

# Any OpenAI-compatible endpoint:
OPENAI_API_KEY=... go run ./examples/chat -adapter openai -model YOUR_MODEL_ID -prompt 'Hello'
```

It makes real model requests only when you run it. `-coding-tools` gives the
model `read`, `write`, `edit`, and `bash` in the current directory. Review tools
and isolate the process before giving it untrusted input or credentials. Pi
credential commands and the selected SDK execute with host permissions; they
are not sandboxed.
