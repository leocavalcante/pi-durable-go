# Code mode

Code mode gives the model one tool, `codemode`, whose input is JavaScript. The
script calls the conversation's other tools as async functions, so the model
can loop, branch, and combine results in one turn instead of one tool call
per turn:

```js
const files = await tools.bash({ command: "ls src" });
for (const name of files.output.split("\n").filter((n) => n.endsWith(".go"))) {
  const text = await tools.read({ path: `src/${name}` });
  console.log(name, text.split("\n").length, "lines");
}
```

It is a port of upstream's `@earendil-works/pi-durable/codemode`, and it keeps
upstream's durability: a script survives a restart.

## Installing it

```go
sandbox := &codemode.QuickJSSandbox{} // or &codemode.NodeSandbox{}
defer sandbox.Close()
err := registry.Install(codemode.Extension(codemode.Options{
    Sandbox: sandbox,
    // Models gives scripts models.*: the catalog, classifiers, and image
    // generation; nil leaves models.* out.
    Models: model.Pi{},
}))
```

The extension adds the `codemode` tool and a prompt section that describes
the tools scripts can call, within an inline budget of tokens
(`Options.InlineBudget`). Scripts find the rest with `searchTools`,
`describeTool`, and `describeNamespace`; `Options.Namespaces` groups tools by
name prefix, such as an MCP server's.

## Durability

Every result a script receives is journaled before the script sees it. When a
crash interrupts a script, the next run replays it from the start in a fresh
VM, delivering the journaled results in their original order instead of
calling the tools again, until the script reaches the call that was in flight.
The VM is deterministic, as upstream's is: its clock and `Math.random` are
seeded, so a replayed script takes the same path. A script that takes another
path, or a VM whose fingerprint changed, ends with an error rather than doing
work twice.

## The sandboxes

- `codemode.QuickJSSandbox`, the default, runs pi-codemode's QuickJS VM
  (QuickJS-NG compiled to WebAssembly) in Go through wazero: no Node, no CGO.
  Scripts have no file system or network of their own; they reach the outside
  world only through tools.
- `codemode.NodeSandbox` runs pi-codemode itself from an installed Pi 1.5.2,
  with Node.js 22.19 or newer.

Both report the same fingerprint, so a script started in one resumes in the
other.

## Scripts

- `tools.<name>(args)` calls a tool and resolves with its result (its
  structured output when the tool has one); a failed call rejects.
- The script's `console.log` lines, the text and images it adds with `text()`
  and `image()`, and its return value make up the tool's result, within a
  token budget; longer text and the images are also saved to files in the
  environment's temporary directory, with their paths in the result.
- `store(key, value)` and `load(key)` keep values in the conversation, across
  scripts; a fork sees them as of its entry.
- A first line `// @options: {"max_output_tokens": 1000, "timeout_ms": 30000}`
  sets the script's limits.

[COMPATIBILITY.md](../../COMPATIBILITY.md#code-mode) lists where code mode
differs from upstream's.
