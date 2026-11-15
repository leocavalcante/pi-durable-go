# Getting started

This walk-through builds a durable agent step by step: a first conversation,
a real model, persistent storage, tools, and recovery from a crash.

## Install

You need Go 1.26 or newer. Nothing else: SQLite storage is pure Go, and the
OpenAI-compatible model adapter needs no Node.

```sh
go get github.com/leocavalcante/pi-durable-go
```

## A first conversation

A **Harness** runs conversations over a **storage**, asking a **model** for
answers. This one uses memory storage and a model that echoes, so it runs
anywhere, with no keys:

```go
package main

import (
    "context"
    "fmt"
    "log"

    durable "github.com/leocavalcante/pi-durable-go"
)

type echo struct{}

func (echo) Generate(ctx context.Context, req durable.ModelRequest, _ func(string) error) (durable.ModelResponse, error) {
    last := req.Messages[len(req.Messages)-1]
    return durable.ModelResponse{Content: "Echo: " + last.Content}, nil
}

func main() {
    ctx := context.Background()
    h, err := durable.Open(ctx, durable.NewMemoryStorage(), durable.HarnessOptions{Model: echo{}})
    if err != nil {
        log.Fatal(err)
    }
    defer h.Close(ctx)

    root, err := h.Root(ctx, durable.ConversationOptions{
        Agent: durable.Agent{Model: durable.ModelRef{Provider: "example", ID: "echo"}},
    })
    if err != nil {
        log.Fatal(err)
    }
    submission, err := root.Submit(ctx, durable.SubmissionRequest{Content: "Hello"})
    if err != nil {
        log.Fatal(err)
    }
    result, err := submission.Wait(ctx)
    if err != nil {
        log.Fatal(err)
    }
    state, err := h.Snapshot(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(state.Entries[result.Answer].Message.Content) // Echo: Hello
}
```

The same program is in [examples/basic](../examples/basic/main.go):
`go run ./examples/basic`.

Every conversation is a transcript of **entries**: the user's input, the
system prompt and tools, the model's answers, and tool results. A
**submission** asks the conversation to do something, and settles once the run
it started has answered.

## A real model

`model.OpenAI` speaks OpenAI-compatible Chat Completions in pure Go, and knows
pi-ai's catalog of 728 models:

```go
llm, ok := model.OpenAIModel("groq", "llama-3.3-70b-versatile")
if !ok {
    log.Fatal("not in the catalog")
}
llm.APIKey = os.Getenv("GROQ_API_KEY")

h, err := durable.Open(ctx, store, durable.HarnessOptions{Model: llm})
root, err := h.Root(ctx, durable.ConversationOptions{
    Agent: durable.Agent{Model: durable.ModelRef{Provider: "groq", ID: "llama-3.3-70b-versatile"}},
})
```

With Pi installed, `model.Pi{}` instead reaches every provider pi-ai supports,
with the credentials of Pi's `/login`. See
[tools and models](guides/tools-and-models.md).

## Storage that outlives the process

Swap memory storage for SQLite (or JSONL), and call `Resume` once the
extensions are installed, so work an earlier process left continues:

```go
store, err := storage.OpenSQLite(ctx, "agent.sqlite")
if err != nil {
    log.Fatal(err)
}
h, err := durable.Open(ctx, store, options)
if err != nil {
    store.Close()
    log.Fatal(err)
}
h.Resume()
```

Every change is committed before anyone sees it: the transcript, the tasks
that make up a run, and your own documents. See
[persistence and recovery](guides/persistence-and-recovery.md).

## Tools

Tools are installed as extensions in a registry. The coding tools give the
model `read`, `write`, `edit`, and `bash` in an execution environment:

```go
registry := durable.NewRegistry()
if err := registry.Install(tools.Coding()); err != nil {
    log.Fatal(err)
}
h, err := durable.Open(ctx, store, durable.HarnessOptions{
    Model:    llm,
    Registry: registry,
    Environment: func(c durable.ConversationRecord) (durable.ExecutionEnv, error) {
        return tools.NewLocalEnv(c.Agent.Cwd)
    },
})
```

`LocalEnv` is no sandbox: run untrusted work in a container or VM. Your own
tools are `durable.ToolDefinition`s; see
[tools and models](guides/tools-and-models.md#your-own-tools).

## Surviving a crash

[examples/recovery](../examples/recovery/main.go) shows the point of it all.
A job of three tool calls dies in the middle of the second; the next run opens
the same database, runs step 2 again, and finishes:

```console
$ go run ./examples/recovery -crash
  step 1: running
  step 1: done
  step 2: running
  step 2: the process dies here
$ go run ./examples/recovery
  step 2: running
  step 2: done
  step 3: running
  step 3: done
job 2: done
```

A tool marked `ReplaySafe` runs again after a crash; any other tool's
interrupted call ends with an error result the model sees, because its effects
are unknown.

## Following a run

A conversation's `pi.live` document shows its run as it goes: the generation
attempt, the streamed partial answer, and each tool call's output.
`Conversation.Live` reads it, and `Conversation.WatchWindow` streams a window
of the transcript with it, for user interfaces. See
[conversations and runs](guides/conversations.md).

## Next

- [Concepts](concepts.md): how the pieces fit together.
- The [guides](README.md#guides), for each part in depth.
- [COMPATIBILITY.md](../COMPATIBILITY.md): how the port differs from upstream.
