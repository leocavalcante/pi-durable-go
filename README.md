# pi-durable-go

An experimental Go port of [Pi Durable](https://github.com/earendil-works/pi/tree/main/packages/durable): persistent agent conversations, checkpointed tasks, transactional application state, and restart recovery.

**This is an independent project, not the Pi coding-agent CLI or a complete upstream-compatible replacement.** It was last compared against Pi Durable v1.3.0; the Go implementation supports a subset of its contracts. Read [COMPATIBILITY.md](COMPATIBILITY.md) before relying on it.

## Requirements

Go 1.24 or newer. SQLite uses a pure-Go driver; CGO is not required.

```sh
go get github.com/leocavalcante/pi-durable-go
```

## Quick start

```go
package main

import (
    "context"
    "fmt"

    durable "github.com/leocavalcante/pi-durable-go"
)

type Echo struct{}

func (Echo) Generate(ctx context.Context, req durable.ModelRequest,
    delta func(string) error) (durable.ModelResponse, error) {
    if err := ctx.Err(); err != nil {
        return durable.ModelResponse{}, err
    }
    return durable.ModelResponse{Content: "Hello from a durable agent"}, nil
}

func main() {
    ctx := context.Background()
    h, err := durable.Open(ctx, durable.NewMemoryStorage(),
        durable.HarnessOptions{Model: Echo{}})
    if err != nil { panic(err) }
    defer h.Close(ctx)

    root, err := h.Root(ctx, durable.ConversationOptions{
        Agent: durable.Agent{Model: durable.ModelRef{Provider: "example", ID: "echo"}},
    })
    if err != nil { panic(err) }
    submission, err := root.Submit(ctx, durable.SubmissionRequest{
        Content: "Hello", RequestID: "hello-1",
    })
    if err != nil { panic(err) }
    result, err := submission.Wait(ctx)
    if err != nil { panic(err) }
    if result.Status != "done" { panic(result.Reason) }
    state, err := h.Snapshot(ctx)
    if err != nil { panic(err) }
    fmt.Println(state.Entries[result.Answer].Message.Content)
}
```

The runnable [basic example](examples/basic/main.go) makes no network requests:

```sh
go run ./examples/basic
```

## Persistence and recovery

Replace memory storage with SQLite or JSONL:

```go
store, err := storage.OpenSQLite(ctx, "agent.sqlite")
// Or: storage.OpenJSONL(ctx, "agent-state")
if err != nil { return err }
h, err := durable.Open(ctx, store, options)
if err != nil { store.Close(); return err }
h.Resume()
```

Import `github.com/leocavalcante/pi-durable-go/storage`. Reinstall application extensions on each reopen; `Open` restores built-in definitions. `Resume`, submission, or a task/submission wait starts scheduling; opening alone does not.

A submission's `RequestID` deduplicates admission within one conversation. Retrying it returns the original even if the content changed; reusing it for another submission type is rejected. Persist `submission.ID()` for reconnection with `h.Submission(ctx, id)`. Inputs may use `Entry` to attach a custom non-`pi.*` kind and JSON data to the user message. Display-only write submissions settle immediately during a run unless a reset is queued.

**Admission deduplication is not exactly-once external execution.** A crash between an external effect and the commit recording its result leaves uncertainty. Use service-side idempotency keys, queryable receipts, or reconciliation. Interrupted tools replay only when both their stored and current policies declare `ReplaySafe`. Coding tools default to unsafe.

## Tools and models

- `tools.Coding()` supplies `read`, `write`, `edit`, and `bash`.
- `tools.NewLocalEnv(cwd)` supplies local filesystem and process execution. **A working directory is not a sandbox.**
- `model.Pi` reuses an installed Pi coding-agent SDK's authentication and provider transports, including existing `/login` OAuth credentials and refresh. Requires Node.js 22.19+ and the tested Pi SDK 1.5.2 baseline; no Pi agent/tool loop is started.
- `model.OpenAI` supplies a pure-Go streaming OpenAI-compatible Chat Completions adapter with an explicit API key.
- Implement `durable.Model` for other integrations. Multimodal APIs and a native Go provider/OAuth catalog are not implemented. See [model/README.md](model/README.md) for setup, credential discovery, and limits.

The [chat example](examples/chat/main.go) wires these together with SQLite and defaults to Pi authentication:

```sh
# First sign in using /login in Pi. Use a provider/model from Pi's /model picker.
go run ./examples/chat -provider openai-codex -model YOUR_PI_MODEL_ID -prompt 'Hello'
# Or keep the standalone pure-Go adapter:
OPENAI_API_KEY=... go run ./examples/chat -adapter openai -model YOUR_MODEL_ID -prompt 'Hello'
```

It makes real model requests only when you run it. Review tools and isolate the process before giving it untrusted input or credentials. Pi credential commands and the selected SDK execute with host permissions; they are not sandboxed.

## Runtime model

A Harness owns one Session and scheduler. A conversation is a transcript, not a goroutine. Entries, tasks, submissions, and JSON documents change on one serialized atomic commit line. Storage succeeds before snapshots or progress are published.

Tasks are checkpointed state machines, not persisted Go stacks. Define their initial checkpoint, phase handler, and optional abort/migration handlers. Task records include start/end times; assistant and tool-result messages include per-attempt durations. Owned child tasks and conversations support cancellation trees. `TaskRuntime.OwnedTasks(ctx)` lists live direct task children, including those from earlier invocations; `AbortOwned(ctx, id)` durably marks a direct child and waits for it to settle. These calls belong outside commit callbacks. Background work can outlive an ordinary conversation abort.

Set `TaskOptions.AbandonOnRestart` for work whose creator only waits in memory and cannot resume after a restart. A later Harness marks still-live opted-in tasks with `AbortReason: "restart"` at its first scheduling pass, cascading to ordinary owned work before any run handler can replay. Inspection-only opens do not mark them, and newly created tasks in the current Harness are unaffected. Abort handlers can use the reason for cleanup; missing/incompatible definitions remain blocked until installed. An explicit abort request replaces a restart reason, while `AbortOwned` preserves it. Neither abandonment nor admission deduplication makes external effects exactly once.

Application documents have explicit versions, scopes, and fork policies. Use `UpdateDocument[T]` inside a commit and `ReadDocument[T]` outside it. Reads do not initialize absent state. Rewindable `asOf` documents fork at the entry's entire commit.

`Conversation.Watch` and `WatchDocument` deliver current snapshots with bounded latest-state buffering. They are not audit logs. Retain important facts as immutable entries.

## Safety and limits

- One owning Session per storage. Database transaction safety does not coordinate multiple schedulers.
- SQLite uses WAL and `synchronous=FULL`; JSONL fsyncs complete records. Initial file/directory creation is not directory-fsynced, and guarantees depend on the OS/filesystem/device. Do not assume full power-loss durability.
- Storage currently serializes whole snapshots. This is simple but unsuitable for very large transcripts or high-frequency progress without further work.
- An uncertain storage error poisons and automatically closes the Session: observers end, pending waits fail, and running invocations are signalled. The failing call returns the original error; later operations return `SessionFailedError`, matching `ErrPoisoned` and unwrapping to the first cause. Never continue mutating it. Reopen only after shutdown settles.
- `Done()` and `WaitClosed(ctx)` on a Harness or Session expose shutdown completion (`SessionEnd{Reason: "closed"}` or `"failed"` with `Err`). `Err()` reports sealed admission/failure immediately. Waiting does not initiate shutdown; cancelling a wait does not cancel cleanup.
- `Close` stops local invocations without durably cancelling pending work. `AbortTask` requests persisted cancellation. Cancellation is cooperative: task code that ignores it keeps shutdown pending and storage open. A timed-out `Close(ctx)` seals admission and continues cleanup; call again or use `WaitClosed` to join it.
- Never make network/model calls, wait for approval, or nest a commit inside a commit callback.
- History and files can contain sensitive information. Do not publish runtime databases or sessions.

## Development

```sh
make check
```

Runs formatting checks, `go vet`, unit/integration tests, and the race detector. Tests use fake models, temporary files, and local HTTP servers. No provider credentials or paid calls are required. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License and provenance

[MIT](LICENSE), the same license as Pi and Pi Durable. The upstream copyright notice is preserved. See [NOTICE](NOTICE) for the exact source revision and [DEPENDENCIES.md](DEPENDENCIES.md) for dependency licenses.
