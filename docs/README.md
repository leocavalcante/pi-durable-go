# pi-durable-go documentation

## Start here

- [Getting started](getting-started.md): a durable agent, step by step.
- [Concepts](concepts.md): the Harness, conversations, tasks, documents, and
  what durability promises.
- [Examples](../examples): runnable programs, including a
  [crash recovery](../examples/recovery/main.go) demo.

## Guides

- [Conversations and runs](guides/conversations.md): submissions, runs, live
  state, and tool calls.
- [Persistence and recovery](guides/persistence-and-recovery.md): storage,
  crashes, restart limits, and external effects.
- [Retries and compaction](guides/retries-and-compaction.md): failing models
  and full contexts.
- [Tasks, documents, and the runtime model](guides/tasks.md): durable work and
  application state.
- [Tools and models](guides/tools-and-models.md): `model.OpenAI`, `model.Pi`,
  the coding tools, and your own.
- [The execution environment](guides/execution-environment.md): files,
  commands, and watching, locally or your own way.
- [Code mode](guides/code-mode.md): model-written JavaScript that survives
  restarts.

## Reference

- [API reference](https://pkg.go.dev/github.com/leocavalcante/pi-durable-go)
  on pkg.go.dev.
- [Compatibility](../COMPATIBILITY.md): what differs from upstream Pi Durable.
- [Safety and limits](safety.md) and the [security policy](../SECURITY.md).
- [Upgrading](upgrading.md) and the [changelog](../CHANGELOG.md).
- [model/README.md](../model/README.md): the model adapters in detail.

## The project

- [Development](development.md): testing, CI, the catalog, and releases.
- [Upstream cross-checks](upstream-cross-checks.md): how the port is compared
  with upstream's own code.
- [Design records](plans/README.md).
- [Contributing](../CONTRIBUTING.md) and the
  [code of conduct](../CODE_OF_CONDUCT.md).
