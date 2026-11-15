// Package durable runs AI agents whose every step is durable: conversations,
// tool calls, and long-running tasks that survive crashes, restarts, and
// deploys. It is a Go port of upstream Pi Durable
// (https://github.com/earendil-works/pi/tree/v1.5.2/packages/durable).
//
// A [Harness] runs conversations over a [Storage], asking a [Model] for
// answers. Every change it makes, from a model's answer to a tool's result,
// is a commit on one serialized line, accepted by storage before any reader
// sees it. Opening the same storage again after a crash continues the work:
// tasks resume from their checkpoints, interrupted model requests are retried
// as failed attempts, and replay-safe tools run again.
//
// The pieces:
//
//   - [Open] opens a Harness; [Harness.Root] and [Harness.Conversation] give
//     a [Conversation], a durable transcript of [Entry] values.
//   - [Conversation.Submit] asks a conversation to act; the [Submission]
//     settles once the run it started has answered.
//   - Generations, tool calls, and compactions are durable tasks, and so is
//     the work an application defines ([TaskDefinition]).
//   - Documents ([DocumentDefinition], [ReadDocument], [UpdateDocument]) keep
//     application state, committed with the transcript.
//   - Extensions ([Extension], [Registry]) bundle tools ([ToolDefinition]),
//     prompt sections, hooks, and task definitions.
//
// The storage package has JSONL and SQLite storage, the model package the
// model adapters, the tools package the coding tools and the local execution
// environment, and the codemode package code mode.
//
// The documentation, with a getting-started guide, is at
// https://github.com/leocavalcante/pi-durable-go/tree/main/docs, and
// COMPATIBILITY.md there lists how the port differs from upstream.
package durable
