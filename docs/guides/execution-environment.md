# The execution environment

Tools that touch files and processes reach them through an execution
environment, which the Harness asks for per conversation:

```go
h, err := durable.Open(ctx, store, durable.HarnessOptions{
    Model:    llm,
    Registry: registry,
    Environment: func(c durable.ConversationRecord) (durable.ExecutionEnv, error) {
        return tools.NewLocalEnv(c.Agent.Cwd) // the agent's working directory
    },
})
```

`tools.LocalEnv` is the local one. It ports upstream's `NodeExecutionEnv`: the
same results, and the same errors, Node's messages included, so tools and
models see what they would see upstream.

> **A working directory is not a sandbox.** Absolute paths, `..`, symbolic
> links, and shell commands reach anything the process may reach. Isolate the
> process (a container, a VM, a dedicated account) before running untrusted
> work. See [safety](../safety.md).

## Options

```go
env, err := tools.NewLocalEnvWith(dir, tools.LocalEnvOptions{
    ShellPath: "/bin/bash",                      // default: /bin/bash, bash on PATH, else sh
    ShellEnv:  map[string]string{"CI": "1"},     // added to every command's environment
    Watch:     tools.WatchOptions{Mode: "polling"},
})
```

## Files

Paths resolve as upstream resolves them: relative to the working directory,
with `~`, `~/`, and `file://` expanded. Writes go in place, as Node's
`fs.writeFile` does (through symbolic links, keeping inodes and modes; not
atomic), creating directories as needed.

| Method | Upstream |
|---|---|
| `ReadFile`, `ReadBinaryFile`, `ReadTextFile`, `ReadTextLines`, `OpenTextLineReader` | `readBinaryFile`, `readTextFile`, `readTextLines`, `openTextLineReader` |
| `OpenBinaryReader` (`Info`, `Read`, `ScanLines`) | `openBinaryReader` |
| `WriteFile`, `AppendFile`, `TruncateFile`, `FlushFile`, `RenameFile` | the same |
| `FileInfo`, `ListDir`, `OpenDirReader`, `Exists`, `CanonicalPath` | the same |
| `CreateDir`, `Remove`, `CreateTempDir`, `CreateTempFile` | the same |
| `AbsolutePath`, `JoinPath`, `ID` | the same |

Failures are `*tools.FileError`s with upstream's codes (`not_found`,
`permission_denied`, `not_directory`, `is_directory`, `invalid`, `aborted`,
`unknown`) and Node's messages, such as
`ENOENT: no such file or directory, open '/work/missing.txt'`. They wrap the
original error, so `errors.Is(err, fs.ErrNotExist)` still works. An operation
whose context has ended fails as `aborted` without effects.

## Commands

`Exec` runs a command through the shell and streams its output;
`ExecWith` and `ExecArgvWith` take upstream's options:

```go
result, err := env.ExecWith(ctx, "go test ./...", tools.ExecOptions{
    Cwd:     "service",                        // relative to the environment's
    Env:     map[string]string{"GOFLAGS": "-count=1"},
    Timeout: 120,                              // seconds
    Spill:   &tools.SpillOptions{AfterBytes: 50 * 1024, AfterLines: 2000},
    OnOutput: func(text string, info tools.OutputInfo) error {
        fmt.Print(text) // info.Stream is "stdout" or "stderr"
        return nil
    },
})
```

`ShellExecResult` carries the exit code (128 plus the signal for a killed
process) and, when output passed the spill thresholds, the file with all of it.
A command that cannot finish fails with an `*tools.ExecutionError`: `timeout`,
`aborted`, `spawn_error` (Node's `spawn bash ENOENT`), `shell_unavailable`,
`callback_error`, or `unknown`. Each output stream is decoded as UTF-8 on its
own, so a character split between reads survives. `Cleanup` ends the commands
still running, with their process groups.

## Watching files

`Watch` reports changes to files and directories, for hosts that load
resources such as `AGENTS.md` or skills from the environment:

```go
watcher, err := env.Watch(ctx, []tools.WatchTarget{
    {Path: "AGENTS.md"},
    {Path: ".pi/skills", Recursive: true, ExcludeHidden: true},
}, func(change tools.FileChange) {
    switch change.Kind {
    case "paths":    // something at or below each of change.Paths changed
    case "overflow": // coverage was uncertain: reload everything
    case "failed":   // the watcher stopped with change.Err
    }
})
defer watcher.Stop(ctx)
```

It is upstream's snapshot watcher: native events only trigger a rescan, and
changes are the difference between snapshots, so replaced files, renamed or
recreated parents, and directories created with their contents are all seen.
Linux watches natively with inotify; other systems, and network or FUSE file
systems, poll (`WatchOptions.PollInterval`, default two seconds).

## Your own environment

Any type with `ReadFile`, `WriteFile`, and `Exec` is a `durable.ExecutionEnv`.
Optional interfaces unlock more of the tools: `tools.PathResolver` (path
variants for `read`), `tools.FileOpener` (streamed reads of large files),
`tools.ArgvExecutor` (the `powershell` tool), `tools.ShellRunner` (the shell
tools' options and spills), and `durable.TempDirEnv` (code mode's output
files). A remote machine, a container, or an in-memory file system for tests
all fit.
