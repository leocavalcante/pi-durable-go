# Plan 0012: the execution environment's file system

Status: Done (M1–M4)

## Goal

Port the rest of upstream's `FileSystem` (`@earendil-works/pi-durable/env`) to
`tools.LocalEnv`, with the semantics and errors of upstream's
`NodeExecutionEnv` (`env/node.ts`). Upstream's coding tools use a part of it;
hosts and extensions build their own tools on all of it.

Go's `durable.ExecutionEnv` stays the small interface the Harness needs. The
file system is methods of `LocalEnv`, so a custom environment keeps working
and implements what it supports.

## Upstream behavior

- Failures are `FileError`s with a code (`aborted`, `not_found`,
  `permission_denied`, `not_directory`, `is_directory`, `invalid`,
  `not_supported`, `unknown`), Node's message, and the path. `toFileError`
  maps Node's codes: `ENOENT` → `not_found`, `EACCES` and `EPERM` →
  `permission_denied`, `ENOTDIR`, `EISDIR`, `EINVAL`, and anything else
  `unknown`.
- `FileInfo` is `{name, path, kind, size, mtimeMs}` from `lstat`; kinds other
  than file, directory, and symbolic link are `invalid` ("Unsupported file
  type"), and listings skip them.
- `listDir` lists in byte order (libuv sorts `scandir`); a directory reader
  pages entries in the file system's order and skips entries removed before
  their `lstat`.
- `readTextFile` decodes as Node's `utf8` does, a byte-order mark kept; text
  line readers decode as `StreamDecoder` (a leading mark dropped) and split at
  `\n` only.
- Binary readers read positionally from one opened regular file, refuse
  symbolic links with `noFollow`, and scan lines with `LineScanner`.
- Writes and appends create parent directories; truncation and flushing open
  the file read-write; renames report the source path.

## Milestones

1. M1: `FileError` and the plain methods: `ID`, `JoinPath`, `ReadTextFile`,
   `ReadBinaryFile`, `AppendFile`, `TruncateFile`, `FlushFile`, `RenameFile`,
   `FileInfo`, `ListDir`, `OpenDirReader`, `CanonicalPath`, `Exists`,
   `CreateDir`, `Remove`, and `Cleanup`, compared with `NodeExecutionEnv` by a
   new oracle op on the same files.
2. M2: `OpenTextLineReader`, `ReadTextLines`, and `OpenBinaryReader` with
   `Info`, `Read`, and `ScanLines` (a port of `LineScanner`).
3. M3: `Watch`, a port of `NodeFileWatcher`: snapshots compared after
   debounced native events, inotify on Linux, polling elsewhere. `listFiles`
   is optional, and `NodeExecutionEnv` does not implement it.
4. M4: `exec` with its options (`ExecWith`, `ExecArgvWith`, `ExecutionError`,
   `NewLocalEnvWith`), and the bash and powershell tools' `commandPrefix` and
   `prepare`, running through the environment's spill.

## Implementation record

- M1, M2: `FileError` with upstream's codes, `FileInfo`, the plain methods, directory, text-line, and binary readers, and `LineScanner`; the `envOps` oracle op compares about 90 operations with `NodeExecutionEnv`, all equal (recorded in `tools/testdata/env-ops-upstream.json`). Node's recursive `mkdir` and `rename(2)`'s errors are ported where Go's `MkdirAll` and `os.Rename` differ.
- M3: `Watch` with `WatchOptions` (`SetWatchOptions`), the snapshot watcher, inotify through `golang.org/x/sys/unix` on Linux with libuv's mask and reporting, polling elsewhere; upstream's nine watch conformance cases and eight reader cases are Go tests, the watch cases in both modes. macOS polls where upstream uses FSEvents.
- M4: `ExecWith` and `ExecArgvWith` with `ExecOptions` and `ShellExecResult`, upstream's shell choice and environment, Node's spawn errors, raw spills over both streams, and `ExecutionError`; `ShellToolOptions` for bash and powershell. The `envExec` oracle op compares 18 cases (recorded for `TestExecAsNodes`), and a coding scenario compares the bash tool's options. `Exec` and `ExecArgv` keep their API on top. The output window is not ported, as `NodeExecutionEnv` ignores it.
