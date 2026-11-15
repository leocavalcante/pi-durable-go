# Security policy

## Supported versions

pi-durable-go is pre-1.0. Security fixes go into the newest release; older
releases are not patched.

| Version | Supported |
|---|---|
| Latest release | Yes |
| Older releases | No |

## Reporting a vulnerability

Report vulnerabilities privately, never in a public issue:

- [open a private security advisory](https://github.com/leocavalcante/pi-durable-go/security/advisories/new) on GitHub, or
- email the maintainer at `git@leocavalcante.dev`.

Include what is affected, how to reproduce it, and its impact. Do not include
credentials, prompts, transcripts, database contents, or private service URLs
that are not needed to show the problem. You can expect an answer within a
week, and a coordinated disclosure once a fix is released.

## Threat model

This is an experimental agent runtime. Model-generated instructions and
commands are untrusted. The local coding tools run with the permissions of the
hosting process. A working directory, tool allowlist, or prompt is not an OS
security boundary. Use a dedicated account, container, or VM, narrowly scoped
credentials, and restricted network and filesystem access for untrusted tasks.

The optional `model.Pi` adapter executes a trusted installed Pi SDK and shares
its auth and model configuration. Credential commands run with host
permissions; set explicit package, Node, and agent-directory paths for
controlled deployments. Assistant-native replay data is sensitive transcript
content, not a place for credentials. Neither the bridge nor a Pi working
directory is a sandbox.

Code mode runs model-written JavaScript in a QuickJS WebAssembly sandbox
without filesystem or network access of its own; scripts reach the outside
world only through the tools the conversation offers them.

History and files can contain sensitive information. Do not publish runtime
databases or sessions.
