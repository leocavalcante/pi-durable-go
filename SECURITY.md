# Security

This is an experimental agent runtime. Model-generated instructions and commands
are untrusted. The local coding tools run with the permissions of the hosting
process. A working directory, tool allowlist, or prompt is not an OS security
boundary. Use a dedicated account, container or VM, narrowly scoped credentials,
and restricted network/filesystem access for untrusted tasks.

Do not disclose credentials, prompts, transcripts, database contents, or private
service URLs in a public issue. For a security-sensitive report, contact the
maintainer privately at `git@leocavalcante.dev` before public disclosure.

No version currently carries a production security-support guarantee.
