# Security

This is an experimental agent runtime. Model-generated instructions and commands
are untrusted. The local coding tools run with the permissions of the hosting
process. A working directory, tool allowlist, or prompt is not an OS security
boundary. Use a dedicated account, container or VM, narrowly scoped credentials,
and restricted network/filesystem access for untrusted tasks.

The optional `model.Pi` adapter executes a trusted installed Pi SDK and shares
its auth/model configuration. Credential commands run with host permissions;
set explicit package, Node, and agent-directory paths for controlled deployments.
Assistant-native replay data is sensitive transcript content, not a place for
credentials. Neither the bridge nor a Pi working directory is a sandbox.

Do not disclose credentials, prompts, transcripts, database contents, or private
service URLs in a public issue. For a security-sensitive report, contact the
maintainer privately at `git@leocavalcante.dev` before public disclosure.

No version currently carries a production security-support guarantee.
