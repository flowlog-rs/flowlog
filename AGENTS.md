# FlowLog Contributor Guide

Guidance for contributors and AI coding agents working on FlowLog.

## Writing code

Read the guide for the area you are changing:

- `docs/dev/comments.md`: comment and rustdoc style. Read it before writing
  or editing comments, docs, or any prose inside source files.
- `docs/dev/code.md`: code-shaping conventions (Display vs Debug, exhaustive
  matches, when a helper earns existence).
- `docs/dev/testing.md`: unit test style. Read it before writing or editing
  tests.
- `docs/dev/errors.md`: error handling conventions. Read it before adding
  error paths, panics, or assertions.

## Running tests

`tests/README.md` covers the local correctness suites (unit tests, fixture
diffs, and the Souffle oracle) and how to run each. Read it before running or
adding end-to-end tests.

## CI

Every change must pass CI before it can merge:

- **DCO**: sign off each commit (`git commit -s`, which adds a
  `Signed-off-by:` line).
- **rustfmt**: `cargo +nightly fmt --all --check` (formatting runs on nightly).
- **clippy**: `cargo clippy --workspace --all-targets -- -D warnings`.
- **tests**: `cargo test` plus the end-to-end fixture suite (see
  `tests/README.md`).
- Also gated: `cargo-deny` (licenses and advisories), `typos` (spelling), and
  `taplo` (TOML format and lint).
