# Changelog

## [Unreleased]

## [0.6.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-compiler-v0.5.0...flowlog-compiler-v0.6.0) - 2026-09-14

### Added

- SQLite input and output for compiled programs (#337).
- Shared Cargo target directories for generated projects (#331).
- Runtime input and output directory overrides (#330).
- Question-mark-prefixed variables (#329).

### Changed

- Generated projects depend on flowlog-runtime 0.4.0.
- Arithmetic evaluates `*`, `/`, and `%` before `+` and `-` (#333).
  For example, `1 + 2 * 3` now yields `7`; write `(1 + 2) * 3` to retain `9`.
- `.printsize` reports on stdout, and `-D -` routes relation output to
  stdout (#309). Scripts consuming output should account for these changes.
- Extended execution modes and loop blocks have been removed (#286).
