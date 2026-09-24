# Changelog

## [Unreleased]

## [0.7.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-compiler-v0.6.0...flowlog-compiler-v0.7.0) - 2026-09-23

### Added

- *(planner)* share collections across rules by canonical form ([#374](https://github.com/flowlog-rs/flowlog/pull/374))
- *(planner)* describe every collection by a canonical form ([#373](https://github.com/flowlog-rs/flowlog/pull/373))

### Fixed

- *(compiler)* use direct local runtime dependencies for fixtures ([#357](https://github.com/flowlog-rs/flowlog/pull/357))
- *(runtime)* [**breaking**] emit zero for empty global count and sum ([#360](https://github.com/flowlog-rs/flowlog/pull/360))
- *(parser)* accept question mark prefixes in declarations ([#363](https://github.com/flowlog-rs/flowlog/pull/363))
- *(parser)* [**breaking**] avoid recursive backtracking in nested syntax ([#361](https://github.com/flowlog-rs/flowlog/pull/361))
- *(parser)* resolve top-level component-qualified types ([#359](https://github.com/flowlog-rs/flowlog/pull/359))
- *(planner)* merge atoms with equal variable sets before propagation ([#362](https://github.com/flowlog-rs/flowlog/pull/362))
- *(runtime)* [**breaking**] preserve whitespace in text string fields ([#364](https://github.com/flowlog-rs/flowlog/pull/364))

### Other

- [**breaking**] goodbye SIP ([#365](https://github.com/flowlog-rs/flowlog/pull/365))
- unify compiler and library releases with release-plz ([#358](https://github.com/flowlog-rs/flowlog/pull/358))
- *(runtime)* [**breaking**] unify dedup dispatch by diff and timestamp ([#353](https://github.com/flowlog-rs/flowlog/pull/353))
- *(planner)* share the preludes of earlier strata by canonical form ([#377](https://github.com/flowlog-rs/flowlog/pull/377))
- *(planner)* keep one fingerprint per collection and let the form share ([#376](https://github.com/flowlog-rs/flowlog/pull/376))
- *(planner)* fold semijoins once and push original filters down the plan ([#368](https://github.com/flowlog-rs/flowlog/pull/368))

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
