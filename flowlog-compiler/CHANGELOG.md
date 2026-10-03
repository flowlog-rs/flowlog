# Changelog

## [Unreleased]

## [0.8.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-compiler-v0.7.0...flowlog-compiler-v0.8.0) - 2026-10-03

### Added

- [**breaking**] give every collection the weight of its own mutability ([#393](https://github.com/flowlog-rs/flowlog/pull/393))
- define relation mutability and infer it per stratum ([#385](https://github.com/flowlog-rs/flowlog/pull/385))
- *(parser)* let a component inherit from several parents ([#383](https://github.com/flowlog-rs/flowlog/pull/383))
- bitwise operators and power in arithmetic expressions ([#381](https://github.com/flowlog-rs/flowlog/pull/381))
- *(parser)* hexadecimal literals and native-only `.plan` ([#379](https://github.com/flowlog-rs/flowlog/pull/379))
- *(planner)* give every planned collection a mutability ([#387](https://github.com/flowlog-rs/flowlog/pull/387))

### Other

- generate lint-clean code and allow only user-caused lints ([#402](https://github.com/flowlog-rs/flowlog/pull/402))
- *(codegen)* one fragment struct per side, and strata that compose their own head steps ([#401](https://github.com/flowlog-rs/flowlog/pull/401))
- split codegen into its own flowlog-codegen crate ([#398](https://github.com/flowlog-rs/flowlog/pull/398))
- codegen cleanups deferred from the mutability sweep ([#396](https://github.com/flowlog-rs/flowlog/pull/396))
- *(tests)* merge the batch and inc fixture directories ([#394](https://github.com/flowlog-rs/flowlog/pull/394))
- *(runtime)* [**breaking**] name update weights by relation class ([#354](https://github.com/flowlog-rs/flowlog/pull/354))

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
