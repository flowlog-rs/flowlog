# Changelog

## [Unreleased]

## [0.9.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-compiler-v0.8.0...flowlog-compiler-v0.9.0) - 2026-10-31

### Added

- *(planner)* order joins along a join tree and avoid cross products ([#448](https://github.com/flowlog-rs/flowlog/pull/448))

### Fixed

- *(compiler)* name the crate after its build directory too ([#508](https://github.com/flowlog-rs/flowlog/pull/508))
- build a program with no input, or with every output pruned ([#484](https://github.com/flowlog-rs/flowlog/pull/484))
- define integer arithmetic in the runtime, allow type-limit comparisons ([#483](https://github.com/flowlog-rs/flowlog/pull/483))
- *(runtime)* [**breaking**] refuse bad input and return load errors instead of skipping ([#454](https://github.com/flowlog-rs/flowlog/pull/454))
- *(codegen)* spell out a feedback variable's row type ([#506](https://github.com/flowlog-rs/flowlog/pull/506))
- *(codegen)* cast a compound substr index in one pair of parentheses ([#500](https://github.com/flowlog-rs/flowlog/pull/500))
- merge `min<string>` and `max<string>` columns by their text ([#485](https://github.com/flowlog-rs/flowlog/pull/485))
- *(codegen)* test an atom's constants before the rule's comparisons ([#481](https://github.com/flowlog-rs/flowlog/pull/481))
- give a literal in `as()` its target's width ([#487](https://github.com/flowlog-rs/flowlog/pull/487))
- *(parser)* reject a rule body with no positive atom or aggregate ([#516](https://github.com/flowlog-rs/flowlog/pull/516))
- *(runtime)* keep `1 ^ e` and `-1 ^ e` exact for a negative exponent ([#480](https://github.com/flowlog-rs/flowlog/pull/480))
- *(parser)* pin literals inside `match` and `contains` operands ([#477](https://github.com/flowlog-rs/flowlog/pull/477))
- *(parser)* check every `as()` operand against its target's root ([#492](https://github.com/flowlog-rs/flowlog/pull/492))
- *(parser)* reject body atoms whose arity disagrees with their .decl ([#452](https://github.com/flowlog-rs/flowlog/pull/452))
- *(planner)* emit a stratum's relations and entered collections in a fixed order ([#510](https://github.com/flowlog-rs/flowlog/pull/510))
- *(planner)* hold a collection only maps read as rows ([#488](https://github.com/flowlog-rs/flowlog/pull/488))
- *(planner)* keep a shared collection keyed while an aggregate reads it ([#479](https://github.com/flowlog-rs/flowlog/pull/479))
- *(runtime)* never project the rows a negation removes ([#482](https://github.com/flowlog-rs/flowlog/pull/482))
- *(runtime)* accept aggregate matches arranged by the key alone ([#478](https://github.com/flowlog-rs/flowlog/pull/478))
- *(runtime)* wrap `^` exactly for an exponent past `u32` ([#489](https://github.com/flowlog-rs/flowlog/pull/489))
- *(runtime)* skip keys interned while the output snapshot is built ([#493](https://github.com/flowlog-rs/flowlog/pull/493))
- *(deps)* update rust crate rusqlite to 0.41 ([#461](https://github.com/flowlog-rs/flowlog/pull/461))

### Other

- *(runtime)* replace lasso with an in-crate string table ([#501](https://github.com/flowlog-rs/flowlog/pull/501))
- *(planner)* generate logical dedup/aggregation on head in planning phase ([#502](https://github.com/flowlog-rs/flowlog/pull/502))
- dual-license under MIT OR Apache-2.0 ([#451](https://github.com/flowlog-rs/flowlog/pull/451))
- *(parser)* [**breaking**] make `_` in a tuple an anonymous variable ([#458](https://github.com/flowlog-rs/flowlog/pull/458))
- *(parser)* [**breaking**] give rule variables a type instead of a String ([#456](https://github.com/flowlog-rs/flowlog/pull/456))
- *(runtime)* commit SQLite output without waiting for the disk ([#519](https://github.com/flowlog-rs/flowlog/pull/519))
- *(runtime)* merge ordered outputs with a loser tree and sort their batches in parallel ([#517](https://github.com/flowlog-rs/flowlog/pull/517))
- *(runtime)* cache interner lookups per thread ([#499](https://github.com/flowlog-rs/flowlog/pull/499))

## [0.8.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-compiler-v0.7.0...flowlog-compiler-v0.8.0) - 2026-10-19

### Added

- *(parser)* [**breaking**] parse a program from a string, and rename parse to parse_file ([#436](https://github.com/flowlog-rs/flowlog/pull/436))
- append relations accept inserts across epochs and never delete ([#407](https://github.com/flowlog-rs/flowlog/pull/407))
- *(runtime)* [**breaking**] a mutable input is a set, not a bag ([#405](https://github.com/flowlog-rs/flowlog/pull/405))
- *(compiler)* [**breaking**] insert and delete replace put and file in the shell ([#403](https://github.com/flowlog-rs/flowlog/pull/403))
- [**breaking**] give every collection the weight of its own mutability ([#393](https://github.com/flowlog-rs/flowlog/pull/393))
- [**breaking**] aggregate expressions and lattice relations ([#431](https://github.com/flowlog-rs/flowlog/pull/431))
- aggregate per rule, so a relation unions its rules' answers ([#427](https://github.com/flowlog-rs/flowlog/pull/427))
- define relation mutability and infer it per stratum ([#385](https://github.com/flowlog-rs/flowlog/pull/385))
- *(parser)* let a component inherit from several parents ([#383](https://github.com/flowlog-rs/flowlog/pull/383))
- bitwise operators and power in arithmetic expressions ([#381](https://github.com/flowlog-rs/flowlog/pull/381))
- *(parser)* hexadecimal literals and native-only `.plan` ([#379](https://github.com/flowlog-rs/flowlog/pull/379))
- *(planner)* give every planned collection a mutability ([#387](https://github.com/flowlog-rs/flowlog/pull/387))

### Fixed

- *(runtime)* [**breaking**] report a no-change epoch's count as zero at its own epoch ([#429](https://github.com/flowlog-rs/flowlog/pull/429))
- *(planner)* keep the copying step of a rule that reads one relation as it is ([#428](https://github.com/flowlog-rs/flowlog/pull/428))
- *(parser)* read a factor's and an aggregate item's prefix once ([#438](https://github.com/flowlog-rs/flowlog/pull/438))
- *(planner)* number a pushdown copy's filter apart from its target ([#437](https://github.com/flowlog-rs/flowlog/pull/437))
- *(planner)* hold a keyed collection no join reads as rows ([#415](https://github.com/flowlog-rs/flowlog/pull/415))
- *(planner)* land a pushdown copy at the top of its target's map chain ([#413](https://github.com/flowlog-rs/flowlog/pull/413))

### Other

- *(deps)* update dependency rustyline to v19 ([#444](https://github.com/flowlog-rs/flowlog/pull/444))
- generate lint-clean code and allow only user-caused lints ([#402](https://github.com/flowlog-rs/flowlog/pull/402))
- *(codegen)* one fragment struct per side, and strata that compose their own head steps ([#401](https://github.com/flowlog-rs/flowlog/pull/401))
- split codegen into its own flowlog-codegen crate ([#398](https://github.com/flowlog-rs/flowlog/pull/398))
- codegen cleanups deferred from the mutability sweep ([#396](https://github.com/flowlog-rs/flowlog/pull/396))
- *(codegen)* merge a lattice relation's union without a dedup ([#433](https://github.com/flowlog-rs/flowlog/pull/433))
- *(codegen)* reuse presence membership for printsize ([#419](https://github.com/flowlog-rs/flowlog/pull/419))
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
