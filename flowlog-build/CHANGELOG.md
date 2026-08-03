# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.4.0...flowlog-build-v0.5.0) - 2026-08-03

### Other

- *(batch)* adaptive spin-then-park draining the dataflow to fixpoint ([#138](https://github.com/flowlog-rs/flowlog/pull/138))
- *(build)* [**breaking**] extract planner into flowlog-planner crate ([#273](https://github.com/flowlog-rs/flowlog/pull/273))

## [0.4.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.3.4...flowlog-build-v0.4.0) - 2026-07-26

### Breaking

- generated code targets timely 0.31 / differential-dataflow 0.25 ([#227](https://github.com/flowlog-rs/flowlog/pull/227)); pair with flowlog-runtime 0.3

### Added

- *(profiler)* periodically flush metrics during incremental commits ([#267](https://github.com/flowlog-rs/flowlog/pull/267))
- *(profiler)* periodically flush metrics during batch runs ([#246](https://github.com/flowlog-rs/flowlog/pull/246))
- *(profiler)* record plan graph in codegen and read metrics back in… ([#242](https://github.com/flowlog-rs/flowlog/pull/242))
- *(planner)* fuse spanning equality comparisons into join keys ([#219](https://github.com/flowlog-rs/flowlog/pull/219))

### Fixed

- *(codegen)* deterministic `ord` via single-thread fact interning ([#208](https://github.com/flowlog-rs/flowlog/pull/208))

### Other

- *(release)* prep first crates.io publish; flowlog-compiler 0.5.0 ([#268](https://github.com/flowlog-rs/flowlog/pull/268))
- Migrate to differential-dataflow 0.25 / timely 0.31 ([#227](https://github.com/flowlog-rs/flowlog/pull/227))
- fold flowlog-typechecker into flowlog-parser ([#224](https://github.com/flowlog-rs/flowlog/pull/224))
- *(codegen)* emit in-place filter/map_in_place for type-preserving row transforms ([#220](https://github.com/flowlog-rs/flowlog/pull/220))
- *(typechecker)* post-typecheck constant folding pass ([#209](https://github.com/flowlog-rs/flowlog/pull/209))
- extract `flowlog-typechecker` crate; per-site spelling in type errors ([#202](https://github.com/flowlog-rs/flowlog/pull/202))
- *(codegen)* elide identity projections (drop no-op flat_maps) ([#200](https://github.com/flowlog-rs/flowlog/pull/200))
- *(codegen)* fuse chained unions into a single n-ary concatenate ([#205](https://github.com/flowlog-rs/flowlog/pull/205))
- *(codegen)* use threshold_total for outer-scope dedup in incremental mode ([#201](https://github.com/flowlog-rs/flowlog/pull/201)) ([#201](https://github.com/flowlog-rs/flowlog/pull/201))
- factor out `flowlog-common` and `flowlog-parser` ([#196](https://github.com/flowlog-rs/flowlog/pull/196))
- Extended profiler follow-ups ([#192](https://github.com/flowlog-rs/flowlog/pull/192))
- support tuple syntax ([#194](https://github.com/flowlog-rs/flowlog/pull/194))

## [0.3.4](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.3.3...flowlog-build-v0.3.4) - 2026-06-18

### Fixed

- *(parser)* reject non-constant ground facts instead of panicking ([#184](https://github.com/flowlog-rs/flowlog/pull/184))

### Other

- add typos spell-check gate

## [0.3.3](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.3.2...flowlog-build-v0.3.3) - 2026-06-13

### Other

- flatten workspace — move crates from crates/ to repo root

## [0.3.2](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.3.1...flowlog-build-v0.3.2) - 2026-06-12

### Other

- *(planner)* share arrangements across rules via content-canonical materialization ([#148](https://github.com/flowlog-rs/flowlog/pull/148))
- faster, leaner string `.output` (combines #135 + #136) ([#137](https://github.com/flowlog-rs/flowlog/pull/137))

## [0.3.1](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.3.0...flowlog-build-v0.3.1) - 2026-06-07

### Added

- *(engine)* bridge Datalog/Soufflé gaps for DOOP end-to-end compilation ([#130](https://github.com/flowlog-rs/flowlog/pull/130))
- support multi-head/multi-body rules in .comp bodies
- several features make doop migration easier
- support  hint

### Fixed

- *(codegen)* escape relation names that collide with Rust keywords ([#131](https://github.com/flowlog-rs/flowlog/pull/131))
- comp-internal directive targeting an enclosing/global relation
- resolve qualified references to sibling/enclosing-scope instances
- resolve component-local .type aliases as bare attribute types
- non texture order type inference in .comp
- cargo clippy
- *(codegen)* bridge Spur/String at UDF boundary under --str-intern ([#118](https://github.com/flowlog-rs/flowlog/pull/118))

## [0.3.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.2.3...flowlog-build-v0.3.0) - 2026-05-27

### Added

- `override` keyword in `.comp`
- support OR predicates
- support template
- support subtype
- build in function
- support escape string
- *(planner)* inter-stratum sharing ([#112](https://github.com/flowlog-rs/flowlog/pull/112))

### Fixed

- non recursive pre seed IDB error
- cargo clippy

## [0.2.3](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.2.2...flowlog-build-v0.2.3) - 2026-05-10

### Fixed

- *(planner)* key TransformationInfo Eq/Hash on output fingerprint ([#108](https://github.com/flowlog-rs/flowlog/pull/108))

### Other

- *(parser)* tidy AST/Display/Parser impls + new round-trip test ([#104](https://github.com/flowlog-rs/flowlog/pull/104))
- *(errors)* tidy diagnostic formatting + label boilerplate ([#105](https://github.com/flowlog-rs/flowlog/pull/105))
- *(planner)* extract helpers across rule planner + transformations ([#106](https://github.com/flowlog-rs/flowlog/pull/106))
- split correctness from perf; modular harness for the correctness surface ([#101](https://github.com/flowlog-rs/flowlog/pull/101))
- *(imports)* consolidate and regroup `use` blocks across the workspace
- *(docs)* fix stale and misleading doc comments
- *(idiomatic)* small idiomatic-Rust rewrites across the workspace
- *(dry)* extract small DRY helpers across catalog/parser/planner/profiler
