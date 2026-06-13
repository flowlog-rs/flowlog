# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.4](https://github.com/flowlog-rs/flowlog/compare/flowlog-build-v0.3.3...flowlog-build-v0.3.4) - 2026-06-13

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
