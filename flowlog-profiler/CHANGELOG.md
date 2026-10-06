# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-profiler-v0.3.0...flowlog-profiler-v0.4.0) - 2026-10-06

### Added

- append relations accept inserts across epochs and never delete ([#407](https://github.com/flowlog-rs/flowlog/pull/407))
- *(runtime)* [**breaking**] a mutable input is a set, not a bag ([#405](https://github.com/flowlog-rs/flowlog/pull/405))
- [**breaking**] give every collection the weight of its own mutability ([#393](https://github.com/flowlog-rs/flowlog/pull/393))

### Other

- *(codegen)* one fragment struct per side, and strata that compose their own head steps ([#401](https://github.com/flowlog-rs/flowlog/pull/401))
- split codegen into its own flowlog-codegen crate ([#398](https://github.com/flowlog-rs/flowlog/pull/398))
- *(tests)* merge the batch and inc fixture directories ([#394](https://github.com/flowlog-rs/flowlog/pull/394))
- *(runtime)* [**breaking**] name update weights by relation class ([#354](https://github.com/flowlog-rs/flowlog/pull/354))

## [0.3.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-profiler-v0.2.0...flowlog-profiler-v0.3.0) - 2026-09-23

### Fixed

- *(runtime)* [**breaking**] emit zero for empty global count and sum ([#360](https://github.com/flowlog-rs/flowlog/pull/360))

### Other

- [**breaking**] goodbye SIP ([#365](https://github.com/flowlog-rs/flowlog/pull/365))
- *(runtime)* [**breaking**] unify dedup dispatch by diff and timestamp ([#353](https://github.com/flowlog-rs/flowlog/pull/353))

## [0.2.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-profiler-v0.1.0...flowlog-profiler-v0.2.0) - 2026-09-14

### Added

- *(runtime)* own set-semantics dedup and antijoin ([#283](https://github.com/flowlog-rs/flowlog/pull/283))

### Other

- [**breaking**] drop extended execution modes and loop blocks ([#286](https://github.com/flowlog-rs/flowlog/pull/286))

## [0.1.0](https://github.com/flowlog-rs/flowlog/releases/tag/flowlog-profiler-v0.1.0) - 2026-07-26

### Added

- *(profiler)* record plan graph in codegen and read metrics back in… ([#242](https://github.com/flowlog-rs/flowlog/pull/242))

### Other

- *(release)* prep first crates.io publish; flowlog-compiler 0.5.0 ([#268](https://github.com/flowlog-rs/flowlog/pull/268))
- *(codegen)* elide identity projections (drop no-op flat_maps) ([#200](https://github.com/flowlog-rs/flowlog/pull/200))
- *(codegen)* use threshold_total for outer-scope dedup in incremental mode ([#201](https://github.com/flowlog-rs/flowlog/pull/201)) ([#201](https://github.com/flowlog-rs/flowlog/pull/201))
- factor out `flowlog-common` and `flowlog-parser` ([#196](https://github.com/flowlog-rs/flowlog/pull/196))
- Extended profiler follow-ups ([#192](https://github.com/flowlog-rs/flowlog/pull/192))
