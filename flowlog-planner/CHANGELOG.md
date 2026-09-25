# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1](https://github.com/flowlog-rs/flowlog/compare/flowlog-planner-v0.2.0...flowlog-planner-v0.2.1) - 2026-09-25

### Other

- updated the following local packages: flowlog-parser

## [0.2.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-planner-v0.1.0...flowlog-planner-v0.2.0) - 2026-09-23

### Added

- *(planner)* share collections across rules by canonical form ([#374](https://github.com/flowlog-rs/flowlog/pull/374))
- *(planner)* describe every collection by a canonical form ([#373](https://github.com/flowlog-rs/flowlog/pull/373))

### Fixed

- *(planner)* merge atoms with equal variable sets before propagation ([#362](https://github.com/flowlog-rs/flowlog/pull/362))
- *(parser)* [**breaking**] avoid recursive backtracking in nested syntax ([#361](https://github.com/flowlog-rs/flowlog/pull/361))

### Other

- *(planner)* share the preludes of earlier strata by canonical form ([#377](https://github.com/flowlog-rs/flowlog/pull/377))
- *(planner)* keep one fingerprint per collection and let the form share ([#376](https://github.com/flowlog-rs/flowlog/pull/376))
- *(planner)* fold semijoins once and push original filters down the plan ([#368](https://github.com/flowlog-rs/flowlog/pull/368))
- [**breaking**] goodbye SIP ([#365](https://github.com/flowlog-rs/flowlog/pull/365))

## [0.1.0](https://github.com/flowlog-rs/flowlog/releases/tag/flowlog-planner-v0.1.0) - 2026-09-14

### Other

- [**breaking**] drop extended execution modes and loop blocks ([#286](https://github.com/flowlog-rs/flowlog/pull/286))
- *(planner)* harden catalog metadata ([#275](https://github.com/flowlog-rs/flowlog/pull/275))
- *(build)* [**breaking**] extract planner into flowlog-planner crate ([#273](https://github.com/flowlog-rs/flowlog/pull/273))
