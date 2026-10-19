# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/flowlog-rs/flowlog/releases/tag/flowlog-codegen-v0.1.0) - 2026-10-19

### Added

- [**breaking**] aggregate expressions and lattice relations ([#431](https://github.com/flowlog-rs/flowlog/pull/431))
- aggregate per rule, so a relation unions its rules' answers ([#427](https://github.com/flowlog-rs/flowlog/pull/427))
- append relations accept inserts across epochs and never delete ([#407](https://github.com/flowlog-rs/flowlog/pull/407))
- *(runtime)* [**breaking**] a mutable input is a set, not a bag ([#405](https://github.com/flowlog-rs/flowlog/pull/405))

### Fixed

- *(runtime)* [**breaking**] report a no-change epoch's count as zero at its own epoch ([#429](https://github.com/flowlog-rs/flowlog/pull/429))
- *(planner)* keep the copying step of a rule that reads one relation as it is ([#428](https://github.com/flowlog-rs/flowlog/pull/428))

### Other

- *(codegen)* merge a lattice relation's union without a dedup ([#433](https://github.com/flowlog-rs/flowlog/pull/433))
- *(codegen)* reuse presence membership for printsize ([#419](https://github.com/flowlog-rs/flowlog/pull/419))
- generate lint-clean code and allow only user-caused lints ([#402](https://github.com/flowlog-rs/flowlog/pull/402))
- *(codegen)* one fragment struct per side, and strata that compose their own head steps ([#401](https://github.com/flowlog-rs/flowlog/pull/401))
- split codegen into its own flowlog-codegen crate ([#398](https://github.com/flowlog-rs/flowlog/pull/398))
