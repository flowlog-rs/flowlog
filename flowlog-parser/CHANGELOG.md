# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-parser-v0.1.0...flowlog-parser-v0.2.0) - 2026-08-03

### Other

- *(build)* [**breaking**] extract planner into flowlog-planner crate ([#273](https://github.com/flowlog-rs/flowlog/pull/273))

## [0.1.0](https://github.com/flowlog-rs/flowlog/releases/tag/flowlog-parser-v0.1.0) - 2026-07-26

### Added

- *(profiler)* record plan graph in codegen and read metrics back in… ([#242](https://github.com/flowlog-rs/flowlog/pull/242))

### Other

- *(release)* prep first crates.io publish; flowlog-compiler 0.5.0 ([#268](https://github.com/flowlog-rs/flowlog/pull/268))
- fold flowlog-typechecker into flowlog-parser ([#224](https://github.com/flowlog-rs/flowlog/pull/224))
- *(typechecker)* post-typecheck constant folding pass ([#209](https://github.com/flowlog-rs/flowlog/pull/209))
- extract `flowlog-typechecker` crate; per-site spelling in type errors ([#202](https://github.com/flowlog-rs/flowlog/pull/202))
- factor out `flowlog-common` and `flowlog-parser` ([#196](https://github.com/flowlog-rs/flowlog/pull/196))
