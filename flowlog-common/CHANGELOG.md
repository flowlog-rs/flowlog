# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-common-v0.1.0...flowlog-common-v0.2.0) - 2026-08-31

### Other

- [**breaking**] drop extended execution modes and loop blocks ([#286](https://github.com/flowlog-rs/flowlog/pull/286))

## [0.1.0](https://github.com/flowlog-rs/flowlog/releases/tag/flowlog-common-v0.1.0) - 2026-07-26

### Added

- *(profiler)* periodically flush metrics during incremental commits ([#267](https://github.com/flowlog-rs/flowlog/pull/267))
- *(profiler)* periodically flush metrics during batch runs ([#246](https://github.com/flowlog-rs/flowlog/pull/246))

### Fixed

- *(codegen)* deterministic `ord` via single-thread fact interning ([#208](https://github.com/flowlog-rs/flowlog/pull/208))

### Other

- *(release)* prep first crates.io publish; flowlog-compiler 0.5.0 ([#268](https://github.com/flowlog-rs/flowlog/pull/268))
- factor out `flowlog-common` and `flowlog-parser` ([#196](https://github.com/flowlog-rs/flowlog/pull/196))
