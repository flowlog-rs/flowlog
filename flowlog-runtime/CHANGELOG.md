# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.5](https://github.com/flowlog-rs/flowlog/compare/flowlog-runtime-v0.2.4...flowlog-runtime-v0.2.5) - 2026-06-13

### Other

- flatten workspace — move crates from crates/ to repo root

## [0.2.4](https://github.com/flowlog-rs/flowlog/compare/flowlog-runtime-v0.2.3...flowlog-runtime-v0.2.4) - 2026-06-12

### Other

- faster, leaner string `.output` (combines #135 + #136) ([#137](https://github.com/flowlog-rs/flowlog/pull/137))
- release flowlog-runtime v0.2.3, flowlog-build v0.3.1, flowlog-compiler v0.4.1 ([#133](https://github.com/flowlog-rs/flowlog/pull/133))

## [0.2.3](https://github.com/flowlog-rs/flowlog/compare/flowlog-runtime-v0.2.2...flowlog-runtime-v0.2.3) - 2026-06-07

### Added

- *(engine)* bridge Datalog/Soufflé gaps for DOOP end-to-end compilation ([#130](https://github.com/flowlog-rs/flowlog/pull/130))

### Other

- Migrate to differential-dataflow 0.24 / timely 0.30

## [0.2.2](https://github.com/flowlog-rs/flowlog/compare/flowlog-runtime-v0.2.1...flowlog-runtime-v0.2.2) - 2026-05-10

### Other

- split correctness from perf; modular harness for the correctness surface ([#101](https://github.com/flowlog-rs/flowlog/pull/101))
