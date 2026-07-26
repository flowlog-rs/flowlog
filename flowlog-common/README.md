# flowlog-common

Shared support crate for [FlowLog](https://github.com/flowlog-rs/flowlog), a
Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow)
compiler.

This crate holds the primitives the FlowLog crates share. It is an internal
dependency of [`flowlog-parser`](https://crates.io/crates/flowlog-parser),
[`flowlog-build`](https://crates.io/crates/flowlog-build), and
[`flowlog-profiler`](https://crates.io/crates/flowlog-profiler); you
typically don't depend on it directly.

## What it offers

- `Config` / `ExecutionMode` — shared pipeline configuration.
- `Diagnostic` — the error-reporting trait each compiler stage implements,
  plus the renderer behind FlowLog's diagnostics.
- Source primitives — spans, file identifiers, and the source map.
- Formatting helpers — Rust token pretty-printing and the report layout
  used in diagnostic dumps.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
