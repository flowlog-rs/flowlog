# flowlog-common

Shared support crate for [FlowLog](https://github.com/flowlog-rs/flowlog), a Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow) compiler. Internal dependency of the other FlowLog crates; you typically don't depend on it directly.

## Layout

- `config` — shared pipeline configuration (`Config`, `ExecutionMode`).
- `fmt` — Rust token pretty-printing and diagnostic report layout.
- `hash` — hashing helper.

Errors, spans, and diagnostic rendering live in
[`flowlog-error`](../flowlog-error); depend on that crate directly for
`FlowlogError`, `Span`, `SourceMap`, and `Diagnostic`.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
