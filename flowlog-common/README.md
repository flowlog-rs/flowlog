# flowlog-common

Shared support crate for [FlowLog](https://github.com/flowlog-rs/flowlog), a
Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow)
compiler. Internal dependency of the other FlowLog crates; you typically
don't depend on it directly.

## Layout

- `config` — shared pipeline configuration (`Config`, `ExecutionMode`).
- `diag` — the `Diagnostic` trait and its renderer.
- `fmt` — Rust token pretty-printing and diagnostic report layout.
- `hash` — hashing helper.
- `source` — spans, file identifiers, and the source map.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
