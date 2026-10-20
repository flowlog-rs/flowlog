# flowlog-common

Shared support crate for [FlowLog](https://github.com/flowlog-rs/flowlog), a Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow) compiler. Internal dependency of the other FlowLog crates; you typically don't depend on it directly.

## Layout

- `config` — shared pipeline configuration.
- `diag` — the `Diagnostic` trait and its renderer.
- `fmt` — Rust token pretty-printing and diagnostic report layout.
- `hash` — hashing helper.
- `source` — spans, file identifiers, and the source map.

## License

MIT OR Apache-2.0, at your option — see [LICENSE-MIT](./LICENSE-MIT) and [LICENSE-APACHE](./LICENSE-APACHE).
