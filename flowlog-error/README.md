# flowlog-error

Shared error vocabulary for [FlowLog](https://github.com/flowlog-rs/flowlog), a Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow) compiler. Internal dependency of the other FlowLog crates; you typically don't depend on it directly.

Every FlowLog error type implements `FlowlogError`, so a caller can read a message and tell a bug in FlowLog from a mistake in the user's program, data, or environment — without knowing which stage produced it.

## Layout

- `error` — the `FlowlogError` trait, and `InternalError` for an invariant violation inside FlowLog.
- `source` — spans, file identifiers, and the source map.
- `diag` — the `Diagnostic` trait and its renderer.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
