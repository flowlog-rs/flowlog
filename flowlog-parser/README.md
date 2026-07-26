# flowlog-parser

Parser and typechecker for [FlowLog](https://github.com/flowlog-rs/flowlog), a
Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow)
compiler: from source text to a checked, optimized `Program`. Consumed by
[`flowlog-build`](https://crates.io/crates/flowlog-build); you typically
don't depend on it directly.

## Layout

- `types` — the type vocabulary (`DataType`, the registry).
- `syntax` — what the user wrote: grammar, string decoding, and the AST
  node layers.
- `pipeline` — what happens to it: the stages from source text to a
  checked, optimized `Program`, in execution order.
- `program` — what comes out: the `Program` container.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
