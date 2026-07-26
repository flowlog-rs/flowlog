# flowlog-parser

Parser and typechecker for [FlowLog](https://github.com/flowlog-rs/flowlog), a
Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow)
compiler.

This crate is the FlowLog frontend: it takes Datalog source text through
parsing, type checking, constant folding, and pruning to a checked
`Program`. It is consumed by
[`flowlog-build`](https://crates.io/crates/flowlog-build); you typically
don't depend on it directly.

## What it offers

The crate is laid out as the compilation story:

- `types` — the type vocabulary (`DataType`, the registry).
- `syntax` — what the user wrote: grammar, string decoding, and the AST
  node layers.
- `pipeline` — what happens to it: the stages from source text to a
  checked, optimized `Program`, in execution order.
- `program` — what comes out: the `Program` container.

`parse` runs the whole pipeline; `check_program`, `fold_constants`, and
`prune` expose the individual stages.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
