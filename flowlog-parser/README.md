# flowlog-parser

The frontend for [FlowLog](https://github.com/flowlog-rs/flowlog)'s Datalog language. `parse` takes a `.dl` program (resolving `.include`s) through type checking, constant folding, and pruning unused components, and returns a fully-typed, immutable `Program`, ready for a code generator, analyzer, linter, or any other tool that consumes FlowLog programs.

## Usage

```rust
use flowlog_common::{Config, SourceMap};
use flowlog_parser::parse;

let mut sm = SourceMap::new();
let mut config = Config::default();
let program = parse("program.dl", &[], &mut sm, &mut config)?;
```

`parse` runs the whole pipeline; `check_program`, `fold_constants`, and `prune` expose the individual stages, and the AST layer is public for tools that work on the syntax directly.

## Layout

- `types` — the type vocabulary (`DataType`, the registry).
- `syntax` — what the user wrote: grammar, string decoding, and the AST
  node layers.
- `pipeline` — what happens to it: the stages from source text to a
  checked, optimized `Program`, in execution order.
- `program` — what comes out: the `Program` container.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
