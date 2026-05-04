# `build/` — library-mode pipeline orchestrator

Library-mode entry point. When you call `flowlog_build::compile("foo.dl")` from `build.rs`, this is what runs end-to-end: parse, type-check, plan, codegen, then **stitch** a single self-contained `.rs` file into `$OUT_DIR/<stem>.rs` for your crate to `include!`.

> Binary mode lives in [`flowlog-compiler/`](../../../flowlog-compiler/). Both share every upstream stage; only the final assembly differs.

```mermaid
flowchart LR
    user["build.rs"] --> pipe[Pipeline::build]
    pipe --> parse --> tc[typecheck] --> strat[stratify] --> plan --> cg[codegen]
    cg --> as["assemble (this module)"] --> out["\$OUT_DIR/&lt;stem&gt;.rs"]
    user -.|"include!()"| out
```

## What `Pipeline::build` returns

```rust
struct Pipeline {
    config:    Config,        // mode, sip, str_intern, …
    parts:     CodeParts,     // codegen output (mode-agnostic)
    program:   Program,       // for relation metadata
    relations: TokenStream,   // {Name}Input handlers + Inputs container
    features:  Features,      // what the program actually uses
}
```

`assemble(&pipeline, out_dir)` then stitches that into one source string:

| Section | Source |
|---|---|
| `use` lines (re-routed through `::flowlog_runtime::`) | `imports::gen_lib_imports` |
| EDB handler structs (`{Name}Input`) + `Inputs` container | `relation/handler.rs` |
| User-facing tuple aliases (`pub mod rel { … }`) | `relation/user.rs` |
| Engine struct (`DatalogBatchEngine` or `DatalogIncrementalEngine`) | `engine/batch.rs`, `engine/incremental.rs` |
| `BatchResults` / `IncrementalResults` | `results.rs` |
| Semiring `mod` re-export pointing at `$OUT_DIR/semiring/` | `assembly::gen_semiring_mod` |

The whole bundle is wrapped in `mod __flowlog_gen { … } pub use __flowlog_gen::*;` so the user's `include!()`d source carries one private module but exposes a flat public API.

## Layout

| File / dir | Role |
|---|---|
| [`mod.rs`](mod.rs) | Re-exports (`Pipeline`, `assemble`, `BuildError`). |
| [`pipeline.rs`](pipeline.rs) | `Pipeline::build` driver (parse → check → plan → codegen). |
| [`assembly.rs`](assembly.rs) | `assemble()` — `Pipeline` → final source string. |
| [`engine/`](engine/) | Library engine struct + `run` / `Transaction::commit`. `batch.rs`, `incremental.rs`. |
| [`relation/`](relation/) | Per-EDB input handlers (`handler.rs`) + user tuple aliases (`user.rs`). |
| [`results.rs`](results.rs) | `BatchResults` / `IncrementalResults` codegen. |
| [`imports.rs`](imports.rs) | The `use` block emitted at the top of `<stem>.rs`. |
| [`error.rs`](error.rs) | `BuildError` — the type returned from `flowlog_build::compile`. |

## Library vs binary

Both modes share `crates/flowlog-build/src/codegen/` for the dataflow graph itself. What differs:

| Concern | Library (this module) | Binary ([`flowlog-compiler`](../../../flowlog-compiler/)) |
|---|---|---|
| Final output | `.rs` file `include!()`d by user crate | Cargo project + `cargo build --release` |
| EDB API | Inherent methods on `{Name}Input` | `Relation` trait + `Rel{Name}` handlers |
| Result | `BatchResults` / `IncrementalResults` struct | tuples to stdout / files |
| Ext crates | `::flowlog_runtime::serde`, etc. | direct `serde`, `timely`, `differential_dataflow` |
| Driver | host thread (library caller) | `timely::execute` from emitted `main.rs` |
