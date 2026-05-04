# `build/` — library-mode pipeline orchestrator

The library-mode entry point. When you call `flowlog_build::compile("foo.dl")`
from `build.rs`, this is what runs. End-to-end it: parses, type-checks, plans,
runs codegen, then **stitches** a single self-contained `.rs` file into
`$OUT_DIR/<stem>.rs` for your crate to `include!`.

> Binary mode (the standalone `flowlog` CLI) has its own pipeline / scaffolding
> in [`crates/flowlog-compiler/`](../../../flowlog-compiler/). Both modes share
> the upstream stages (parser, typechecker, …, codegen).

## Where this fits

```mermaid
flowchart LR
    USER["build.rs<br/>flowlog_build::compile"] --> PIPE["build::Pipeline::build"]
    subgraph stages["upstream stages (shared with binary mode)"]
        PIPE --> P[parse]
        P --> TC[typecheck]
        TC --> S[stratify]
        S --> PL[plan]
        PL --> CG[codegen]
    end
    CG --> AS["build::assemble<br/>(this module)"]
    AS --> OUT["$OUT_DIR/&lt;stem&gt;.rs"]
    USER -.|"include!()"| OUT
```

## Two-phase orchestration

`Pipeline::build` runs the upstream pipeline and returns a `Pipeline` struct
holding everything the library frontend needs:

```rust
struct Pipeline {
    config:    Config,        // mode, sip, str_intern, …
    parts:     CodeParts,     // codegen output (mode-agnostic)
    program:   Program,       // for relation metadata
    relations: TokenStream,   // library-mode `{Name}Input` handlers + Inputs container
    features:  Features,      // what the program actually uses
}
```

`assemble(&pipeline, out_dir)` then **stitches** that into one source string by
combining mode-agnostic `parts` with library-mode-specific scaffolding:

| Section | Source |
|---|---|
| `use` lines (re-routed through `::flowlog_runtime::`) | `imports::gen_lib_imports` |
| EDB handler structs (`{Name}Input`) + `Inputs` container | `relation/handler.rs` |
| User-facing tuple aliases (`pub mod rel { pub type Edge = (i32, i32); … }`) | `relation/user.rs` |
| `DatalogBatchEngine` / `DatalogIncrementalEngine` struct | `engine/batch.rs`, `engine/incremental.rs` |
| `BatchResults` / `IncrementalResults` (return type for `.run()` / `Transaction::commit()`) | `results.rs` |
| Per-aggregation semiring `mod` re-export pointing at `$OUT_DIR/semiring/` | `assembly::gen_semiring_mod` |

The whole assembly is wrapped in `mod __flowlog_gen { … } pub use __flowlog_gen::*;`
so `include!()`d code carries one private-module bundle but exposes a flat
public API.

## Submodule map

| File / dir | Role |
|---|---|
| [`mod.rs`](mod.rs) | Re-exports (`Pipeline`, `assemble`, `BuildError`). |
| [`pipeline.rs`](pipeline.rs) | The `Pipeline` struct + `Pipeline::build` driver (parse → check → plan → codegen). |
| [`assembly.rs`](assembly.rs) | `assemble()` — the stitching pass; turns `Pipeline` into a `String`. |
| [`engine/`](engine/) | Library-mode engine struct + `run` / `Transaction::commit` impl. `batch.rs` for run-once mode, `incremental.rs` for `Transaction`-driven mode. |
| [`relation/`](relation/) | Per-EDB input handler + `Inputs` container codegen (`handler.rs`) and user-facing tuple aliases (`user.rs`). |
| [`results.rs`](results.rs) | `BatchResults` / `IncrementalResults` struct codegen — the return shape of the user's call. |
| [`imports.rs`](imports.rs) | The `use` block emitted at the top of `<stem>.rs`, re-routing every external crate through `::flowlog_runtime::`. |
| [`error.rs`](error.rs) | `BuildError` — the error type surfaced from `flowlog_build::compile`. |

## Library vs binary code: who emits what

Both modes share `crates/flowlog-build/src/codegen/` for the *dataflow graph
itself* (DD operator chains, drain code, profiling). What differs:

| Concern | Library mode (this module) | Binary mode (`flowlog-compiler`) |
|---|---|---|
| Final output | one `.rs` file `include!()`d by user crate | a Cargo project + `cargo build --release` |
| EDB input API | inherent methods on `{Name}Input` (no trait) | `Relation` trait + `Rel{Name}` handlers |
| Result type | `BatchResults` / `IncrementalResults` struct | tuples written to stdout / files |
| Ext crate paths | `::flowlog_runtime::serde`, etc. (single dep) | direct `serde`, `timely`, `differential_dataflow` |
| Driver | host thread (library caller) | `timely::execute` from the emitted `main.rs` |
