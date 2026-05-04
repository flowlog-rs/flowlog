# `common/` — stage-agnostic foundation

Source spans, diagnostics, the `Config` struct, formatting helpers — everything every other stage uses. **Stage-agnostic** is the rule: if it touches `Pair<Rule>` or `Catalog` or `Transformation`, it doesn't belong here.

> Exposed as `pub mod common` from `flowlog-build/src/lib.rs` but `#[doc(hidden)]` — `flowlog-compiler` and integration tests reach into it. External crates should not.

## Files

| File | Public surface | Purpose |
|---|---|---|
| [`source.rs`](source.rs) | `FileId`, `Span`, `SourceMap`, `Ignored<T>` | Anchor every AST node to a byte range; `SourceMap` impls `codespan_reporting::files::Files`. `Ignored<T>` opts a field out of derived `PartialEq`/`Eq`/`Hash` — used for spans and profiler counters that shouldn't affect structural equality. |
| [`diag.rs`](diag.rs) | `Diagnostic` trait, `BoxError`, `InternalError`, `emit`, `BUG_URL` | Each stage defines its own error enum and `impl Diagnostic`; the blanket `From` boxes them. `emit` renders against a `SourceMap` for `cargo build`-friendly output. |
| [`config.rs`](config.rs) | `Config`, `ExecutionMode`, `get_example_files` | The Clap entry point + `Builder` projection. |
| [`formatter.rs`](formatter.rs) | `pretty_print` re-export, `SECTION_BAR`, `SUBSECTION_BAR` | Wraps `prettyplease`; ASCII bars decorate phase boundaries in tracing logs. |
| [`macros.rs`](macros.rs) | `INTERN_MAX_RETRIES` | Shared constants/macros that don't belong to any one stage. |
| [`mod.rs`](mod.rs) | `compute_fp` (intra-crate) | Re-exports + the `compute_fp<T: Hash>` helper. |

## Load-bearing details

- `FileId::DUMMY` marks synthesised AST nodes (e.g. desugared rule heads). `primary_label` / `secondary_label` return `None` for dummy spans so the renderer doesn't point at a bogus offset.
- `Diagnostic` is a **trait**, not a struct. There's no central `enum AnyError` — this keeps stage-local errors strongly typed and free of upstream types.
- `Ignored<T>` is the canonical "metadata" wrapper. Two `FlowLogRule`s differing only in span hash equal — that's what enables fingerprint-based dedup. Wrap any new `Span`-like field in `Ignored<T>`.
- `compute_fp` uses `DefaultHasher`. Deterministic within a build, **not** stable across Rust versions — don't persist a fingerprint outside one compilation.

## Don't add things here that…

- Depend on `parser::*`, `catalog::*`, `planner::*`, `codegen::*` — those belong in their respective stages.
- Are mode-specific (library vs binary) — those belong in `build/` or `flowlog-compiler/`.
- Are pure data types only one stage needs.
