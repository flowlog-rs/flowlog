//! Codegen for what a compiled program emits once its dataflow is done.
//!
//! - [`merge`]: picks the sink for each IDB and sequences the blocks.
//! - [`file`]: one delimited file per relation under `<outdir>`.
//! - [`stdout`]: rows and counts for std out, in a bracketed debug shape.

mod file;
mod merge;
mod stdout;
