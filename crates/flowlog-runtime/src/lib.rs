//! FlowLog runtime — types and re-exports consumed by generated code.
//!
//! This crate is the runtime half of the FlowLog library-mode toolchain.
//! Pair it with [`flowlog-build`] in your `[build-dependencies]`:
//!
//! ```toml
//! [dependencies]
//! flowlog-runtime = "0.1.1"
//!
//! [build-dependencies]
//! flowlog-build = "0.1"
//! ```
//!
//! ## What's in this crate
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`Relation`] | Trait implemented by every generated input struct |
//! | [`io`] | Byte-range file reader + first-column sharding for parallel ingestion |
//! | [`intern`] | Thread-safe string interning pool (`lasso`) |
//! | [`txn`] | Transaction state types shared with incremental drivers |
//!
//! The re-exported crates (`timely`, `differential_dataflow`, etc.) are
//! used internally by the generated code — you should not need to
//! reference them directly.

pub mod intern;
pub mod io;
pub mod sort;
pub mod txn;

/// Trait implemented by every generated input relation struct.
///
/// The generated `DatalogBatchEngine` calls [`Relation::to_tuple`] at
/// insert time to convert user-facing structs (e.g. `Edge { x: 1, y: 2 }`)
/// into the internal differential-dataflow tuple representation.
///
/// You don't implement this trait manually — `flowlog-build` generates an
/// impl for each `.input` relation declared in your `.dl` program.
pub trait Relation: Sized {
    /// The internal DD tuple type (e.g. `(i32, i32)`).
    type Tuple;
    /// Relation name (lowercase), matching the DD input session key.
    fn relation_name() -> &'static str;
    /// Convert self into the internal tuple layout.
    fn to_tuple(self) -> Self::Tuple;
}

// Re-exports for generated code. The `include!()`'d code references these
// via `::flowlog_runtime::timely::*`, `::flowlog_runtime::differential_dataflow::*`,
// etc. Users should not need to use them directly.
#[doc(hidden)]
pub use differential_dataflow;
#[doc(hidden)]
pub use lasso;
#[doc(hidden)]
pub use ordered_float;
#[doc(hidden)]
pub use serde;
#[doc(hidden)]
pub use timely;
