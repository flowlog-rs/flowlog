//! FlowLog runtime: types and re-exports consumed by generated code.
//!
//! This crate is the runtime half of the FlowLog library-mode toolchain.
//! Pair it with `flowlog-build` in your `[build-dependencies]`:
//!
//! ```toml
//! [dependencies]
//! flowlog-runtime = "0.3"
//!
//! [build-dependencies]
//! flowlog-build = "0.4"
//! ```
//!
//! ## What's in this crate
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`io`] | Reading relations into the engine, and the helpers around it |
//! | [`error`] | [`RuntimeError`], everything the runtime can fail at |
//! | [`intern`] | Thread-safe string interning pool (`lasso`) |
//! | [`operators`] | Named dataflow operators used by generated rules |
//! | [`txn`] | Transaction state types shared with incremental drivers |
//!
//! The re-exported crates (`timely`, `differential_dataflow`, etc.) are
//! used internally by the generated code; you should not need to
//! reference them directly.

#[cfg(feature = "cli")]
mod args;
pub mod error;
pub mod intern;
pub mod io;
pub mod operators;
pub mod sort;
pub mod txn;

// Re-exports for generated code. The `include!()`'d code references these
// via `::flowlog_runtime::timely::*`, `::flowlog_runtime::differential_dataflow::*`,
// etc. Users should not need to use them directly.
#[doc(hidden)]
pub use differential_dataflow;
pub use error::Position;
pub use error::RuntimeError;
#[doc(hidden)]
pub use lasso;
#[doc(hidden)]
pub use ordered_float;
#[doc(hidden)]
pub use regex;
#[doc(hidden)]
pub use serde;
#[doc(hidden)]
pub use timely;

#[cfg(feature = "cli")]
pub use crate::args::RuntimeArgs;
