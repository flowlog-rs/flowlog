//! FlowLog runtime: what compiled FlowLog programs run on.
//!
//! Both toolchain halves link this crate. A binary compiled by
//! `flowlog-compiler` drives its relations through [`io`]; a host program
//! using library mode pairs it with `flowlog-build` in
//! `[build-dependencies]`:
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
//! | [`io`] | Reading relations into the engine and writing them back out |
//! | [`intern`] | The process-global string pool; interned keys are what string columns hold |
//! | [`txn`] | The epoch-broadcast protocol incremental drivers coordinate through |
//! | [`error`] | [`RuntimeError`], everything the runtime can fail at |
//!
//! The re-exported crates (`timely`, `differential_dataflow`, etc.) are
//! used internally by the generated code; you should not need to
//! reference them directly.

pub mod error;
pub mod intern;
pub mod io;
pub mod txn;

// Re-exports for generated code. The `include!()`'d code references these
// via `::flowlog_runtime::timely::*`, `::flowlog_runtime::differential_dataflow::*`,
// etc. Users should not need to use them directly.
#[doc(hidden)]
pub use differential_dataflow;
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
