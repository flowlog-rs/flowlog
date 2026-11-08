//! FlowLog runtime: types and re-exports consumed by generated code.
//!
//! This crate is the runtime half of the FlowLog library-mode toolchain.
//! Pair it with `flowlog-build` in your `[build-dependencies]`:
//!
//! ```toml
//! [dependencies]
//! flowlog-runtime = "0.6"
//!
//! [build-dependencies]
//! flowlog-build = "0.7"
//! ```
//!
//! Call [`initialize`] during startup, before runtime I/O or worker threads.
//!
//! ## What's in this crate
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`diff`] | Update weights and their arithmetic |
//! | [`io`] | Reading relations into the engine, and the helpers around it |
//! | [`error`] | Runtime I/O and profiling registration errors |
//! | [`intern`] | Thread-safe string interning pool |
//! | [`profile`] | Operator metrics, heap accounting, and profiling tables |
//! | [`operators`] | Named dataflow operators used by generated rules |
//! | [`arith`] | Arithmetic operators whose semantics FlowLog defines itself |
//! | [`time`] | Engine and loop timestamps |
//! | [`txn`] | Transactions, and their broadcast to incremental workers |
//! | `shell` | Interactive shell of incremental executables (`shell` feature) |
//!
//! The re-exported crates (`timely`, `differential_dataflow`, etc.) are
//! used internally by the generated code; you should not need to
//! reference them directly.

#[cfg(feature = "cli")]
mod args;
pub mod arith;
pub mod diff;
pub mod error;
mod failure;
mod init;
pub mod intern;
pub mod io;
pub mod operators;
pub mod profile;
#[cfg(feature = "shell")]
pub mod shell;
pub mod time;
pub mod txn;

// Re-exports for generated code, which names these crates only through
// them (`::flowlog_runtime::timely::*` and so on) and so always builds
// against this crate's versions. Users should not need them directly.
#[doc(hidden)]
pub use differential_dataflow;
pub use error::Origin;
pub use error::Position;
pub use error::RuntimeError;
pub use failure::Failure;
pub use init::initialize;
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
