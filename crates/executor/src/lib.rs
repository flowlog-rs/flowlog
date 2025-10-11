//! Macaron Executor Library
//! Executes planned strata and rules produced by the optimizer.
//!
//! At this stage, this crate exposes a minimal facade and logs the execution
//! order. Later it can be extended to evaluate plans against a runtime.

pub mod args;
pub mod executor;

pub use executor::Executor;
