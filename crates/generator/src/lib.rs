//! Macaron Generator Library
//! Generates execution plans and code from planned strata and rules produced by the optimizer.
//!
//! At this stage, this crate exposes a minimal facade and logs the generation
//! process. Later it can be extended to generate optimized execution code.

pub mod generator;
pub mod io;
pub mod scaffold;
