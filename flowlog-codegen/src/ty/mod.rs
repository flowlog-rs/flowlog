//! Types of the `(Data, Diff, Time)` triple every collection carries:
//!
//! - [`data`]: each collection's key and value types, and their Rust tokens.
//! - [`diff`]: each collection's weight, and the mutability recorded for it.
//! - [`time`]: each collection's clock, the engine's time alias, and each
//!   loop's inner time.
//!
//! Only the engine's time is declared program-wide, as `type Ts`: each
//! collection names its own weight, each scope its clock, and each loop
//! its inner time.

pub(crate) mod data;
pub(super) mod diff;
pub(super) mod time;
