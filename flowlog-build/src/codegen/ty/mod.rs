//! Types of the `(Data, Diff, Time)` triple every collection carries:
//!
//! - [`data`]: each collection's key and value types, and their Rust tokens.
//! - [`diff`]: each collection's weight, and the mutability recorded for it.
//! - [`time`]: the outer timestamp alias and each loop's inner time.
//!
//! Only the outer time is declared program-wide, as `type Ts`: each
//! collection names its own weight, and each loop its inner time.

pub(crate) mod data;
pub(super) mod diff;
pub(super) mod time;
