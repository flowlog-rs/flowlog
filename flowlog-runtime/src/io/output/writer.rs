//! Consumption and completion of relation output.
//!
//! [`Writer`] owns encoding, sink errors, and final result storage. [`file`]
//! formats partitions in bounded waves; [`stdout`] locks complete records;
//! [`host`] moves converted rows into the returned library result.

pub(super) mod file;
pub(super) mod host;
#[cfg(feature = "sqlite")]
pub(super) mod sqlite;
pub(super) mod stdout;

use crate::io::Relation;

/// Consumes updates in delivery order and completes the selected output.
///
/// Rows arrive owned so typed sinks can move fields; text sinks borrow them
/// while encoding. `INCREMENTAL` selects batch or delta layouts for sinks
/// that distinguish them.
pub trait Writer<R: Relation, T, const INCREMENTAL: bool = false> {
    type Output;
    type Error;

    fn write_row(&mut self, update: (R::Tuple, T, i32)) -> Result<(), Self::Error>;

    /// Delivers partitions in order without requiring a merged row vector.
    ///
    /// Implementations may drain rows or borrow them for parallel formatting.
    /// The caller discards remaining rows and recycles storage after success
    /// or failure.
    fn write_batch(&mut self, batches: &mut [Vec<(R::Tuple, T, i32)>]) -> Result<(), Self::Error>;

    /// Completes successful delivery, reporting flush errors or returning the
    /// final result. A delivery error drops the writer without calling this.
    fn finish(self) -> Result<Self::Output, Self::Error>;
}
