//! The sink contract, and one writer per kind of destination.
//!
//! - [`text`]: a delimited text file, formatted through
//!   [`Encode<TextRows>`](crate::io::output::encode::Encode).
//! - [`vec`](mod@vec): host-facing tuples, converted through
//!   [`Encode<Vec<U>>`](crate::io::output::encode::Encode).
//!
//! Laid out as the `reader` module is, and the same split: the writer
//! owns the destination, [`Encode`](crate::io::output::encode::Encode) owns
//! the conversion of one row, and the loop lives outside both, in
//! [`drain`](crate::io::output).

pub mod text;
pub mod vec;

use crate::error::RuntimeError;
use crate::io::spec::OutputSpec;
use crate::txn::Diff;

/// A relation's destination, taking drained rows one at a time.
///
/// The write-side counterpart of the `Reader` trait, pushed into rather
/// than pulled from. Rows arrive owned, because the drain owns the buffers
/// it took: a `String` slot moves into the host tuple instead of being
/// cloned.
///
/// Two places it is deliberately a weaker mirror than `Reader`:
///
/// - `open` yields the writer itself, where `Reader::open` yields an
///   `Option` meaning "no share for this worker". A drain runs after the
///   rows have been gathered onto one worker, so there is no share left to
///   answer. The worker-0 gate stays one check around the whole merge
///   section rather than being asked once per relation.
/// - `finish` has no read-side counterpart. Reading ends when the source
///   runs out, which the source itself signals; writing ends when the
///   producer says so, and the final flush can fail.
pub trait Writer<T>: Sized {
    /// What the caller receives once the last row is in: `()` for a file,
    /// the rows themselves for a host `Vec`.
    type Out;

    /// Acquire the destination `spec` names.
    fn open(spec: &OutputSpec<'_>) -> Result<Self, RuntimeError>;

    /// Take one row, with its multiplicity when the sink writes one.
    ///
    /// `diff` is the caller's call rather than the row's: a nullary
    /// relation carries none even in an incremental epoch, where every
    /// other relation does.
    fn push(&mut self, row: T, diff: Option<Diff>);

    /// Finish the destination and hand it over.
    ///
    /// Fallible because a buffered sink's last write happens here, and
    /// letting `Drop` do it would discard the error.
    fn finish(self) -> Result<Self::Out, RuntimeError>;
}
