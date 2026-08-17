//! How a source's record becomes a slot tuple.
//!
//! One trait, with one impl set per kind of record, split on whether the
//! record's fields already carry types: that is what decides whether
//! decoding one can fail at all.
//!
//! - [`untyped`]: a delimited [`TextRow`](untyped::TextRow),
//!   parsed cell by cell, and refusable.
//! - [`typed`]: an already-typed record, reshaped per position.
//!
//! Five words, fixed here so neither half reaches for a sixth:
//!
//! | word   | what it names                                          |
//! |--------|--------------------------------------------------------|
//! | record | whatever [`Decode`] consumes, whichever source it is   |
//! | row    | the text record, one line of a file or one `put`       |
//! | tuple  | the slot tuple built from it, which the dataflow holds |
//! | cell   | one column of a row, still text and still to parse    |
//! | field  | one column of a typed record, a value to reshape      |
//!
//! Every reader ends at the same call, `T::decode(&record)`, where `T` is
//! the relation's slot tuple. Generated code names the pair as its
//! handler's `Tuple` and `Rows` types, and that pair selects an impl that
//! already exists here: no relation generates a decoder of its own, and a
//! mispaired one does not compile.

pub mod typed;
pub mod untyped;

use crate::error::RuntimeError;

/// Build a slot tuple from one record of a source.
///
/// Fallible for every source alike, so a caller reports a refusal the same
/// way whatever it was reading; a source whose records cannot be refused
/// never returns `Err`.
pub trait Decode<Src: ?Sized>: Sized {
    /// Decode one record, or report why it is not this tuple.
    fn decode(src: &Src) -> Result<Self, RuntimeError>;
}
