//! How a source's record becomes a slot tuple.
//!
//! One trait, with one impl set per kind of record:
//!
//! - [`text`]: a delimited [`Line`](text::Line), parsed cell by cell.
//! - [`typed`]: an already-typed record, converted per position.
//!
//! Every reader ends at the same call, `T::decode(&record)`, where `T` is
//! the relation's slot tuple. Generated code names the pair as its
//! handler's `Tuple` and `Rows` types, and that pair selects an impl that
//! already exists here: no relation generates a decoder of its own, and a
//! mispaired one does not compile.

pub mod text;
pub mod typed;

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
