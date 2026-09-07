//! Decoding source rows for dataflow input.
//!
//! [`Decode`] is the shared contract. [`text`] parses text rows through
//! [`DecodeCell`](text::DecodeCell), while [`typed`] converts typed rows
//! through [`DecodeField`](typed::DecodeField).

pub mod text;
pub mod typed;

use crate::error::RuntimeError;

/// Converts source rows into values for dataflow input.
pub trait Decode<Src: ?Sized>: Sized {
    /// Return an error when any part of the row cannot be decoded.
    fn decode(src: &Src) -> Result<Self, RuntimeError>;
}
