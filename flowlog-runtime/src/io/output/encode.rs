//! Row conversion independent of collection and destination ownership.
//!
//! [`Encode`] accepts owned or borrowed values.
//! [`typed::TypedEncoder`] consumes tuples to produce ordinary Rust values.
//! [`text::TextEncoder`] borrows columns for plain or debug text. Writers
//! supply the record layout.

#[cfg(feature = "sqlite")]
pub(super) mod sqlite;
pub(super) mod text;
pub(super) mod typed;

/// Converts a source value without requiring an intermediate row or buffer.
/// `Src` determines ownership: text borrows rows, typed output moves them.
pub(super) trait Encode<Src> {
    type Output;

    /// Encodes one value, returning sink errors when the output is fallible.
    fn encode(&mut self, src: Src) -> Self::Output;
}
