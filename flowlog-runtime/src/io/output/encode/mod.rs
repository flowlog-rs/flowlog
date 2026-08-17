//! How a slot tuple becomes a sink's record.
//!
//! One trait, with one impl set per kind of sink:
//!
//! - [`text`]: delimited bytes, appended to a [`TextRows`](text::TextRows).
//! - [`typed`]: a host tuple, pushed to the caller's `Vec`.
//!
//! The write direction of [`decode`](crate::io::input::decode), and laid out the
//! same way. A writer never formats anything itself: it calls `encode` and
//! owns only the sink.

pub mod text;
pub mod typed;

/// Append one record to a destination.
///
/// The dual of [`Decode`](crate::io::input::decode::Decode): `Src` is where a
/// record comes from, `Dst` is where one goes, and in neither case is that
/// parameter the produced value. Both mean "exactly one record".
///
/// Consumes `self`, because the drain owns its rows by the time it gets
/// here: a `String` slot moves into the host tuple rather than cloning.
///
/// Infallible, unlike `Decode`. A well-typed slot has no form it cannot
/// take; the failure that does exist belongs to the file, and lives on
/// [`Writer::finish`](crate::io::output::writer::Writer::finish).
pub trait Encode<Dst: ?Sized> {
    /// Append this record to `dst`.
    fn encode(self, dst: &mut Dst);
}
