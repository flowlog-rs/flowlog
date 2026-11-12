//! Relation declarations, input loading, output sinks, and atomic writing.
//!
//! [`Relation`] declares relations independently of their I/O. [`input`]
//! owns loading and [`output`] owns output conversion and writing.
//! [`write_atomic`] replaces completed files without exposing a partial write.
//! [`SqliteLayout`] is the shape both sides give a SQLite database, and
//! `sqlite_identifier` provides the quoting both use.

pub mod input;
pub mod output;
mod relation;

pub use output::atomic::write_atomic;
pub use relation::Relation;

/// SQLite relation storage format.
#[cfg(feature = "sqlite")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteLayout {
    /// Stores strings directly in relation tables.
    Plain,
    /// Stores string IDs backed by a shared dictionary.
    Dictionary,
}

/// Quotes a SQLite identifier, escaping embedded double quotes.
#[cfg(feature = "sqlite")]
#[must_use]
pub(crate) fn sqlite_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
