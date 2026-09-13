//! Relation declarations, input loading, output sinks, and atomic writing.
//!
//! [`Relation`] declares relations independently of their I/O. [`input`]
//! owns loading and [`output`] owns output conversion and writing.
//! [`write_atomic`] replaces completed files without exposing a partial write.

pub mod input;
pub mod output;
mod relation;

pub use output::atomic::write_atomic;
pub use relation::Relation;
