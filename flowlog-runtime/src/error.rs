//! Runtime I/O errors, with [`Position`] locating rejected input rows.
//! SQLite failures cover database access, storage values, and output weights.

use std::fmt;
use std::io;
#[cfg(feature = "sqlite")]
use std::path::PathBuf;

#[cfg(feature = "sqlite")]
use rusqlite::Error as DatabaseError;

// =============================================================================
// Position
// =============================================================================

/// Where a text row came from, as a diagnostic names it.
///
/// Non-exhaustive for the same reason as [`RuntimeError`]: a new kind of
/// source is not a breaking release downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Position {
    /// Line `n` of a file, counting from 1.
    Line(u64),
    /// A `put` tuple, which has no place in any file.
    Put,
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Line(n) => write!(f, "line {n}"),
            Self::Put => f.write_str("put"),
        }
    }
}

// =============================================================================
// RuntimeError
// =============================================================================

/// A failure to read input or write SQLite output.
///
/// A row error carries the row's own coordinates only; which relation and
/// file it was read for is the reporting caller's context, added exactly
/// once there.
///
/// Non-exhaustive so a new failure is not a breaking release downstream;
/// in-crate matches stay exhaustive.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeError {
    #[cfg(feature = "sqlite")]
    #[error(transparent)]
    Sqlite(#[from] DatabaseError),

    #[cfg(feature = "sqlite")]
    #[error("SQLite database '{}': {source}", path.display())]
    SqlitePath {
        path: PathBuf,
        source: Box<RuntimeError>,
    },

    #[cfg(feature = "sqlite")]
    #[error("SQLite column {column}: {value} is not {expected}")]
    SqliteValue {
        column: usize,
        value: String,
        expected: &'static str,
    },

    #[cfg(feature = "sqlite")]
    #[error("invalid SQLite schema: {0}")]
    SqliteSchema(String),

    #[cfg(feature = "sqlite")]
    #[error("relation `{relation}` has incremental weight {diff}; expected +1 or -1")]
    SqliteWeight { relation: &'static str, diff: i32 },

    #[error("worker index {index} requires nonzero peers and index < peers, got {peers} peers")]
    InvalidWorker { peers: usize, index: usize },

    #[error("delimiter byte {delimiter} must be ASCII")]
    InvalidDelimiter { delimiter: u8 },

    /// A failed `std::io` call. The caller adds relation or path context.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// A line whose bytes are not UTF-8: a corrupt file rather than a bad
    /// cell, so it stops a load where a bad cell would be skipped.
    #[error("{at} is not UTF-8")]
    NotUtf8 { at: Position },

    /// A row that ended before the column a tuple asked for: it held
    /// exactly `column` cells.
    #[error("{at} has only {column} columns")]
    MissingColumn { at: Position, column: usize },

    /// A cell that does not spell a value of its column's type.
    #[error("{at} column {column}: {value:?} is not {expected}")]
    Malformed {
        at: Position,
        column: usize,
        value: String,
        expected: &'static str,
    },
}
