//! Runtime I/O errors, with [`Origin`] naming where a relation's input came
//! from and [`Position`] locating a rejected row in it. SQLite failures
//! cover database access, storage values, and output weights.

use std::fmt;
use std::io;
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
            Self::Put => f.write_str("tuple"),
        }
    }
}

// =============================================================================
// Origin
// =============================================================================

/// Where a relation's input came from, as a diagnostic names it.
///
/// Non-exhaustive for the same reason as [`RuntimeError`]: a new kind of
/// source is not a breaking release downstream.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Origin {
    /// A delimited text file.
    File(PathBuf),
    /// A SQLite database.
    Database(PathBuf),
    /// One `put` tuple.
    Put,
    /// Rows a host program handed over.
    Rows,
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File(path) => write!(f, "file `{}`", path.display()),
            Self::Database(path) => write!(f, "database `{}`", path.display()),
            Self::Put => f.write_str("put"),
            Self::Rows => f.write_str("rows"),
        }
    }
}

// =============================================================================
// RuntimeError
// =============================================================================

/// A failure to read input or write SQLite output.
///
/// A row error carries the row's own coordinates only; the loader wraps it
/// in [`RuntimeError::Load`] with the relation and origin it was read for,
/// exactly once, so a caller reports the error as it is.
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

    /// An update to a static relation once its initial load has closed it.
    #[error("relation `{relation}` is static and cannot change after its initial load")]
    StaticRelation { relation: &'static str },

    /// A deletion from an append relation, which accepts insertions only.
    #[error("relation `{relation}` is append and accepts no deletion")]
    AppendRelation { relation: &'static str },

    /// A failed `std::io` call. The caller adds relation or path context.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// An input file that could not be opened.
    #[error("cannot open: {0}")]
    CannotOpen(#[source] io::Error),

    /// A failure loading `relation` from `origin`.
    #[error("relation `{relation}` from {origin}: {source}")]
    Load {
        relation: &'static str,
        origin: Origin,
        source: Box<RuntimeError>,
    },

    /// A line whose bytes are not UTF-8.
    #[error("{at} is not UTF-8")]
    NotUtf8 { at: Position },

    /// A row that ended before the column a tuple asked for: it held
    /// exactly `column` cells.
    #[error("{at} has only {column} columns")]
    MissingColumn { at: Position, column: usize },

    /// A row with cells past the `expected` columns a tuple asked for. A
    /// trailing delimiter is one such cell, empty.
    #[error("{at} has more than {expected} columns")]
    ExtraColumns { at: Position, expected: usize },

    /// A cell that does not spell a value of its column's type.
    #[error("{at} column {column}: {value:?} is not {expected}")]
    Malformed {
        at: Position,
        column: usize,
        value: String,
        expected: &'static str,
    },
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(Position::Line(7), "line 7")]
    #[case(Position::Put, "tuple")]
    fn a_position_renders_as_prose(#[case] at: Position, #[case] expected: &str) {
        assert_eq!(at.to_string(), expected);
    }

    #[rstest]
    #[case(Origin::File(PathBuf::from("facts/Edge.csv")), "file `facts/Edge.csv`")]
    #[case(Origin::Database(PathBuf::from("facts.db")), "database `facts.db`")]
    #[case(Origin::Put, "put")]
    #[case(Origin::Rows, "rows")]
    fn an_origin_renders_as_prose(#[case] origin: Origin, #[case] expected: &str) {
        assert_eq!(origin.to_string(), expected);
    }

    /// A load error reads as one line: the relation, where it was read
    /// from, and the row's own message.
    #[test]
    fn a_load_error_renders_its_relation_origin_and_cause() {
        let error = RuntimeError::Load {
            relation: "Edge",
            origin: Origin::File(PathBuf::from("Edge.csv")),
            source: Box::new(RuntimeError::Malformed {
                at: Position::Line(7),
                column: 0,
                value: "x".to_owned(),
                expected: "i32",
            }),
        };
        assert_eq!(
            error.to_string(),
            "relation `Edge` from file `Edge.csv`: line 7 column 0: \"x\" is not i32"
        );
    }
}
