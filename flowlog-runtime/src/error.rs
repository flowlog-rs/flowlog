//! The crate's error currency, [`RuntimeError`].

/// Anything the runtime can fail at.
///
/// One currency for the whole crate, so generated code reacts in one place
/// rather than one per subsystem.
///
/// A row error carries only the row's own coordinates; which relation and
/// file it was read for is the reporting caller's context, added exactly
/// once there. `position` is a line number in a text file, or 0 for a
/// source without positions.
///
/// Non-exhaustive so a new failure is not a breaking release downstream;
/// in-crate matches stay exhaustive.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeError {
    // --- Reading a relation's input ---
    /// Any `?` on a `std::io` call lands here, so it names no relation or
    /// path; a write knows both and reports through [`Self::Output`].
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("row {position} has {arity} columns, so it has no column {column}")]
    MissingColumn {
        position: u64,
        column: usize,
        arity: usize,
    },

    #[error("row {position} column {column}: {value} is not {expected}")]
    Malformed {
        position: u64,
        column: usize,
        value: String,
        expected: &'static str,
    },

    // --- Writing a relation's output ---
    #[error("relation '{relation}': cannot write {path}: {source}")]
    Output {
        relation: String,
        path: String,
        source: std::io::Error,
    },
}

impl flowlog_error::FlowlogError for RuntimeError {}
