//! Readers for a worker's share of an input source.
//!
//! [`Reader`] supplies decoded rows to [`ingest`], which stops at the first
//! error. The [`file`](mod@file), [`host`], and [`put`] modules select and
//! read each source's share; `sqlite` queries its share of a database and
//! hands each row to a callback instead.

pub(crate) mod file;
pub(crate) mod host;
pub(crate) mod put;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;

use crate::error::RuntimeError;

// =============================================================================
// Reader
// =============================================================================

/// Yields decoded rows from one worker's share of an input source.
///
/// Source-specific constructors select the share before reading begins.
pub(crate) trait Reader<T> {
    /// Returns the next decoded row, or `Ok(None)` at the end. A source
    /// failure and a row that cannot be decoded are both errors; a reader
    /// is not read again after one.
    fn next(&mut self) -> Result<Option<T>, RuntimeError>;
}

/// Applies decoded rows in source order, stopping at the first error. The
/// rows before it stay applied.
pub(crate) fn ingest<R, T>(mut reader: R, mut apply: impl FnMut(T)) -> Result<(), RuntimeError>
where
    R: Reader<T>,
{
    while let Some(row) = reader.next()? {
        apply(row);
    }
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::slice;

    use super::*;
    use crate::error::Position;

    enum Step {
        Row(i32),
        Refuse,
    }

    struct Scripted<'s>(slice::Iter<'s, Step>);

    impl Reader<i32> for Scripted<'_> {
        fn next(&mut self) -> Result<Option<i32>, RuntimeError> {
            match self.0.next() {
                None => Ok(None),
                Some(Step::Row(n)) => Ok(Some(*n)),
                Some(Step::Refuse) => Err(RuntimeError::Malformed {
                    at: Position::Put,
                    column: 0,
                    value: String::new(),
                    expected: "i32",
                }),
            }
        }
    }

    #[test]
    fn rows_apply_in_order() {
        let steps = [Step::Row(1), Step::Row(2)];
        let mut applied = Vec::new();
        ingest(Scripted(steps.iter()), |row| applied.push(row)).expect("load");
        assert_eq!(applied, vec![1, 2]);
    }

    #[test]
    fn a_refused_row_stops_the_load_after_the_rows_before_it() {
        let steps = [Step::Row(1), Step::Refuse, Step::Row(2)];
        let mut applied = Vec::new();
        let error = ingest(Scripted(steps.iter()), |row| applied.push(row)).expect_err("refused");
        assert_eq!(applied, vec![1]);
        assert!(matches!(
            error,
            RuntimeError::Malformed {
                at: Position::Put,
                column: 0,
                ..
            }
        ));
    }

    #[test]
    fn an_empty_reader_applies_nothing() {
        ingest(Scripted([].iter()), |_| {
            panic!("empty input must not apply a row")
        })
        .expect("empty reader");
    }
}
