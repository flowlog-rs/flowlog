//! Readers for a worker's share of an input source.
//!
//! [`Reader`] supplies decoded rows to [`ingest`]. The [`file`](mod@file),
//! [`host`], and [`put`] modules select and read each source's share.

pub(crate) mod file;
pub(crate) mod host;
pub(crate) mod put;

use crate::error::RuntimeError;

// =============================================================================
// Reader
// =============================================================================

/// Yields decoded rows from one worker's share of an input source.
///
/// Source-specific constructors select the share before reading begins.
pub(crate) trait Reader<T> {
    /// Returns the next decoded row, or `Ok(None)` at the end.
    ///
    /// An inner error rejects one row; reading can continue. An outer
    /// error stops the load: the source cannot guarantee further progress.
    fn next(&mut self) -> Result<Option<Result<T, RuntimeError>>, RuntimeError>;
}

/// Applies decoded rows in source order, passing rejected rows to `on_skip`.
///
/// Continues after a rejected row and returns the first source error.
pub(crate) fn ingest<R, T>(
    mut reader: R,
    mut apply: impl FnMut(T),
    mut on_skip: impl FnMut(RuntimeError),
) -> Result<(), RuntimeError>
where
    R: Reader<T>,
{
    while let Some(row) = reader.next()? {
        match row {
            Ok(tuple) => apply(tuple),
            Err(error) => on_skip(error),
        }
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
        Fail,
    }

    struct Scripted<'s>(slice::Iter<'s, Step>);

    impl Reader<i32> for Scripted<'_> {
        fn next(&mut self) -> Result<Option<Result<i32, RuntimeError>>, RuntimeError> {
            Ok(match self.0.next() {
                None => None,
                Some(Step::Row(n)) => Some(Ok(*n)),
                Some(Step::Refuse) => Some(Err(RuntimeError::Malformed {
                    at: Position::Put,
                    column: 0,
                    value: String::new(),
                    expected: "i32",
                })),
                Some(Step::Fail) => return Err(RuntimeError::NotUtf8 { at: Position::Put }),
            })
        }
    }

    #[test]
    fn rows_apply_in_order_and_rejected_rows_reach_the_callback() {
        let steps = [Step::Row(1), Step::Refuse, Step::Row(2)];
        let mut applied = Vec::new();
        let mut skipped = Vec::new();
        ingest(
            Scripted(steps.iter()),
            |row| applied.push(row),
            |error| skipped.push(error),
        )
        .expect("load");
        assert_eq!(applied, vec![1, 2]);
        assert!(matches!(
            skipped.as_slice(),
            [RuntimeError::Malformed {
                at: Position::Put,
                column: 0,
                value,
                expected: "i32",
            }] if value.is_empty()
        ));
    }

    #[test]
    fn a_source_error_stops_the_load_without_calling_on_skip() {
        let steps = [Step::Row(1), Step::Fail, Step::Row(2)];
        let mut applied = Vec::new();
        let error = ingest(
            Scripted(steps.iter()),
            |row| applied.push(row),
            |_| panic!("source errors must not reach on_skip"),
        )
        .expect_err("source failure");
        assert_eq!(applied, vec![1]);
        assert!(matches!(error, RuntimeError::NotUtf8 { at: Position::Put }));
    }

    #[test]
    fn an_empty_reader_applies_nothing() {
        ingest(
            Scripted([].iter()),
            |_| panic!("empty input must not apply a row"),
            |_| panic!("empty input must not reject a row"),
        )
        .expect("empty reader");
    }
}
