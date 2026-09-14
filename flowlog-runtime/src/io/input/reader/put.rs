//! Reading a single text tuple broadcast to all workers.
//!
//! [`PutReader`] uses the operation's ordinal to select the owning worker.

use crate::error::Position;
use crate::error::RuntimeError;
use crate::io::input::decode::Decode;
use crate::io::input::decode::text::TextRow;
use crate::io::input::reader::Reader;

// =============================================================================
// PutReader
// =============================================================================

/// Yields the owning worker's tuple once, even when decoding rejects it.
#[derive(Debug)]
pub(crate) struct PutReader<'src>(Option<TextRow<'src>>);

impl<'src> PutReader<'src> {
    /// Selects the worker at `ordinal % peers`, returning `None` for others.
    ///
    /// `delimiter` must be ASCII and `index` must be less than `peers`.
    ///
    /// # Panics
    ///
    /// Panics if `peers` is zero.
    pub(crate) fn open(
        text: &'src str,
        ordinal: usize,
        delimiter: u8,
        peers: usize,
        index: usize,
    ) -> Option<Self> {
        let row = TextRow {
            text,
            delim: delimiter,
            at: Position::Put,
        };
        (ordinal % peers == index).then_some(Self(Some(row)))
    }
}

impl<T: for<'l> Decode<TextRow<'l>>> Reader<T> for PutReader<'_> {
    fn next(&mut self) -> Result<Option<Result<T, RuntimeError>>, RuntimeError> {
        Ok(self.0.take().map(|row| T::decode(&row)))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_ordinal_has_one_owner_and_ordinals_spread() {
        let owners: Vec<usize> = (0..8)
            .map(|ordinal| {
                let opened: Vec<usize> = (0..4)
                    .filter(|&i| PutReader::open("1,2", ordinal, b',', 4, i).is_some())
                    .collect();
                assert_eq!(opened.len(), 1, "ordinal {ordinal}");
                opened[0]
            })
            .collect();
        assert_eq!(owners, vec![0, 1, 2, 3, 0, 1, 2, 3]);
    }

    #[test]
    fn the_owner_yields_the_tuple_once() {
        let mut reader = PutReader::open("1|2", 0, b'|', 1, 0).expect("owner");
        let first = Reader::<(i32, i32)>::next(&mut reader).expect("source");
        assert_eq!(first.map(|row| row.expect("row")), Some((1, 2)));
        assert!(
            Reader::<(i32, i32)>::next(&mut reader)
                .expect("source")
                .is_none()
        );
    }

    #[test]
    fn a_rejected_put_reports_its_position_and_is_consumed() {
        let mut reader = PutReader::open("1,x", 0, b',', 1, 0).expect("owner");
        let error = Reader::<(i32, i32)>::next(&mut reader)
            .expect("source")
            .expect("row")
            .expect_err("x is not i32");
        assert!(
            matches!(
                &error,
                RuntimeError::Malformed {
                    at: Position::Put,
                    column: 1,
                    value,
                    expected: "i32",
                } if value == "x"
            ),
            "got: {error}"
        );
        assert!(
            Reader::<(i32, i32)>::next(&mut reader)
                .expect("source")
                .is_none()
        );
    }
}
