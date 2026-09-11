//! Reading typed rows supplied by a host program.
//!
//! [`HostReader`] borrows a worker's index range and decodes each row on
//! demand.

use std::slice;

use crate::error::RuntimeError;
use crate::io::input::decode::Decode;
use crate::io::input::reader::Reader;

// =============================================================================
// HostReader
// =============================================================================

/// Borrows a contiguous share of typed rows without rearranging the input.
#[derive(Debug)]
pub(crate) struct HostReader<'src, U>(slice::Iter<'src, U>);

impl<'src, U> HostReader<'src, U> {
    /// Returns `None` when the worker's share is empty.
    ///
    /// # Panics
    ///
    /// May panic if `peers` is zero or `index >= peers`.
    pub(crate) fn open(rows: &'src [U], peers: usize, index: usize) -> Option<Self> {
        // Widen before multiplying so valid worker counts cannot overflow.
        let len = rows.len() as u128;
        let start = (len * index as u128 / peers as u128) as usize;
        let end = (len * (index as u128 + 1) / peers as u128) as usize;
        let share = &rows[start..end];
        (!share.is_empty()).then(|| Self(share.iter()))
    }
}

impl<U, T: Decode<U>> Reader<T> for HostReader<'_, U> {
    fn next(&mut self) -> Result<Option<Result<T, RuntimeError>>, RuntimeError> {
        Ok(self.0.next().map(T::decode))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn read_share(rows: &[(i32,)], peers: usize, index: usize) -> Vec<(i32,)> {
        let mut seen = Vec::new();
        if let Some(mut reader) = HostReader::open(rows, peers, index) {
            while let Some(row) = reader.next().expect("source") {
                seen.push(row.expect("row"));
            }
        }
        seen
    }

    #[rstest]
    #[case(1)]
    #[case(3)]
    #[case(4)]
    #[case(11)]
    fn shares_cover_the_rows_once_in_order(#[case] peers: usize) {
        let rows = [(0,), (1,), (2,), (3,), (4,)];
        let seen: Vec<_> = (0..peers)
            .flat_map(|index| read_share(&rows, peers, index))
            .collect();
        assert_eq!(seen, vec![(0,), (1,), (2,), (3,), (4,)]);
    }

    #[rstest]
    #[case(0, &[(0,), (1,)])]
    #[case(1, &[(2,), (3,)])]
    #[case(2, &[(4,), (5,), (6,)])]
    fn workers_read_contiguous_shares(#[case] index: usize, #[case] expected: &[(i32,)]) {
        let rows = [(0,), (1,), (2,), (3,), (4,), (5,), (6,)];
        assert_eq!(read_share(&rows, 3, index), expected);
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    fn empty_rows_open_as_no_share(#[case] index: usize) {
        assert!(HostReader::<(i32,)>::open(&[], 2, index).is_none());
    }

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(4)]
    #[case(7)]
    fn a_single_row_belongs_only_to_the_last_worker(#[case] peers: usize) {
        let rows = [(1,)];
        for index in 0..peers - 1 {
            assert!(HostReader::open(&rows, peers, index).is_none());
        }
        assert_eq!(read_share(&rows, peers, peers - 1), vec![(1,)]);
    }

    #[test]
    fn large_worker_counts_do_not_overflow_row_boundaries() {
        assert_eq!(
            read_share(&[(1,), (2,)], usize::MAX, usize::MAX - 1),
            vec![(2,)]
        );
    }
}
