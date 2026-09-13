//! Output ordering with supplied comparators.
//!
//! [`drain_merge`] merges sorted partitions while retaining their storage.
//! [`topk`] selects and sorts a limited prefix.

use std::cmp::Ordering;

/// Drains sorted partitions in merge order, retaining their allocations.
/// A sink error stops delivery and discards the remaining rows.
///
/// Each partition must already be sorted by `cmp`. Equal keys keep partition
/// order. Head selection takes O(k) comparisons per element and O(k)
/// auxiliary storage, where k is the number of partitions.
pub(super) fn drain_merge<T, F, S, E>(
    per_worker: &mut [Vec<T>],
    cmp: F,
    mut sink: S,
) -> Result<(), E>
where
    F: Fn(&T, &T) -> Ordering,
    S: FnMut(T) -> Result<(), E>,
{
    if let [rows] = per_worker {
        return rows.drain(..).try_for_each(sink);
    }
    let mut iters: Vec<_> = per_worker.iter_mut().map(|rows| rows.drain(..)).collect();
    let mut heads: Vec<Option<T>> = iters.iter_mut().map(Iterator::next).collect();

    while let Some(best) = heads
        .iter()
        .enumerate()
        .filter_map(|(i, h)| h.as_ref().map(|v| (i, v)))
        .min_by(|(_, a), (_, b)| cmp(a, b))
        .map(|(i, _)| i)
    {
        sink(heads[best].take().expect("selected merge head is present"))?;
        heads[best] = iters[best].next();
    }
    Ok(())
}

/// Returns at most `k` rows in comparator order.
///
/// Uses `select_nth_unstable_by` for O(n) partitioning then sorts the
/// retained prefix. Falls back to a full sort when `rows.len() <= k`.
/// Equal keys need not retain their input order when selection is required.
pub(super) fn topk<T, F>(mut rows: Vec<T>, k: usize, cmp: F) -> Vec<T>
where
    F: Fn(&T, &T) -> Ordering,
{
    if k == 0 {
        rows.clear();
    } else if rows.len() > k {
        rows.select_nth_unstable_by(k, |a, b| cmp(a, b));
        rows.truncate(k);
    }
    rows.sort_by(cmp);
    rows
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::no_partitions(vec![], vec![])]
    #[case::empty(vec![vec![], vec![]], vec![])]
    #[case::single(vec![vec![1, 3, 8]], vec![1, 3, 8])]
    #[case::multiple(vec![vec![1, 8], vec![], vec![2, 3]], vec![1, 2, 3, 8])]
    fn merge_drains_partitions_without_releasing_storage(
        #[case] mut partitions: Vec<Vec<i32>>,
        #[case] expected: Vec<i32>,
    ) {
        let storage: Vec<_> = partitions
            .iter()
            .map(|rows| (rows.as_ptr(), rows.capacity()))
            .collect();
        let mut output = Vec::new();
        drain_merge(&mut partitions, i32::cmp, |row| {
            output.push(row);
            Ok::<_, Infallible>(())
        })
        .expect("merge rows");
        assert_eq!(output, expected);
        assert!(partitions.iter().all(Vec::is_empty));
        assert_eq!(
            partitions
                .iter()
                .map(|rows| (rows.as_ptr(), rows.capacity()))
                .collect::<Vec<_>>(),
            storage,
        );
    }

    #[test]
    fn equal_merge_keys_keep_partition_order() {
        let mut partitions = vec![vec![(1, "a"), (1, "b"), (2, "c")], vec![(1, "d"), (2, "e")]];
        let mut output = Vec::new();
        drain_merge(
            &mut partitions,
            |a, b| a.0.cmp(&b.0),
            |row| {
                output.push(row);
                Ok::<_, Infallible>(())
            },
        )
        .expect("merge rows");
        assert_eq!(output, [(1, "a"), (1, "b"), (1, "d"), (2, "c"), (2, "e")]);
    }

    #[rstest]
    #[case::single(vec![vec![1, 2, 3]])]
    #[case::multiple(vec![vec![1, 3], vec![], vec![2, 4]])]
    fn merge_errors_discard_remaining_rows_and_retain_storage(
        #[case] mut partitions: Vec<Vec<i32>>,
    ) {
        let storage: Vec<_> = partitions
            .iter()
            .map(|rows| (rows.as_ptr(), rows.capacity()))
            .collect();
        let mut output = Vec::new();
        let result = drain_merge(&mut partitions, i32::cmp, |row| {
            if row == 2 {
                Err("closed sink")
            } else {
                output.push(row);
                Ok(())
            }
        });
        assert_eq!(result, Err("closed sink"));
        assert_eq!(output, [1]);
        assert!(partitions.iter().all(Vec::is_empty));
        assert_eq!(
            partitions
                .iter()
                .map(|rows| (rows.as_ptr(), rows.capacity()))
                .collect::<Vec<_>>(),
            storage,
        );
    }

    #[rstest]
    #[case::empty(vec![], 3, vec![])]
    #[case::zero(vec![5, 1, 3], 0, vec![])]
    #[case::one(vec![5, 1, 3], 1, vec![5])]
    #[case::prefix(vec![5, 1, 3, 3], 3, vec![5, 3, 3])]
    #[case::all(vec![5, 1, 3], 3, vec![5, 3, 1])]
    #[case::excess(vec![5, 1, 3], 4, vec![5, 3, 1])]
    fn topk_respects_the_limit_and_comparator(
        #[case] rows: Vec<i32>,
        #[case] limit: usize,
        #[case] expected: Vec<i32>,
    ) {
        assert_eq!(topk(rows, limit, |a, b| b.cmp(a)), expected);
    }
}
