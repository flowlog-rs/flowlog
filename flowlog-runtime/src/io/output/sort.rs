//! Output ordering with supplied comparators.
//!
//! [`drain_merge`] merges sorted partitions while retaining their storage.
//! [`topk`] selects and sorts a limited prefix.

use std::cmp::Ordering;
use std::mem;

/// Drains sorted partitions in merge order, retaining their allocations.
/// A sink error stops delivery and discards the remaining rows.
///
/// Each partition must already be sorted by `cmp`. Equal keys keep partition
/// order. For k >= 2 partitions, head selection uses at most k - 1 initial
/// comparisons and ceil(log2 k) per row, with O(k) auxiliary storage.
pub(super) fn drain_merge<T, F, S, E>(
    per_worker: &mut [Vec<T>],
    cmp: F,
    mut sink: S,
) -> Result<(), E>
where
    F: Fn(&T, &T) -> Ordering,
    S: FnMut(T) -> Result<(), E>,
{
    match per_worker {
        [] => return Ok(()),
        [rows] => return rows.drain(..).try_for_each(sink),
        _ => {}
    }
    let k = per_worker.len();
    let mut iters: Vec<_> = per_worker.iter_mut().map(|rows| rows.drain(..)).collect();
    let mut heads: Vec<Option<T>> = iters.iter_mut().map(Iterator::next).collect();
    let precedes = |heads: &[Option<T>], a: usize, b: usize| match (&heads[a], &heads[b]) {
        (Some(x), Some(y)) => cmp(x, y).then(a.cmp(&b)).is_lt(),
        (Some(_), None) => true,
        (None, Some(_) | None) => false,
    };

    // Leaf k + i represents partition i. Each internal node stores the loser
    // between its children's winners; only the overall winner's path needs
    // replaying when its head changes.
    let mut losers = vec![0; k];
    let mut winner = {
        let mut winners = vec![0; k];
        winners.extend(0..k);
        let nodes = (1..k).rev();
        for node in nodes {
            let (left, right) = (winners[2 * node], winners[2 * node + 1]);
            (winners[node], losers[node]) = if precedes(&heads, left, right) {
                (left, right)
            } else {
                (right, left)
            };
        }
        winners[1]
    };
    // An exhausted winner means that every partition is exhausted.
    while let Some(row) = heads[winner].take() {
        sink(row)?;
        heads[winner] = iters[winner].next();
        let mut node = (k + winner) / 2;
        while node > 0 {
            if precedes(&heads, losers[node], winner) {
                mem::swap(&mut losers[node], &mut winner);
            }
            node /= 2;
        }
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
    use std::cell::Cell;
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

    // Cases: partitions, expected rows.
    #[rstest]
    #[case::two(
        vec![vec![(1, "a"), (1, "b"), (2, "c")], vec![(1, "d"), (2, "e")]],
        vec![(1, "a"), (1, "b"), (1, "d"), (2, "c"), (2, "e")],
    )]
    #[case::five(
        vec![vec![(1, "a"), (1, "b"), (3, "c")], vec![], vec![(1, "d")],
             vec![(2, "e")], vec![(1, "f"), (3, "g")]],
        vec![(1, "a"), (1, "b"), (1, "d"), (1, "f"), (2, "e"), (3, "c"), (3, "g")],
    )]
    #[case::eight(
        vec![vec![], vec![(2, "a")], vec![(1, "b"), (1, "c")], vec![],
             vec![(1, "d")], vec![(3, "e")], vec![(2, "f")], vec![(1, "g")]],
        vec![(1, "b"), (1, "c"), (1, "d"), (1, "g"), (2, "a"), (2, "f"), (3, "e")],
    )]
    fn equal_merge_keys_keep_partition_order(
        #[case] mut partitions: Vec<Vec<(i32, &str)>>,
        #[case] expected: Vec<(i32, &str)>,
    ) {
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
        assert_eq!(output, expected);
    }

    // Cases: partitions, rows per partition, comparison budget.
    #[rstest]
    #[case(2, 50, 1 + 100)]
    #[case(5, 20, 4 + 100 * 3)]
    #[case(32, 10, 31 + 320 * 5)]
    fn merge_selects_each_row_in_logarithmic_comparisons(
        #[case] k: usize,
        #[case] rows: usize,
        #[case] budget: usize,
    ) {
        let mut partitions: Vec<Vec<usize>> = (0..k)
            .map(|p| (0..rows).map(|j| j * k + p).collect())
            .collect();
        let comparisons = Cell::new(0);
        drain_merge(
            &mut partitions,
            |a, b| {
                comparisons.set(comparisons.get() + 1);
                a.cmp(b)
            },
            |_| Ok::<_, Infallible>(()),
        )
        .expect("merge rows");
        assert!(
            comparisons.get() <= budget,
            "{} comparisons exceed the {budget} budget for {k} partitions of {rows} rows",
            comparisons.get()
        );
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
