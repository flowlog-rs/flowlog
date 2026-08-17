//! Feeding a relation's gathered rows to its writer.
//!
//! The write direction of [`ingest`](crate::io::input::ingest): the loop
//! lives here rather than in the writer, so every sink is fed the same way
//! and only the order differs.
//!
//! One pump per shape a `.output` can ask for, in two forms. `for_each_*`
//! hands the whole row to a closure, which is what the stderr sink needs to
//! report the time. `drain_*` is that with a [`Writer`] on the end, taking
//! it by value and finishing it so no caller can skip the flush.

use std::cmp::Ordering;

use crate::error::RuntimeError;
use crate::io::output::writer::Writer;
use crate::txn::Diff;

/// One buffered output row: the slot tuple, the time it was derived at,
/// and its multiplicity.
///
/// The time is carried for the sinks that report it and ignored by the
/// rest; a comparator never reads it.
pub type Row<T, Ts> = (T, Ts, Diff);

// =============================================================================
// Ordering
// =============================================================================

/// Stream a k-way merge of pre-sorted per-worker buffers into `sink`.
///
/// Every `per_worker[i]` must already be sorted by `cmp`; correctness of
/// the merge is the caller's to establish. Head selection is a linear scan,
/// O(k) per row, which is fine because k is the worker count.
fn k_way_merge<T, F, S>(per_worker: Vec<Vec<T>>, cmp: F, mut sink: S)
where
    F: Fn(&T, &T) -> Ordering,
    S: FnMut(T),
{
    let mut iters: Vec<_> = per_worker.into_iter().map(Vec::into_iter).collect();
    let mut heads: Vec<Option<T>> = iters.iter_mut().map(Iterator::next).collect();

    while let Some(best) = heads
        .iter()
        .enumerate()
        .filter_map(|(i, h)| h.as_ref().map(|v| (i, v)))
        .min_by(|(_, a), (_, b)| cmp(a, b))
        .map(|(i, _)| i)
    {
        sink(heads[best].take().unwrap());
        heads[best] = iters[best].next();
    }
}

/// Return the top-`k` of `rows` by `cmp`, fully sorted.
///
/// Partitions with `select_nth_unstable_by` and sorts the retained prefix,
/// so which of several `cmp`-equal rows survive the cut is arbitrary.
fn topk<T, F>(mut rows: Vec<T>, k: usize, cmp: F) -> Vec<T>
where
    F: Fn(&T, &T) -> Ordering,
{
    if k == 0 {
        rows.clear();
    } else if rows.len() > k {
        rows.select_nth_unstable_by(k, |a, b| cmp(a, b));
        rows.truncate(k);
    }
    rows.sort_by(|a, b| cmp(a, b));
    rows
}

// =============================================================================
// Pumps
// =============================================================================

/// Hand every row to `f`, worker by worker, in the order they were flushed.
///
/// The general form: `f` sees the whole row, including the time, which the
/// stderr sink reports and every other sink ignores. [`drain_flat`] is this
/// with a [`Writer`] on the end.
pub fn for_each_flat<T, Ts, F: FnMut(T, Ts, Diff)>(per_worker: Vec<Vec<Row<T, Ts>>>, mut f: F) {
    for buffer in per_worker {
        for (tuple, time, diff) in buffer {
            f(tuple, time, diff);
        }
    }
}

/// Hand every row to `f` in `cmp` order.
pub fn for_each_sorted<T, Ts, F, C>(mut per_worker: Vec<Vec<Row<T, Ts>>>, cmp: C, mut f: F)
where
    F: FnMut(T, Ts, Diff),
    C: Fn(&Row<T, Ts>, &Row<T, Ts>) -> Ordering,
{
    for buffer in &mut per_worker {
        buffer.sort_by(&cmp);
    }
    // The merge comparator must be the one the runs were sorted with, or
    // the merge silently interleaves them wrong.
    k_way_merge(per_worker, &cmp, |(tuple, time, diff)| f(tuple, time, diff));
}

/// Hand the first `n` rows in `cmp` order to `f`.
pub fn for_each_topk<T, Ts, F, C>(per_worker: Vec<Vec<Row<T, Ts>>>, n: usize, cmp: C, mut f: F)
where
    F: FnMut(T, Ts, Diff),
    C: Fn(&Row<T, Ts>, &Row<T, Ts>) -> Ordering,
{
    let all: Vec<Row<T, Ts>> = per_worker.into_iter().flatten().collect();
    for (tuple, time, diff) in topk(all, n, cmp) {
        f(tuple, time, diff);
    }
}

/// Write every row, worker by worker, in the order they were flushed.
///
/// The order across workers is whichever won the flush lock, so a relation
/// that needs a reproducible order asks for `ORDER BY` and takes
/// [`drain_sorted`] instead.
pub fn drain_flat<T, Ts, W: Writer<T>>(
    per_worker: Vec<Vec<Row<T, Ts>>>,
    mut writer: W,
    with_diff: bool,
) -> Result<W::Out, RuntimeError> {
    for_each_flat(per_worker, |tuple, _time, diff| {
        writer.push(tuple, with_diff.then_some(diff));
    });
    writer.finish()
}

/// Write every row in `cmp` order.
///
/// Sorts each worker's rows, then merges the sorted runs, which is cheaper
/// than sorting the concatenation and is what makes the result independent
/// of the order the workers flushed in.
pub fn drain_sorted<T, Ts, W, F>(
    per_worker: Vec<Vec<Row<T, Ts>>>,
    cmp: F,
    mut writer: W,
    with_diff: bool,
) -> Result<W::Out, RuntimeError>
where
    W: Writer<T>,
    F: Fn(&Row<T, Ts>, &Row<T, Ts>) -> Ordering,
{
    for_each_sorted(per_worker, cmp, |tuple, _time, diff| {
        writer.push(tuple, with_diff.then_some(diff));
    });
    writer.finish()
}

/// Write the first `n` rows in `cmp` order.
///
/// Which of several `cmp`-equal rows survive the cut is arbitrary, because
/// the selection is unstable. Peak memory is the whole relation regardless
/// of `n`: every worker's rows are gathered before any are discarded.
pub fn drain_topk<T, Ts, W, F>(
    per_worker: Vec<Vec<Row<T, Ts>>>,
    n: usize,
    cmp: F,
    mut writer: W,
    with_diff: bool,
) -> Result<W::Out, RuntimeError>
where
    W: Writer<T>,
    F: Fn(&Row<T, Ts>, &Row<T, Ts>) -> Ordering,
{
    for_each_topk(per_worker, n, cmp, |tuple, _time, diff| {
        writer.push(tuple, with_diff.then_some(diff));
    });
    writer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::output::writer::vec::DeltaWriter;
    use crate::io::output::writer::vec::VecWriter;
    use crate::io::spec::OutputSpec;

    fn spec() -> OutputSpec<'static> {
        OutputSpec {
            relation: "Out",
            path: "",
            delim: b'\t',
        }
    }

    fn writer() -> VecWriter<(i32,)> {
        Writer::<(i32,)>::open(&spec()).expect("open")
    }

    /// Two workers' rows, `[[3, 1], [2]]`, with a distinct diff per row.
    fn buffers() -> Vec<Vec<Row<(i32,), u32>>> {
        vec![vec![((3,), 0, 10), ((1,), 0, 20)], vec![((2,), 0, 30)]]
    }

    fn by_first(a: &Row<(i32,), u32>, b: &Row<(i32,), u32>) -> Ordering {
        a.0.0.cmp(&b.0.0)
    }

    /// The flat pump preserves each worker's order and visits workers in
    /// buffer order.
    #[test]
    fn flat_keeps_worker_then_row_order() {
        let rows = drain_flat(buffers(), writer(), false).expect("drain");
        assert_eq!(rows, vec![(3,), (1,), (2,)]);
    }

    /// The sorted pump orders across workers, not just within one.
    #[test]
    fn sorted_orders_across_workers() {
        let rows = drain_sorted(buffers(), by_first, writer(), false).expect("drain");
        assert_eq!(rows, vec![(1,), (2,), (3,)]);
    }

    /// Top-k keeps the ordered prefix and drops the rest.
    #[test]
    fn topk_keeps_the_ordered_prefix() {
        let rows = drain_topk(buffers(), 2, by_first, writer(), false).expect("drain");
        assert_eq!(rows, vec![(1,), (2,)]);
    }

    /// A limit of zero writes nothing, and still finishes the writer.
    #[test]
    fn a_zero_limit_writes_nothing() {
        let rows = drain_topk(buffers(), 0, by_first, writer(), false).expect("drain");
        assert!(rows.is_empty());
    }

    /// Each row keeps the diff it was buffered with, through whichever
    /// pump reordered it.
    #[test]
    fn diffs_follow_their_rows_through_reordering() {
        let writer: DeltaWriter<(i32,)> = Writer::<(i32,)>::open(&spec()).expect("open");
        let rows = drain_sorted(buffers(), by_first, writer, true).expect("drain");
        assert_eq!(rows, vec![((1,), 20), ((2,), 30), ((3,), 10)]);
    }

    /// With `with_diff` off the sink is told nothing about multiplicity,
    /// which is how a batch drain reaches a delta-shaped sink.
    #[test]
    fn diffs_are_withheld_when_the_sink_writes_none() {
        let writer: DeltaWriter<(i32,)> = Writer::<(i32,)>::open(&spec()).expect("open");
        let rows = drain_flat(buffers(), writer, false).expect("drain");
        assert_eq!(rows, vec![((3,), 1), ((1,), 1), ((2,), 1)]);
    }

    /// A relation that derived nothing still finishes its writer.
    #[test]
    fn no_rows_still_finishes() {
        let rows = drain_flat(Vec::<Vec<Row<(i32,), u32>>>::new(), writer(), false).expect("drain");
        assert!(rows.is_empty());
    }
}
