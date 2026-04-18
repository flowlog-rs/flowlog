//! Sort/merge helpers for `ORDER BY` / `LIMIT` on generated IDB output.
//!
//! Used by both binary mode (drain → sort → write file) and library mode
//! (drain → sort → populate `BatchResults`). Pure algorithms over
//! user-supplied comparators — no allocation beyond the inputs.

use std::cmp::Ordering;

/// Stream a k-way merge of pre-sorted per-worker buffers into `sink`.
///
/// Each `per_worker[i]` must already be sorted by `cmp`. Repeatedly pops the
/// smallest current head across all buffers and feeds it to `sink` until all
/// buffers are drained. Linear-scan head selection (O(k) per element) — fine
/// for k = number of timely workers (small).
pub fn k_way_merge<T, F, S>(per_worker: Vec<Vec<T>>, cmp: F, mut sink: S)
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
/// Uses `select_nth_unstable_by` for O(n) partitioning then sorts the
/// retained prefix. Falls back to a full sort when `rows.len() <= k`.
pub fn topk<T, F>(mut rows: Vec<T>, k: usize, cmp: F) -> Vec<T>
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
