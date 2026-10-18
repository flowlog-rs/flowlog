//! Differential's collection operators that arrange on the way, on
//! FlowLog's routing: [`route`] decides the worker every arrangement sends
//! an update to, and each function here is differential's operator of the
//! same name with that one exchange swapped in. The set arrangement in
//! `arrange` builds its exchange on [`route`] as well.

use std::hash::BuildHasher;
use std::hash::Hash;

use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Abelian;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::implementations::KeyBatcher;
use differential_dataflow::trace::implementations::KeyBuilder;
use differential_dataflow::trace::implementations::KeySpine;
use differential_dataflow::trace::implementations::ValBatcher;
use differential_dataflow::trace::implementations::ValBuilder;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use timely::dataflow::channels::pact::Exchange;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

/// One update of a `(key, value)` collection.
pub(super) type Update<K, V, T, R> = ((K, V), T, R);

/// A container of updates. A chunk of a sealed chain holds them sorted
/// by pair, then by time.
pub(super) type Chunk<K, V, T, R> = Vec<Update<K, V, T, R>>;

/// Returns the number that routes an update of a `(key, value)`
/// collection to a worker, from its key: the same for a key on every
/// worker and in every process of one run, and with its low bits as well
/// mixed as its high ones, since timely masks a power-of-two worker
/// count.
pub(super) fn route<K: Hash, V, T, R>(update: &Update<K, V, T, R>) -> u64 {
    // Fixed seeds rather than foldhash's random ones: every worker must
    // send a key to the same place, and a random seed differs per process.
    // foldhash rather than differential's FNV: FNV's low bits depend on
    // the input's low bits alone, so keys whose columns are correlated,
    // such as `(x, x)`, reach a quarter of 32 workers (issue #432). A
    // folded multiply mixes every input bit into every output bit, in a
    // sixth of FNV's time per key. The cost is an output that foldhash
    // does not keep stable across its versions or across platforms, which
    // routing never needs: one run computes it on one binary.
    foldhash::fast::FixedState::default().hash_one(&(update.0).0)
}

/// Arranges a `(key, value)` collection by key.
pub(super) fn arrange_by_key<'scope, T, K, V, R>(
    rows: VecCollection<'scope, T, (K, V), R>,
    name: &str,
) -> Arranged<'scope, TraceAgent<ValSpine<K, V, T, R>>>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hash,
    V: ExchangeData,
    R: ExchangeData + Semigroup,
{
    arrange_core::<
        _,
        _,
        ContainerChunker<Chunk<K, V, T, R>>,
        ValBatcher<K, V, T, R>,
        ValBuilder<K, V, T, R>,
        _,
    >(rows.inner, Exchange::new(route), name)
}

/// Arranges a key-only collection by itself.
pub(super) fn arrange_by_self<'scope, T, K, R>(
    rows: VecCollection<'scope, T, K, R>,
    name: &str,
) -> Arranged<'scope, TraceAgent<KeySpine<K, T, R>>>
where
    T: Timestamp + Lattice + Ord,
    K: ExchangeData + Hash,
    R: ExchangeData + Semigroup,
{
    arrange_core::<
        _,
        _,
        ContainerChunker<Chunk<K, (), T, R>>,
        KeyBatcher<K, T, R>,
        KeyBuilder<K, T, R>,
        _,
    >(rows.map(|key| (key, ())).inner, Exchange::new(route), name)
}

/// Sums each row's updates at a time and drops the rows that sum to zero.
pub(super) fn consolidate<'scope, T, D, R>(
    rows: VecCollection<'scope, T, D, R>,
) -> VecCollection<'scope, T, D, R>
where
    T: Timestamp + Lattice + Ord,
    D: ExchangeData + Hash,
    R: ExchangeData + Semigroup,
{
    arrange_by_self(rows, "Consolidate").as_collection(|key, &()| key.clone())
}

/// Reweighs each row's accumulated updates under a total order:
/// `thresh` sees the row, its new accumulation, and its previous one if
/// any, and returns the update to emit, or none.
pub(super) fn threshold_semigroup<'scope, T, K, R, R2, F>(
    rows: VecCollection<'scope, T, K, R>,
    thresh: F,
) -> VecCollection<'scope, T, K, R2>
where
    T: Timestamp + TotalOrder + Lattice + Ord,
    K: ExchangeData + Hash,
    R: ExchangeData + Semigroup,
    R2: Semigroup + 'static,
    F: FnMut(&K, &R, Option<&R>) -> Option<R2> + 'static,
{
    arrange_by_self(rows, "Arrange: ThresholdTotal").threshold_semigroup(thresh)
}

/// Reweighs each row's accumulated updates under a total order, emitting
/// the change from `thresh`'s previous answer to its new one.
pub(super) fn threshold_total<'scope, T, K, R, R2, F>(
    rows: VecCollection<'scope, T, K, R>,
    thresh: F,
) -> VecCollection<'scope, T, K, R2>
where
    T: Timestamp + TotalOrder + Lattice + Ord,
    K: ExchangeData + Hash,
    R: ExchangeData + Semigroup,
    R2: Abelian + 'static,
    F: FnMut(&K, &R) -> R2 + 'static,
{
    arrange_by_self(rows, "Arrange: ThresholdTotal").threshold_total(thresh)
}

/// Reweighs each row's accumulated updates under any order, emitting the
/// change from `thresh`'s previous answer to its new one; keeps an output
/// trace to correct answers at incomparable times.
pub(super) fn threshold<'scope, T, K, R, R2, F>(
    rows: VecCollection<'scope, T, K, R>,
    mut thresh: F,
) -> VecCollection<'scope, T, K, R2>
where
    T: Timestamp + Lattice + Ord,
    K: ExchangeData + Hash,
    R: ExchangeData + Semigroup,
    R2: Ord + Abelian + 'static,
    F: FnMut(&K, &R) -> R2 + 'static,
{
    arrange_by_self(rows, "Arrange: Threshold")
        .reduce_abelian::<_, KeyBuilder<K, T, R2>, KeySpine<K, T, R2>, _, _>(
            "Threshold",
            // A key-only arrangement holds one value per key, and the
            // reduce calls this only for a key with input, so the group
            // is exactly `input[0]`.
            move |key, input, output| output.push(((), thresh(key, &input[0].1))),
            |buffer, key, updates| {
                buffer.clear();
                buffer.extend(
                    updates
                        .drain(..)
                        .map(|(value, time, diff)| ((key.clone(), value), time, diff)),
                );
            },
        )
        .as_collection(|key, &()| key.clone())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Timely masks a power-of-two worker count onto the route's low
    /// bits. Keys whose columns are correlated must still reach every
    /// worker in a near-even share; under differential's FNV, `(x, x)`
    /// reached eight of thirty-two (issue #432).
    // Cases: key shape.
    #[rstest]
    #[case::diagonal(|x: i32| (x, x))]
    #[case::successor(|x: i32| (x, x + 1))]
    #[case::scaled(|x: i32| (x.wrapping_mul(4096), x.wrapping_mul(4096)))]
    fn correlated_keys_reach_every_worker(#[case] key: fn(i32) -> (i32, i32)) {
        const KEYS: usize = 1_000_000;
        const WORKERS: usize = 32;
        let mut shares = [0usize; WORKERS];
        for x in 0..KEYS {
            let update = ((key(x as i32), ()), (), 1);
            shares[(route(&update) & (WORKERS as u64 - 1)) as usize] += 1;
        }
        let largest = shares.iter().copied().max().unwrap_or_default();
        assert!(shares.iter().all(|&share| share > 0), "{shares:?}");
        assert!(largest < 2 * KEYS / WORKERS, "{shares:?}");
    }
}
