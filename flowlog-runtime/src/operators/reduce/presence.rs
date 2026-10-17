//! `diff::Static` building blocks: contributions accumulated as semiring
//! weights, which can only add answers. Rows lift to weighted keys, the
//! weights settle into answers as they first appear or supersede the one
//! before, and settled weights lower back to rows.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use timely::container::DrainContainer;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use super::semiring::Semiring;
use crate::diff;
use crate::operators::map::flowlog_map;

/// Converts each row to a group key weighted by its contribution, and
/// `key_seed`, a key to answer without a row, to that key weighted by
/// the accumulator's zero, once, at the minimum timestamp, across all
/// workers.
pub(in crate::operators) fn lift<'scope, T, D, K, V, S>(
    rows: VecCollection<'scope, T, D, diff::Static>,
    name: &str,
    key_seed: Option<K>,
    contribute: impl Fn(&V) -> S + 'static,
    mut split: impl FnMut(D) -> (K, V) + 'static,
) -> VecCollection<'scope, T, K, S>
where
    T: Timestamp,
    D: Data,
    K: Data,
    S: Semiring,
{
    let mut lift_row = move |row, time, _| {
        let (key, value) = split(row);
        std::iter::once((key, time, contribute(&value)))
    };
    let Some(key) = key_seed else {
        return flowlog_map(rows, name, lift_row);
    };

    // Inject the accumulator's zero, not an input row: a row of 0 would
    // count as 1. For count/sum the zero is a live weight, so an empty
    // group is observable without changing a nonempty answer. Only worker
    // 0 emits it; the group has one default across all workers.
    let first_worker = rows.inner.scope().index() == 0;
    rows.inner
        .unary(Pipeline, name, move |capability, _| {
            // Emit from this existing operator to avoid a separate source
            // and routing later batches through a concat. The remaining cost
            // is one seed record and one Option check per activation; input
            // rows do not gain an additional branch.
            let mut seed = first_worker.then(|| (capability, (key, T::minimum(), S::zero())));
            move |input, output| {
                // This closure survives all recursive iterations. Taking the
                // seed emits it once, not once per timestamp or iteration.
                // Dropping its capability lets progress advance past the
                // minimum timestamp, even when no input ever arrives.
                if let Some((capability, row)) = seed.take() {
                    output.session(&capability).give(row);
                }
                input.for_each_time(|time, data| {
                    output.session(&time).give_iterator(
                        data.flat_map(DrainContainer::drain)
                            .flat_map(|(row, time, diff)| lift_row(row, time, diff)),
                    );
                });
            }
        })
        .as_collection()
}

/// Settles the lifted weights into answers: each group's accumulated
/// weight, emitted when it first appears and whenever it supersedes the
/// answer before.
pub(in crate::operators) fn settle<'scope, T, K, S>(
    lifted: VecCollection<'scope, T, K, S>,
) -> VecCollection<'scope, T, K, S>
where
    T: Timestamp + TotalOrder + Lattice,
    K: ExchangeData + Hashable,
    S: Semiring + ExchangeData,
{
    // Unchanged weights carry no new answer. Suppressing them avoids
    // restating a contribution downstream at later timestamps.
    lifted.threshold_semigroup(|_key, new, current| match current {
        Some(current) => new.supersedes(current).then_some(*new),
        None => (!new.is_zero()).then_some(*new),
    })
}

/// Converts settled weights back into output rows with `diff::Static`.
pub(in crate::operators) fn lower<'scope, T, K, S, O>(
    collection: VecCollection<'scope, T, K, S>,
    name: &str,
    mut merge: impl FnMut(K, S::Value) -> O + 'static,
) -> VecCollection<'scope, T, O, diff::Static>
where
    T: Timestamp,
    K: Data,
    S: Semiring,
    O: Data,
{
    flowlog_map(collection, name, move |key, time, accumulated| {
        std::iter::once((merge(key, accumulated.finish()), time, diff::Static))
    })
}
