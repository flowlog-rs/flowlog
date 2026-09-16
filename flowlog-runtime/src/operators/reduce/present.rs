//! Present aggregation through semiring weights.
//!
//! The in-loop reduction and final recursive fold share contribution lifting
//! and conversion back to rows.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Present;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use timely::container::DrainContainer;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use super::Aggregation;
use super::ReduceStrategy;
use super::semiring::Scalar;
use super::semiring::Semiring;
use crate::operators::map::flowlog_map;

impl<T: Timestamp + TotalOrder + Lattice> ReduceStrategy<T> for Present {
    fn reduce<'scope, D, K, V, S, O>(
        collection: VecCollection<'scope, T, D, Self>,
        name: &str,
        empty_group: Option<(K, S::Value)>,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, Self>
    where
        D: Data,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
    {
        let lifted = lift(collection, name, empty_group, contribute, split);
        // Unchanged aggregate weights carry no new answer. Suppressing them
        // avoids restating a contribution downstream at later timestamps.
        let thresholded = lifted.threshold_semigroup(|_key, new, current| match current {
            Some(current) => new.supersedes(current).then_some(*new),
            None => (!new.is_zero()).then_some(*new),
        });
        lower(thresholded, name, merge)
    }
}

/// Produces each group's final `Present` result after recursion finishes.
///
/// Count, sum, and average require rows before the in-scope aggregation,
/// so each original contribution is folded once. Min and max may instead
/// consume intermediate bounds, which preserve the final extreme.
///
/// The input and `empty_key` follow the contracts of [`super::flowlog_reduce`].
pub fn flowlog_reduce_leave<'inner, 'outer, A, TInner, TOuter, D, K, V, C, O>(
    collection: VecCollection<'inner, TInner, D, Present>,
    outer: Scope<'outer, TOuter>,
    name: &str,
    _aggregation: A,
    empty_key: Option<K>,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'outer, TOuter, O, Present>
where
    A: Aggregation<V, C>,
    C: Scalar,
    TInner: Timestamp + Refines<TOuter>,
    TOuter: Timestamp + Lattice,
    D: Data,
    K: ExchangeData + Hashable,
    V: Data,
    A::Semiring: ExchangeData,
    O: Data,
{
    // Collapse iteration timestamps before converting contributions to rows,
    // so consolidation produces one final answer. Count/sum's raw input
    // branch has no in-loop seed; its own neutral default covers empty input.
    let lifted = lift(
        collection,
        name,
        empty_key.zip(A::EMPTY_RESULT),
        A::contribute,
        split,
    )
    .leave(outer);
    lower(lifted.consolidate(), name, merge)
}

/// Converts each row to a group key weighted by its contribution.
/// An optional empty-group contribution appears once, at the minimum
/// timestamp, across all workers.
fn lift<'scope, T, D, K, V, S>(
    collection: VecCollection<'scope, T, D, Present>,
    name: &str,
    empty_group: Option<(K, S::Value)>,
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
    let Some((key, empty)) = empty_group else {
        return flowlog_map(collection, name, lift_row);
    };

    // Inject a contribution, not an input row: count(0) would contribute 1.
    // For count/sum, the lifted 0 remains a live weight, so an empty group
    // is observable without changing a nonempty answer.
    // Only worker 0 emits it; the group has one default across all workers.
    let first_worker = collection.inner.scope().index() == 0;
    collection
        .inner
        .unary(Pipeline, name, move |capability, _| {
            // Emit from this existing operator to avoid a separate source
            // and routing later batches through a concat. The remaining cost
            // is one seed record and one Option check per activation; input
            // rows do not gain an additional branch.
            let mut seed = first_worker.then(|| (capability, (key, T::minimum(), S::lift(empty))));
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

/// Converts settled aggregate weights back into output rows with `Present`.
fn lower<'scope, T, K, S, O>(
    collection: VecCollection<'scope, T, K, S>,
    name: &str,
    mut merge: impl FnMut(K, S::Value) -> O + 'static,
) -> VecCollection<'scope, T, O, Present>
where
    T: Timestamp,
    K: Data,
    S: Semiring,
    O: Data,
{
    flowlog_map(collection, name, move |key, time, aggregate| {
        std::iter::once((merge(key, aggregate.finish()), time, Present))
    })
}
