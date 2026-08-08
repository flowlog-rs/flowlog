//! Named arranged joins and antijoins for generated FlowLog rules.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Present;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::join::join_traces;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::BatchVal;
use differential_dataflow::trace::BatchValOwn;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::containers::BatchContainer;
use timely::container::PushInto;
use timely::dataflow::operators::vec::Map;
use timely::progress::Timestamp;

use crate::operators::dedup::DedupTime;
use crate::operators::dedup::FlowlogDedupRetained;
use crate::operators::dedup::flowlog_dedup;
use crate::operators::dedup::flowlog_dedup_retained;
use crate::operators::map::flowlog_flat_map;

// =========================================================================
// Join
// =========================================================================

/// Joins two arrangements and records `name` on the resulting timely operator.
///
/// The closure receives the shared key and each matching value. Its returned
/// iterator determines the records emitted for that pair. The output weight is
/// the product of the input weights.
///
/// # Panics
///
/// Panics if the input arrangements violate Differential Dataflow's trace
/// progress invariants.
pub fn flowlog_join_core<'scope, Tr1, Tr2, I, L, R1, R2, KC>(
    arranged1: Arranged<'scope, Tr1>,
    arranged2: Arranged<'scope, Tr2>,
    name: &str,
    mut result: L,
) -> VecCollection<'scope, Tr1::Time, I::Item, <R1 as Multiply<R2>>::Output>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
    BatchCursor<Tr1>: Cursor<Diff = R1, Time = Tr1::Time, KeyContainer = KC>,
    BatchCursor<Tr2>: Cursor<Diff = R2, Time = Tr1::Time>,
    KC: BatchContainer,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    R1: Multiply<R2, Output: Semigroup + 'static> + Clone,
    I: IntoIterator<Item: Data>,
    L: FnMut(KC::ReadItem<'_>, BatchVal<'_, Tr1>, BatchVal<'_, Tr2>) -> I + 'static,
{
    let mut emit = move |key: KC::ReadItem<'_>,
                         left: BatchVal<'_, Tr1>,
                         right: BatchVal<'_, Tr2>,
                         time: Tr1::Time,
                         left_diff: &R1,
                         right_diff: &R2| {
        let diff = left_diff.clone().multiply(right_diff);
        result(key, left, right)
            .into_iter()
            .map(move |datum| (datum, time.clone(), diff.clone()))
    };

    join_traces::<
        _,
        _,
        _,
        _,
        differential_dataflow::consolidation::ConsolidatingContainerBuilder<_>,
    >(
        arranged1,
        arranged2,
        name,
        move |key, left, right, time, left_diff, right_diff, output| {
            for datum in emit(key, left, right, time, left_diff, right_diff) {
                output.push_into(datum);
            }
        },
    )
    .as_collection()
}

// =========================================================================
// Antijoin
// =========================================================================

/// Emits every `source` pair whose key is absent from `filter`, projected
/// through `logic`, recording `name` on the operators it builds.
///
/// Set difference has no diff-type expression, because `Present` cannot
/// negate: both arms are re-weighted to `+1` / `-1` `i32` and concatenated
/// so matched pairs cancel, then the survivors are clamped back to the
/// ambient diff.
///
/// Under a `Present` diff the result is append-only, since that semiring
/// has no inverse to retract with: a key arriving in `filter` after a pair
/// was emitted cannot withdraw it. Stratified negation keeps that from
/// mattering, because `filter` is complete before this runs.
///
/// # Panics
///
/// Panics if the input arrangements violate Differential Dataflow's trace
/// progress invariants.
pub fn flowlog_antijoin<'scope, Tr1, Tr2, KC, I, L, R>(
    filter: Arranged<'scope, Tr1>,
    source: Arranged<'scope, Tr2>,
    name: &str,
    logic: L,
) -> VecCollection<'scope, Tr1::Time, I::Item, R>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
    Tr1::Time: DedupTime,
    BatchCursor<Tr1>: Cursor<Diff = R, Time = Tr1::Time, KeyContainer = KC>,
    BatchCursor<Tr2>: Cursor<Diff = R, Time = Tr1::Time>,
    KC: BatchContainer,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    R: AntijoinWeight + Multiply<R, Output = R> + ExchangeData + Semigroup,
    (KC::Owned, BatchValOwn<Tr2>): ExchangeData + Hashable,
    I: IntoIterator<Item: ExchangeData + Hashable>,
    L: FnMut((KC::Owned, BatchValOwn<Tr2>)) -> I + 'static,
    VecCollection<'scope, Tr1::Time, I::Item, i32>:
        FlowlogDedupRetained<R, Output = VecCollection<'scope, Tr1::Time, I::Item, R>>,
{
    // Both arms must cancel on the same datum, so each rebuilds the owned
    // (key, value) pair from its cursor's borrowed view. Each arm is
    // finished before the next one starts, which keeps the operators in
    // the order the profiler's address prediction expects.
    let positive = R::encode_pos(source.clone().flat_map_ref(|key, val| {
        std::iter::once((
            KC::into_owned(key),
            <BatchCursor<Tr2> as Cursor>::owned_val(val),
        ))
    }));
    let negative = R::encode_neg(flowlog_join_core(filter, source, name, |key, _, val| {
        std::iter::once((
            KC::into_owned(key),
            <BatchCursor<Tr2> as Cursor>::owned_val(val),
        ))
    }));

    flowlog_dedup_retained::<_, R>(flowlog_flat_map(positive.concat(negative), name, logic))
}

/// Diff families that carry the antijoin's `+1` / `-1` weight encoding.
///
/// `i32` arms are set-ified first: duplicate derivations would otherwise
/// accumulate weights the cancelling sum cannot tell apart from a match.
/// `Present` arms are already sets, so they only take the weight.
pub trait AntijoinWeight: Sized {
    /// Set-normalizes an arm and leaves its weights positive.
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable;

    /// Set-normalizes an arm and flips its weights negative, so
    /// concatenating it subtracts.
    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable;
}

/// Overwrites every update's diff with a fixed weight.
///
/// Stays a `flat_map` rather than a `map` because the profiler's address
/// prediction counts this step as a `FlatMap`; see
/// `flowlog_profiler::plan::steps`.
fn weigh<'scope, T, D, R>(
    arm: VecCollection<'scope, T, D, R>,
    weight: i32,
) -> VecCollection<'scope, T, D, i32>
where
    T: Timestamp,
    D: Data,
    R: Semigroup + 'static,
{
    arm.inner
        .flat_map(move |(data, time, _)| std::iter::once((data, time, weight)))
        .as_collection()
}

impl AntijoinWeight for Present {
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable,
    {
        weigh(arm, 1)
    }

    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable,
    {
        weigh(arm, -1)
    }
}

impl AntijoinWeight for i32 {
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable,
    {
        flowlog_dedup(arm)
    }

    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable,
    {
        // Negate rather than overwrite: incrementally the clamped arm also
        // carries retractions, and those have to flip back to derivations.
        flowlog_dedup(arm)
            .inner
            .map_in_place(|(_, _, diff)| *diff = -*diff)
            .as_collection()
    }
}
