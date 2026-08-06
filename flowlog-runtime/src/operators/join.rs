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

use crate::operators::dedup::DedupTime;
use crate::operators::dedup::FlowlogDedupRetained;
use crate::operators::dedup::flowlog_dedup;
use crate::operators::dedup::flowlog_dedup_retained;
use crate::operators::map::flowlog_map;
use crate::operators::map::flowlog_map_in_place;

// =========================================================================
// Join
// =========================================================================

/// An equijoin of two arrangements sharing a common key type, under the
/// name FlowLog gives the join step.
///
/// `result` extracts an iterator from each matching pair, and its full
/// contents are emitted with the product of the two input weights.
/// Correctness depends heavily on the behavior of `result`.
///
/// `name` is accepted but not yet recorded on the operator: released
/// differential-dataflow hard-codes `Join`. It reaches `join_traces` once
/// upstream ships the hook, without touching any call site.
pub fn flowlog_join<'scope, Tr1, Tr2, I, L, R1, R2, KC>(
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
    // Recording the name waits on a differential-dataflow release whose
    // `join_traces` takes one; the pinned bump is parked in PR #281.
    let _ = name;

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

/// Emits every `source` pair whose key is absent from `filter`, mapped
/// through `logic`, under the name FlowLog gives the step.
///
/// The difference is taken by arithmetic rather than by a diff type,
/// because `Present` cannot negate: both arms are re-weighted to `+1` and
/// `-1` `i32` and concatenated so matched pairs cancel, then the survivors
/// are clamped back to the ambient weight.
///
/// Under a `Present` weight the result is append-only, since that semiring
/// has no inverse: a key arriving in `filter` after a pair was emitted
/// cannot withdraw it. Stratified negation keeps that from mattering,
/// because `filter` is complete before this runs.
pub fn flowlog_antijoin<'scope, Tr1, Tr2, KC, D, L, R>(
    filter: Arranged<'scope, Tr1>,
    source: Arranged<'scope, Tr2>,
    name: &str,
    mut logic: L,
) -> VecCollection<'scope, Tr1::Time, D, R>
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
    D: ExchangeData + Hashable,
    L: FnMut((KC::Owned, BatchValOwn<Tr2>)) -> D + 'static,
    VecCollection<'scope, Tr1::Time, D, i32>:
        FlowlogDedupRetained<R, Output = VecCollection<'scope, Tr1::Time, D, R>>,
{
    // Both arms must cancel on the same datum, so each rebuilds the owned
    // (key, value) pair from its cursor's borrowed view. Each arm is
    // finished before the next one starts, which keeps the operators in
    // the order address prediction expects.
    let positive = R::encode_pos(
        source.clone().flat_map_ref(|key, val| {
            std::iter::once((
                KC::into_owned(key),
                <BatchCursor<Tr2> as Cursor>::owned_val(val),
            ))
        }),
        name,
    );
    let negative = R::encode_neg(
        flowlog_join(filter, source, name, |key, _, val| {
            std::iter::once((
                KC::into_owned(key),
                <BatchCursor<Tr2> as Cursor>::owned_val(val),
            ))
        }),
        name,
    );

    // The projection maps one pair to one row, so the timestamp and weight
    // it arrived with move straight through. Taking a row projection rather
    // than an update one also keeps the `+1` / `-1` encoding above from
    // reaching the caller, which never sees a weight of its own.
    let projected = flowlog_map(positive.concat(negative), name, move |data, t, d| {
        std::iter::once((logic(data), t, d))
    });
    flowlog_dedup_retained::<_, R>(projected)
}

/// The weight families an antijoin arm can carry, each knowing how to
/// encode itself as the `+1` / `-1` the cancellation needs.
///
/// `i32` arms are set-normalized first: duplicate derivations would
/// otherwise accumulate weights the cancelling sum cannot tell apart from
/// a match. `Present` arms are already sets, so they only take the weight.
pub trait AntijoinWeight: Sized {
    /// Encodes an arm at `+1`, so concatenating it adds.
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable;

    /// Encodes an arm at `-1`, so concatenating it subtracts.
    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable;
}

impl AntijoinWeight for Present {
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable,
    {
        flowlog_map(arm, name, |data, t, _| std::iter::once((data, t, 1)))
    }

    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable,
    {
        flowlog_map(arm, name, |data, t, _| std::iter::once((data, t, -1)))
    }
}

impl AntijoinWeight for i32 {
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        _name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable,
    {
        flowlog_dedup(arm)
    }

    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: DedupTime,
        D: ExchangeData + Hashable,
    {
        // Negate rather than overwrite: incrementally the clamped arm also
        // carries retractions, and those have to flip back to derivations.
        flowlog_map_in_place(flowlog_dedup(arm), name, |_, _, diff| *diff = -*diff)
    }
}
