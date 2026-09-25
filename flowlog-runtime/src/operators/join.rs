//! Named arranged joins and antijoins for generated FlowLog rules.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Present;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
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
use timely::order::Product;
use timely::progress::Timestamp;

use crate::operators::dedup::Epoch;
use crate::operators::dedup::FlowlogDedup;
use crate::operators::dedup::first_occurrences;
use crate::operators::dedup::flowlog_dedup;
use crate::operators::map::flowlog_map;
use crate::operators::map::flowlog_map_in_place;

// =============================================================================
// Join
// =============================================================================

/// An equijoin of two arrangements sharing a common key type, recorded on
/// the timely operator under the name FlowLog gives the join step.
///
/// `result` extracts an iterator from each matching pair, and its full
/// contents are emitted with the product of the two input weights.
/// Correctness depends heavily on the behavior of `result`.
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

// =============================================================================
// Antijoin
// =============================================================================

/// Emits every `source` pair whose key is absent from `filter`, mapped
/// through `logic`, under the name FlowLog gives the step.
///
/// Both arms are encoded as signed membership updates so matching pairs
/// cancel, then survivors are clamped to the output weight. Signed inputs
/// must have nonnegative accumulated multiplicities.
///
/// With two `Present` inputs, `filter` must be a fixed key-only set. Each
/// key must occur once in its arranged history, at a time less than or
/// equal to every matching source update. Source pairs may recur. Repeated
/// filter occurrences cause extra subtraction; later additions that block
/// earlier source rows require retractions this output cannot represent.
/// Stratified batch negation satisfies these requirements.
pub fn flowlog_antijoin<'scope, Tr1, Tr2, KC, D, L, R>(
    filter: Arranged<'scope, Tr1>,
    source: Arranged<'scope, Tr2>,
    name: &str,
    mut logic: L,
) -> VecCollection<'scope, Tr1::Time, D, R>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
    BatchCursor<Tr1>: Cursor<Diff = R, Time = Tr1::Time, KeyContainer = KC>,
    BatchCursor<Tr2>: Cursor<Diff = R, Time = Tr1::Time>,
    KC: BatchContainer,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    R: AntijoinWeight
        + AntijoinOutput<Tr1::Time>
        + Multiply<R, Output = R>
        + ExchangeData
        + Semigroup,
    (KC::Owned, BatchValOwn<Tr2>): ExchangeData + Hashable,
    D: ExchangeData + Hashable,
    L: FnMut((KC::Owned, BatchValOwn<Tr2>)) -> D + 'static,
    VecCollection<'scope, Tr1::Time, (KC::Owned, BatchValOwn<Tr2>), i32>: FlowlogDedup,
    VecCollection<'scope, Tr1::Time, D, i32>: FlowlogDedup,
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
    R::decode(projected)
}

/// The weight families an antijoin arm can carry, each knowing how to
/// encode itself as the `+1` / `-1` the cancellation needs.
///
/// `i32` arms are set-normalized first: duplicate derivations would
/// otherwise accumulate weights the cancelling sum cannot tell apart from
/// a match. `Present` arms use the unit weight under the input guarantees
/// of [`flowlog_antijoin`].
pub trait AntijoinWeight: Sized {
    /// Encodes an arm at `+1`, so concatenating it adds.
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup;

    /// Encodes an arm at `-1`, so concatenating it subtracts.
    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup;
}

impl AntijoinWeight for Present {
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup,
    {
        flowlog_map(arm, name, |data, t, _| std::iter::once((data, t, 1)))
    }

    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup,
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
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup,
    {
        flowlog_dedup(arm)
    }

    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, i32>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup,
    {
        // Negate rather than overwrite: incrementally the clamped arm also
        // carries retractions, and those have to flip back to derivations.
        flowlog_map_in_place(flowlog_dedup(arm), name, |_, _, diff| *diff = -*diff)
    }
}

/// Restores set membership after antijoin's signed cancellation.
/// Presence output requires the input guarantees of [`flowlog_antijoin`].
pub trait AntijoinOutput<T: Timestamp + Lattice>: Sized {
    /// Decodes the projected difference of the source and matching pairs.
    fn decode<'scope, D>(
        rows: VecCollection<'scope, T, D, i32>,
    ) -> VecCollection<'scope, T, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup;
}

impl AntijoinOutput<()> for Present {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, (), D, i32>,
    ) -> VecCollection<'scope, (), D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, (), D, i32>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(Present))
    }
}

impl<T: Epoch> AntijoinOutput<T> for Present {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, T, D, i32>,
    ) -> VecCollection<'scope, T, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(Present))
    }
}

impl<I: Epoch> AntijoinOutput<Product<(), I>> for Present {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, Product<(), I>, D, i32>,
    ) -> VecCollection<'scope, Product<(), I>, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, Product<(), I>, D, i32>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(Present))
    }
}

impl<E: Epoch, I: Epoch> AntijoinOutput<Product<E, I>> for Present {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, Product<E, I>, D, i32>,
    ) -> VecCollection<'scope, Product<E, I>, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, Product<E, I>, D, i32>: FlowlogDedup,
    {
        first_occurrences(rows)
    }
}

impl<T: Timestamp + Lattice> AntijoinOutput<T> for i32 {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, T, D, i32>,
    ) -> VecCollection<'scope, T, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, i32>: FlowlogDedup,
    {
        flowlog_dedup(rows)
    }
}
