//! Named arranged joins and antijoins for generated FlowLog rules.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Multiply;
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

use crate::diff;
use crate::operators::dedup::Counter;
use crate::operators::dedup::FlowlogDedup;
use crate::operators::dedup::first_occurrences;
use crate::operators::dedup::flowlog_dedup;
use crate::operators::map::flowlog_map;
use crate::operators::map::flowlog_map_in_place;
use crate::time::LexLoop;

// =============================================================================
// Join
// =============================================================================

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

// =============================================================================
// Antijoin
// =============================================================================

/// Emits every `source` pair whose key is absent from `filter`, mapped
/// through `logic`, under the name FlowLog gives the step.
///
/// Both arms are encoded as signed membership updates so matching pairs
/// cancel, then survivors are clamped to the output weight. The output
/// weight is the product of the filter's and the source's, so the output
/// is `diff::Static` only when both inputs are. Signed inputs must have
/// nonnegative accumulated multiplicities.
///
/// With two `diff::Static` inputs, `filter` must be a fixed key-only set. Each
/// key must occur once in its arranged history, at a time less than or
/// equal to every matching source update. Source pairs may recur. Repeated
/// filter occurrences cause extra subtraction; later additions that block
/// earlier source rows require retractions this output cannot represent.
/// Stratified negation over static relations satisfies these requirements.
pub fn flowlog_antijoin<'scope, Tr1, Tr2, KC, D, L, Rf, Rs, R>(
    filter: Arranged<'scope, Tr1>,
    source: Arranged<'scope, Tr2>,
    name: &str,
    mut logic: L,
) -> VecCollection<'scope, Tr1::Time, D, R>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
    BatchCursor<Tr1>: Cursor<Diff = Rf, Time = Tr1::Time, KeyContainer = KC>,
    BatchCursor<Tr2>: Cursor<Diff = Rs, Time = Tr1::Time>,
    KC: BatchContainer,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    Rf: Multiply<Rs, Output = R> + Clone,
    Rs: AntijoinWeight + Semigroup + 'static,
    R: AntijoinWeight + AntijoinOutput<Tr1::Time> + ExchangeData + Semigroup,
    (KC::Owned, BatchValOwn<Tr2>): ExchangeData + Hashable,
    D: ExchangeData + Hashable,
    L: FnMut((KC::Owned, BatchValOwn<Tr2>)) -> D + 'static,
    VecCollection<'scope, Tr1::Time, (KC::Owned, BatchValOwn<Tr2>), diff::Mutable>: FlowlogDedup,
    VecCollection<'scope, Tr1::Time, D, diff::Mutable>: FlowlogDedup,
{
    // Both arms must cancel on the same datum, so each rebuilds the owned
    // (key, value) pair from its cursor's borrowed view. Each arm is
    // finished before the next one starts, which keeps the operators in
    // the order address prediction expects.
    let positive = Rs::encode_pos(
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
/// `diff::Mutable` arms are set-normalized first: duplicate derivations would
/// otherwise accumulate weights the cancelling sum cannot tell apart from
/// a match. `diff::Static` arms use the unit weight under the input
/// guarantees of [`flowlog_antijoin`]. A static source under a mutable
/// filter needs no normalization either: the arranged source holds each
/// static pair once, since every update of a static collection is at its
/// scope's minimum time, where presence consolidates.
pub trait AntijoinWeight: Sized {
    /// Encodes an arm at `+1`, so concatenating it adds.
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, diff::Mutable>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup;

    /// Encodes an arm at `-1`, so concatenating it subtracts.
    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, diff::Mutable>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup;
}

impl AntijoinWeight for diff::Static {
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, diff::Mutable>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup,
    {
        flowlog_map(arm, name, |data, t, _| std::iter::once((data, t, 1)))
    }

    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, diff::Mutable>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup,
    {
        flowlog_map(arm, name, |data, t, _| std::iter::once((data, t, -1)))
    }
}

impl AntijoinWeight for diff::Mutable {
    fn encode_pos<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        _name: &str,
    ) -> VecCollection<'scope, T, D, diff::Mutable>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup,
    {
        flowlog_dedup(arm)
    }

    fn encode_neg<'scope, T, D>(
        arm: VecCollection<'scope, T, D, Self>,
        name: &str,
    ) -> VecCollection<'scope, T, D, diff::Mutable>
    where
        T: Timestamp + Lattice,
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup,
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
        rows: VecCollection<'scope, T, D, diff::Mutable>,
    ) -> VecCollection<'scope, T, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup;
}

impl AntijoinOutput<()> for diff::Static {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, (), D, diff::Mutable>,
    ) -> VecCollection<'scope, (), D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, (), D, diff::Mutable>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(diff::Static))
    }
}

impl<T: Counter> AntijoinOutput<T> for diff::Static {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, T, D, diff::Mutable>,
    ) -> VecCollection<'scope, T, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(diff::Static))
    }
}

impl<I: Counter> AntijoinOutput<Product<(), I>> for diff::Static {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, Product<(), I>, D, diff::Mutable>,
    ) -> VecCollection<'scope, Product<(), I>, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, Product<(), I>, D, diff::Mutable>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(diff::Static))
    }
}

impl AntijoinOutput<LexLoop> for diff::Static {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, LexLoop, D, diff::Mutable>,
    ) -> VecCollection<'scope, LexLoop, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, LexLoop, D, diff::Mutable>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(diff::Static))
    }
}

impl<E: Counter, I: Counter> AntijoinOutput<Product<E, I>> for diff::Static {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, Product<E, I>, D, diff::Mutable>,
    ) -> VecCollection<'scope, Product<E, I>, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, Product<E, I>, D, diff::Mutable>: FlowlogDedup,
    {
        first_occurrences(rows)
    }
}

impl<T: Timestamp + Lattice> AntijoinOutput<T> for diff::Mutable {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, T, D, diff::Mutable>,
    ) -> VecCollection<'scope, T, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup,
    {
        flowlog_dedup(rows)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use timely::dataflow::operators::probe::Handle;

    use super::*;

    type Row = (u64, char);

    /// A static source against a mutable filter must retract a row that a
    /// later filter key blocks, and restore it when the key goes away.
    #[test]
    fn static_source_retracts_rows_a_mutable_filter_blocks() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<Row, diff::Static>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_antijoin(
                    keys.arrange_by_self(),
                    rows.arrange_by_key(),
                    "Antijoin",
                    |row| row,
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (source, blocked)
            });
            source.update((1, 'a'), diff::Static);
            source.update((2, 'b'), diff::Static);
            source.close();
            blocked.advance_to(1);
            blocked.update(2, 1);
            blocked.advance_to(2);
            blocked.update(2, -1);
            blocked.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(
            actual,
            vec![
                ((1, 'a'), 0, 1),
                ((2, 'b'), 0, 1),
                ((2, 'b'), 1, -1),
                ((2, 'b'), 2, 1)
            ]
        );
    }

    /// A mutable source against a static filter keeps its own insertions
    /// and deletions, minus every row the filter blocks.
    #[test]
    fn mutable_source_keeps_its_changes_outside_a_static_filter() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<Row, diff::Mutable>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_antijoin(
                    keys.arrange_by_self(),
                    rows.arrange_by_key(),
                    "Antijoin",
                    |row| row,
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (source, blocked)
            });
            blocked.update(2, diff::Static);
            blocked.close();
            source.update((1, 'a'), 1);
            source.update((2, 'b'), 1);
            source.advance_to(1);
            source.update((1, 'a'), -1);
            source.update((2, 'c'), 1);
            source.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, vec![((1, 'a'), 0, 1), ((1, 'a'), 1, -1)]);
    }

    /// A join of a static side with a signed side carries the signed
    /// side's count, whichever side the static one is on.
    #[test]
    fn static_join_carries_the_signed_count() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut fixed, mut counted) = worker.dataflow::<u32, _, _>(|scope| {
                let (fixed, left) = scope.new_collection::<(u64, char), diff::Static>();
                let (counted, right) = scope.new_collection::<(u64, char), diff::Mutable>();
                let (left, right) = (left.arrange_by_key(), right.arrange_by_key());
                let seen_left = Rc::clone(&seen);
                let seen_right = Rc::clone(&seen);
                flowlog_join(left.clone(), right.clone(), "Join", |_, l, r| {
                    Some((*l, *r))
                })
                .inspect(move |update| seen_left.borrow_mut().push(*update))
                .probe_with(&probe);
                flowlog_join(right, left, "Join", |_, r, l| Some((*l, *r)))
                    .inspect(move |update| seen_right.borrow_mut().push(*update))
                    .probe_with(&probe);
                (fixed, counted)
            });
            fixed.update((1, 'x'), diff::Static);
            fixed.close();
            counted.update((1, 'y'), 2);
            counted.advance_to(1);
            counted.update((1, 'y'), -1);
            counted.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, vec![(('x', 'y'), 0, 4), (('x', 'y'), 1, -2)]);
    }

    /// A static antijoin decodes at every clock a static collection lives
    /// at, a lexicographic loop included; a mutable one at any clock.
    #[test]
    fn every_supported_clock_decodes_an_antijoin() {
        fn admits<T: Timestamp + Lattice>()
        where
            diff::Static: AntijoinOutput<T>,
            diff::Mutable: AntijoinOutput<T>,
        {
        }
        admits::<()>();
        admits::<u32>();
        admits::<Product<(), u16>>();
        admits::<Product<u32, u16>>();
        admits::<LexLoop>();
    }
}
