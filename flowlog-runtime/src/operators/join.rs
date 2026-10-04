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
use crate::diff::Presence;
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
/// cancel, then survivors are decoded to the output weight. The source arm
/// counts `+1` per source pair and the matched arm, the join of the two
/// inputs, `-1` per blocked pair, each encoded by its own weight
/// ([`AntijoinWeight`]). The filter's weight picks the output
/// ([`AntijoinOutputWeight`]): a static filter is complete at its scope's
/// minimum time, before any source row, so the output keeps the source's
/// weight; a filter that grows blocks rows already emitted, so its output
/// is signed whatever the source.
///
/// | filter \ source | `Static` | `Append` | `Mutable` |
/// |---|---|---|---|
/// | `Static` | `Static` | `Append` | `Mutable` |
/// | `Append` | `Mutable` | `Mutable` | `Mutable` |
/// | `Mutable` | `Mutable` | `Mutable` | `Mutable` |
///
/// A presence arrangement must hold each key, or each pair, once, so that
/// a presence arm is exactly one update per pair and the cancellation is
/// exact. A static one does by living at its scope's minimum time, where
/// presence consolidates; an append one by being arranged with
/// [`flowlog_arrange`](crate::operators::flowlog_arrange). Signed inputs
/// must have nonnegative accumulated multiplicities.
pub fn flowlog_antijoin<'scope, Tr1, Tr2, KC, D, L, Rf, Rs, P, R>(
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
    Rf: Multiply<Rs, Output = P> + AntijoinOutputWeight<Rs, Output = R> + Clone,
    Rs: AntijoinWeight + Semigroup + 'static,
    P: AntijoinWeight + Semigroup + 'static,
    R: AntijoinOutput<Tr1::Time> + ExchangeData + Semigroup,
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
    let negative = P::encode_neg(
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

/// The weight an antijoin produces, by its filter's weight over a source
/// of weight `Rs`: a static filter keeps the source's weight, and a filter
/// that grows makes the output signed whatever the source. The output
/// then decodes through [`AntijoinOutput`].
pub trait AntijoinOutputWeight<Rs> {
    /// The weight of the antijoin's result.
    type Output;
}

impl<Rs> AntijoinOutputWeight<Rs> for diff::Static {
    type Output = Rs;
}

impl<Rs> AntijoinOutputWeight<Rs> for diff::Append {
    type Output = diff::Mutable;
}

impl<Rs> AntijoinOutputWeight<Rs> for diff::Mutable {
    type Output = diff::Mutable;
}

/// The weight families an antijoin arm can carry, each knowing how to
/// encode itself as the `+1` / `-1` the cancellation needs.
///
/// A presence arm is one update per pair already, under the input
/// guarantees of [`flowlog_antijoin`], so it maps to the unit weight. A
/// `diff::Mutable` arm is set-normalized first: duplicate derivations
/// would otherwise accumulate weights the cancelling sum cannot tell apart
/// from a match.
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

impl<R: Presence> AntijoinWeight for R {
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

impl<R: Presence> AntijoinOutput<()> for R {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, (), D, diff::Mutable>,
    ) -> VecCollection<'scope, (), D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, (), D, diff::Mutable>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(R::one()))
    }
}

impl<T: Counter, R: Presence> AntijoinOutput<T> for R {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, T, D, diff::Mutable>,
    ) -> VecCollection<'scope, T, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, T, D, diff::Mutable>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(R::one()))
    }
}

impl<I: Counter, R: Presence> AntijoinOutput<Product<(), I>> for R {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, Product<(), I>, D, diff::Mutable>,
    ) -> VecCollection<'scope, Product<(), I>, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, Product<(), I>, D, diff::Mutable>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(R::one()))
    }
}

impl<R: Presence> AntijoinOutput<LexLoop> for R {
    fn decode<'scope, D>(
        rows: VecCollection<'scope, LexLoop, D, diff::Mutable>,
    ) -> VecCollection<'scope, LexLoop, D, Self>
    where
        D: ExchangeData + Hashable,
        VecCollection<'scope, LexLoop, D, diff::Mutable>: FlowlogDedup,
    {
        rows.threshold_semigroup(|_, _, prior| prior.is_none().then_some(R::one()))
    }
}

impl<E: Counter, I: Counter, R: Presence> AntijoinOutput<Product<E, I>> for R {
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
    use differential_dataflow::operators::arrange::TraceAgent;
    use differential_dataflow::trace::implementations::KeySpine;
    use differential_dataflow::trace::implementations::ValSpine;
    use timely::dataflow::operators::probe::Handle;

    use super::*;
    use crate::operators::arrange::FlowlogArrange;
    use crate::operators::arrange::FlowlogArrangeSelf;
    use crate::operators::flowlog_arrange;
    use crate::operators::flowlog_arrange_self;

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

    /// A join of an append side with a static side carries append presence,
    /// whichever side the append one is on.
    #[test]
    fn append_join_with_static_carries_presence() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut fixed, mut growing) = worker.dataflow::<u32, _, _>(|scope| {
                let (fixed, left) = scope.new_collection::<(u64, char), diff::Static>();
                let (growing, right) = scope.new_collection::<(u64, char), diff::Append>();
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
                (fixed, growing)
            });
            fixed.update((1, 'x'), diff::Static);
            fixed.close();
            growing.update((1, 'y'), diff::Append);
            growing.advance_to(1);
            growing.update((1, 'z'), diff::Append);
            growing.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(
            actual,
            vec![(('x', 'y'), 0, diff::Append), (('x', 'z'), 1, diff::Append)]
        );
    }

    /// Every weight decodes an antijoin at every clock a collection lives
    /// at, a lexicographic loop included.
    #[test]
    fn every_supported_clock_decodes_an_antijoin() {
        fn admits<T: Timestamp + Lattice>()
        where
            diff::Static: AntijoinOutput<T>,
            diff::Append: AntijoinOutput<T>,
            diff::Mutable: AntijoinOutput<T>,
        {
        }
        admits::<()>();
        admits::<u32>();
        admits::<Product<(), u16>>();
        admits::<Product<u32, u16>>();
        admits::<LexLoop>();
    }

    /// Runs `filter` and `source` epoch by epoch through an antijoin at
    /// `u32`, both arranged with the runtime's arrange, closing both after
    /// the last epoch, and returns the output consolidated. `filter[e]`
    /// and `source[e]` are epoch `e`'s updates.
    fn antijoin_epochs<Rf, Rs, P, R>(
        filter: Vec<Vec<(u64, Rf)>>,
        source: Vec<Vec<(Row, Rs)>>,
    ) -> Vec<(Row, u32, R)>
    where
        Rf: Multiply<Rs, Output = P>
            + AntijoinOutputWeight<Rs, Output = R>
            + ExchangeData
            + Semigroup
            + Sync,
        Rs: AntijoinWeight + ExchangeData + Semigroup + Sync,
        P: AntijoinWeight + Semigroup + 'static,
        R: AntijoinOutput<u32> + ExchangeData + Semigroup,
        for<'scope> VecCollection<'scope, u32, u64, Rf>:
            FlowlogArrangeSelf<Arranged = Arranged<'scope, TraceAgent<KeySpine<u64, u32, Rf>>>>,
        for<'scope> VecCollection<'scope, u32, Row, Rs>:
            FlowlogArrange<Arranged = Arranged<'scope, TraceAgent<ValSpine<u64, char, u32, Rs>>>>,
    {
        let epochs = filter.len().max(source.len());
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut keys, mut rows) = worker.dataflow::<u32, _, _>(|scope| {
                let (keys, blocked) = scope.new_collection::<u64, Rf>();
                let (rows, source) = scope.new_collection::<Row, Rs>();
                let seen = Rc::clone(&seen);
                flowlog_antijoin(
                    flowlog_arrange_self(blocked, "Filter"),
                    flowlog_arrange(source, "Source"),
                    "Antijoin",
                    |row| row,
                )
                .inspect(move |update| seen.borrow_mut().push(update.clone()))
                .probe_with(&probe);
                (keys, rows)
            });
            for epoch in 0..epochs {
                for (key, diff) in filter.get(epoch).into_iter().flatten() {
                    keys.update(*key, diff.clone());
                }
                for (row, diff) in source.get(epoch).into_iter().flatten() {
                    rows.update(*row, diff.clone());
                }
                let next = epoch as u32 + 1;
                keys.advance_to(next);
                keys.flush();
                rows.advance_to(next);
                rows.flush();
                worker.step_while(|| probe.less_than(&next));
            }
            keys.close();
            rows.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        actual
    }

    /// An append filter key that arrives after a static row retracts it,
    /// once, however often the filter announces the key again.
    #[test]
    fn append_filter_retracts_a_static_row_it_comes_to_block() {
        let actual = antijoin_epochs::<diff::Append, diff::Static, _, _>(
            vec![
                vec![],
                vec![(2, diff::Append)],
                vec![(2, diff::Append)],
                vec![(2, diff::Append), (3, diff::Append)],
            ],
            vec![vec![
                ((1, 'a'), diff::Static),
                ((2, 'b'), diff::Static),
                ((3, 'c'), diff::Static),
            ]],
        );
        assert_eq!(
            actual,
            vec![
                ((1, 'a'), 0, 1),
                ((2, 'b'), 0, 1),
                ((2, 'b'), 1, -1),
                ((3, 'c'), 0, 1),
                ((3, 'c'), 3, -1),
            ]
        );
    }

    /// An append source row announced at several epochs is retracted once
    /// when an append filter key blocks it later, and a row announced after
    /// the key never appears. The filter key arrives epochs after the
    /// repeats, when compaction may have merged them.
    #[test]
    fn append_filter_over_append_source_cancels_repeat_announcements() {
        let actual = antijoin_epochs::<diff::Append, diff::Append, _, _>(
            vec![vec![], vec![], vec![], vec![(5, diff::Append)]],
            vec![
                vec![((5, 'a'), diff::Append), ((6, 'b'), diff::Append)],
                vec![((5, 'a'), diff::Append)],
                vec![],
                vec![],
                vec![((5, 'c'), diff::Append), ((6, 'b'), diff::Append)],
            ],
        );
        assert_eq!(
            actual,
            vec![((5, 'a'), 0, 1), ((5, 'a'), 3, -1), ((6, 'b'), 0, 1)]
        );
    }

    /// A mutable source under an append filter keeps its own insertions and
    /// deletions outside the filter, and a row reinserted under a blocked
    /// key stays absent.
    #[test]
    fn append_filter_over_mutable_source_tracks_the_source() {
        let actual = antijoin_epochs::<diff::Append, diff::Mutable, _, _>(
            vec![
                vec![],
                vec![(2, diff::Append)],
                vec![],
                vec![(2, diff::Append)],
            ],
            vec![
                vec![((1, 'a'), 1), ((2, 'b'), 1)],
                vec![],
                vec![((2, 'b'), -1), ((1, 'a'), -1)],
                vec![((2, 'b'), 1), ((1, 'a'), 1)],
                vec![((2, 'c'), 1)],
            ],
        );
        assert_eq!(
            actual,
            vec![
                ((1, 'a'), 0, 1),
                ((1, 'a'), 2, -1),
                ((1, 'a'), 3, 1),
                ((2, 'b'), 0, 1),
                ((2, 'b'), 1, -1),
            ]
        );
    }

    /// A static filter keeps an append source's presence: a blocked row
    /// never appears, and an open row is announced once however often the
    /// source announces it.
    #[test]
    fn static_filter_keeps_an_append_source_present() {
        let actual = antijoin_epochs::<diff::Static, diff::Append, _, _>(
            vec![vec![(2, diff::Static)]],
            vec![
                vec![((1, 'a'), diff::Append), ((2, 'b'), diff::Append)],
                vec![((1, 'a'), diff::Append), ((3, 'c'), diff::Append)],
                vec![((2, 'b'), diff::Append)],
            ],
        );
        assert_eq!(
            actual,
            vec![((1, 'a'), 0, diff::Append), ((3, 'c'), 1, diff::Append)]
        );
    }

    /// A mutable filter over an append source retracts a blocked row and
    /// restores it when the key goes away, reading the source's presence as
    /// a count of one however often the source announced the row.
    #[test]
    fn mutable_filter_over_append_source_retracts_and_restores() {
        let actual = antijoin_epochs::<diff::Mutable, diff::Append, _, _>(
            vec![vec![], vec![(2, 1)], vec![], vec![(2, -1)]],
            vec![
                vec![((1, 'a'), diff::Append), ((2, 'b'), diff::Append)],
                vec![],
                vec![((2, 'b'), diff::Append)],
            ],
        );
        assert_eq!(
            actual,
            vec![
                ((1, 'a'), 0, 1),
                ((2, 'b'), 0, 1),
                ((2, 'b'), 1, -1),
                ((2, 'b'), 3, 1),
            ]
        );
    }
}
