//! Named arranged joins and antijoins for generated FlowLog rules.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
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

use crate::diff;
use crate::diff::Max;
use crate::diff::Weight;
use crate::operators::dedup::FlowlogDedup;
use crate::operators::dedup::flowlog_dedup;
use crate::operators::lookup::lookup_absent;
use crate::operators::map::flowlog_map;

// =============================================================================
// Join
// =============================================================================

/// An equijoin of two arrangements sharing a common key type, recorded on
/// the timely operator under the name FlowLog gives the join step.
///
/// `result` extracts an iterator from each matching pair, and its full
/// contents are emitted with the [`diff::product`] of the two input
/// weights. Correctness depends heavily on the behavior of `result`.
pub fn flowlog_join<'scope, Tr1, Tr2, I, L, R1, R2, KC>(
    arranged1: Arranged<'scope, Tr1>,
    arranged2: Arranged<'scope, Tr2>,
    name: &str,
    mut result: L,
) -> VecCollection<'scope, Tr1::Time, I::Item, <R1 as Max<R2>>::Output>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
    BatchCursor<Tr1>: Cursor<Diff = R1, Time = Tr1::Time, KeyContainer = KC>,
    BatchCursor<Tr2>: Cursor<Diff = R2, Time = Tr1::Time>,
    KC: BatchContainer,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    R1: Max<R2>,
    R2: Weight,
    I: IntoIterator<Item: Data>,
    L: FnMut(KC::ReadItem<'_>, BatchVal<'_, Tr1>, BatchVal<'_, Tr2>) -> I + 'static,
{
    let mut emit = move |key: KC::ReadItem<'_>,
                         left: BatchVal<'_, Tr1>,
                         right: BatchVal<'_, Tr2>,
                         time: Tr1::Time,
                         left_diff: &R1,
                         right_diff: &R2| {
        let weight = diff::product(*left_diff, *right_diff);
        result(key, left, right)
            .into_iter()
            .map(move |datum| (datum, time.clone(), weight))
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
/// through `logic`, under the name FlowLog gives the step. As with
/// [`flowlog_join`], two pairs `logic` maps to one row each emit it.
/// `logic` never sees a blocked pair.
///
/// The filter's weight picks the strategy ([`AntijoinStrategy`]) and the
/// output weight. A static filter is complete at its scope's minimum time,
/// before any source row, so source rows are looked up in it and keep
/// their weight. A filter that grows blocks rows already emitted, so its
/// output is signed whatever the source.
///
/// | filter \ source | `Static` | `Append` | `Mutable` |
/// |---|---|---|---|
/// | `Static` | `Static` | `Append` | `Mutable` |
/// | `Append` | `Mutable` | `Mutable` | `Mutable` |
/// | `Mutable` | `Mutable` | `Mutable` | `Mutable` |
pub fn flowlog_antijoin<'scope, Tr1, Tr2, KC, D, L, Rf, Rs>(
    filter: Arranged<'scope, Tr1>,
    source: Arranged<'scope, Tr2>,
    name: &str,
    logic: L,
) -> VecCollection<'scope, Tr1::Time, D, Rf::Output>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
    BatchCursor<Tr1>: Cursor<Diff = Rf, Time = Tr1::Time, KeyContainer = KC>,
    BatchCursor<Tr2>: Cursor<Diff = Rs, Time = Tr1::Time>,
    KC: BatchContainer,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    Rf: AntijoinStrategy<Rs>,
    (KC::Owned, BatchValOwn<Tr2>): ExchangeData + Hashable,
    D: Data,
    L: FnMut((KC::Owned, BatchValOwn<Tr2>)) -> D + 'static,
    VecCollection<'scope, Tr1::Time, (KC::Owned, BatchValOwn<Tr2>), diff::Mutable>: FlowlogDedup,
{
    Rf::antijoin(filter, source, name, logic)
}

// =============================================================================
// Strategies
// =============================================================================

/// Removes the blocked pairs of a source of weight `Rs` by the operations
/// a filter of weight `Self` supports.
pub trait AntijoinStrategy<Rs>: Weight {
    /// The antijoin's output weight.
    type Output: Weight;

    /// Applies the contracts of [`flowlog_antijoin`].
    fn antijoin<'scope, Tr1, Tr2, KC, D, L>(
        filter: Arranged<'scope, Tr1>,
        source: Arranged<'scope, Tr2>,
        name: &str,
        logic: L,
    ) -> VecCollection<'scope, Tr1::Time, D, Self::Output>
    where
        Tr1: TraceReader<Batch: Navigable> + 'static,
        Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
        BatchCursor<Tr1>: Cursor<Diff = Self, Time = Tr1::Time, KeyContainer = KC>,
        BatchCursor<Tr2>: Cursor<Diff = Rs, Time = Tr1::Time>,
        KC: BatchContainer,
        for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        (KC::Owned, BatchValOwn<Tr2>): ExchangeData + Hashable,
        D: Data,
        L: FnMut((KC::Owned, BatchValOwn<Tr2>)) -> D + 'static,
        VecCollection<'scope, Tr1::Time, (KC::Owned, BatchValOwn<Tr2>), diff::Mutable>:
            FlowlogDedup;
}

/// Looks each pair up in the filter (`lookup_absent`): a pair at time `t`
/// is kept iff the filter holds its key at no time at or before `t`. A
/// static filter is complete at its scope's minimum time, so pairs wait
/// only for it to load, and keep the source's weight. Only negation
/// through recursion adds static keys later, and there every head of the
/// loop is static, so the source is presence: such a key blocks the pairs
/// at or after its time and retracts none emitted before.
impl<Rs: Weight> AntijoinStrategy<Rs> for diff::Static {
    type Output = Rs;

    fn antijoin<'scope, Tr1, Tr2, KC, D, L>(
        filter: Arranged<'scope, Tr1>,
        source: Arranged<'scope, Tr2>,
        name: &str,
        mut logic: L,
    ) -> VecCollection<'scope, Tr1::Time, D, Rs>
    where
        Tr1: TraceReader<Batch: Navigable> + 'static,
        Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
        BatchCursor<Tr1>: Cursor<Diff = Self, Time = Tr1::Time, KeyContainer = KC>,
        BatchCursor<Tr2>: Cursor<Diff = Rs, Time = Tr1::Time>,
        KC: BatchContainer,
        for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        (KC::Owned, BatchValOwn<Tr2>): ExchangeData + Hashable,
        D: Data,
        L: FnMut((KC::Owned, BatchValOwn<Tr2>)) -> D + 'static,
        VecCollection<'scope, Tr1::Time, (KC::Owned, BatchValOwn<Tr2>), diff::Mutable>:
            FlowlogDedup,
    {
        lookup_absent(filter, source, name, move |key, val| {
            logic((
                KC::into_owned(key),
                <BatchCursor<Tr2> as Cursor>::owned_val(val),
            ))
        })
    }
}

/// `diff::Append` and `diff::Mutable`, the filters that grow (those at
/// least `diff::Append`), cancel. The source arm adds each source pair at
/// its weight's count ([`Weight::count`]), the matched arm, the join of
/// the two inputs, subtracts each match at its product's count, and a
/// signed dedup keeps the pairs whose sum is positive. A pair the source
/// counts `s` times, under a key the filter counts `f` times, sums to
/// `s - s * f`, positive exactly when `s > 0` and `f = 0`, provided each
/// count is exact and never negative:
///
/// - a signed input must have nonnegative accumulated multiplicities;
/// - a presence arrangement must hold each key, or each pair, once, so
///   that its arm is one update per pair. A static one does by living at
///   its scope's minimum time, where presence consolidates; an append one
///   by being arranged with
///   [`flowlog_arrange`](crate::operators::flowlog_arrange).
impl<Rf, Rs> AntijoinStrategy<Rs> for Rf
where
    diff::Append: Max<Rf, Output = Rf>,
    Rf: Max<Rs>,
    Rs: Weight,
{
    type Output = diff::Mutable;

    fn antijoin<'scope, Tr1, Tr2, KC, D, L>(
        filter: Arranged<'scope, Tr1>,
        source: Arranged<'scope, Tr2>,
        name: &str,
        mut logic: L,
    ) -> VecCollection<'scope, Tr1::Time, D, diff::Mutable>
    where
        Tr1: TraceReader<Batch: Navigable> + 'static,
        Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + Clone + 'static,
        BatchCursor<Tr1>: Cursor<Diff = Self, Time = Tr1::Time, KeyContainer = KC>,
        BatchCursor<Tr2>: Cursor<Diff = Rs, Time = Tr1::Time>,
        KC: BatchContainer,
        for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        (KC::Owned, BatchValOwn<Tr2>): ExchangeData + Hashable,
        D: Data,
        L: FnMut((KC::Owned, BatchValOwn<Tr2>)) -> D + 'static,
        VecCollection<'scope, Tr1::Time, (KC::Owned, BatchValOwn<Tr2>), diff::Mutable>:
            FlowlogDedup,
    {
        // Both arms must cancel on the same datum, so each rebuilds the
        // owned (key, value) pair from its cursor's borrowed view. Each arm
        // is finished before the next one starts, which keeps the operators
        // in the order address prediction expects. Neither arm is deduped:
        // the sum decides each pair by its sign, so deduping a signed arm
        // first would only add an arrangement.
        let pairs = source.clone().flat_map_ref(|key, val| {
            std::iter::once((
                KC::into_owned(key),
                <BatchCursor<Tr2> as Cursor>::owned_val(val),
            ))
        });
        let positive = flowlog_map(pairs, name, |pair, t, weight| {
            std::iter::once((pair, t, weight.count()))
        });
        let matches = flowlog_join(filter, source, name, |key, _, val| {
            std::iter::once((
                KC::into_owned(key),
                <BatchCursor<Tr2> as Cursor>::owned_val(val),
            ))
        });
        let negative = flowlog_map(matches, name, |pair, t, weight| {
            std::iter::once((pair, t, -weight.count()))
        });

        // Dedup before projecting: concatenating does not consolidate, so a
        // blocked pair still travels as its two arms' counts until the dedup
        // cancels them, and projecting first would hand it to `logic`. A
        // head such as `100 / x` would then divide by the `x = 0` the
        // negation excludes. Each surviving pair maps to one row, so the
        // timestamp and weight it arrived with move straight through. The
        // dedup then holds a pair where it held a row, which costs more
        // state where `logic` merges many pairs into one row.
        let survivors = flowlog_dedup(positive.concat(negative));
        flowlog_map(survivors, name, move |data, t, d| {
            std::iter::once((logic(data), t, d))
        })
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

    /// A pair the filter blocks never reaches `logic`: a projection that
    /// divides by the key runs on the open key 4 alone, never on the
    /// blocked key 0.
    #[test]
    fn blocked_pair_never_reaches_the_projection() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<Row, diff::Static>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_antijoin(
                    keys.arrange_by_self(),
                    rows.arrange_by_key(),
                    "Antijoin",
                    |(key, val)| (100 / key, val),
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (source, blocked)
            });
            blocked.update(0, diff::Static);
            blocked.close();
            source.update((0, 'a'), diff::Static);
            source.update((4, 'b'), diff::Static);
            source.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, vec![((25, 'b'), 0, diff::Static)]);
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

    /// Runs `filter` and `source` epoch by epoch through an antijoin at
    /// `u32`, both arranged with the runtime's arrange, closing both after
    /// the last epoch, and returns the output consolidated. `filter[e]`
    /// and `source[e]` are epoch `e`'s updates.
    fn antijoin_epochs<Rf, Rs>(
        filter: Vec<Vec<(u64, Rf)>>,
        source: Vec<Vec<(Row, Rs)>>,
    ) -> Vec<(Row, u32, Rf::Output)>
    where
        Rf: AntijoinStrategy<Rs> + Sync,
        Rs: Weight + Sync,
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
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (keys, rows)
            });
            for epoch in 0..epochs {
                for (key, diff) in filter.get(epoch).into_iter().flatten() {
                    keys.update(*key, *diff);
                }
                for (row, diff) in source.get(epoch).into_iter().flatten() {
                    rows.update(*row, *diff);
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
        let actual = antijoin_epochs::<diff::Append, diff::Static>(
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
        let actual = antijoin_epochs::<diff::Append, diff::Append>(
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
        let actual = antijoin_epochs::<diff::Append, diff::Mutable>(
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
        let actual = antijoin_epochs::<diff::Static, diff::Append>(
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
        let actual = antijoin_epochs::<diff::Mutable, diff::Append>(
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

    /// Counts above one cancel exactly: a row the source counts twice
    /// stays blocked while the filter counts its key twice, then once, and
    /// returns when that count reaches zero; an open row leaves only when
    /// its own count does.
    #[test]
    fn mutable_filter_over_mutable_source_cancels_by_counts() {
        let actual = antijoin_epochs::<diff::Mutable, diff::Mutable>(
            vec![vec![], vec![(2, 2)], vec![(2, -1)], vec![(2, -1)]],
            vec![
                vec![((1, 'a'), 2), ((2, 'b'), 2)],
                vec![],
                vec![((1, 'a'), -1)],
                vec![],
                vec![((1, 'a'), -1)],
            ],
        );
        assert_eq!(
            actual,
            vec![
                ((1, 'a'), 0, 1),
                ((1, 'a'), 4, -1),
                ((2, 'b'), 0, 1),
                ((2, 'b'), 1, -1),
                ((2, 'b'), 3, 1),
            ]
        );
    }
}
