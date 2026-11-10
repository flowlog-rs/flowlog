//! Aggregate expressions: the matches of an aggregate's braces, arranged
//! by the group key, reduced by the reduction over its target, and the
//! groups answered without a match. A presence aggregate folds each new
//! match into its group's weight; a `diff::Mutable` one reads all of a
//! changed group's matches again.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::KeySpine;
use timely::dataflow::Scope;
use timely::dataflow::operators::core::ToStream;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use super::reduce::Reduction;
use super::reduce::presence::fold;
use super::reduce::presence::lower;
use super::reduce::presence::settle;
use super::reduce::semiring::Scalar;
use super::reduce::semiring::Semiring;
use super::reduce::signed::defaults;
use super::reduce::signed::reduce;
use super::reduce::signed::replace;
use crate::diff;
use crate::diff::Presence;
use crate::operators::map::flowlog_map;

/// The groups an aggregate answers for even without a match, when the
/// reduction is defined on empty; ignored otherwise.
pub enum Groups<'scope, T, K, R>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    R: Semigroup + Ord + 'static,
{
    /// No group: a key with no match has no answer.
    None,
    /// One group, of this key.
    Key(K),
    /// Every key of this arrangement, once however many times it holds
    /// the key, answering from when its key appears until it goes. A
    /// `diff::Static` aggregate never withdraws an answer, so there a key
    /// must not precede its matches: its empty result would stay beside
    /// the answer over them.
    Arranged(Arranged<'scope, TraceAgent<KeySpine<K, T, R>>>),
}

/// Aggregates `matches`, arranged by the group key, under `reduction`:
/// `target` reads the aggregated column out of a match's value, the
/// unit that tells two matches of a group apart, and `merge` rebuilds an
/// answer row from a key and its answer. Each of `groups` answers the
/// reduction's empty group while it has no match, when the reduction
/// answers one; `merge` is cloned to build those answers. Matches
/// arranged by the key alone have the value `()`: one match per key.
///
/// Each distinct match counts once: under `diff::Mutable` whatever its
/// count, under a presence weight at its first time. A `diff::Static`
/// aggregate can emit a new answer at a later timestamp but never
/// retract one; the others replace each answer through insertions and
/// retractions.
///
/// Floating-point sums and means may differ with batch boundaries, worker
/// scheduling, or execution strategy; bitwise agreement with a batch
/// recomputation is not guaranteed.
///
/// Every key the matches hold must be a group's when `groups` names
/// them: such a key's empty answer is cancelled by its answer.
#[must_use]
pub fn flowlog_aggregate<'scope, A, Tr, T, K, V, X, C, O, R, Rg>(
    matches: Arranged<'scope, Tr>,
    groups: Groups<'scope, T, K, Rg>,
    name: &str,
    _reduction: A,
    target: impl Fn(&V) -> X + 'static,
    merge: impl FnMut(K, C) -> O + Clone + 'static,
) -> VecCollection<'scope, T, O, R::Output>
where
    A: Reduction<X, C>,
    A::Semiring: ExchangeData,
    C: Scalar,
    Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T, Diff = R>,
    T: Timestamp + Lattice,
    R: AggregateStrategy<T, Rg>,
    Rg: Semigroup + Ord + 'static,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    O: Data,
{
    R::aggregate(
        matches,
        groups,
        name,
        A::empty(),
        move |value| A::contribute(&target(value)),
        merge,
    )
}

// =============================================================================
// Strategies
// =============================================================================

/// Aggregates arranged matches using the operations the matches' weight
/// supports, for groups of weight `Rg`: as
/// [`super::lattice::LatticeStrategy`], over an arrangement built
/// already. A presence aggregate takes presence groups, as it adds each
/// group's empty accumulation once, as a weight, and never takes it back;
/// `diff::Mutable` takes groups of any weight. Matches restricted to
/// mutable groups are mutable too, so every aggregate the planner builds
/// has a strategy.
pub trait AggregateStrategy<T: Timestamp + Lattice, Rg: Semigroup + Ord + 'static>:
    Semigroup + Ord + Sized + 'static
{
    /// The answers' weight.
    type Output: Semigroup + 'static;

    /// Applies the contracts of [`flowlog_aggregate`], each of `groups`
    /// without a match answering `empty`, finished, if there is one, and
    /// nothing otherwise.
    #[must_use]
    fn aggregate<'scope, Tr, K, V, S, O>(
        matches: Arranged<'scope, Tr>,
        groups: Groups<'scope, T, K, Rg>,
        name: &str,
        empty: Option<S>,
        contribute: impl Fn(&V) -> S + 'static,
        merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
    ) -> VecCollection<'scope, T, O, Self::Output>
    where
        Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
        for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T, Diff = Self>,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data;
}

impl<T, Rg> AggregateStrategy<T, Rg> for diff::Static
where
    T: Timestamp + TotalOrder + Lattice,
    Rg: Presence,
{
    type Output = diff::Static;

    fn aggregate<'scope, Tr, K, V, S, O>(
        matches: Arranged<'scope, Tr>,
        groups: Groups<'scope, T, K, Rg>,
        name: &str,
        empty: Option<S>,
        contribute: impl Fn(&V) -> S + 'static,
        merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
    ) -> VecCollection<'scope, T, O, Self>
    where
        Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
        for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T, Diff = Self>,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
    {
        let weights = group_weights(matches, groups, name, empty, contribute);
        lower(settle(weights), name, merge)
    }
}

impl<T, Rg> AggregateStrategy<T, Rg> for diff::Append
where
    T: Timestamp + TotalOrder + Lattice,
    Rg: Presence,
{
    type Output = diff::Mutable;

    fn aggregate<'scope, Tr, K, V, S, O>(
        matches: Arranged<'scope, Tr>,
        groups: Groups<'scope, T, K, Rg>,
        name: &str,
        empty: Option<S>,
        contribute: impl Fn(&V) -> S + 'static,
        merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
    ) -> VecCollection<'scope, T, O, Self::Output>
    where
        Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
        for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T, Diff = Self>,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
    {
        // Accumulating weights avoids rescanning each group's history.
        // Float sums and means accept rounding differences from batch
        // merging in exchange for the same incremental cost as integers.
        let weights = group_weights(matches, groups, name, empty, contribute);
        replace(weights, name, merge)
    }
}

impl<T, Rg> AggregateStrategy<T, Rg> for diff::Mutable
where
    T: Timestamp + Lattice,
    Rg: Semigroup + Ord + 'static,
{
    type Output = diff::Mutable;

    fn aggregate<'scope, Tr, K, V, S, O>(
        matches: Arranged<'scope, Tr>,
        groups: Groups<'scope, T, K, Rg>,
        name: &str,
        empty: Option<S>,
        contribute: impl Fn(&V) -> S + 'static,
        merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
    ) -> VecCollection<'scope, T, O, Self::Output>
    where
        Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
        for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T, Diff = Self>,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
    {
        signed_aggregate(matches, groups, name, empty, contribute, merge)
    }
}

/// Folds arranged presence `matches` into their keys' weights, each of
/// `groups` seeded with `empty` if there is one: a group with matches adds
/// their contributions to it.
#[must_use]
fn group_weights<'scope, Tr, T, K, V, S, Rg>(
    matches: Arranged<'scope, Tr>,
    groups: Groups<'scope, T, K, Rg>,
    name: &str,
    empty: Option<S>,
    contribute: impl Fn(&V) -> S + 'static,
) -> VecCollection<'scope, T, K, S>
where
    Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T, Diff: Presence>,
    T: Timestamp + TotalOrder + Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    S: Semiring,
    Rg: Presence,
{
    let scope = matches.stream.scope();
    let weights = fold(matches, name, contribute);
    let seeds = match (groups, empty) {
        (Groups::None, _) | (Groups::Key(_) | Groups::Arranged(_), None) => return weights,
        (Groups::Key(key), Some(empty)) => single_default(scope, key, empty),
        (Groups::Arranged(groups), Some(empty)) => {
            let keys = groups.as_collection(|key, ()| key.clone());
            flowlog_map(keys, name, move |key, time, _| {
                std::iter::once((key, time, empty.clone()))
            })
        }
    };
    weights.concat(seeds)
}

/// Aggregates arranged `matches`, each of `groups` answering `empty`,
/// finished, while it has no match, if there is one: the groups' defaults,
/// one row per group, concatenated with the answers, each of which cancels
/// its group's default.
#[must_use]
fn signed_aggregate<'scope, Tr, T, K, V, S, O, Rg>(
    matches: Arranged<'scope, Tr>,
    groups: Groups<'scope, T, K, Rg>,
    name: &str,
    empty: Option<S>,
    contribute: impl Fn(&V) -> S + 'static,
    mut merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
) -> VecCollection<'scope, T, O, diff::Mutable>
where
    Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T>,
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    S: Semiring + ExchangeData,
    O: Data,
    Rg: Semigroup + Ord + 'static,
{
    // Each group's default row, and the seed the groups' sums start from,
    // with the keys it seeds: `None` for every key, as every match's key is
    // a group's. The seeds are those `group_weights` gives a presence
    // aggregate, so both answer alike.
    let (defaults, seed) = match (groups, empty) {
        (Groups::None, _) | (Groups::Key(_) | Groups::Arranged(_), None) => (None, None),
        (Groups::Key(key), Some(empty)) => {
            let default = merge(key.clone(), empty.clone().finish());
            let default = single_default(matches.stream.scope(), default, 1);
            (Some(default), Some((empty, Some(key))))
        }
        (Groups::Arranged(groups), Some(empty)) => (
            Some(defaults(
                groups,
                name,
                empty.clone().finish(),
                merge.clone(),
            )),
            Some((empty, None)),
        ),
    };
    let answers = reduce(matches, name, contribute, seed, merge);
    match defaults {
        Some(defaults) => answers.concat(defaults),
        None => answers,
    }
}

/// The default of a single group: `datum` at `diff`, once across all
/// workers, at the minimum time so that every later time sees it.
#[must_use]
fn single_default<'scope, T, D, R>(
    scope: Scope<'scope, T>,
    datum: D,
    diff: R,
) -> VecCollection<'scope, T, D, R>
where
    T: Timestamp,
    D: Data,
    R: Semigroup + 'static,
{
    // A source runs once, so a loop's later rounds do not insert it again.
    (scope.index() == 0)
        .then(|| (datum, T::minimum(), diff))
        .into_iter()
        .to_stream(scope)
        .as_collection()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::ops::RangeInclusive;
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use ordered_float::OrderedFloat;
    use rstest::rstest;
    use timely::dataflow::operators::probe::Handle;

    use super::*;
    use crate::diff;
    use crate::operators::reduce::Count;
    use crate::operators::reduce::Max;
    use crate::operators::reduce::Mean;
    use crate::operators::reduce::Min;
    use crate::operators::reduce::Sum;
    use crate::time::LexLoop;

    /// A global aggregate's one group, or none.
    fn global<'scope, T, R>(key: Option<()>) -> Groups<'scope, T, (), R>
    where
        T: Timestamp + Lattice,
        R: Semigroup + Ord + 'static,
    {
        key.map_or(Groups::None, Groups::Key)
    }

    /// The answers of `reduction` over `rows`, a global aggregate of
    /// one epoch under weight `R`, answered once per `empty_key`.
    fn global_aggregate<A, R, X, C>(
        reduction: A,
        rows: Vec<X>,
        empty_key: Option<()>,
    ) -> Vec<(C, (), R)>
    where
        A: Reduction<X, C> + Send + Sync,
        A::Semiring: ExchangeData,
        R: AggregateStrategy<(), R, Output = R> + ExchangeData + Sync + FromOne,
        X: ExchangeData + Sync,
        C: Scalar,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<X, R>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.map(|value| ((), value)).arrange_by_key(),
                    global::<(), R>(empty_key),
                    "Reduce",
                    reduction,
                    X::clone,
                    |(), value| value,
                )
                .inspect(move |update| seen.borrow_mut().push(update.clone()));
                input
            });
            for value in rows {
                input.update(value, R::from_one());
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        actual
    }

    /// A weight's unit, to insert one row at.
    trait FromOne {
        fn from_one() -> Self;
    }

    impl FromOne for diff::Static {
        fn from_one() -> Self {
            diff::Static
        }
    }

    impl FromOne for diff::Mutable {
        fn from_one() -> Self {
            1
        }
    }

    /// A group without a match answers the reduction's empty result
    /// when the reduction defines one and the group is named, under
    /// either weight.
    // Cases: reduction, weight, empty key, answers.
    #[rstest]
    #[case(Count, diff::Static, Some(()), vec![(0, (), diff::Static)])]
    #[case(Count, 1_i32, Some(()), vec![(0, (), 1)])]
    #[case(Count, diff::Static, None, vec![])]
    #[case(Count, 1_i32, None, vec![])]
    #[case(Sum, diff::Static, Some(()), vec![(0, (), diff::Static)])]
    #[case(Sum, 1_i32, Some(()), vec![(0, (), 1)])]
    #[case(Sum, diff::Static, None, vec![])]
    #[case(Sum, 1_i32, None, vec![])]
    #[case(Min, diff::Static, Some(()), vec![])]
    #[case(Min, 1_i32, Some(()), vec![])]
    #[case(Max, diff::Static, Some(()), vec![])]
    #[case(Max, 1_i32, Some(()), vec![])]
    fn empty_groups_emit_only_defined_results<A, R>(
        #[case] reduction: A,
        #[case] _weight: R,
        #[case] empty_key: Option<()>,
        #[case] expected: Vec<(i64, (), R)>,
    ) where
        A: Reduction<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
        R: AggregateStrategy<(), R, Output = R> + ExchangeData + Sync + FromOne,
    {
        assert_eq!(global_aggregate(reduction, vec![], empty_key), expected);
    }

    /// A global count counts distinct matches, a row twice being one.
    // Cases: weight, answers.
    #[rstest]
    #[case(diff::Static, vec![(3, (), diff::Static)])]
    #[case(1_i32, vec![(3, (), 1)])]
    fn a_global_count_counts_distinct_matches<R>(
        #[case] _weight: R,
        #[case] expected: Vec<(i64, (), R)>,
    ) where
        R: AggregateStrategy<(), R, Output = R> + ExchangeData + Sync + FromOne,
    {
        assert_eq!(
            global_aggregate(Count, vec![10_i64, 20, 30, 20], Some(())),
            expected
        );
    }

    /// Two distinct matches with equal targets both count: the target is
    /// read out of the match, which stays the unit of distinctness.
    #[test]
    fn equal_targets_of_distinct_matches_both_count() {
        let actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<(u8, (i64, i64)), i32>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.arrange_by_key(),
                    Groups::<_, _, i32>::None,
                    "Sum",
                    Sum,
                    |(_, target)| *target,
                    |key, sum: i64| (key, sum),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                input
            });
            for row in [(1, (1, 5)), (1, (2, 5)), (2, (1, 7))] {
                input.update(row, 1);
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, vec![((1, 10), (), 1), ((2, 7), (), 1)]);
    }

    /// A mean answers a float over any column.
    #[test]
    fn a_mean_answers_a_float() {
        let actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.map(|value| ((), value)).arrange_by_key(),
                    Groups::<_, _, diff::Static>::Key(()),
                    "Mean",
                    Mean,
                    |value| *value,
                    |(), mean: OrderedFloat<f64>| mean,
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                input
            });
            for value in [1, 2] {
                input.update(value, diff::Static);
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, vec![(OrderedFloat(1.5), (), diff::Static)]);
    }

    /// A mutable aggregate answers what a static one does, bit for bit,
    /// even where an identity could show: a named group's sum starts from
    /// the sum of no rows, `0`, and every other accumulation from its first
    /// row.
    // Cases: reduction, row, group, answer.
    #[rstest]
    #[case::sum_of_a_named_group(Sum, -0.0, Some(()), 0.0)]
    #[case::sum_of_the_matches(Sum, -0.0, None, -0.0)]
    #[case::mean(Mean, -0.0, Some(()), -0.0)]
    #[case::min(Min, f64::NAN, Some(()), f64::NAN)]
    fn a_mutable_aggregate_answers_as_a_static_one<A>(
        #[case] reduction: A,
        #[case] row: f64,
        #[case] group: Option<()>,
        #[case] answer: f64,
    ) where
        A: Reduction<OrderedFloat<f64>, OrderedFloat<f64>> + Copy + Send + Sync,
        A::Semiring: ExchangeData,
    {
        fn bits<R>(answers: Vec<(OrderedFloat<f64>, (), R)>) -> Vec<u64> {
            answers
                .into_iter()
                .map(|(answer, (), _)| answer.0.to_bits())
                .collect()
        }
        let rows = vec![OrderedFloat(row)];
        let expected = vec![answer.to_bits()];
        let static_answers =
            global_aggregate::<_, diff::Static, _, _>(reduction, rows.clone(), group);
        let mutable_answers = global_aggregate::<_, diff::Mutable, _, _>(reduction, rows, group);
        assert_eq!(bits(static_answers), expected);
        assert_eq!(bits(mutable_answers), expected);
    }

    /// A presence aggregate answers once, and answers again when a later
    /// time changes the aggregate.
    // Cases: reduction, answers.
    #[rstest]
    #[case(Count, vec![(0, 0, diff::Static), (1, 1, diff::Static), (2, 3, diff::Static)])]
    #[case(Sum, vec![(0, 0, diff::Static), (7, 1, diff::Static), (15, 3, diff::Static)])]
    fn a_presence_aggregate_emits_once_and_allows_later_input<A>(
        #[case] reduction: A,
        #[case] expected: Vec<(i64, u32, diff::Static)>,
    ) where
        A: Reduction<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.map(|value| ((), value)).arrange_by_key(),
                    Groups::<_, _, diff::Static>::Key(()),
                    "Reduce",
                    reduction,
                    |value| *value,
                    |(), value| value,
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                input
            });
            for (epoch, value) in [None, Some(7), None, Some(8)].into_iter().enumerate() {
                if let Some(value) = value {
                    input.update(value, diff::Static);
                }
                let next = epoch as u32 + 1;
                input.advance_to(next);
                input.flush();
                worker.step_while(|| probe.less_than(&next));
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, expected);
    }

    /// A signed global aggregate retracts each superseded answer and
    /// restores the empty result once every row is gone.
    // Cases: reduction, answers.
    #[rstest]
    #[case(Count, vec![
        (0, 0, 1), (0, 1, -1), (0, 3, 1), (0, 4, -1), (0, 5, 1), (0, 6, -1),
        (1, 2, 1), (1, 3, -1), (1, 4, 1), (1, 5, -1), (1, 6, 1),
        (2, 1, 1), (2, 2, -1),
    ])]
    #[case(Sum, vec![
        (0, 0, 1), (0, 1, -1), (0, 3, 1), (0, 6, -1),
        (8, 2, 1), (8, 3, -1), (9, 6, 1), (15, 1, 1), (15, 2, -1),
    ])]
    fn a_signed_global_aggregate_retracts_and_restores_the_empty_result<A>(
        #[case] reduction: A,
        #[case] expected: Vec<(i64, u32, i32)>,
    ) where
        A: Reduction<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, i32>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.map(|value| ((), value)).arrange_by_key(),
                    Groups::<_, _, i32>::Key(()),
                    "Reduce",
                    reduction,
                    |value| *value,
                    |(), value| value,
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                input
            });
            for (epoch, updates) in [
                vec![],
                vec![(7, 1), (8, 1)],
                vec![(7, -1)],
                vec![(8, -1)],
                vec![(0, 1)],
                vec![(0, -1)],
                vec![(9, 1)],
            ]
            .into_iter()
            .enumerate()
            {
                for (value, diff) in updates {
                    input.update(value, diff);
                }
                let next = epoch as u32 + 1;
                input.advance_to(next);
                input.flush();
                worker.step_while(|| probe.less_than(&next));
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, expected);
    }

    type GroupRow = (u8, i64);

    /// Inside an incremental loop the one group's empty result is scoped
    /// by its key and by the loop's partially ordered times.
    // Cases: reduction, answers.
    #[rstest]
    #[case(Count, vec![
        ((0, 0), (0, 0), 1), ((0, 0), (0, 1), -1),
        ((0, 0), (1, 0), -1), ((0, 0), (1, 1), 1),
        ((0, 1), (0, 1), 1), ((0, 1), (1, 0), 1), ((0, 1), (1, 1), -2),
        ((0, 2), (1, 1), 1), ((1, 1), (0, 0), 1),
    ])]
    #[case(Sum, vec![
        ((0, 0), (0, 0), 1), ((0, 0), (0, 1), -1),
        ((0, 0), (1, 0), -1), ((0, 0), (1, 1), 1),
        ((0, 7), (0, 1), 1), ((0, 7), (1, 1), -1),
        ((0, 8), (1, 0), 1), ((0, 8), (1, 1), -1),
        ((0, 15), (1, 1), 1), ((1, 9), (0, 0), 1),
    ])]
    fn the_empty_result_is_scoped_by_key_and_partial_time<A>(
        #[case] reduction: A,
        #[case] expected: Vec<(GroupRow, (u32, u32), i32)>,
    ) where
        A: Reduction<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<(GroupRow, u32), i32>();
                let seen = Rc::clone(&seen);
                scope.iterative::<u32, _, _>(|inner| {
                    let rows = rows
                        .enter_at(inner, |(_, iteration)| *iteration)
                        .map(|(row, _)| row);
                    flowlog_aggregate(
                        rows.arrange_by_key(),
                        Groups::<_, _, i32>::Key(0),
                        "Reduce",
                        reduction,
                        |value| *value,
                        |key, value| (key, value),
                    )
                    .inspect(move |&(row, time, diff)| {
                        seen.borrow_mut()
                            .push((row, (time.outer, time.inner), diff));
                    })
                    .leave(scope)
                });
                input
            });
            input.update_at(((0, 7), 1), 0, 1);
            input.update_at(((0, 8), 0), 1, 1);
            input.update_at(((1, 9), 0), 0, 1);
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, expected);
    }

    /// A group the groups arrangement names answers the empty result
    /// while it has no match, under either weight.
    // Cases: weight.
    #[rstest]
    #[case(diff::Static)]
    #[case(1_i32)]
    fn groups_without_a_match_answer_the_empty_result<R>(#[case] weight: R)
    where
        R: AggregateStrategy<(), R, Output = R> + ExchangeData + Sync,
    {
        let expected: Vec<((u8, i64), (), R)> = [(1, 2), (2, 1), (4, 0)]
            .into_iter()
            .map(|row| (row, (), weight.clone()))
            .collect();
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut rows, mut groups) = worker.dataflow::<(), _, _>(|scope| {
                let (rows_input, rows) = scope.new_collection::<(u8, i64), R>();
                let (groups_input, groups) = scope.new_collection::<u8, R>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.arrange_by_key(),
                    Groups::Arranged(groups.arrange_by_self()),
                    "Count",
                    Count,
                    |value| *value,
                    |key, count: i64| (key, count),
                )
                .inspect(move |update| seen.borrow_mut().push(update.clone()));
                (rows_input, groups_input)
            });
            for key in [1, 2, 4] {
                groups.update(key, weight.clone());
            }
            for row in [(1, 10), (1, 20), (2, 30)] {
                rows.update(row, weight.clone());
            }
            rows.close();
            groups.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, expected);
    }

    /// Matches arranged by the key alone, having no value, are one match
    /// per key however many times the arrangement holds it, under either
    /// weight.
    // Cases: weight.
    #[rstest]
    #[case(diff::Static)]
    #[case(1_i32)]
    fn matches_arranged_by_key_alone_count_each_key_once<R>(#[case] weight: R)
    where
        R: AggregateStrategy<(), R, Output = R> + ExchangeData + Sync,
    {
        let expected: Vec<((u8, i64), (), R)> = [(1, 1), (2, 0)]
            .into_iter()
            .map(|row| (row, (), weight.clone()))
            .collect();
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut rows, mut groups) = worker.dataflow::<(), _, _>(|scope| {
                let (rows_input, rows) = scope.new_collection::<u8, R>();
                let (groups_input, groups) = scope.new_collection::<u8, R>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.arrange_by_self(),
                    Groups::Arranged(groups.arrange_by_self()),
                    "Count",
                    Count,
                    |_| (),
                    |key, count: i64| (key, count),
                )
                .inspect(move |update| seen.borrow_mut().push(update.clone()));
                (rows_input, groups_input)
            });
            for key in [1, 2] {
                groups.update(key, weight.clone());
            }
            for key in [1, 1] {
                rows.update(key, weight.clone());
            }
            rows.close();
            groups.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, expected);
    }

    /// A match announced by every worker must still count once.
    // Cases: weight, rows each worker announces, answers.
    #[rstest]
    #[case(diff::Static, vec![], vec![(0, (), diff::Static)])]
    #[case(diff::Static, vec![10, 20, 30], vec![(3, (), diff::Static)])]
    #[case(1_i32, vec![], vec![(0, (), 1)])]
    #[case(1_i32, vec![10, 20, 30], vec![(3, (), 1)])]
    fn a_global_count_answers_once_on_many_workers<R>(
        #[case] _weight: R,
        #[case] rows: Vec<i64>,
        #[case] expected: Vec<(i64, (), R)>,
    ) where
        R: AggregateStrategy<(), R, Output = R> + ExchangeData + Sync + FromOne,
    {
        let mut actual: Vec<_> = timely::execute(timely::Config::process(4), move |worker| {
            // The 'static inspect closure and the test both need the results.
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, R>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.map(|value| ((), value)).arrange_by_key(),
                    Groups::<_, _, R>::Key(()),
                    "Count",
                    Count,
                    |value| *value,
                    |(), count| count,
                )
                .inspect(move |update| seen.borrow_mut().push(update.clone()));
                input
            });
            for &value in &rows {
                input.update(value, R::from_one());
            }
            input.close();
            while worker.step() {}
            seen.take()
        })
        .unwrap()
        .join()
        .into_iter()
        .flat_map(Result::unwrap)
        .collect();
        consolidate_updates(&mut actual);
        assert_eq!(actual, expected);
    }

    /// Duplicate announcements across workers must not multiply a group's
    /// count or its empty result.
    #[test]
    fn a_presence_count_answers_each_group_once_on_many_workers() {
        let mut actual: Vec<_> = timely::execute(timely::Config::process(4), |worker| {
            // The 'static inspect closure and the test both need the results.
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut rows, mut groups) = worker.dataflow::<(), _, _>(|scope| {
                let (rows_input, rows) = scope.new_collection::<(u8, i64), diff::Static>();
                let (groups_input, groups) = scope.new_collection::<u8, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.arrange_by_key(),
                    Groups::Arranged(groups.arrange_by_self()),
                    "Count",
                    Count,
                    |value| *value,
                    |key, count: i64| (key, count),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                (rows_input, groups_input)
            });
            for key in 0..8 {
                groups.update(key, diff::Static);
                for value in 0..i64::from(key) {
                    rows.update((key, value), diff::Static);
                }
            }
            rows.close();
            groups.close();
            while worker.step() {}
            seen.take()
        })
        .unwrap()
        .join()
        .into_iter()
        .flat_map(Result::unwrap)
        .collect();
        actual.sort();
        let expected = vec![
            ((0, 0), (), diff::Static),
            ((1, 1), (), diff::Static),
            ((2, 2), (), diff::Static),
            ((3, 3), (), diff::Static),
            ((4, 4), (), diff::Static),
            ((5, 5), (), diff::Static),
            ((6, 6), (), diff::Static),
            ((7, 7), (), diff::Static),
        ];
        assert_eq!(actual, expected);
    }

    /// One epoch's signed updates to the groups, then to the rows.
    type Epoch = (Vec<(u8, i32)>, Vec<((u8, i64), i32)>);

    /// A group's empty result goes with its key once, however many ways
    /// the key is derived, and a first match replaces it; both retract
    /// under `diff::Mutable`.
    #[test]
    fn a_groups_empty_result_follows_its_key() {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut rows, mut groups) = worker.dataflow::<u32, _, _>(|scope| {
                let (rows_input, rows) = scope.new_collection::<(u8, i64), i32>();
                let (groups_input, groups) = scope.new_collection::<u8, i32>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.arrange_by_key(),
                    Groups::Arranged(groups.arrange_by_self()),
                    "Count",
                    Count,
                    |value| *value,
                    |key, count: i64| (key, count),
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (rows_input, groups_input)
            });
            // Epoch 0: groups 1, derived twice, and 4. Epoch 1: a match
            // for 1. Epoch 2: the match goes. Epoch 3: group 4 goes.
            let epochs: [Epoch; 4] = [
                (vec![(1, 2), (4, 1)], vec![]),
                (vec![], vec![((1, 10), 1)]),
                (vec![], vec![((1, 10), -1)]),
                (vec![(4, -1)], vec![]),
            ];
            for (epoch, (group_updates, row_updates)) in epochs.into_iter().enumerate() {
                for (key, diff) in group_updates {
                    groups.update(key, diff);
                }
                for (row, diff) in row_updates {
                    rows.update(row, diff);
                }
                let next = epoch as u32 + 1;
                rows.advance_to(next);
                groups.advance_to(next);
                rows.flush();
                groups.flush();
                worker.step_while(|| probe.less_than(&next));
            }
            rows.close();
            groups.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(
            actual,
            vec![
                ((1, 0), 0, 1),
                ((1, 0), 1, -1),
                ((1, 0), 2, 1),
                ((1, 1), 1, 1),
                ((1, 1), 2, -1),
                ((4, 0), 0, 1),
                ((4, 0), 3, -1),
            ]
        );
    }

    /// Static groups seed a signed aggregate as they are: the groups'
    /// weight need not be the matches'.
    #[test]
    fn static_groups_seed_a_signed_aggregate() {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut rows, mut groups) = worker.dataflow::<(), _, _>(|scope| {
                let (rows_input, rows) = scope.new_collection::<(u8, i64), i32>();
                let (groups_input, groups) = scope.new_collection::<u8, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.arrange_by_key(),
                    Groups::Arranged(groups.arrange_by_self()),
                    "Count",
                    Count,
                    |value| *value,
                    |key, count: i64| (key, count),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                (rows_input, groups_input)
            });
            for key in [1, 2] {
                groups.update(key, diff::Static);
            }
            rows.update((1, 10), 1);
            rows.close();
            groups.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, vec![((1, 1), (), 1), ((2, 0), (), 1)]);
    }

    /// Runs an append aggregate of `reduction` over `epochs`, each
    /// epoch's rows inserted at its index, and returns the signed answers
    /// consolidated.
    fn append_epochs<A, X, C>(reduction: A, epochs: Vec<Vec<X>>) -> Vec<(C, u32, diff::Mutable)>
    where
        A: Reduction<X, C> + Send + Sync,
        A::Semiring: ExchangeData,
        X: ExchangeData + Sync,
        C: Scalar,
    {
        let mut actual = timely::execute_directly(move |worker| {
            // The operator's 'static callback and the test both need the
            // results; shared ownership lets the test read them after stepping.
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<X, diff::Append>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.map(|value| ((), value)).arrange_by_key(),
                    Groups::<_, _, diff::Append>::Key(()),
                    "Reduce",
                    reduction,
                    X::clone,
                    |(), value| value,
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                input
            });
            let epochs = epochs.into_iter().enumerate();
            for (epoch, values) in epochs {
                for value in values {
                    input.update(value, diff::Append);
                }
                let next = epoch as u32 + 1;
                input.advance_to(next);
                input.flush();
                worker.step_while(|| probe.less_than(&next));
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        actual
    }

    /// An append count answers in signed changes: the empty group's zero
    /// is retracted by the first answer, and each later answer retracts
    /// the one before, so the answers always form a one-row set.
    #[test]
    fn append_count_retracts_each_superseded_answer() {
        assert_eq!(
            append_epochs(Count, vec![vec![], vec![7], vec![], vec![8, 9]]),
            vec![(0, 0, 1), (0, 1, -1), (1, 1, 1), (1, 3, -1), (3, 3, 1)]
        );
    }

    #[test]
    fn append_min_changes_only_when_the_bound_tightens() {
        assert_eq!(
            append_epochs(Min, vec![vec![10], vec![20], vec![5]]),
            vec![(5, 2, 1), (10, 0, 1), (10, 2, -1)]
        );
    }

    #[test]
    fn append_min_of_an_empty_group_emits_nothing() {
        assert_eq!(append_epochs(Min, vec![Vec::<i64>::new()]), vec![]);
    }

    // Cases: reduction, signed answers.
    #[rstest]
    #[case(Sum, vec![
        (OrderedFloat(0.0), 0, 1), (OrderedFloat(0.0), 1, -1),
        (OrderedFloat(1.5), 1, 1), (OrderedFloat(1.5), 2, -1),
        (OrderedFloat(4.0), 2, 1),
    ])]
    #[case(Mean, vec![
        (OrderedFloat(1.5), 1, 1), (OrderedFloat(1.5), 2, -1),
        (OrderedFloat(2.0), 2, 1),
    ])]
    fn append_float_aggregates_replace_answers_and_ignore_repeated_matches<A>(
        #[case] reduction: A,
        #[case] expected: Vec<(OrderedFloat<f64>, u32, diff::Mutable)>,
    ) where
        A: Reduction<OrderedFloat<f64>, OrderedFloat<f64>> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let epochs = vec![
            vec![],
            vec![OrderedFloat(1.5)],
            vec![OrderedFloat(2.5)],
            vec![OrderedFloat(1.5)],
        ];
        assert_eq!(append_epochs(reduction, epochs), expected);
    }

    /// Rounding may change the last bit, but must not leave stale answers
    /// or turn a replacement into multiple live rows.
    // Cases: reduction, allowed answers after each epoch.
    #[rstest]
    #[case(Sum, [
        1e16..=1e16, 1e16..=1e16, 1e16..=10_000_000_000_000_002.0,
    ])]
    #[case(Mean, [
        1e16..=1e16, 5e15..=5e15,
        3_333_333_333_333_333.0..=3_333_333_333_333_334.0,
    ])]
    fn append_float_rounding_preserves_one_current_answer<A>(
        #[case] reduction: A,
        #[case] bounds: [RangeInclusive<f64>; 3],
    ) where
        A: Reduction<OrderedFloat<f64>, OrderedFloat<f64>> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let epochs = vec![
            vec![OrderedFloat(1e16)],
            vec![OrderedFloat(1.0)],
            vec![OrderedFloat(0.5)],
        ];
        let updates = append_epochs(reduction, epochs);
        let mut live = BTreeMap::new();
        let epochs = bounds.into_iter().enumerate();
        for (epoch, expected) in epochs {
            for &(answer, time, diff) in &updates {
                if time == epoch as u32 {
                    *live.entry(answer).or_insert(0) += diff;
                }
            }
            live.retain(|_, diff| *diff != 0);
            assert_eq!(live.len(), 1);
            let (&answer, &diff) = live.first_key_value().unwrap();
            assert_eq!(diff, 1);
            assert!(expected.contains(&answer.0), "epoch {epoch}: {answer}");
        }
    }

    /// An append group answers zero from when its key appears, however
    /// many times it is derived, and each match replaces the answer
    /// before.
    // Cases: reduction, answers.
    #[rstest]
    #[case(Count, vec![
        ((1, 0), 0, 1), ((1, 0), 1, -1), ((1, 1), 1, 1), ((1, 1), 2, -1), ((1, 2), 2, 1),
        ((2, 0), 0, 1), ((2, 0), 2, -1), ((2, 1), 2, 1),
        ((3, 0), 2, 1), ((3, 0), 3, -1), ((3, 1), 3, 1),
    ])]
    #[case(Sum, vec![
        ((1, 0), 0, 1), ((1, 0), 1, -1), ((1, 10), 1, 1), ((1, 10), 2, -1), ((1, 30), 2, 1),
        ((2, 0), 0, 1), ((2, 0), 2, -1), ((2, 5), 2, 1),
        ((3, 0), 2, 1), ((3, 0), 3, -1), ((3, 7), 3, 1),
    ])]
    fn append_groups_answer_zero_until_a_match_replaces_it<A>(
        #[case] reduction: A,
        #[case] expected: Vec<(GroupRow, u32, diff::Mutable)>,
    ) where
        A: Reduction<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let mut actual = timely::execute_directly(move |worker| {
            // The operator's 'static callback and the test both need the
            // results; shared ownership lets the test read them after stepping.
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut rows, mut groups) = worker.dataflow::<u32, _, _>(|scope| {
                let (rows_input, rows) = scope.new_collection::<(u8, i64), diff::Append>();
                let (groups_input, groups) = scope.new_collection::<u8, diff::Append>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.arrange_by_key(),
                    Groups::Arranged(groups.arrange_by_self()),
                    "Reduce",
                    reduction,
                    |value| *value,
                    |key, answer: i64| (key, answer),
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (rows_input, groups_input)
            });
            let epochs = [
                (vec![1, 1, 2], vec![]),
                (vec![], vec![(1, 10)]),
                (vec![1, 3], vec![(1, 20), (2, 5)]),
                (vec![], vec![(3, 7)]),
            ];
            let epochs = epochs.into_iter().enumerate();
            for (epoch, (keys, matches)) in epochs {
                for key in keys {
                    groups.update(key, diff::Append);
                }
                for row in matches {
                    rows.update(row, diff::Append);
                }
                let next = epoch as u32 + 1;
                rows.advance_to(next);
                groups.advance_to(next);
                rows.flush();
                groups.flush();
                worker.step_while(|| probe.less_than(&next));
            }
            rows.close();
            groups.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, expected);
    }

    /// Every strategy runs inside a lexicographic loop: its times are
    /// totally ordered, as the presence strategies require.
    #[test]
    fn every_strategy_admits_a_lex_loop() {
        fn admits<R: AggregateStrategy<LexLoop, R>>() {}
        admits::<diff::Static>();
        admits::<diff::Append>();
        admits::<diff::Mutable>();
    }
}
