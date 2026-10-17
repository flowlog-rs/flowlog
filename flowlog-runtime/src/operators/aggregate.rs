//! Aggregate expressions: the matches of an aggregate's braces, arranged
//! by the group key, reduced by the reduction over its target, and the
//! groups answered without a match. An aggregate reads the arrangement
//! its matches come as, and a signed one the groups' too.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::KeySpine;
use differential_dataflow::trace::implementations::ValSpine;
use timely::dataflow::operators::core::ToStream;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use super::reduce::Reduction;
use super::reduce::presence::lift;
use super::reduce::presence::lower;
use super::reduce::presence::settle;
use super::reduce::semiring::Scalar;
use super::reduce::semiring::Semiring;
use super::reduce::signed::defaults;
use super::reduce::signed::reduce;
use crate::diff;
use crate::diff::Presence;
use crate::operators::dedup::FlowlogDedup;
use crate::operators::dedup::flowlog_dedup;
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
    /// the key. Under a signed weight a group answers from when its key
    /// appears until it goes. Under a presence weight an answer is never
    /// withdrawn, so a key must not precede its matches: its empty result
    /// would stay beside the answer over them.
    Arranged(Arranged<'scope, TraceAgent<KeySpine<K, T, R>>>),
}

/// Aggregates `matches`, arranged by the group key, under `reduction`:
/// `target` reads the aggregated column out of a match's value, the
/// unit that tells two matches of a group apart, and `merge` rebuilds an
/// answer row from a key and its answer. Each of `groups` answers the
/// reduction's zero while it has no match, when the reduction answers an
/// empty group; `merge` is cloned to build those answers.
///
/// A signed aggregate counts each distinct match once whatever its
/// count, and replaces each answer through insertions and retractions.
/// A presence aggregate counts each match once at its first time, and
/// can emit a new answer at a later timestamp but never retract one.
///
/// Every key the matches hold must be a group's when `groups` names
/// them: such a key's zero is cancelled by its answer.
pub fn flowlog_aggregate<'scope, A, T, K, V, X, C, O, R, Rg>(
    matches: Arranged<'scope, TraceAgent<ValSpine<K, V, T, R>>>,
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
    T: Timestamp + Lattice,
    R: AggregateStrategy<T, Rg>,
    Rg: Semigroup + Ord + 'static,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    O: Data,
    VecCollection<'scope, T, (K, V), R>: FlowlogDedup,
{
    R::aggregate(
        matches,
        groups,
        name,
        A::DEFINED_ON_EMPTY,
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
/// already. A presence aggregate takes presence
/// groups, as it cannot withdraw a group's empty result; a signed one
/// takes groups of any weight.
pub trait AggregateStrategy<T: Timestamp + Lattice, Rg: Semigroup + Ord + 'static>:
    Semigroup + Ord + Sized + 'static
{
    /// The answers' weight.
    type Output: Semigroup + 'static;

    /// Applies the contracts of [`flowlog_aggregate`].
    ///
    /// With the reduction `defined_on_empty`, each of `groups` without a
    /// match answers `S::zero()`, which must then be a live weight;
    /// without it they answer nothing.
    fn aggregate<'scope, K, V, S, O>(
        matches: Arranged<'scope, TraceAgent<ValSpine<K, V, T, Self>>>,
        groups: Groups<'scope, T, K, Rg>,
        name: &str,
        defined_on_empty: bool,
        contribute: impl Fn(&V) -> S + 'static,
        merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
    ) -> VecCollection<'scope, T, O, Self::Output>
    where
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
        VecCollection<'scope, T, (K, V), Self>: FlowlogDedup;
}

impl<T, Rg> AggregateStrategy<T, Rg> for diff::Static
where
    T: Timestamp + TotalOrder + Lattice,
    Rg: Presence,
{
    type Output = diff::Static;

    fn aggregate<'scope, K, V, S, O>(
        matches: Arranged<'scope, TraceAgent<ValSpine<K, V, T, Self>>>,
        groups: Groups<'scope, T, K, Rg>,
        name: &str,
        defined_on_empty: bool,
        contribute: impl Fn(&V) -> S + 'static,
        merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
    ) -> VecCollection<'scope, T, O, Self>
    where
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
        VecCollection<'scope, T, (K, V), Self>: FlowlogDedup,
    {
        // The arrangement holds a match once per time it was announced;
        // the dedup keeps its first, so each match contributes once.
        let rows = flowlog_dedup(matches.as_collection(|key, value| (key.clone(), value.clone())));
        let (key_seed, seeds) = match (groups, defined_on_empty) {
            (Groups::None, _) | (_, false) => (None, None),
            (Groups::Key(key), true) => (Some(key), None),
            (Groups::Arranged(groups), true) => {
                // A group's zero is a live weight, as the key seed below:
                // a group with matches adds its contributions to it.
                let keys = groups.as_collection(|key, ()| key.clone());
                let seeds = flowlog_map(keys, name, move |key, time, _| {
                    std::iter::once((key, time, S::zero()))
                });
                (None, Some(seeds))
            }
        };
        let lifted = lift(rows, name, key_seed, contribute, |(key, value)| {
            (key, value)
        });
        let lifted = match seeds {
            Some(seeds) => lifted.concat(seeds),
            None => lifted,
        };
        lower(settle(lifted), name, merge)
    }
}

/// Implements the strategy for a signed weight, whose answers are
/// `diff::Mutable`.
macro_rules! signed {
    ($weight:ty) => {
        impl<T: Timestamp + Lattice, Rg: Semigroup + Ord + 'static> AggregateStrategy<T, Rg>
            for $weight
        {
            type Output = diff::Mutable;

            fn aggregate<'scope, K, V, S, O>(
                matches: Arranged<'scope, TraceAgent<ValSpine<K, V, T, Self>>>,
                groups: Groups<'scope, T, K, Rg>,
                name: &str,
                defined_on_empty: bool,
                contribute: impl Fn(&V) -> S + 'static,
                merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
            ) -> VecCollection<'scope, T, O, Self::Output>
            where
                K: ExchangeData + Hashable,
                V: ExchangeData,
                S: Semiring + ExchangeData,
                O: Data,
                VecCollection<'scope, T, (K, V), Self>: FlowlogDedup,
            {
                signed_aggregate(matches, groups, name, defined_on_empty, contribute, merge)
            }
        }
    };
}

signed!(diff::Mutable);
signed!(diff::Append);

/// Aggregates arranged `matches` with the zero of each of `groups`: the
/// groups' defaults, one row per group, concatenated with the answers,
/// each of which cancels its group's default.
fn signed_aggregate<'scope, T, K, V, S, O, R, Rg>(
    matches: Arranged<'scope, TraceAgent<ValSpine<K, V, T, R>>>,
    groups: Groups<'scope, T, K, Rg>,
    name: &str,
    defined_on_empty: bool,
    contribute: impl Fn(&V) -> S + 'static,
    mut merge: impl FnMut(K, S::Value) -> O + Clone + 'static,
) -> VecCollection<'scope, T, O, diff::Mutable>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    S: Semiring + ExchangeData,
    O: Data,
    R: Semigroup + ExchangeData,
    Rg: Semigroup + Ord + 'static,
{
    // The default of each group, and the keys the answers cancel a
    // default for: `None` for every key, as every match's key is a
    // group's.
    let (defaults, cancelled) = match (groups, defined_on_empty) {
        (Groups::None, _) | (_, false) => (None, None),
        (Groups::Key(key), true) => {
            // Minimum time makes the default visible at every later
            // logical time. The iterator is consumed once on worker 0; a
            // recursive iteration does not construct a new source or
            // insert it again.
            let scope = matches.stream.scope();
            let default = (scope.index() == 0)
                .then(|| (merge(key.clone(), S::zero().finish()), T::minimum(), 1))
                .into_iter()
                .to_stream(scope)
                .as_collection();
            (Some(default), Some(Some(key)))
        }
        (Groups::Arranged(groups), true) => (
            Some(defaults(groups, name, S::zero().finish(), merge.clone())),
            Some(None),
        ),
    };
    let answers = reduce(matches, name, contribute, cancelled, merge);
    match defaults {
        Some(defaults) => answers.concat(defaults),
        None => answers,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
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
    fn global_aggregate<A, R>(
        reduction: A,
        rows: Vec<i64>,
        empty_key: Option<()>,
    ) -> Vec<(i64, (), R)>
    where
        A: Reduction<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
        R: AggregateStrategy<(), R, Output = R> + ExchangeData + Sync + FromOne,
        for<'a> VecCollection<'a, (), ((), i64), R>: FlowlogDedup,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, R>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.map(|value| ((), value)).arrange_by_key(),
                    global::<(), R>(empty_key),
                    "Reduce",
                    reduction,
                    |value| *value,
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
        for<'a> VecCollection<'a, (), ((), i64), R>: FlowlogDedup,
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
        for<'a> VecCollection<'a, (), ((), i64), R>: FlowlogDedup,
    {
        assert_eq!(
            global_aggregate(Count, vec![10, 20, 30, 20], Some(())),
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
        for<'a> VecCollection<'a, (), (u8, i64), R>: FlowlogDedup,
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
    fn append_epochs<A>(reduction: A, epochs: Vec<Vec<i64>>) -> Vec<(i64, u32, diff::Mutable)>
    where
        A: Reduction<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, diff::Append>();
                let seen = Rc::clone(&seen);
                flowlog_aggregate(
                    rows.map(|value| ((), value)).arrange_by_key(),
                    Groups::<_, _, diff::Append>::Key(()),
                    "Reduce",
                    reduction,
                    |value| *value,
                    |(), value| value,
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                input
            });
            for (epoch, values) in epochs.into_iter().enumerate() {
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

    /// An append minimum changes only when a smaller value arrives, so a
    /// larger later value emits nothing.
    #[test]
    fn append_min_changes_only_when_the_bound_tightens() {
        assert_eq!(
            append_epochs(Min, vec![vec![10], vec![20], vec![5]]),
            vec![(5, 2, 1), (10, 0, 1), (10, 2, -1)]
        );
    }

    /// An append group with no rows and no defined empty result emits
    /// nothing.
    #[test]
    fn append_min_of_an_empty_group_emits_nothing() {
        assert_eq!(append_epochs(Min, vec![vec![]]), vec![]);
    }

    /// Both strategies run inside a lexicographic loop: its times are
    /// totally ordered, as the `diff::Static` strategy requires.
    #[test]
    fn both_strategies_admit_a_lex_loop() {
        fn admits<R: AggregateStrategy<LexLoop, R>>() {}
        admits::<diff::Static>();
        admits::<diff::Mutable>();
    }
}
