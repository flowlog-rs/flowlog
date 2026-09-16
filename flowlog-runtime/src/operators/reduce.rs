//! Group-by aggregation for generated FlowLog rules.
//!
//! [`Aggregation`] defines row contributions and optional empty results.
//! [`ReduceStrategy`] selects the implementation: `present` accumulates
//! contributions as weights; `incremental` maintains retractable results.
//! The accumulators live in `semiring`.
//!
//! [`flowlog_reduce`] runs inside either kind of dataflow. Recursive
//! `Present` aggregates also use [`flowlog_reduce_leave`] to produce one
//! final answer after the loop, since they cannot retract earlier answers.

mod incremental;
mod present;
mod semiring;

use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
pub use present::flowlog_reduce_leave;
use semiring::Largest;
use semiring::Mean;
use semiring::Scalar;
use semiring::Semiring;
use semiring::Smallest;
use semiring::Total;
use timely::progress::Timestamp;

// =============================================================================
// Operators
// =============================================================================

/// Groups rows by key and reduces each group under `aggregation`.
///
/// `split` produces the group key and aggregated value; `merge` rebuilds
/// an output row. The pairs produced by `split` must already be deduplicated
/// so both weight strategies count the same set of contributions.
///
/// `empty_key` names a group that exists even without input. That group
/// receives the aggregation's empty result when defined; no other absent
/// keys are invented. Passing `None` disables empty-group output.
///
/// With `i32`, each result is replaced through insertions and retractions.
/// Raw updates may cancel at the same timestamp.
///
/// `Present` can emit a new answer at a later timestamp but cannot retract
/// earlier answers, including an initial empty result. Recursive final
/// output therefore requires [`flowlog_reduce_leave`].
pub fn flowlog_reduce<'scope, A, T, D, K, V, C, O, R>(
    collection: VecCollection<'scope, T, D, R>,
    name: &str,
    _aggregation: A,
    empty_key: Option<K>,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'scope, T, O, R>
where
    A: Aggregation<V, C>,
    A::Semiring: ExchangeData,
    C: Scalar,
    T: Timestamp + Lattice,
    R: ReduceStrategy<T>,
    D: Data,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    O: Data,
{
    // Both conditions are needed: a known group key and a defined empty
    // result. A caller cannot request zero for min/max/avg by passing a key.
    R::reduce(
        collection,
        name,
        empty_key.zip(A::EMPTY_RESULT),
        A::contribute,
        split,
        merge,
    )
}

// =============================================================================
// ReduceStrategy
// =============================================================================

/// Accumulates groups using the operations supported by the input weight.
///
/// `Present` has no inverse: it accumulates contributions as semiring
/// weights and can only add answers. `i32` supports retractions and keeps
/// the current answer through Differential Dataflow's reduce.
///
/// The `Present` strategy requires totally ordered timestamps. The `i32`
/// strategy also supports the partially ordered timestamps of incremental
/// recursion.
pub trait ReduceStrategy<T: Timestamp + Lattice>: Semigroup + Sized {
    /// Applies the input and output contracts of [`flowlog_reduce`].
    ///
    /// `empty_group` pairs a known key with a result whose lifted weight
    /// must be neutral and remain visible to consolidation.
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
        O: Data;
}

// =============================================================================
// Aggregations
// =============================================================================

/// What one row contributes to its group, for a given aggregated column.
///
/// Both strategies use this definition: `Present` contributes per row,
/// `i32` per arranged entry. They must agree on what one distinct input
/// contributes so batch and incremental modes produce the same result.
pub trait Aggregation<V, C>: 'static {
    /// The accumulator this aggregation runs in.
    type Semiring: Semiring<Value = C>;

    /// Result for a known group with no input; `None` leaves it undefined.
    ///
    /// A defined result must lift to a neutral contribution that remains
    /// visible as a weight. Numeric zero must not mean an absent record.
    const EMPTY_RESULT: Option<C> = None;

    /// Turns one row's column into that row's contribution.
    fn contribute(value: &V) -> Self::Semiring;
}

/// Declares one aggregation over the column's own type.
macro_rules! aggregation {
    ($name:ident, $semiring:ident, $doc:literal $(, empty = $empty:ident)?) => {
        #[doc = $doc]
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub struct $name;

        impl<V: Scalar> Aggregation<V, V> for $name {
            type Semiring = $semiring<V>;

            $(const EMPTY_RESULT: Option<V> = Some(V::$empty);)?

            #[inline]
            fn contribute(value: &V) -> Self::Semiring {
                $semiring::lift(*value)
            }
        }
    };
}

aggregation!(Min, Smallest, "Smallest value in the column.");
aggregation!(Max, Largest, "Largest value in the column.");
aggregation!(Sum, Total, "Total of the column.", empty = ZERO);
aggregation!(Avg, Mean, "Mean of the column.");

/// Number of rows in the group.
///
/// Unlike the others this one reports a type unrelated to the column
/// it reads: `count` accepts a column of anything and answers with a number.
/// The column is still read, because both strategies rely on it to tell
/// otherwise-identical rows apart, and dropping it earlier would let two
/// rows that differ only there collapse into one.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Count;

impl<V, C: Scalar> Aggregation<V, C> for Count {
    type Semiring = Total<C>;

    const EMPTY_RESULT: Option<C> = Some(C::ZERO);

    #[inline]
    fn contribute(_value: &V) -> Self::Semiring {
        Total::lift(C::ONE)
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
    use differential_dataflow::difference::Present;
    use differential_dataflow::input::Input;
    use rstest::rstest;
    use timely::dataflow::operators::probe::Handle;

    use super::*;

    #[rstest]
    #[case(Count, Present, Some(()), vec![(0, (), Present)])]
    #[case(Count, 1_i32, Some(()), vec![(0, (), 1)])]
    #[case(Count, Present, None, vec![])]
    #[case(Count, 1_i32, None, vec![])]
    #[case(Sum, Present, Some(()), vec![(0, (), Present)])]
    #[case(Sum, 1_i32, Some(()), vec![(0, (), 1)])]
    #[case(Sum, Present, None, vec![])]
    #[case(Sum, 1_i32, None, vec![])]
    #[case(Min, Present, Some(()), vec![])]
    #[case(Min, 1_i32, Some(()), vec![])]
    #[case(Max, Present, Some(()), vec![])]
    #[case(Max, 1_i32, Some(()), vec![])]
    #[case(Avg, Present, Some(()), vec![])]
    #[case(Avg, 1_i32, Some(()), vec![])]
    fn empty_groups_emit_only_defined_results<A, R>(
        #[case] aggregation: A,
        #[case] _weight: R,
        #[case] empty_key: Option<()>,
        #[case] expected: Vec<(i64, (), R)>,
    ) where
        A: Aggregation<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
        R: ReduceStrategy<()> + ExchangeData + Sync,
    {
        let actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, R>();
                let seen = Rc::clone(&seen);
                flowlog_reduce(
                    rows,
                    "Reduce",
                    aggregation,
                    empty_key,
                    |value| ((), value),
                    |(), value| value,
                )
                .inspect(move |update| seen.borrow_mut().push(update.clone()));
                input
            });
            input.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case(Present, vec![(3, (), Present)])]
    #[case(1_i32, vec![(3, (), 1)])]
    fn global_count_counts_tuple_values<R>(#[case] weight: R, #[case] expected: Vec<(u32, (), R)>)
    where
        R: ReduceStrategy<()> + ExchangeData + Sync,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<(i64, i64), R>();
                let seen = Rc::clone(&seen);
                flowlog_reduce(
                    rows,
                    "Count",
                    Count,
                    Some(()),
                    |value| ((), value),
                    |(), count: u32| count,
                )
                .inspect(move |update| seen.borrow_mut().push(update.clone()));
                input
            });
            for value in [(1, 10), (1, 20), (2, 30)] {
                input.update(value, weight.clone());
                input.flush();
                worker.step();
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, expected);
    }

    #[rstest]
    #[case(Count, vec![(0, 0, Present), (1, 1, Present), (2, 3, Present)])]
    #[case(Sum, vec![(0, 0, Present), (7, 1, Present), (15, 3, Present)])]
    fn present_default_emits_once_and_allows_later_input<A>(
        #[case] aggregation: A,
        #[case] expected: Vec<(i64, u32, Present)>,
    ) where
        A: Aggregation<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, Present>();
                let seen = Rc::clone(&seen);
                flowlog_reduce(
                    rows,
                    "Reduce",
                    aggregation,
                    Some(()),
                    |value| ((), value),
                    |(), value| value,
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                input
            });
            for (epoch, value) in [None, Some(7), None, Some(8)].into_iter().enumerate() {
                if let Some(value) = value {
                    input.update(value, Present);
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
    fn global_aggregates_retract_results_and_restore_zero<A>(
        #[case] aggregation: A,
        #[case] expected: Vec<(i64, u32, i32)>,
    ) where
        A: Aggregation<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, i32>();
                let seen = Rc::clone(&seen);
                flowlog_reduce(
                    rows,
                    "Reduce",
                    aggregation,
                    Some(()),
                    |value| ((), value),
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
    fn default_group_is_scoped_by_key_and_partial_time<A>(
        #[case] aggregation: A,
        #[case] expected: Vec<(GroupRow, (u32, u32), i32)>,
    ) where
        A: Aggregation<i64, i64> + Send + Sync,
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
                    flowlog_reduce(
                        rows,
                        "Reduce",
                        aggregation,
                        Some(0),
                        |row| row,
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
}
