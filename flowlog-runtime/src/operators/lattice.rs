//! Lattice merges: a lattice relation's rows, keyed by every column but
//! the lattice column, merged to one row per key by the relation's `min`
//! or `max`. A merge builds its own arrangement, as nothing else arranges
//! a relation's rows; no group is answered without a row, as an extreme
//! over nothing is no answer.

use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use super::reduce::Reduction;
use super::reduce::presence::lift;
use super::reduce::presence::lower;
use super::reduce::presence::settle;
use super::reduce::semiring::Scalar;
use super::reduce::semiring::Semiring;
use super::reduce::signed::reduce;
use crate::diff;

/// Merges `rows` by key under `reduction`, an extreme, so a row the
/// collection holds several times merges as one: `split` produces the
/// group key and the merged column, `merge` rebuilds a row from a key
/// and its answer.
///
/// With `diff::Mutable`, each answer is replaced through insertions and
/// retractions. `diff::Static` can emit a new answer at a later
/// timestamp but cannot retract earlier answers, so a recursive merge's
/// final output needs [`flowlog_lattice_leave`].
pub fn flowlog_lattice<'scope, A, T, D, K, V, C, O, R>(
    rows: VecCollection<'scope, T, D, R>,
    name: &str,
    _reduction: A,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'scope, T, O, R::Output>
where
    A: Reduction<V, C>,
    A::Semiring: ExchangeData,
    C: Scalar,
    T: Timestamp + Lattice,
    R: LatticeStrategy<T>,
    D: Data,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    O: Data,
{
    R::lattice(rows, name, A::contribute, split, merge)
}

/// Produces each group's final `diff::Static` merge after recursion
/// finishes.
///
/// Min and max consume intermediate bounds, which preserve the final
/// extreme, so each round's answers fold once more. The input, `split`
/// and `merge` follow the contracts of [`flowlog_lattice`].
pub fn flowlog_lattice_leave<'inner, 'outer, A, TInner, TOuter, D, K, V, C, O>(
    rows: VecCollection<'inner, TInner, D, diff::Static>,
    outer: Scope<'outer, TOuter>,
    name: &str,
    _reduction: A,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'outer, TOuter, O, diff::Static>
where
    A: Reduction<V, C>,
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
    // so consolidation produces one final answer.
    let lifted = lift(rows, name, None, A::contribute, split).leave(outer);
    lower(lifted.consolidate(), name, merge)
}

// =============================================================================
// Strategies
// =============================================================================

/// Merges a lattice relation's rows using the operations the rows' weight
/// supports.
///
/// `diff::Static` has no inverse: it accumulates contributions as semiring
/// weights and can only add answers; it requires totally ordered
/// timestamps. `diff::Mutable` and `diff::Append` arrange the rows and
/// keep the current answer through Differential Dataflow's reduce, under
/// the partially ordered timestamps of incremental recursion too.
pub trait LatticeStrategy<T: Timestamp + Lattice>: Semigroup + Ord + Sized + 'static {
    /// The answers' weight.
    type Output: Semigroup + 'static;

    /// Applies the contracts of [`flowlog_lattice`].
    fn lattice<'scope, D, K, V, S, O>(
        rows: VecCollection<'scope, T, D, Self>,
        name: &str,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, Self::Output>
    where
        D: Data,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data;
}

impl<T: Timestamp + TotalOrder + Lattice> LatticeStrategy<T> for diff::Static {
    type Output = diff::Static;

    fn lattice<'scope, D, K, V, S, O>(
        rows: VecCollection<'scope, T, D, Self>,
        name: &str,
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
        lower(
            settle(lift(rows, name, None, contribute, split)),
            name,
            merge,
        )
    }
}

/// Implements the strategy for a signed weight, whose answers are
/// `diff::Mutable`.
macro_rules! signed {
    ($weight:ty) => {
        impl<T: Timestamp + Lattice> LatticeStrategy<T> for $weight {
            type Output = diff::Mutable;

            fn lattice<'scope, D, K, V, S, O>(
                rows: VecCollection<'scope, T, D, Self>,
                name: &str,
                contribute: impl Fn(&V) -> S + 'static,
                split: impl FnMut(D) -> (K, V) + 'static,
                merge: impl FnMut(K, S::Value) -> O + 'static,
            ) -> VecCollection<'scope, T, O, Self::Output>
            where
                D: Data,
                K: ExchangeData + Hashable,
                V: ExchangeData,
                S: Semiring + ExchangeData,
                O: Data,
            {
                reduce(
                    rows.map(split).arrange_by_key(),
                    name,
                    contribute,
                    None,
                    merge,
                )
            }
        }
    };
}

signed!(diff::Mutable);
signed!(diff::Append);

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use rstest::rstest;

    use super::*;
    use crate::operators::reduce::Max;
    use crate::operators::reduce::Min;
    use crate::time::LexLoop;

    /// A merge keeps one answer per key, the extreme of its rows.
    // Cases: weight, answers.
    #[rstest]
    #[case(diff::Static, vec![((1, 5), (), diff::Static), ((2, 7), (), diff::Static)])]
    #[case(1_i32, vec![((1, 5), (), 1), ((2, 7), (), 1)])]
    fn a_merge_keeps_the_extreme_per_key<R>(
        #[case] weight: R,
        #[case] expected: Vec<((u8, i64), (), R)>,
    ) where
        R: LatticeStrategy<(), Output = R> + ExchangeData + Sync,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<(u8, i64), R>();
                let seen = Rc::clone(&seen);
                flowlog_lattice(
                    rows,
                    "Merge",
                    Min,
                    |(key, value)| (key, value),
                    |key, value| (key, value),
                )
                .inspect(move |update| seen.borrow_mut().push(update.clone()));
                input
            });
            for row in [(1, 9), (1, 5), (2, 7)] {
                input.update(row, weight.clone());
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, expected);
    }

    /// A static loop's merge leaves as one final answer per key, the
    /// extreme over every round, whatever bounds the rounds announced.
    #[test]
    fn a_static_loops_merge_leaves_one_final_answer_per_key() {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<((u8, i64), u16), diff::Static>();
                let seen = Rc::clone(&seen);
                scope.iterative::<u16, _, _>(|inner| {
                    let rows = rows
                        .enter_at(inner, |(_, round)| *round)
                        .map(|(row, _)| row);
                    flowlog_lattice_leave(
                        rows,
                        scope,
                        "MergeLeave",
                        Max,
                        |(key, value)| (key, value),
                        |key, value| (key, value),
                    )
                    .inspect(move |update| seen.borrow_mut().push(*update));
                });
                input
            });
            // Key 1 grows over three rounds, key 2 shrinks: both leave with
            // their largest value only.
            for row in [
                ((1, 1), 0),
                ((1, 5), 1),
                ((1, 3), 2),
                ((2, 9), 0),
                ((2, 4), 1),
            ] {
                input.update(row, diff::Static);
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(
            actual,
            vec![((1, 5), (), diff::Static), ((2, 9), (), diff::Static)]
        );
    }

    /// Both strategies run inside a lexicographic loop: its times are
    /// totally ordered, as the `diff::Static` strategy requires.
    #[test]
    fn both_strategies_admit_a_lex_loop() {
        fn admits<R: LatticeStrategy<LexLoop>>() {}
        admits::<diff::Static>();
        admits::<diff::Mutable>();
    }
}
