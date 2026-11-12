//! Lattice merges: a lattice relation's rows, keyed by every column but
//! the lattice column, merged to one row per key by the relation's `min`
//! or `max`. No group is answered without a row.
//!
//! A merge climbs or pays. [`flowlog_lattice_climb`] answers at its rows'
//! presence weight: a better answer joins the answers and a beaten one
//! stays. A reader that reads the answers only monotonically, inside a
//! loop or as the seed of the stratum finishing the relation, derives
//! nothing from a beaten answer that the better one does not derive or
//! beat. [`flowlog_lattice`] pays where the rows are final: it takes back
//! each beaten answer, so its answers are signed. A loop that climbed
//! pays once, as its answers leave, by merging them again outside.

use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use super::reduce::Reduction;
use super::reduce::presence::lift;
use super::reduce::presence::lower;
use super::reduce::presence::settle;
use super::reduce::semiring::Semiring;
use super::reduce::signed::reduce;
use super::reduce::signed::replace;
use crate::diff;
use crate::diff::Presence;

/// Merges presence `rows` by key under `reduction`, an extreme, into
/// answers of the same weight: a key answers when its first row arrives
/// and again whenever a row beats its answer, which stays. So a row the
/// collection holds several times merges as one: `split` produces the
/// group key and the merged column, `merge` rebuilds a row from a key and
/// its answer.
///
/// Within one time a key has one answer, the extreme of its rows, so
/// where all rows arrive at once, as static rows outside a loop do, the
/// merge pays as well. Beating an answer once and for all takes totally
/// ordered times.
#[must_use]
pub fn flowlog_lattice_climb<'scope, A, T, D, K, V, C, O, R>(
    rows: VecCollection<'scope, T, D, R>,
    name: &str,
    _reduction: A,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'scope, T, O, R>
where
    A: Reduction<V, C>,
    A::Semiring: ExchangeData,
    C: ExchangeData,
    T: Timestamp + TotalOrder + Lattice,
    R: Presence,
    D: Data,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    O: Data,
{
    lower(settle(lift(rows, name, A::contribute, split)), name, merge)
}

/// Merges `rows` by key under `reduction`, an extreme, into signed
/// answers: a key's answer is replaced, through a retraction and an
/// insertion, whenever its rows' extreme changes. `split` and `merge`
/// follow the contracts of [`flowlog_lattice_climb`].
#[must_use]
pub fn flowlog_lattice<'scope, A, T, D, K, V, C, O, R>(
    rows: VecCollection<'scope, T, D, R>,
    name: &str,
    _reduction: A,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'scope, T, O, diff::Mutable>
where
    A: Reduction<V, C>,
    A::Semiring: ExchangeData,
    C: ExchangeData,
    T: Timestamp + Lattice,
    R: LatticeStrategy<T>,
    D: Data,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    O: Data,
{
    R::lattice(rows, name, A::contribute, split, merge)
}

// =============================================================================
// Strategies
// =============================================================================

/// Replaces a lattice relation's answers using the operations the rows'
/// weight supports. Contributions must merge repeated rows idempotently:
/// adding the same row again must not change the answer.
///
/// A static merge has no strategy: its rows arrive at one time, where
/// [`flowlog_lattice_climb`] pays already.
pub trait LatticeStrategy<T: Timestamp + Lattice>: Semigroup + Ord + Sized + 'static {
    /// Applies the contracts of [`flowlog_lattice`].
    #[must_use]
    fn lattice<'scope, D, K, V, S, O>(
        rows: VecCollection<'scope, T, D, Self>,
        name: &str,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, diff::Mutable>
    where
        D: Data,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data;
}

impl<T: Timestamp + Lattice> LatticeStrategy<T> for diff::Append {
    fn lattice<'scope, D, K, V, S, O>(
        rows: VecCollection<'scope, T, D, Self>,
        name: &str,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, diff::Mutable>
    where
        D: Data,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
    {
        // Rows only arrive, so a key's extreme takes each new row in, where
        // a reduce would read every row of the key again.
        replace(lift(rows, name, contribute, split), name, merge)
    }
}

impl<T: Timestamp + Lattice> LatticeStrategy<T> for diff::Mutable {
    fn lattice<'scope, D, K, V, S, O>(
        rows: VecCollection<'scope, T, D, Self>,
        name: &str,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, diff::Mutable>
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::iterate::Variable;
    use rstest::rstest;
    use timely::dataflow::operators::probe::Handle;

    use super::*;
    use crate::operators::reduce::Max;
    use crate::operators::reduce::Min;
    use crate::time::LexLoop;

    type Row = (u8, i64);

    /// Feeds `epochs` of rows at `weight`, one epoch per time, through
    /// `merged`, and returns its answers consolidated.
    fn answers<D, R, W>(
        epochs: Vec<Vec<D>>,
        weight: R,
        merged: impl for<'s> Fn(VecCollection<'s, u32, D, R>) -> VecCollection<'s, u32, D, W>
        + Send
        + Sync
        + 'static,
    ) -> Vec<(D, u32, W)>
    where
        D: ExchangeData + Send + Sync,
        R: Semigroup + ExchangeData + Send + Sync,
        W: Semigroup + ExchangeData,
    {
        let mut actual = timely::execute_directly(move |worker| {
            // The operator's 'static callback and the test both need the
            // results; shared ownership lets the test read them after
            // stepping.
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<D, R>();
                let seen = Rc::clone(&seen);
                merged(rows)
                    .inspect(move |update| seen.borrow_mut().push(update.clone()))
                    .probe_with(&probe);
                input
            });
            for (epoch, rows) in epochs.into_iter().enumerate() {
                for row in rows {
                    input.update(row, weight.clone());
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

    fn split<K, V>(row: (K, V)) -> (K, V) {
        row
    }

    fn merge<K, V>(key: K, value: V) -> (K, V) {
        (key, value)
    }

    /// Within one time a merge keeps one answer per key, the extreme of
    /// its rows, whether it climbs or pays.
    #[test]
    fn a_merge_keeps_the_extreme_per_key() {
        let rows = || vec![vec![(1, 9), (1, 5), (2, 7)]];
        let climbed = answers(rows(), diff::Static, |rows: VecCollection<_, Row, _>| {
            flowlog_lattice_climb(rows, "Merge", Min, split, merge)
        });
        assert_eq!(
            climbed,
            vec![((1, 5), 0, diff::Static), ((2, 7), 0, diff::Static)]
        );
        let paid = answers(rows(), 1_i32, |rows: VecCollection<_, Row, _>| {
            flowlog_lattice(rows, "Merge", Min, split, merge)
        });
        assert_eq!(paid, vec![((1, 5), 0, 1), ((2, 7), 0, 1)]);
    }

    /// A string column merges to its least text per key, as a number does
    /// to its least value.
    #[test]
    fn a_merge_keeps_the_least_text_per_key() {
        let rows = vec![vec![
            (1_u8, "pear".to_string()),
            (1, "apple".into()),
            (2, "fig".into()),
        ]];
        let climbed = answers(rows, diff::Static, |rows| {
            flowlog_lattice_climb(rows, "Merge", Min, split, merge)
        });
        assert_eq!(
            climbed,
            vec![
                ((1, "apple".into()), 0, diff::Static),
                ((2, "fig".into()), 0, diff::Static)
            ]
        );
    }

    /// Over appended rows, a climb answers every bound that beats the one
    /// before and keeps it; a pay replaces it, so only a key's best bound
    /// remains.
    // Cases: reduction, climbing answers, paid answers.
    #[rstest]
    #[case::min(
        Min,
        vec![((1, 5), 2, diff::Append), ((1, 9), 0, diff::Append), ((2, 7), 1, diff::Append)],
        vec![((1, 5), 2, 1), ((1, 9), 0, 1), ((1, 9), 2, -1), ((2, 7), 1, 1)],
    )]
    #[case::max(
        Max,
        vec![((1, 9), 0, diff::Append), ((1, 12), 1, diff::Append), ((2, 7), 1, diff::Append)],
        vec![((1, 9), 0, 1), ((1, 9), 1, -1), ((1, 12), 1, 1), ((2, 7), 1, 1)],
    )]
    fn an_append_merge_climbs_or_replaces_beaten_bounds<A>(
        #[case] reduction: A,
        #[case] climbing: Vec<(Row, u32, diff::Append)>,
        #[case] paid: Vec<(Row, u32, diff::Mutable)>,
    ) where
        A: Reduction<i64, i64> + Copy + Send + Sync + 'static,
        A::Semiring: ExchangeData,
    {
        let epochs = || {
            vec![
                vec![(1, 9)],
                vec![(1, 12), (2, 7)],
                vec![(1, 5)],
                vec![(1, 5), (1, 9)],
            ]
        };
        let climbed = answers(epochs(), diff::Append, move |rows| {
            flowlog_lattice_climb(rows, "Merge", reduction, split, merge)
        });
        assert_eq!(climbed, climbing);
        let replaced = answers(epochs(), diff::Append, move |rows| {
            flowlog_lattice(rows, "Merge", reduction, split, merge)
        });
        assert_eq!(replaced, paid);
    }

    /// Shortest distances along the chain 0 -> 1 -> 2 -> 3, each edge of
    /// length 1, from the seeds of each epoch: a loop that climbs on its
    /// seeds' weight and merges its answers again as they leave, through
    /// `leave`.
    fn chain_distances<R, W>(
        epochs: Vec<Vec<Row>>,
        weight: R,
        leave: impl for<'s> Fn(VecCollection<'s, u32, Row, R>) -> VecCollection<'s, u32, Row, W>
        + Send
        + Sync
        + 'static,
    ) -> Vec<(Row, u32, W)>
    where
        R: Presence + Send + Sync,
        W: Semigroup + ExchangeData,
    {
        answers(epochs, weight, move |seeds| {
            let scope = seeds.scope();
            let distances = scope.scoped::<LexLoop, _, _>("Iterative", |inner| {
                let (variable, feedback) = Variable::new(inner, (0, 1));
                let step = feedback.flat_map(|(node, distance): Row| {
                    (node < 3).then_some((node + 1, distance + 1))
                });
                let next = flowlog_lattice_climb(
                    seeds.enter(inner).concat(step),
                    "Merge",
                    Min,
                    split,
                    merge,
                );
                variable.set(next.clone());
                next.leave(scope)
            });
            leave(distances)
        })
    }

    /// A static loop keeps every round's better bound, and merging its
    /// answers as they leave answers each key once, with its final bound:
    /// node 2 is first reached at 5 by its seed, then at 2 along the chain.
    #[test]
    fn a_static_loop_answers_its_final_bounds_as_they_leave() {
        let actual = chain_distances(vec![vec![(0, 0), (2, 5)]], diff::Static, |left| {
            flowlog_lattice_climb(left, "Merge", Min, split, merge)
        });
        assert_eq!(
            actual,
            vec![
                ((0, 0), 0, diff::Static),
                ((1, 1), 0, diff::Static),
                ((2, 2), 0, diff::Static),
                ((3, 3), 0, diff::Static),
            ]
        );
    }

    /// An append loop climbs across epochs, keeping its beaten bounds, and
    /// pays as they leave: a seed appended at epoch 1 shortens nodes 1 to 3,
    /// whose bounds from epoch 0 are taken back.
    #[test]
    fn an_append_loop_takes_back_beaten_bounds_as_they_leave() {
        let actual = chain_distances(vec![vec![(0, 10)], vec![(1, 0)]], diff::Append, |left| {
            flowlog_lattice(left, "Merge", Min, split, merge)
        });
        assert_eq!(
            actual,
            vec![
                ((0, 10), 0, 1),
                ((1, 0), 1, 1),
                ((1, 11), 0, 1),
                ((1, 11), 1, -1),
                ((2, 1), 1, 1),
                ((2, 12), 0, 1),
                ((2, 12), 1, -1),
                ((3, 2), 1, 1),
                ((3, 13), 0, 1),
                ((3, 13), 1, -1),
            ]
        );
    }

    /// Both paying strategies run inside a lexicographic loop.
    #[test]
    fn every_strategy_admits_a_lex_loop() {
        fn admits<R: LatticeStrategy<LexLoop>>() {}
        admits::<diff::Append>();
        admits::<diff::Mutable>();
    }
}
