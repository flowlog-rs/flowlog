//! Choice merges: a `choice-domain` relation's rows, keyed by the domain's
//! columns, merged to one row per key, the key's choice. A merge builds its
//! own arrangement, as nothing else arranges a relation's rows.
//!
//! A key's choice is its least candidate under [`ValueOrd`]. In a loop it
//! is the least of the first round with a candidate, and later rounds
//! leave it. So the choice is the same on any number of workers, every key
//! with a candidate gets one, and each epoch chooses as a batch run over
//! its rows would.
//!
//! A choice is a lattice merge under the [`Choice`] reduction, so each
//! weight's lattice strategy makes it, and [`ChoiceClock`] keeps a loop's
//! first round at each time.

use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;

use super::dedup::FlowlogDedup;
use super::dedup::flowlog_dedup;
use super::lattice::LatticeStrategy;
use super::lattice::flowlog_lattice;
use super::map::flowlog_map;
use super::reduce::Choice;
use super::reduce::semiring::ValueOrd;
use crate::time::Epoch;
use crate::time::EpochLoop;
use crate::time::LexLoop;
use crate::time::Once;
use crate::time::OnceLoop;

/// Keeps one row per key of `rows`, the key's choice: `split` produces a
/// row's key, the domain's columns, and the value chosen among, its other
/// columns; `merge` rebuilds a row from a key and its choice.
///
/// With `diff::Static`, a choice is final: made at the first time its key
/// has a candidate, the least then, and never revised, so the answers stay
/// static. With `diff::Append` and `diff::Mutable`, a key's choice is its
/// least current candidate, replaced as candidates arrive or leave, so the
/// answers are `diff::Mutable`. In a loop either way, the candidates are
/// those of the first round with one; [`ChoiceClock`] says how each time
/// finds that round.
#[must_use]
pub fn flowlog_choice<'scope, T, D, K, V, O, R>(
    rows: VecCollection<'scope, T, D, R>,
    name: &str,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, V) -> O + 'static,
) -> VecCollection<'scope, T, O, R::Output>
where
    T: ChoiceClock,
    R: LatticeStrategy<T>,
    D: Data,
    K: ExchangeData + Hashable,
    V: ValueOrd,
    O: Data,
    for<'a> VecCollection<'a, T, D, R>: FlowlogDedup,
{
    T::choose(rows, name, split, merge)
}

/// A time a choice runs at, which decides how a loop's choice keeps the
/// first round with a candidate.
///
/// A totally ordered time keeps it through the lattice strategy alone: a
/// static choice settles at its key's first time, and no other weight
/// reaches a loop at such a time with a choice, as a choice makes its loop
/// mutable. An [`EpochLoop`] orders `(epoch, iteration)` pairs only when
/// both agree, so nothing settles there; each candidate carries its round
/// instead.
pub trait ChoiceClock: Timestamp + Lattice {
    /// Applies [`flowlog_choice`]'s contract at this time.
    #[must_use]
    fn choose<'scope, D, K, V, O, R>(
        rows: VecCollection<'scope, Self, D, R>,
        name: &str,
        split: impl FnMut(D) -> (K, V) + 'static,
        merge: impl FnMut(K, V) -> O + 'static,
    ) -> VecCollection<'scope, Self, O, R::Output>
    where
        R: LatticeStrategy<Self>,
        D: Data,
        K: ExchangeData + Hashable,
        V: ValueOrd,
        O: Data,
        for<'a> VecCollection<'a, Self, D, R>: FlowlogDedup,
    {
        flowlog_lattice(rows, name, Choice, split, merge)
    }
}

impl ChoiceClock for Once {}

impl ChoiceClock for Epoch {}

impl ChoiceClock for OnceLoop {}

impl ChoiceClock for LexLoop {}

impl ChoiceClock for EpochLoop {
    /// Puts each candidate's round, `time.inner`, ahead of its value, so
    /// the least `(round, value)` is the least candidate of the earliest
    /// round with one. A candidate derived through a key's own choice
    /// appears in a later round than that choice, so it never replaces
    /// it; the least value over every round could, leaving rows that only
    /// support each other. An epoch can still retract a candidate, or add
    /// one of an earlier round, and the choice follows.
    fn choose<'scope, D, K, V, O, R>(
        rows: VecCollection<'scope, Self, D, R>,
        name: &str,
        mut split: impl FnMut(D) -> (K, V) + 'static,
        mut merge: impl FnMut(K, V) -> O + 'static,
    ) -> VecCollection<'scope, Self, O, R::Output>
    where
        R: LatticeStrategy<Self>,
        D: Data,
        K: ExchangeData + Hashable,
        V: ValueOrd,
        O: Data,
        for<'a> VecCollection<'a, Self, D, R>: FlowlogDedup,
    {
        // Dedup before stamping: a row derived again in a later round would
        // otherwise carry that round too, one more value for the reduce to
        // read on every change of its key. As a set, each row changes only
        // at the round it first appears in.
        let stamped = flowlog_map(
            flowlog_dedup(rows),
            name,
            move |row, time: EpochLoop, weight| {
                let (key, value) = split(row);
                std::iter::once(((key, (time.inner, value)), time, weight))
            },
        );
        flowlog_lattice(
            stamped,
            name,
            Choice,
            |stamped| stamped,
            move |key, (_round, value)| merge(key, value),
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
    use rstest::rstest;
    use timely::dataflow::operators::probe::Handle;

    use super::*;
    use crate::diff;
    use crate::time::Iteration;

    /// A key's row and its other column, as a choice splits it.
    fn split((key, value): (u8, i64)) -> (u8, i64) {
        (key, value)
    }

    /// A row rebuilt from its key and choice.
    fn merge(key: u8, value: i64) -> (u8, i64) {
        (key, value)
    }

    /// The updates a choice over `epochs` of `rows` answers, each epoch
    /// processed before the next arrives.
    fn choices<R>(epochs: Vec<Vec<((u8, i64), R)>>) -> Vec<((u8, i64), u32, R::Output)>
    where
        R: LatticeStrategy<u32, Output: ExchangeData> + ExchangeData + Sync,
        for<'a> VecCollection<'a, u32, (u8, i64), R>: FlowlogDedup,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<(u8, i64), R>();
                let seen = Rc::clone(&seen);
                flowlog_choice(rows, "Choice", split, merge)
                    .inspect(move |update| seen.borrow_mut().push(update.clone()))
                    .probe_with(&probe);
                input
            });
            for (epoch, rows) in epochs.into_iter().enumerate() {
                for (row, weight) in rows {
                    input.update(row, weight);
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

    /// `epochs` of inserted rows, each at `weight`.
    fn inserts<R: Clone>(epochs: &[Vec<(u8, i64)>], weight: R) -> Vec<Vec<((u8, i64), R)>> {
        epochs
            .iter()
            .map(|rows| rows.iter().map(|&row| (row, weight.clone())).collect())
            .collect()
    }

    /// A choice keeps one row per key, its least candidate.
    // Cases: weight, answers.
    #[rstest]
    #[case(diff::Static, vec![((1, 5), 0, diff::Static), ((2, 7), 0, diff::Static)])]
    #[case(diff::Append, vec![((1, 5), 0, 1), ((2, 7), 0, 1)])]
    #[case(1_i32, vec![((1, 5), 0, 1), ((2, 7), 0, 1)])]
    fn a_choice_keeps_the_least_candidate_per_key<R>(
        #[case] weight: R,
        #[case] expected: Vec<((u8, i64), u32, R::Output)>,
    ) where
        R: LatticeStrategy<u32, Output: ExchangeData> + ExchangeData + Sync,
        for<'a> VecCollection<'a, u32, (u8, i64), R>: FlowlogDedup,
    {
        let rows = vec![(1, 9), (1, 5), (2, 7)];
        assert_eq!(choices(inserts(&[rows], weight)), expected);
    }

    /// Each epoch chooses as a batch run over its rows would, so a lesser
    /// candidate arriving later replaces a key's choice, and append and
    /// mutable rows inserted alike answer alike.
    #[test]
    fn an_epoch_chooses_as_a_batch_run_would() {
        let epochs = [vec![(1, 9)], vec![(1, 5), (2, 7)], vec![(1, 3), (2, 1)]];
        let expected = vec![
            ((1, 3), 2, 1),
            ((1, 5), 1, 1),
            ((1, 5), 2, -1),
            ((1, 9), 0, 1),
            ((1, 9), 1, -1),
            ((2, 1), 2, 1),
            ((2, 7), 1, 1),
            ((2, 7), 2, -1),
        ];
        assert_eq!(choices(inserts(&epochs, diff::Append)), expected);
        assert_eq!(choices(inserts(&epochs, 1_i32)), expected);
    }

    /// A mutable choice is the least current candidate: retracting the
    /// chosen one promotes the next least.
    #[test]
    fn retracting_a_mutable_choice_promotes_the_next_least() {
        let epochs = vec![vec![((1, 5), 1), ((1, 9), 1)], vec![((1, 5), -1)]];
        assert_eq!(
            choices(epochs),
            vec![((1, 5), 0, 1), ((1, 5), 1, -1), ((1, 9), 1, 1)]
        );
    }

    /// In a static loop a key's choice is made in the first round it has a
    /// candidate, the least of that round's, and stands: one row per key
    /// leaves the loop, though a later round offers a lesser candidate.
    #[test]
    fn a_static_loops_choice_keeps_the_earliest_round() {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<((u8, i64), u16), diff::Static>();
                let seen = Rc::clone(&seen);
                scope.iterative::<u16, _, _>(|inner| {
                    let rows = rows
                        .enter_at(inner, |(_, round)| *round)
                        .map(|(row, _)| row);
                    flowlog_choice(rows, "Choice", split, merge)
                        .leave(scope)
                        .inspect(move |update| seen.borrow_mut().push(*update));
                });
                input
            });
            for row in [((1, 5), 0), ((1, 1), 1), ((2, 9), 0), ((2, 4), 0)] {
                input.update(row, diff::Static);
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(
            actual,
            vec![((1, 5), (), diff::Static), ((2, 4), (), diff::Static)]
        );
    }

    /// In a loop over changing rows a key's choice is the least candidate
    /// of the earliest round that has one: a lesser candidate of a later
    /// round leaves it, one of an earlier round replaces it, and retracting
    /// that one restores it.
    #[test]
    fn a_changing_loops_choice_keeps_the_earliest_round() {
        // Each epoch's updates: key, value, round, weight.
        let epochs: Vec<Vec<(u8, i64, Iteration, i32)>> = vec![
            vec![(1, 5, 3, 1)],
            vec![(1, 1, 5, 1)],
            vec![(1, 9, 2, 1)],
            vec![(1, 9, 2, -1)],
        ];
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<((u8, i64), Iteration), i32>();
                let seen = Rc::clone(&seen);
                scope.iterative::<Iteration, _, _>(|inner| {
                    let rows = rows
                        .enter_at(inner, |(_, round)| *round)
                        .map(|(row, _)| row);
                    flowlog_choice(rows, "Choice", split, merge)
                        .leave(scope)
                        .inspect(move |update| seen.borrow_mut().push(*update))
                        .probe_with(&probe);
                });
                input
            });
            for (epoch, updates) in epochs.into_iter().enumerate() {
                for (key, value, round, weight) in updates {
                    input.update(((key, value), round), weight);
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
        assert_eq!(
            actual,
            vec![
                ((1, 5), 0, 1),
                ((1, 5), 2, -1),
                ((1, 5), 3, 1),
                ((1, 9), 2, 1),
                ((1, 9), 3, -1),
            ]
        );
    }
}
