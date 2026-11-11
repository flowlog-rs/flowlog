//! Choice merges: a `choice-domain` relation's rows, keyed by the domain's
//! columns, merged to one row per key, the key's choice. A merge builds its
//! own arrangement, as nothing else arranges a relation's rows.
//!
//! A key's choice is its least candidate under [`ValueOrd`]. In a static
//! loop it is the least of the first round with a candidate, and later
//! rounds leave it. So the choice is the same on any number of workers,
//! every key with a candidate gets one, and each epoch chooses as a batch
//! run over its rows would.
//!
//! A choice is a lattice merge under the [`Choice`] reduction, so each
//! weight's lattice strategy makes it.

use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;

use super::lattice::LatticeStrategy;
use super::lattice::flowlog_lattice;
use super::reduce::Choice;
use super::reduce::semiring::ValueOrd;

/// Keeps one row per key of `rows`, the key's choice: `split` produces a
/// row's key, the domain's columns, and the value chosen among, its other
/// columns; `merge` rebuilds a row from a key and its choice.
///
/// With `diff::Static`, a choice is final: made at the first time its key
/// has a candidate, the least then, and never revised, so the answers stay
/// static. With `diff::Append` and `diff::Mutable`, a key's choice is its
/// least current candidate, replaced as candidates arrive or leave, so the
/// answers are `diff::Mutable`.
#[must_use]
pub fn flowlog_choice<'scope, T, D, K, V, O, R>(
    rows: VecCollection<'scope, T, D, R>,
    name: &str,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, V) -> O + 'static,
) -> VecCollection<'scope, T, O, R::Output>
where
    T: Timestamp + Lattice,
    R: LatticeStrategy<T>,
    D: Data,
    K: ExchangeData + Hashable,
    V: ValueOrd,
    O: Data,
{
    flowlog_lattice(rows, name, Choice, split, merge)
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
}
