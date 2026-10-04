//! An input relation read as a set.
//!
//! [`FlowlogInputSet`] selects the implementation by weight. A static
//! input keeps its first occurrence of each row, as [`flowlog_dedup`]
//! does. A signed input is read through its membership: a row is present
//! after an epoch whose insertions of it outnumber its deletions, absent
//! after one where deletions outnumber insertions, and unchanged by one
//! where they balance. However often a row was inserted, one deletion
//! removes it, and a deletion of an absent row changes nothing.
//!
//! The membership lives in the arrangement the operator builds: each
//! epoch's updates are rewritten against the trace into the `+1` and `-1`
//! that change membership, and nothing else is stored.

use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::KeyBatcher;
use differential_dataflow::trace::implementations::KeyBuilder;
use differential_dataflow::trace::implementations::KeySpine;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use crate::diff;
use crate::operators::arrange::Update;
use crate::operators::arrange::arrange_rewriting;
use crate::operators::dedup::Counter;
use crate::operators::dedup::FlowlogDedup;
use crate::operators::dedup::flowlog_dedup;

/// Reads an input relation as a set, by the rule its weight gives (see
/// the module doc).
pub fn flowlog_input_set<C: FlowlogInputSet>(collection: C) -> C {
    collection.input_set()
}

// =============================================================================
// FlowlogInputSet
// =============================================================================

/// Compile-time dispatch behind [`flowlog_input_set`].
pub trait FlowlogInputSet: Sized {
    /// See [`flowlog_input_set`].
    fn input_set(self) -> Self;
}

impl<'scope, T, D> FlowlogInputSet for VecCollection<'scope, T, D, diff::Static>
where
    T: Timestamp,
    Self: FlowlogDedup,
{
    fn input_set(self) -> Self {
        flowlog_dedup(self)
    }
}

impl<'scope, E: Counter, D> FlowlogInputSet for VecCollection<'scope, E, D, diff::Mutable>
where
    D: ExchangeData + Hashable,
{
    fn input_set(self) -> Self {
        arrange_rewriting::<
            D,
            (),
            E,
            diff::Mutable,
            KeyBatcher<D, E, diff::Mutable>,
            KeyBuilder<D, E, diff::Mutable>,
            KeySpine<D, E, diff::Mutable>,
            _,
        >(self.map(|row| (row, ())).inner, "InputSet", latch)
        .as_collection(|row, ()| row.clone())
    }
}

/// Rewrites `chain` against the trace's membership: a positive net on an
/// absent row becomes `+1` and makes it present, a negative net on a
/// present row becomes `-1` and makes it absent, and anything else is
/// dropped. The chain is sorted by row then time, so a row's updates in
/// one chain apply in time order. Membership is read through one merged
/// cursor over the batches below the chain, as `threshold_total` reads its
/// counts.
fn latch<D, T>(
    lookup: &mut TraceAgent<KeySpine<D, T, diff::Mutable>>,
    chain: &mut [Vec<Update<D, (), T, diff::Mutable>>],
    description: &Description<T>,
) where
    D: ExchangeData + Hashable,
    T: Timestamp + Lattice + TotalOrder,
{
    let (mut cursor, storage) = lookup
        .cursor_through(description.lower().borrow())
        .expect("the lookup handle's compaction never passes a batch's lower bound");
    let mut last: Option<(D, bool)> = None;
    for chunk in chain.iter_mut() {
        for ((row, ()), _, diff) in chunk.iter_mut() {
            if !matches!(&last, Some((seen, _)) if seen == row) {
                let mut count: i64 = 0;
                cursor.seek_key(&storage, row);
                if cursor.get_key(&storage) == Some(row) {
                    cursor.map_times(&storage, |_, d| count += i64::from(*d));
                }
                last = Some((row.clone(), count > 0));
            }
            let present = &mut last.as_mut().expect("set above").1;
            if *diff > 0 && !*present {
                *diff = 1;
                *present = true;
            } else if *diff < 0 && *present {
                *diff = -1;
                *present = false;
            } else {
                *diff = 0;
            }
        }
        chunk.retain(|(_, _, diff)| *diff != 0);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use timely::dataflow::operators::probe::Handle;
    use timely::order::Product;

    use super::*;
    use crate::time::LexLoop;

    type Row = u64;

    /// Runs `epochs`, one inner vector of signed updates per epoch, through
    /// a signed input set, and returns its output consolidated.
    fn membership_epochs(epochs: Vec<Vec<(Row, diff::Mutable)>>) -> Vec<(Row, u32, diff::Mutable)> {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_input_set(rows)
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });
            for (epoch, updates) in epochs.into_iter().enumerate() {
                for (row, diff) in updates {
                    input.update(row, diff);
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

    /// One deletion removes a row however often it was inserted, a
    /// deletion of an absent row changes nothing, and an insertion brings
    /// it back.
    #[test]
    fn one_deletion_removes_a_row_inserted_twice() {
        assert_eq!(
            membership_epochs(vec![
                vec![(7, 1)],
                vec![(7, 1)],
                vec![(7, -1)],
                vec![(7, -1)],
                vec![(7, 1)],
            ]),
            vec![(7, 0, 1), (7, 2, -1), (7, 4, 1)]
        );
    }

    /// Within one epoch a row's insertions and deletions cancel and the
    /// surplus decides: a balance leaves the row as it was, present or
    /// absent.
    #[test]
    fn an_epochs_commands_on_a_row_decide_by_their_net() {
        assert_eq!(
            membership_epochs(vec![
                vec![(1, 1), (1, -1), (2, 1), (2, 1), (2, -1), (3, -1), (3, 1)],
                vec![(2, -1), (2, 1), (3, 1)],
            ]),
            vec![(2, 0, 1), (3, 1, 1)]
        );
    }

    /// A chain holding two epochs of one row, when the frontier skips
    /// past both at once, applies them in time order.
    #[test]
    fn two_epochs_in_one_batch_apply_in_order() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_input_set(rows).inspect(move |update| seen.borrow_mut().push(*update));
                input
            });
            input.update(7, 1);
            input.advance_to(1);
            input.update(7, 1);
            input.advance_to(2);
            input.update(7, -1);
            input.advance_to(3);
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, vec![(7, 0, 1), (7, 2, -1)]);
    }

    /// A static input keeps its first-occurrence dedup at every clock it
    /// lives at; a signed input has the membership set at the epoch clock.
    #[test]
    fn every_supported_input_weight_and_clock_pairing_compiles() {
        fn admits_static<T: Timestamp + Lattice>()
        where
            VecCollection<'static, T, Row, diff::Static>: FlowlogInputSet,
        {
        }
        fn admits_signed<T: Timestamp + Lattice>()
        where
            VecCollection<'static, T, Row, diff::Mutable>: FlowlogInputSet,
        {
        }
        admits_static::<()>();
        admits_static::<u32>();
        admits_static::<Product<(), u16>>();
        admits_static::<LexLoop>();
        admits_signed::<u32>();
    }

    /// A static input announces each row once whatever repeats it.
    #[test]
    fn a_static_input_keeps_one_announcement_per_row() {
        let actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_input_set(rows).inspect(move |update| seen.borrow_mut().push(*update));
                input
            });
            input.update(7, diff::Static);
            input.update(7, diff::Static);
            input.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, vec![(7, (), diff::Static)]);
    }
}
