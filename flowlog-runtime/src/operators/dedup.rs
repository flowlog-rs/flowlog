//! Set-semantics dedup for generated FlowLog rules.
//!
//! [`FlowlogDedup`] selects an implementation by timestamp and diff.
//! Total clocks use consolidation or streaming thresholds. Epoch-rooted
//! recursive products use [`first_occurrences`] or signed reduction.

use differential_dataflow::AsCollection;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Present;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::cursor_list;
use timely::PartialOrder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::order::Product;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

/// Maintains a set without changing the collection's diff type.
///
/// `i32` output has accumulated weight `1` wherever the input count is
/// positive, and `0` otherwise. Deletions and later reinsertions propagate.
/// `Present` emits a tuple only at times not covered by an earlier
/// occurrence in timely's partial order. In a recursive product, times
/// such as `(0, 5)` and `(1, 1)` are incomparable: both are retained,
/// while a later occurrence at `(1, 5)` is suppressed.
///
/// The diff and timestamp select the implementation at compile time.
/// At `()` a presence needs only consolidation. Advancing clocks retain
/// history, including across iterations in recursive feedback.
pub fn flowlog_dedup<C: FlowlogDedup>(collection: C) -> C {
    collection.dedup()
}

// =============================================================================
// FlowlogDedup
// =============================================================================

/// Compile-time dispatch behind [`flowlog_dedup`].
pub trait FlowlogDedup: Sized {
    /// See [`flowlog_dedup`].
    fn dedup(self) -> Self;
}

impl<'scope, D> FlowlogDedup for VecCollection<'scope, (), D, Present>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        // Every update has the same time, and presence is idempotent.
        // Consolidation suffices without retaining a history trace.
        self.consolidate()
    }
}

impl<'scope, E: Epoch, D> FlowlogDedup for VecCollection<'scope, E, D, Present>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        // In a total order, the first occurrence covers all later ones.
        self.threshold_semigroup(|_, _, prior| prior.is_none().then_some(Present))
    }
}

impl<'scope, I: Epoch, D> FlowlogDedup for VecCollection<'scope, Product<(), I>, D, Present>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        // Only the iteration coordinate advances, so times remain ordered.
        self.threshold_semigroup(|_, _, prior| prior.is_none().then_some(Present))
    }
}

impl<'scope, E: Epoch, I: Epoch, D> FlowlogDedup
    for VecCollection<'scope, Product<E, I>, D, Present>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        first_occurrences(self)
    }
}

impl<'scope, D> FlowlogDedup for VecCollection<'scope, (), D, i32>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        // At the single timestamp, arranged batches contain final counts.
        // Clamp while reading them, without retaining a history trace.
        self.arrange_by_self()
            .stream
            .unary(Pipeline, "Dedup", |_, _| {
                move |input, output| {
                    input.for_each(|capability, batches| {
                        let mut session = output.session(&capability);
                        for batch in batches.drain(..) {
                            let mut cursor = batch.cursor();
                            while let Some(key) = cursor.get_key(&batch) {
                                cursor.map_times(&batch, |&time, &count| {
                                    if count > 0 {
                                        session.give((key.clone(), time, 1));
                                    }
                                });
                                cursor.step_key(&batch);
                            }
                        }
                    });
                }
            })
            .as_collection()
    }
}

impl<'scope, E: Epoch, D> FlowlogDedup for VecCollection<'scope, E, D, i32>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        self.threshold_total(|_, &count| if count > 0 { 1 } else { 0 })
    }
}

impl<'scope, I: Epoch, D> FlowlogDedup for VecCollection<'scope, Product<(), I>, D, i32>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        self.threshold_total(|_, &count| if count > 0 { 1 } else { 0 })
    }
}

impl<'scope, E: Epoch, I: Epoch, D> FlowlogDedup for VecCollection<'scope, Product<E, I>, D, i32>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        // Incomparable derivations can overlap at a later time. Signed
        // output must correct that overlap as well as propagate deletions.
        self.threshold(|_, &count| if count > 0 { 1 } else { 0 })
    }
}

/// Converts nonzero membership to presence at times with no earlier
/// occurrence. Input membership must be monotone: once the accumulated
/// weight is nonzero, it must remain nonzero at all greater times.
/// Signed updates at the same time cancel before presence is tested;
/// arbitrary signed collections do not satisfy the monotonicity contract.
pub(super) fn first_occurrences<'scope, E, I, D, R>(
    collection: VecCollection<'scope, Product<E, I>, D, R>,
) -> VecCollection<'scope, Product<E, I>, D, Present>
where
    E: Epoch,
    I: Epoch,
    D: ExchangeData + Hashable,
    R: ExchangeData + Semigroup,
{
    // Monotone membership needs only the input trace. This avoids an output
    // trace and overlap corrections, at the cost of scanning and sorting
    // each touched row's retained occurrence times.
    let arranged = collection.arrange_by_self_named("Arrange: Dedup");
    let mut trace = arranged.trace;
    arranged
        .stream
        .unary(Pipeline, "Dedup", move |_, _| {
            let mut times = Vec::new();
            move |input, output| {
                input.for_each(|capability, batches| {
                    let mut session = output.session(&capability);
                    for batch in batches.drain(..) {
                        // Keep physical compaction at the processed
                        // boundary so old and incoming batches stay
                        // separable, even when the trace runs ahead.
                        let mut history = Vec::new();
                        trace.map_batches(|old| {
                            if PartialOrder::less_equal(old.upper(), batch.lower()) {
                                history.push(std::rc::Rc::clone(old));
                            }
                        });
                        let (mut prior, history) = cursor_list(history);
                        let mut current = batch.cursor();
                        while let Some(key) = current.get_key(&batch) {
                            prior.seek_key(&history, key);
                            if prior.get_key(&history) == Some(key) {
                                prior.map_times(&history, |time, _| {
                                    times.push((time.clone(), false));
                                });
                            }
                            current.map_times(&batch, |time, _| {
                                times.push((time.clone(), true));
                            });

                            // Product::Ord is lexicographic, while timely's
                            // partial order compares both coordinates.
                            // In outer-time order, a new minimum inner time
                            // is the only way to escape earlier coverage.
                            // Old entries sort first on compaction ties.
                            times.sort_unstable();
                            let mut least_inner: Option<I> = None;
                            for (time, incoming) in times.drain(..) {
                                if least_inner.as_ref().is_none_or(|least| time.inner < *least) {
                                    if incoming {
                                        session.give((key.clone(), time.clone(), Present));
                                    }
                                    least_inner = Some(time.inner);
                                }
                            }
                            current.step_key(&batch);
                        }
                        trace.set_logical_compaction(batch.upper().borrow());
                        trace.set_physical_compaction(batch.upper().borrow());
                    }
                });
            }
        })
        .as_collection()
}

// =============================================================================
// Epoch
// =============================================================================

/// An advancing root or iteration counter: `u16` and `u32` today.
/// Sealed to keep supported widths in one place, on `sealed::Epoch`.
pub trait Epoch: Timestamp + TotalOrder + Lattice + sealed::Epoch {}

impl<E> Epoch for E where E: sealed::Epoch + Timestamp + TotalOrder + Lattice {}

mod sealed {
    pub trait Epoch {}

    impl Epoch for u16 {}
    impl Epoch for u32 {}
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::Data;
    use differential_dataflow::input::Input;
    use differential_dataflow::input::InputSession;
    use differential_dataflow::operators::iterate::Variable;
    use rstest::rstest;
    use timely::dataflow::operators::probe::Handle;
    use timely::order::Product;
    use timely::worker::Worker;

    use super::*;

    type Row = u64;
    type Batch = ();
    type Inc = u32;
    type BatchLoop = Product<(), u16>;
    type IncLoop = Product<u32, u16>;

    #[test]
    fn every_supported_diff_and_clock_pairing_compiles() {
        fn admits<T: Timestamp + Lattice>()
        where
            VecCollection<'static, T, Row, Present>: FlowlogDedup,
            VecCollection<'static, T, Row, i32>: FlowlogDedup,
        {
        }
        admits::<Batch>();
        admits::<u16>();
        admits::<Inc>();
        admits::<BatchLoop>();
        admits::<Product<(), u32>>();
        admits::<Product<u16, u16>>();
        admits::<Product<u16, u32>>();
        admits::<IncLoop>();
        admits::<Product<u32, u32>>();
    }

    #[rstest]
    #[case(vec![(7, Present), (7, Present)], vec![(7, (), Present)])]
    #[case(
        vec![(7, 2_i32), (7, -1), (8, -1), (9, 1), (9, -1)],
        vec![(7, (), 1)]
    )]
    fn batch_dedup_emits_unit_membership<R>(
        #[case] updates: Vec<(Row, R)>,
        #[case] expected: Vec<(Row, (), R)>,
    ) where
        R: ExchangeData + Semigroup + Sync,
        for<'scope> VecCollection<'scope, (), Row, R>: FlowlogDedup,
    {
        let actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, R>();
                let seen = Rc::clone(&seen);
                flowlog_dedup(rows).inspect(move |update| seen.borrow_mut().push(update.clone()));
                input
            });
            for (row, diff) in updates.iter().cloned() {
                input.update(row, diff);
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, expected);
    }

    #[test]
    fn batch_signed_dedup_combines_separate_input_batches() {
        let actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, i32>();
                let seen = Rc::clone(&seen);
                flowlog_dedup(rows).inspect(move |update| seen.borrow_mut().push(*update));
                input
            });
            for diff in [2, -1, -1] {
                input.update(7, diff);
                input.flush();
                worker.step();
            }
            input.update(8, 3);
            input.update(9, -1);
            input.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, vec![(8, (), 1)]);
    }

    fn advance<D, R>(
        worker: &mut Worker,
        input: &mut InputSession<Inc, D, R>,
        probe: &Handle<Inc>,
        time: Inc,
    ) where
        D: Data,
        R: Semigroup + 'static,
    {
        input.advance_to(time);
        input.flush();
        worker.step_while(|| probe.less_than(&time));
    }

    #[rstest]
    #[case::one_batch(false)]
    #[case::separate_epochs(true)]
    fn present_preserves_incomparable_times(#[case] flush_each_epoch: bool) {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<(Row, u16), Present>();
                let seen = Rc::clone(&seen);
                scope
                    .iterative::<u16, _, _>(|inner| {
                        let rows = rows
                            .enter_at(inner, |(_, iteration)| *iteration)
                            .map(|(row, _)| row);
                        flowlog_dedup(rows)
                            .inspect(move |update| seen.borrow_mut().push(*update))
                            .leave(scope)
                    })
                    .probe_with(&probe);
                input
            });

            input.update((7, 6), Present);
            input.update((7, 5), Present);
            input.advance_to(1);
            if flush_each_epoch {
                input.flush();
                worker.step_while(|| probe.less_than(&1));
            }
            input.update((7, 5), Present);
            input.update((7, 1), Present);
            input.update((8, 2), Present);
            input.advance_to(2);
            if flush_each_epoch {
                input.flush();
                worker.step_while(|| probe.less_than(&2));
            }
            input.update((7, 5), Present);
            input.update((8, 2), Present);
            input.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (7, Product::new(0, 5), Present),
                (7, Product::new(1, 1), Present),
                (8, Product::new(1, 2), Present),
            ]
        );
    }

    #[test]
    fn signed_epochs_track_positive_counts_and_reinsertions() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, i32>();
                let seen = Rc::clone(&seen);
                flowlog_dedup(rows)
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });
            input.update(7, 2);
            input.update(8, -1);
            advance(worker, &mut input, &probe, 1);
            input.update(7, -1);
            input.update(8, 1);
            advance(worker, &mut input, &probe, 2);
            input.update(7, -1);
            input.update(8, 1);
            advance(worker, &mut input, &probe, 3);
            input.update(7, 1);
            input.update(8, -2);
            input.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![(7, 0, 1), (7, 2, -1), (7, 3, 1), (8, 2, 1), (8, 3, -1)]
        );
    }

    #[test]
    fn signed_partial_times_correct_overlaps_and_deletions() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<(Row, u16), i32>();
                let seen = Rc::clone(&seen);
                scope
                    .iterative::<u16, _, _>(|inner| {
                        let rows = rows
                            .enter_at(inner, |(_, iteration)| *iteration)
                            .map(|(row, _)| row);
                        flowlog_dedup(rows)
                            .inspect(move |update| seen.borrow_mut().push(*update))
                            .leave(scope)
                    })
                    .probe_with(&probe);
                input
            });
            input.update((7, 2), 2);
            advance(worker, &mut input, &probe, 1);
            input.update((7, 0), 1);
            advance(worker, &mut input, &probe, 2);
            input.update((7, 0), -1);
            input.update((7, 2), -1);
            advance(worker, &mut input, &probe, 3);
            input.update((7, 2), -1);
            input.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (7, Product::new(0, 2), 1),
                (7, Product::new(1, 0), 1),
                (7, Product::new(1, 2), -1),
                (7, Product::new(2, 0), -1),
                (7, Product::new(2, 2), 1),
                (7, Product::new(3, 2), -1),
            ]
        );
    }

    #[test]
    fn present_feedback_converges_across_epochs() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, seeds) = scope.new_collection::<Row, Present>();
                let reach = scope.iterative::<u16, _, _>(|inner| {
                    let (variable, feedback) = Variable::new(inner, Product::new(0, 1));
                    let step = feedback.map(|node| if node < 3 { node + 1 } else { 0 });
                    let next = flowlog_dedup(seeds.enter(inner).concat(step));
                    variable.set(next.clone());
                    next.leave(scope)
                });
                let seen = Rc::clone(&seen);
                flowlog_dedup(reach)
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });

            input.update(0, Present);
            advance(worker, &mut input, &probe, 1);
            input.update(2, Present);
            advance(worker, &mut input, &probe, 2);
            input.update(4, Present);
            advance(worker, &mut input, &probe, 3);
            input.update(0, Present);
            input.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (0, 0, Present),
                (1, 0, Present),
                (2, 0, Present),
                (3, 0, Present),
                (4, 2, Present),
            ]
        );
    }
}
