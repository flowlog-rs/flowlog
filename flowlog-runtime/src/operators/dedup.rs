//! Set-semantics dedup for generated FlowLog rules.
//!
//! Two entry points, by where the collection comes from:
//!
//! - [`flowlog_dedup`], inside a rule. [`FlowlogDedup`] selects an
//!   implementation by timestamp and diff. A presence collection in a
//!   totally ordered loop, a [`LexLoop`] or a [`OnceLoop`], keeps its
//!   first occurrences in a hash table ([`hash_first_occurrences`]); on
//!   other total clocks it consolidates or thresholds. Recursive products
//!   over an advancing counter use [`first_occurrences`] or signed
//!   reduction. A signed collection keeps its counts underneath: a row
//!   stays present while its count is positive.
//! - [`flowlog_input_dedup`], at an input relation. [`FlowlogInputDedup`]
//!   selects by weight. A presence input dedups as a rule does. A signed
//!   input is read as a set under insertions and deletions, not as a bag
//!   with counts: one deletion removes a row however often it was
//!   inserted, and a deletion of an absent row changes nothing.
//!
//! [`OnceLoop`]: crate::time::OnceLoop

use std::hash::Hash;

use differential_dataflow::AsCollection;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::trace::implementations::KeyBatcher;
use differential_dataflow::trace::implementations::KeyBuilder;
use differential_dataflow::trace::implementations::KeySpine;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use timely::PartialOrder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::Operator;
use timely::order::Product;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use crate::diff;
use crate::diff::Presence;
use crate::operators::arrange::flowlog_arrange_set;
use crate::time::LexLoop;

/// Maintains a set without changing the collection's diff type.
///
/// `diff::Mutable` output has accumulated weight `1` wherever the input
/// count is positive, and `0` otherwise. Deletions and later reinsertions
/// propagate. A presence weight emits a tuple only at times not covered by
/// an earlier occurrence in timely's partial order. In a recursive
/// product, times such as `(0, 5)` and `(1, 1)` are incomparable: both are
/// retained, while a later occurrence at `(1, 5)` is suppressed.
///
/// The diff and timestamp select the implementation at compile time.
/// At `()` a presence needs only consolidation. Advancing clocks retain
/// history, including across iterations in recursive feedback.
///
/// `strategy` chooses how a presence loop, a [`OnceLoop`] or a
/// [`LexLoop`], finds first occurrences: [`Hashed`] or [`Sorted`]. Both
/// produce the same output batch for batch; every other clock and weight
/// has one implementation and accepts either.
///
/// [`OnceLoop`]: crate::time::OnceLoop
pub fn flowlog_dedup<C, Dedup>(collection: C, _strategy: Dedup) -> C
where
    C: FlowlogDedup<Dedup>,
{
    collection.dedup()
}

/// Dedups an input relation into a set, by the rule its weight gives.
///
/// A presence weight keeps the first occurrence of each row, as
/// [`flowlog_dedup`] does. `diff::Mutable` tracks membership rather than
/// counts: a row is present after an epoch whose insertions of it
/// outnumber its deletions, absent after one where deletions outnumber
/// insertions, and unchanged by one where they balance. The membership
/// lives in the set arrangement the operator builds, and nothing else is
/// stored.
pub fn flowlog_input_dedup<C: FlowlogInputDedup>(collection: C) -> C {
    collection.input_dedup()
}

// =============================================================================
// FlowlogDedup
// =============================================================================

/// A presence loop's dedup keeps a hash table of the rows it has announced
/// (`hash_first_occurrences`). Each repeat costs one probe rather than a
/// share of a sort, at the price of the table's free slots. It is the
/// default.
#[derive(Debug, Clone, Copy, Default)]
pub struct Hashed;

/// A presence loop's dedup is Differential Dataflow's threshold over an
/// arrangement of every candidate row. Each repeat is sorted with the rest,
/// and the rows are stored packed in the trace.
#[derive(Debug, Clone, Copy, Default)]
pub struct Sorted;

/// Compile-time dispatch behind [`flowlog_dedup`], by clock, weight and the
/// strategy `Dedup`. Only presence loops implement it per strategy.
/// `Dedup` defaults to [`Hashed`], so a bound on a collection whose dedup
/// has one implementation can omit it.
pub trait FlowlogDedup<Dedup = Hashed>: Sized {
    /// See [`flowlog_dedup`].
    fn dedup(self) -> Self;
}

impl<'scope, Dedup, D, R> FlowlogDedup<Dedup> for VecCollection<'scope, (), D, R>
where
    D: ExchangeData + Hashable,
    R: Presence,
{
    fn dedup(self) -> Self {
        // Every update has the same time, and presence is idempotent.
        // Consolidation suffices without retaining a history trace.
        self.consolidate()
    }
}

impl<'scope, Dedup, E: Counter, D, R> FlowlogDedup<Dedup> for VecCollection<'scope, E, D, R>
where
    D: ExchangeData + Hashable,
    R: Presence,
{
    fn dedup(self) -> Self {
        // In a total order, the first occurrence covers all later ones.
        self.threshold_semigroup(|_, _, prior| prior.is_none().then_some(R::one()))
    }
}

impl<'scope, I: Counter, D, R> FlowlogDedup<Hashed> for VecCollection<'scope, Product<(), I>, D, R>
where
    D: ExchangeData + Hashable + Hash + Ord,
    R: Presence,
{
    fn dedup(self) -> Self {
        // Only the iteration coordinate advances, so times remain ordered.
        hash_first_occurrences(self)
    }
}

impl<'scope, I: Counter, D, R> FlowlogDedup<Sorted> for VecCollection<'scope, Product<(), I>, D, R>
where
    D: ExchangeData + Hashable,
    R: Presence,
{
    fn dedup(self) -> Self {
        // Only the iteration coordinate advances, so times remain ordered.
        self.threshold_semigroup(|_, _, prior| prior.is_none().then_some(R::one()))
    }
}

impl<'scope, Dedup, E: Counter, I: Counter, D, R> FlowlogDedup<Dedup>
    for VecCollection<'scope, Product<E, I>, D, R>
where
    D: ExchangeData + Hashable,
    R: Presence,
{
    fn dedup(self) -> Self {
        first_occurrences(self)
    }
}

impl<'scope, D, R> FlowlogDedup<Hashed> for VecCollection<'scope, LexLoop, D, R>
where
    D: ExchangeData + Hashable + Hash + Ord,
    R: Presence,
{
    fn dedup(self) -> Self {
        // Lexicographic loop times are totally ordered.
        hash_first_occurrences(self)
    }
}

impl<'scope, D, R> FlowlogDedup<Sorted> for VecCollection<'scope, LexLoop, D, R>
where
    D: ExchangeData + Hashable,
    R: Presence,
{
    fn dedup(self) -> Self {
        // Lexicographic loop times are totally ordered.
        self.threshold_semigroup(|_, _, prior| prior.is_none().then_some(R::one()))
    }
}

impl<'scope, Dedup, D> FlowlogDedup<Dedup> for VecCollection<'scope, (), D, diff::Mutable>
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

impl<'scope, Dedup, E: Counter, D> FlowlogDedup<Dedup>
    for VecCollection<'scope, E, D, diff::Mutable>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        self.threshold_total(|_, &count| if count > 0 { 1 } else { 0 })
    }
}

impl<'scope, Dedup, I: Counter, D> FlowlogDedup<Dedup>
    for VecCollection<'scope, Product<(), I>, D, diff::Mutable>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        self.threshold_total(|_, &count| if count > 0 { 1 } else { 0 })
    }
}

impl<'scope, Dedup, D> FlowlogDedup<Dedup> for VecCollection<'scope, LexLoop, D, diff::Mutable>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        self.threshold_total(|_, &count| if count > 0 { 1 } else { 0 })
    }
}

impl<'scope, Dedup, E: Counter, I: Counter, D> FlowlogDedup<Dedup>
    for VecCollection<'scope, Product<E, I>, D, diff::Mutable>
where
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        // Incomparable derivations can overlap at a later time. Signed
        // output must correct that overlap as well as propagate deletions.
        self.threshold(|_, &count| if count > 0 { 1 } else { 0 })
    }
}

/// Converts nonzero membership to presence of weight `R` at times with
/// no earlier occurrence. Input membership must be monotone: once the
/// accumulated weight is nonzero, it must remain nonzero at all greater
/// times. Signed updates at the same time cancel before presence is
/// tested; arbitrary signed collections do not satisfy the monotonicity
/// contract.
pub(super) fn first_occurrences<'scope, E, I, D, R1, R>(
    collection: VecCollection<'scope, Product<E, I>, D, R1>,
) -> VecCollection<'scope, Product<E, I>, D, R>
where
    E: Counter,
    I: Counter,
    D: ExchangeData + Hashable,
    R1: ExchangeData + Semigroup,
    R: Presence,
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
                                        session.give((key.clone(), time.clone(), R::one()));
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

/// Announces each row of a presence collection once, at the least time it
/// arrives with, and each time's rows in sorted order. Requires a totally
/// ordered clock, so that every row has a single least time; its output
/// equals a `threshold_semigroup` that passes a row with no prior
/// occurrence, batch for batch.
///
/// Each worker owns the rows that hash to it and holds one table entry per
/// distinct row for the life of the dataflow.
pub(super) fn hash_first_occurrences<'scope, T, D, R>(
    collection: VecCollection<'scope, T, D, R>,
) -> VecCollection<'scope, T, D, R>
where
    T: Timestamp + TotalOrder,
    D: ExchangeData + Hashable + Hash + Ord,
    R: Presence,
{
    // A Datalog fixpoint derives the same row many times over, and
    // `threshold_semigroup` sorts every derivation into a batch before it
    // can tell a repeat from a first occurrence, so each repeat pays its
    // share of a sort over all candidates. A hash probe answers that
    // question per candidate in constant expected time, and only first
    // occurrences reach the sort, when their time closes: the sorted
    // volume shrinks from every derivation to the distinct rows, and no
    // batcher buffers a round's candidates. The cost is memory where
    // derivations rarely repeat: there is no sort to save, and a hash
    // table keeps each distinct row with slack for its load factor, where
    // the trace keeps them packed and sorted. The tables are not shared,
    // which a presence collection never needed: its arrangements are built
    // from the deduped output.
    //
    // The tables' keys are rows the engine derived, never an adversary's,
    // so Fx over SipHash for the per-row win, as the interner does,
    // accepting that the tables would be open to HashDoS if untrusted rows
    // ever reached them.
    let exchange = Exchange::new(|update: &(D, T, R)| update.0.hashed().into());
    collection
        .inner
        .unary_frontier(exchange, "HashDedup", move |_capability, _info| {
            // A row's least time matters only until the frontier closes it:
            // after that no earlier instance can arrive, and the row needs
            // only membership. So rows with an open least time sit in
            // `open` with that time, and announced rows move to `closed`
            // without one. `closed` holds every distinct row and is most of
            // the operator's memory, so its entries carry only the row;
            // `open` holds just the rows of the few times still open.
            let mut open: FxHashMap<D, T> = FxHashMap::default();
            let mut closed: FxHashSet<D> = FxHashSet::default();
            // Rows awaiting their time's close, under a capability for that
            // time. A handful of times are open at once, so a scan beats a
            // map.
            let mut pending: Vec<(Capability<T>, Vec<D>)> = Vec::new();
            move |(input, frontier), output| {
                input.for_each(|capability, data| {
                    for (row, time, _) in data.drain(..) {
                        if closed.contains(&row) {
                            continue;
                        }
                        // A row's instance at a later time can arrive before
                        // its instance at an earlier one: another worker's
                        // earlier round fed back and joined while this
                        // round's derivation was still in flight. Presence
                        // cannot retract, so a row is announced only once
                        // the frontier has closed its least time; until then
                        // `open` tracks that least time and the row waits
                        // under it.
                        let keep = match open.get_mut(&row) {
                            None => {
                                open.insert(row.clone(), time.clone());
                                true
                            }
                            Some(seen) if seen.less_equal(&time) => false,
                            Some(seen) => {
                                *seen = time.clone();
                                true
                            }
                        };
                        if keep {
                            match pending.iter_mut().find(|(held, _)| *held.time() == time) {
                                Some((_, rows)) => rows.push(row),
                                None => pending.push((capability.delayed(&time, 0), vec![row])),
                            }
                        }
                    }
                });
                let closed_times =
                    pending.extract_if(.., |(held, _)| !frontier.less_equal(held.time()));
                for (capability, mut rows) in closed_times {
                    let time = capability.time().clone();
                    // Sorting only the first occurrences keeps the order a
                    // consolidated batch had, which a writer's output and
                    // the head's downstream arrangements both see, for a
                    // small fraction of the sort the threshold paid.
                    rows.sort_unstable();
                    let mut session = output.session(&capability);
                    for row in rows {
                        // A row whose least time moved earlier after it was
                        // queued here is announced under that earlier time.
                        if open.get(&row) == Some(&time) {
                            open.remove(&row);
                            session.give((row.clone(), time.clone(), R::one()));
                            closed.insert(row);
                        }
                    }
                }
            }
        })
        .as_collection()
}

// =============================================================================
// FlowlogInputDedup
// =============================================================================

/// Compile-time dispatch behind [`flowlog_input_dedup`].
pub trait FlowlogInputDedup: Sized {
    /// See [`flowlog_input_dedup`].
    fn input_dedup(self) -> Self;
}

impl<'scope, T, D, R> FlowlogInputDedup for VecCollection<'scope, T, D, R>
where
    T: Timestamp,
    R: Presence,
    Self: FlowlogDedup,
{
    fn input_dedup(self) -> Self {
        // Inputs sit outside every loop, where the strategy has no effect.
        self.dedup()
    }
}

impl<'scope, E: Counter, D> FlowlogInputDedup for VecCollection<'scope, E, D, diff::Mutable>
where
    D: ExchangeData + Hashable,
{
    fn input_dedup(self) -> Self {
        flowlog_arrange_set::<
            D,
            (),
            E,
            diff::Mutable,
            KeyBatcher<D, E, diff::Mutable>,
            KeyBuilder<D, E, diff::Mutable>,
            KeySpine<D, E, diff::Mutable>,
        >(self.map(|row| (row, ())).inner, "InputDedup")
        .as_collection(|row, ()| row.clone())
    }
}

// =============================================================================
// Counter
// =============================================================================

/// An advancing root or iteration counter: `u16` and `u32` today.
/// Sealed to keep supported widths in one place, on `sealed::Counter`.
pub trait Counter: Timestamp + TotalOrder + Lattice + sealed::Counter {}

impl<E> Counter for E where E: sealed::Counter + Timestamp + TotalOrder + Lattice {}

mod sealed {
    pub trait Counter {}

    impl Counter for u16 {}
    impl Counter for u32 {}
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::Data;
    use differential_dataflow::consolidation::consolidate_updates;
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

    /// Both presence weights dedup at every clock, and a signed one too,
    /// under either strategy.
    #[test]
    fn every_supported_diff_and_clock_pairing_compiles() {
        fn admits<T: Timestamp + Lattice, Dedup>()
        where
            VecCollection<'static, T, Row, diff::Static>: FlowlogDedup<Dedup>,
            VecCollection<'static, T, Row, diff::Append>: FlowlogDedup<Dedup>,
            VecCollection<'static, T, Row, i32>: FlowlogDedup<Dedup>,
        {
        }
        fn admits_both<T: Timestamp + Lattice>()
        where
            VecCollection<'static, T, Row, diff::Static>:
                FlowlogDedup<Hashed> + FlowlogDedup<Sorted>,
            VecCollection<'static, T, Row, diff::Append>:
                FlowlogDedup<Hashed> + FlowlogDedup<Sorted>,
            VecCollection<'static, T, Row, i32>: FlowlogDedup<Hashed> + FlowlogDedup<Sorted>,
        {
            admits::<T, Hashed>();
            admits::<T, Sorted>();
        }
        admits_both::<Batch>();
        admits_both::<u16>();
        admits_both::<Inc>();
        admits_both::<BatchLoop>();
        admits_both::<Product<(), u32>>();
        admits_both::<Product<u16, u16>>();
        admits_both::<Product<u16, u32>>();
        admits_both::<IncLoop>();
        admits_both::<Product<u32, u32>>();
        admits_both::<LexLoop>();
    }

    /// Runs a presence feedback loop over a batch iteration, dedupped by
    /// `strategy`, and returns the dedup's announcements in arrival order.
    fn once_loop_announcements<Dedup>(strategy: Dedup) -> Vec<(Row, BatchLoop, diff::Static)>
    where
        Dedup: Copy + Send + Sync + 'static,
        for<'scope> VecCollection<'scope, BatchLoop, Row, diff::Static>: FlowlogDedup<Dedup>,
    {
        timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<Batch, _, _>(|scope| {
                let (input, seeds) = scope.new_collection::<Row, diff::Static>();
                let seen = Rc::clone(&seen);
                scope.iterative::<u16, _, _>(|inner| {
                    let (variable, feedback) = Variable::new(inner, Product::new((), 1));
                    let step = feedback.map(|node| if node < 5 { node + 1 } else { 0 });
                    let next = flowlog_dedup(seeds.enter(inner).concat(step), strategy);
                    next.clone()
                        .inspect(move |update| seen.borrow_mut().push(*update));
                    variable.set(next);
                });
                input
            });
            for seed in [0, 3, 3] {
                input.update(seed, diff::Static);
            }
            input.close();
            while worker.step() {}
            seen.take()
        })
    }

    /// As [`once_loop_announcements`], in a lexicographic loop.
    fn lex_loop_announcements<Dedup>(strategy: Dedup) -> Vec<(Row, LexLoop, diff::Static)>
    where
        Dedup: Copy + Send + Sync + 'static,
        for<'scope> VecCollection<'scope, LexLoop, Row, diff::Static>: FlowlogDedup<Dedup>,
    {
        timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, seeds) = scope.new_collection::<Row, diff::Static>();
                let seen = Rc::clone(&seen);
                scope.scoped::<LexLoop, _, _>("Iterative", |inner| {
                    let (variable, feedback) = Variable::new(inner, LexLoop::NEXT_ITERATION);
                    let step = feedback.map(|node| if node < 5 { node + 1 } else { 0 });
                    let next = flowlog_dedup(seeds.enter(inner).concat(step), strategy);
                    next.clone()
                        .inspect(move |update| seen.borrow_mut().push(*update));
                    variable.set(next);
                });
                input
            });
            for seed in [0, 3, 3] {
                input.update(seed, diff::Static);
            }
            input.close();
            while worker.step() {}
            seen.take()
        })
    }

    /// The threshold is the reference: the hash dedup must announce the
    /// same rows at the same times in the same order, in each loop clock.
    #[test]
    fn hashed_and_sorted_announce_alike_in_a_batch_iteration() {
        let hashed = once_loop_announcements(Hashed);
        assert_eq!(hashed, once_loop_announcements(Sorted));
        assert_eq!(hashed.len(), 6, "every node is reached once");
    }

    #[test]
    fn hashed_and_sorted_announce_alike_in_a_lexicographic_loop() {
        let hashed = lex_loop_announcements(Hashed);
        assert_eq!(hashed, lex_loop_announcements(Sorted));
        assert_eq!(hashed.len(), 6, "every node is reached once");
    }

    #[rstest]
    #[case(vec![(7, diff::Static), (7, diff::Static)], vec![(7, (), diff::Static)])]
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
                flowlog_dedup(rows, Hashed)
                    .inspect(move |update| seen.borrow_mut().push(update.clone()));
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
                flowlog_dedup(rows, Hashed).inspect(move |update| seen.borrow_mut().push(*update));
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
    fn presence_preserves_incomparable_times(#[case] flush_each_epoch: bool) {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<(Row, u16), diff::Static>();
                let seen = Rc::clone(&seen);
                scope
                    .iterative::<u16, _, _>(|inner| {
                        let rows = rows
                            .enter_at(inner, |(_, iteration)| *iteration)
                            .map(|(row, _)| row);
                        flowlog_dedup(rows, Hashed)
                            .inspect(move |update| seen.borrow_mut().push(*update))
                            .leave(scope)
                    })
                    .probe_with(&probe);
                input
            });

            input.update((7, 6), diff::Static);
            input.update((7, 5), diff::Static);
            input.advance_to(1);
            if flush_each_epoch {
                input.flush();
                worker.step_while(|| probe.less_than(&1));
            }
            input.update((7, 5), diff::Static);
            input.update((7, 1), diff::Static);
            input.update((8, 2), diff::Static);
            input.advance_to(2);
            if flush_each_epoch {
                input.flush();
                worker.step_while(|| probe.less_than(&2));
            }
            input.update((7, 5), diff::Static);
            input.update((8, 2), diff::Static);
            input.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (7, Product::new(0, 5), diff::Static),
                (7, Product::new(1, 1), diff::Static),
                (8, Product::new(1, 2), diff::Static),
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
                flowlog_dedup(rows, Hashed)
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
                        flowlog_dedup(rows, Hashed)
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
    fn presence_feedback_converges_across_epochs() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, seeds) = scope.new_collection::<Row, diff::Static>();
                let reach = scope.iterative::<u16, _, _>(|inner| {
                    let (variable, feedback) = Variable::new(inner, Product::new(0, 1));
                    let step = feedback.map(|node| if node < 3 { node + 1 } else { 0 });
                    let next = flowlog_dedup(seeds.enter(inner).concat(step), Hashed);
                    variable.set(next.clone());
                    next.leave(scope)
                });
                let seen = Rc::clone(&seen);
                flowlog_dedup(reach, Hashed)
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });

            input.update(0, diff::Static);
            advance(worker, &mut input, &probe, 1);
            input.update(2, diff::Static);
            advance(worker, &mut input, &probe, 2);
            input.update(4, diff::Static);
            advance(worker, &mut input, &probe, 3);
            input.update(0, diff::Static);
            input.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (0, 0, diff::Static),
                (1, 0, diff::Static),
                (2, 0, diff::Static),
                (3, 0, diff::Static),
                (4, 2, diff::Static),
            ]
        );
    }

    /// Presence feedback in a lexicographic loop announces each derived row
    /// once, at the epoch its inputs arrived.
    #[test]
    fn presence_feedback_in_a_lex_loop_announces_each_row_once() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, seeds) = scope.new_collection::<Row, diff::Static>();
                let reach = scope.scoped::<LexLoop, _, _>("Iterative", |inner| {
                    let (variable, feedback) = Variable::new(inner, LexLoop::NEXT_ITERATION);
                    let step = feedback.map(|node| if node < 3 { node + 1 } else { 0 });
                    let next = flowlog_dedup(seeds.enter(inner).concat(step), Hashed);
                    variable.set(next.clone());
                    next.leave(scope)
                });
                let seen = Rc::clone(&seen);
                reach
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });
            input.update(0, diff::Static);
            input.update(2, diff::Static);
            input.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (0, 0, diff::Static),
                (1, 0, diff::Static),
                (2, 0, diff::Static),
                (3, 0, diff::Static),
            ]
        );
    }

    /// A row's instance at a later time can reach the operator before its
    /// instance at an earlier one, in an earlier activation; the row is
    /// held until the frontier passes its least time, then announced there,
    /// once. An operator that announced each activation's least time
    /// without waiting would emit the row at 2 here, and the later
    /// occurrences would be lost. The helper is driven directly because the
    /// generated loops feed it through feedback, which a single-worker test
    /// cannot reorder.
    #[test]
    fn a_later_time_seen_first_waits_for_the_frontier() {
        let (after_two, after_one, actual) = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Static>();
                let seen = Rc::clone(&seen);
                hash_first_occurrences(rows)
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });
            // The input stays at time 0, so the frontier cannot pass 1 or 2
            // while those updates are processed, each in its own activation.
            // Sixteen steps is more than the update needs to reach the
            // operator and run it; the count is otherwise arbitrary.
            let mut deliver = |row, time| {
                input.update_at(row, time, diff::Static);
                input.flush();
                for _ in 0..16 {
                    worker.step();
                }
                seen.borrow().clone()
            };
            let after_two = deliver(7, 2);
            let after_one = deliver(7, 1);
            input.update_at(7, 0, diff::Static);
            input.close();
            while worker.step() {}
            (after_two, after_one, seen.take())
        });
        assert_eq!((after_two, after_one), (vec![], vec![]));
        assert_eq!(actual, vec![(7, 0, diff::Static)]);
    }

    /// A time's first occurrences leave sorted whatever order they arrived
    /// in: a writer's output and the head's downstream arrangements both
    /// see that order, as they saw a consolidated batch's.
    #[test]
    fn a_times_first_occurrences_are_announced_sorted() {
        let actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Static>();
                let seen = Rc::clone(&seen);
                hash_first_occurrences(rows)
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });
            for row in [9, 3, 7, 3, 5] {
                input.update_at(row, 0, diff::Static);
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(
            actual,
            vec![
                (3, 0, diff::Static),
                (5, 0, diff::Static),
                (7, 0, diff::Static),
                (9, 0, diff::Static),
            ]
        );
    }

    /// Runs `epochs`, one inner vector of signed updates per epoch, through
    /// a signed input dedup, and returns its output consolidated.
    fn input_dedup_epochs(
        epochs: Vec<Vec<(Row, diff::Mutable)>>,
    ) -> Vec<(Row, u32, diff::Mutable)> {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_input_dedup(rows)
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
            input_dedup_epochs(vec![
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
            input_dedup_epochs(vec![
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
                flowlog_input_dedup(rows).inspect(move |update| seen.borrow_mut().push(*update));
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

    /// A presence input keeps its first-occurrence dedup at every clock it
    /// lives at; a signed input has the membership set at the epoch clock.
    #[test]
    fn every_supported_input_weight_and_clock_pairing_compiles() {
        fn admits_presence<T: Timestamp + Lattice>()
        where
            VecCollection<'static, T, Row, diff::Static>: FlowlogInputDedup,
            VecCollection<'static, T, Row, diff::Append>: FlowlogInputDedup,
        {
        }
        fn admits_signed<T: Timestamp + Lattice>()
        where
            VecCollection<'static, T, Row, diff::Mutable>: FlowlogInputDedup,
        {
        }
        admits_presence::<Batch>();
        admits_presence::<Inc>();
        admits_presence::<BatchLoop>();
        admits_presence::<LexLoop>();
        admits_signed::<Inc>();
    }

    /// An append input announces each row once, at the first epoch that
    /// inserts it, whatever later epochs insert it again.
    #[test]
    fn an_append_input_keeps_one_announcement_per_row_across_epochs() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<Inc, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Append>();
                let seen = Rc::clone(&seen);
                flowlog_input_dedup(rows)
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });
            input.update(1, diff::Append);
            input.update(2, diff::Append);
            input.update(2, diff::Append);
            advance(worker, &mut input, &probe, 1);
            input.update(1, diff::Append);
            advance(worker, &mut input, &probe, 2);
            input.update(2, diff::Append);
            input.update(3, diff::Append);
            input.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (1, 0, diff::Append),
                (2, 0, diff::Append),
                (3, 2, diff::Append)
            ]
        );
    }

    /// A static input announces each row once whatever repeats it.
    #[test]
    fn a_static_input_keeps_one_announcement_per_row() {
        let actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_input_dedup(rows).inspect(move |update| seen.borrow_mut().push(*update));
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
