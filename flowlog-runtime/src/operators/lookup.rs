//! Lookups: a stream of rows probing an arrangement, with no arrangement
//! of the stream itself.
//!
//! [`flowlog_join`](crate::operators::flowlog_join) arranges both inputs
//! and answers changes on either. [`flowlog_lookup`] answers only its row
//! stream: each row seeks its key in the arrangement and emits what the
//! values there add to it, so the rows are never arranged and the
//! arrangement is only read. That is what lets a plan extend a row one
//! relation at a time without keeping each partial result. [`Version`]
//! chooses which updates a row sees, so that one lookup per changing side
//! adds up to a join.
//!
//! Every [`LookupClock`] is a total order, on which a visible update is
//! never later than the row, so a match lands at the row's own time and
//! the operator keeps one running sum per value. A mutable loop's
//! `EpochLoop` orders its times only partly, and a match there may land
//! later than the row it answers; that clock is not a [`LookupClock`].
//!
//! The operator follows differential-dogs3 0.25.1's `lookup_map` and
//! `half_join`, with the comparison and the compaction it implies decided
//! here rather than passed in, and without their `Monoid` bound, so
//! presence weights work.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::BatchDiff;
use differential_dataflow::trace::BatchVal;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::trace::implementations::containers::BatchContainer;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::Operator;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use crate::time::Epoch;
use crate::time::LexLoop;
use crate::time::Once;
use crate::time::OnceLoop;

// =============================================================================
// Version
// =============================================================================

/// Which updates of the arrangement a row at time `t` sees: [`New`] or
/// [`Old`], a token [`flowlog_lookup`] takes so that the choice is made at
/// compile time.
///
/// A join of changing collections is the sum of one lookup per changing
/// side, each reading the other side as of `t` or as of just before it,
/// so that every pair of updates is matched by exactly one of them: the
/// lookup driven by the later update, the first among equals.
pub trait Version: Copy + sealed::Version + 'static {
    /// Returns `true` if a row at `row` sees an update at `update`.
    fn sees<T: Ord>(self, update: &T, row: &T) -> bool;

    /// Returns a time no later than `row` up to which the arrangement may
    /// compact while a row at `row` is pending: compacting to it moves no
    /// update across what the row sees.
    fn compaction<T: LookupClock>(self, row: &T) -> T;
}

/// Updates at times up to and including the row's.
#[derive(Debug, Clone, Copy)]
pub struct New;

impl Version for New {
    fn sees<T: Ord>(self, update: &T, row: &T) -> bool {
        update <= row
    }

    /// Compacting to the row's own time keeps every earlier update visible.
    fn compaction<T: LookupClock>(self, row: &T) -> T {
        row.clone()
    }
}

/// Updates at times strictly before the row's.
#[derive(Debug, Clone, Copy)]
pub struct Old;

impl Version for Old {
    fn sees<T: Ord>(self, update: &T, row: &T) -> bool {
        update < row
    }

    fn compaction<T: LookupClock>(self, row: &T) -> T {
        row.hold()
    }
}

// =============================================================================
// LookupClock
// =============================================================================

/// A clock a lookup can run under: one whose timely order is total and is
/// its `Ord`, so that a visible update is never later than the row and
/// every match lands at the row's own time.
pub trait LookupClock: Timestamp + Lattice + TotalOrder + Ord + sealed::LookupClock {
    /// Returns a time no later than `self` up to which the arrangement may
    /// compact while a row that sees strictly before `self` ([`Old`]) is
    /// pending: compacting to it keeps every update before `self` before
    /// it.
    fn hold(&self) -> Self;
}

/// One time: nothing to hold back.
impl LookupClock for Once {
    fn hold(&self) -> Self {}
}

/// Hold at the previous epoch, which every update before `self` reaches
/// and none after it does.
impl LookupClock for Epoch {
    fn hold(&self) -> Self {
        self.saturating_sub(1)
    }
}

/// Hold at the previous iteration.
impl LookupClock for OnceLoop {
    fn hold(&self) -> Self {
        OnceLoop::new((), self.inner.saturating_sub(1))
    }
}

/// Ordered epoch first: hold at the start of the previous epoch. An update
/// before `self` advances at most to a time still before it, and one at
/// or after it is already past the hold.
impl LookupClock for LexLoop {
    fn hold(&self) -> Self {
        let epoch = <LexLoop as Refines<Epoch>>::to_outer(*self);
        <LexLoop as Refines<Epoch>>::to_inner(epoch.saturating_sub(1))
    }
}

mod sealed {
    pub trait Version {}

    impl Version for super::New {}
    impl Version for super::Old {}

    pub trait LookupClock {}

    impl LookupClock for crate::time::Once {}
    impl LookupClock for crate::time::Epoch {}
    impl LookupClock for crate::time::OnceLoop {}
    impl LookupClock for crate::time::LexLoop {}
}

// =============================================================================
// Lookup
// =============================================================================

/// For each row `d` at time `t` with weight `r`, seeks `key(d)` in
/// `arranged` and, for each value there whose updates `version` lets `t`
/// see sum to a nonzero `w`, emits every item of `logic(d, value)` at `t`
/// with weight `r * w`.
///
/// Rows are exchanged by `key`'s hash, as the arrangement's keys are, and
/// wait until no update the arrangement may still receive is visible to
/// them.
///
/// One term of a join, not a join: an update that reaches `arranged` and
/// is not visible to a row is never matched with it, so the lookup equals
/// the join of the two only when `arranged` is complete before any row
/// arrives, as a static relation is inside a batch loop. [`Version`] says
/// how one lookup per changing side adds up to the join.
pub fn flowlog_lookup<'scope, Tr, D, K, R, I, V, F, L>(
    rows: VecCollection<'scope, Tr::Time, D, R>,
    arranged: Arranged<'scope, Tr>,
    name: &str,
    version: V,
    key: F,
    mut logic: L,
) -> VecCollection<'scope, Tr::Time, I::Item, <R as Multiply<BatchDiff<Tr>>>::Output>
where
    Tr: TraceReader<Batch: Navigable, Time: LookupClock> + Clone + 'static,
    V: Version,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: Hashable + Ord + 'static,
    D: ExchangeData,
    R: ExchangeData + Semigroup + Multiply<BatchDiff<Tr>, Output: Semigroup + 'static>,
    I: IntoIterator<Item: Data>,
    F: Fn(&D) -> K + Clone + 'static,
    L: FnMut(&D, BatchVal<'_, Tr>) -> I + 'static,
{
    let Arranged { stream, mut trace } = arranged;
    // Only logical compaction decides what a lookup can tell apart; merging
    // batches physically never changes an answer.
    trace.set_physical_compaction(Antichain::new().borrow());
    let mut trace = Some(trace);
    let mut stash: Stash<D, Tr::Time, R> = Vec::new();

    let route = key.clone();
    let exchange = Exchange::new(move |(row, ..): &(D, Tr::Time, R)| route(row).hashed().into());

    rows.inner
        .binary_frontier(stream, exchange, Pipeline, name, move |_, _| {
            move |(input, input_frontier), (batches, batch_frontier), output| {
                input.for_each(|capability, data| {
                    // A message's rows are at or after the least time of
                    // its stamp, which on a total order every stamp has.
                    let least = capability
                        .retain_least(0)
                        .expect("a message of rows is stamped with a time they are at or after");
                    let time = least.time().clone();
                    hold(&mut stash, &time, std::mem::take(data), move || least);
                });
                // The batches are read through the trace; their stream only
                // tells which updates may still arrive.
                batches.for_each(|_, _| {});

                // A row is ready once no update still to come is visible to
                // it. Readiness only falls as time rises, and every row
                // under a capability is at or after it, so a capability
                // that is not ready holds nothing ready.
                let frontier = batch_frontier.frontier();
                let ready = |time: &Tr::Time| frontier.iter().all(|f| !version.sees(f, time));

                // Rows that must keep waiting move to a capability at their
                // own time, so the one they arrived under is released. A
                // message may carry rows later than its capability, and
                // holding that capability for them would stall everything
                // downstream of it, the arrangement those rows wait on
                // included.
                let mut deferred: Stash<D, Tr::Time, R> = Vec::new();
                if let Some(trace) = trace.as_mut() {
                    for (capability, pending) in stash.iter_mut() {
                        if !ready(capability.time()) {
                            continue;
                        }
                        // Sorted by key so the cursor only moves forward.
                        pending.sort_by_cached_key(|(row, ..)| key(row));
                        let batches = trace
                            .batches_through(Antichain::new().borrow())
                            .expect("a trace yields every batch through the empty frontier");
                        let (mut cursor, storage) = cursor_list(batches);
                        // The cursor seeks a key as the batch lends it,
                        // which only a container of the batch's keys can.
                        let mut seek = <BatchCursor<Tr> as Cursor>::KeyContainer::with_capacity(1);
                        let mut session = output.session(capability);
                        for (row, time, weight) in pending.drain(..) {
                            if !ready(&time) {
                                let held = time.clone();
                                hold(&mut deferred, &held, [(row, time, weight)], || {
                                    capability.delayed(&held)
                                });
                                continue;
                            }
                            seek.clear();
                            seek.push_own(&key(&row));
                            cursor.seek_key(&storage, seek.index(0));
                            if cursor.get_key(&storage) != Some(seek.index(0)) {
                                continue;
                            }
                            while let Some(value) = cursor.get_val(&storage) {
                                if let Some(sum) = visible(&mut cursor, &storage, version, &time) {
                                    let weight = weight.clone().multiply(&sum);
                                    session.give_iterator(
                                        logic(&row, value)
                                            .into_iter()
                                            .map(|item| (item, time.clone(), weight.clone())),
                                    );
                                }
                                cursor.step_val(&storage);
                            }
                            // The next row may carry the same key.
                            cursor.rewind_vals(&storage);
                        }
                    }
                }
                stash.retain(|(_, pending)| !pending.is_empty());
                for (capability, pending) in deferred {
                    let time = capability.time().clone();
                    hold(&mut stash, &time, pending, move || capability);
                }

                // Hold compaction where every pending or future row still
                // tells visible updates from hidden ones.
                let mut bound = Antichain::new();
                for time in input_frontier.frontier().iter() {
                    bound.insert(version.compaction(time));
                }
                for (capability, _) in &stash {
                    bound.insert(version.compaction(capability.time()));
                }
                if let Some(trace) = trace.as_mut() {
                    trace.set_logical_compaction(bound.borrow());
                }
                if input_frontier.is_empty() && stash.is_empty() {
                    trace = None;
                }
            }
        })
        .as_collection()
}

/// Pending rows, by the capability that holds their time. A list rather
/// than a map: a capability is not a sound key, and only a few times are
/// ever pending at once.
type Stash<D, T, R> = Vec<(Capability<T>, Vec<(D, T, R)>)>;

/// Holds `rows` under the capability at `time`: the one `stash` holds
/// there already, or else the one `capability` makes.
fn hold<D, T: Timestamp, R>(
    stash: &mut Stash<D, T, R>,
    time: &T,
    rows: impl IntoIterator<Item = (D, T, R)>,
    capability: impl FnOnce() -> Capability<T>,
) {
    match stash.iter_mut().find(|(held, _)| held.time() == time) {
        Some((_, pending)) => pending.extend(rows),
        None => stash.push((capability(), rows.into_iter().collect())),
    }
}

/// Sums the updates of the cursor's current value that `version` lets a
/// row at `time` see; `None` when they sum to zero.
fn visible<V: Version, C: Cursor<Time: LookupClock>>(
    cursor: &mut C,
    storage: &C::Storage,
    version: V,
    time: &C::Time,
) -> Option<C::Diff> {
    let mut sum: Option<C::Diff> = None;
    cursor.map_times(storage, |update, diff| {
        if version.sees(&C::owned_time(update), time) {
            let diff = C::owned_diff(diff);
            match sum.as_mut() {
                Some(sum) => sum.plus_equals(&diff),
                None => sum = Some(diff),
            }
        }
    });
    sum.filter(|sum| !sum.is_zero())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::fmt::Debug;
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::iterate::Variable;
    use timely::order::Product;

    use super::*;
    use crate::diff;
    use crate::operators::flowlog_arrange;
    use crate::operators::flowlog_dedup;
    use crate::operators::flowlog_join;

    type Edge = (u64, u64);

    /// Runs `build` over two static inputs, `left` and `right`, outside any
    /// loop, and returns the rows it produces as a set.
    fn rows_of<B>(left: Vec<Edge>, right: Vec<Edge>, build: B) -> BTreeSet<Edge>
    where
        B: for<'scope> Fn(
                VecCollection<'scope, Once, Edge, diff::Static>,
                VecCollection<'scope, Once, Edge, diff::Static>,
            ) -> VecCollection<'scope, Once, Edge, diff::Static>
            + Send
            + Sync
            + 'static,
    {
        timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(BTreeSet::new()));
            let (mut l, mut r) = worker.dataflow::<Once, _, _>(|scope| {
                let (l, lc) = scope.new_collection::<Edge, diff::Static>();
                let (r, rc) = scope.new_collection::<Edge, diff::Static>();
                let seen = Rc::clone(&seen);
                build(lc, rc).inspect(move |(row, ..)| {
                    seen.borrow_mut().insert(*row);
                });
                (l, r)
            });
            for row in &left {
                l.update(*row, diff::Static);
            }
            for row in &right {
                r.update(*row, diff::Static);
            }
            l.close();
            r.close();
            while worker.step() {}
            seen.take()
        })
    }

    /// Looking `(x, y)` up in `y -> z` yields what joining the two on `y`
    /// yields.
    #[test]
    fn a_lookup_yields_what_a_join_yields() {
        let left = vec![(1, 10), (2, 10), (3, 20), (4, 30)];
        let right = vec![(10, 100), (10, 101), (20, 200)];
        let looked_up = rows_of(left.clone(), right.clone(), |l, r| {
            flowlog_lookup(
                l,
                flowlog_arrange(r, "Right"),
                "Lookup",
                New,
                |&(_, y)| y,
                |&(x, _), &z| Some((x, z)),
            )
        });
        let joined = rows_of(left, right, |l, r| {
            flowlog_join(
                flowlog_arrange(l.map(|(x, y)| (y, x)), "Left"),
                flowlog_arrange(r, "Right"),
                "Join",
                |_, &x, &z| Some((x, z)),
            )
        });
        assert_eq!(looked_up, joined);
        assert_eq!(
            looked_up,
            BTreeSet::from([(1, 100), (1, 101), (2, 100), (2, 101), (3, 200)])
        );
    }

    /// `logic` returns any number of rows per match, so a lookup carries a
    /// filter with it.
    #[test]
    fn logic_can_drop_a_match() {
        let rows = rows_of(vec![(1, 10), (5, 10)], vec![(10, 3), (10, 7)], |l, r| {
            flowlog_lookup(
                l,
                flowlog_arrange(r, "Right"),
                "Lookup",
                New,
                |&(_, y)| y,
                |&(x, _), &z| (z > x).then_some((x, z)),
            )
        });
        assert_eq!(rows, BTreeSet::from([(1, 3), (1, 7), (5, 7)]));
    }

    /// Deterministic pseudo-random edges among `nodes` nodes.
    fn edges(nodes: u64, count: usize, mut state: u64) -> Vec<Edge> {
        let mut next = move || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % nodes
        };
        (0..count).map(|_| (next(), next())).collect()
    }

    /// Computes `T(x, z) :- T(x, y), E(y, w), T(w, z)` from seed `S` in a
    /// batch loop, by binary joins or by one lookup chain per occurrence
    /// of `T`.
    fn fixpoint(seed: Vec<Edge>, extend: Vec<Edge>, by_lookup: bool) -> BTreeSet<Edge> {
        timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(BTreeSet::new()));
            let (mut s, mut e) = worker.dataflow::<Once, _, _>(|scope| {
                let (s, sc) = scope.new_collection::<Edge, diff::Static>();
                let (e, ec) = scope.new_collection::<Edge, diff::Static>();
                let e_by_y = flowlog_arrange(ec.clone(), "E by y");
                let e_by_w = flowlog_arrange(ec.map(|(y, w)| (w, y)), "E by w");
                let seen = Rc::clone(&seen);
                scope
                    .iterative::<u16, _, _>(|inner| {
                        let (variable, t) = Variable::new(inner, Product::new((), 1));
                        // Named so the arrangements below resolve before `set`.
                        let t: VecCollection<'_, OnceLoop, Edge, diff::Static> = t;
                        let e_by_y = e_by_y.enter(inner);
                        let e_by_w = e_by_w.enter(inner);
                        let t_by_w = flowlog_arrange(t.clone(), "T by w");
                        let t_by_y = flowlog_arrange(t.clone().map(|(x, y)| (y, x)), "T by y");
                        let derived = if by_lookup {
                            // The chain driven by the first T reads the
                            // second as it stood before this iteration; the
                            // chain driven by the second reads the first as
                            // it stands now, so a derivation from two new
                            // rows is made by the second chain alone.
                            let first = flowlog_lookup(
                                t.clone(),
                                e_by_y,
                                "E by y",
                                New,
                                |&(_, y)| y,
                                |&(x, _), &w| Some((w, x)),
                            );
                            let first = flowlog_lookup(
                                first,
                                t_by_w,
                                "old T by w",
                                Old,
                                |&(w, _)| w,
                                |&(_, x), &z| Some((x, z)),
                            );
                            let second = flowlog_lookup(
                                t.clone(),
                                e_by_w,
                                "E by w",
                                New,
                                |&(w, _)| w,
                                |&(_, z), &y| Some((y, z)),
                            );
                            let second = flowlog_lookup(
                                second,
                                t_by_y,
                                "new T by y",
                                New,
                                |&(y, _)| y,
                                |&(_, z), &x| Some((x, z)),
                            );
                            first.concat(second)
                        } else {
                            let te =
                                flowlog_join(t_by_y, e_by_y, "T join E", |_, &x, &w| Some((w, x)));
                            flowlog_join(
                                flowlog_arrange(te, "TE by w"),
                                t_by_w,
                                "TE join T",
                                |_, &x, &z| Some((x, z)),
                            )
                        };
                        let next = flowlog_dedup(derived.concat(sc.enter(inner)));
                        variable.set(next.clone());
                        next.leave(scope)
                    })
                    .inspect(move |(row, ..)| {
                        seen.borrow_mut().insert(*row);
                    });
                (s, e)
            });
            for row in &seed {
                s.update(*row, diff::Static);
            }
            for row in &extend {
                e.update(*row, diff::Static);
            }
            s.close();
            e.close();
            while worker.step() {}
            seen.take()
        })
    }

    /// Two lookup chains, one reading the other occurrence of the relation
    /// as it stood before the iteration and one as it stands now, derive
    /// exactly the fixpoint the binary joins derive.
    #[test]
    fn lookup_chains_reach_the_fixpoint_binary_joins_reach() {
        let seed = edges(60, 120, 7);
        let extend = edges(60, 120, 13);
        let joined = fixpoint(seed.clone(), extend.clone(), false);
        let looked_up = fixpoint(seed, extend, true);
        assert!(joined.len() > 120, "the fixpoint grows past the seed");
        assert_eq!(looked_up, joined);
    }

    /// `New` sees up to and including the row's time and `Old` strictly
    /// before it; outside a loop there is no earlier time.
    #[test]
    fn a_version_sees_updates_up_to_or_before_the_row() {
        let at = |iteration| OnceLoop::new((), iteration);
        assert!(New.sees(&at(2), &at(2)));
        assert!(New.sees(&at(1), &at(2)));
        assert!(!New.sees(&at(3), &at(2)));
        assert!(!Old.sees(&at(2), &at(2)));
        assert!(Old.sees(&at(1), &at(2)));
        assert!(!Old.sees(&at(0), &at(0)));
        assert!(New.sees(&(), &()));
        assert!(!Old.sees(&(), &()));
    }

    /// Every time of a small grid of each clock, as `(epoch, iteration)`
    /// pairs for the clocks that have both.
    fn grid<T>(make: impl Fn(u16, u16) -> T) -> Vec<T> {
        (0..4)
            .flat_map(|e| (0..4).map(move |i| (e, i)))
            .map(|(e, i)| make(e, i))
            .collect()
    }

    /// Compacting an update to a pending row's bound, as differential does
    /// by joining it with the bound, keeps what the version sees of it, on
    /// every clock.
    #[test]
    fn compaction_to_the_bound_changes_no_answer() {
        fn check<T: LookupClock>(times: &[T]) {
            fn with<V: Version + Debug, T: LookupClock>(version: V, times: &[T]) {
                for row in times {
                    let bound = version.compaction(row);
                    assert!(bound.less_equal(row), "bound {bound:?} after {row:?}");
                    for update in times {
                        assert_eq!(
                            version.sees(&update.join(&bound), row),
                            version.sees(update, row),
                            "{version:?} of {update:?} from {row:?}",
                        );
                    }
                }
            }
            with(New, times);
            with(Old, times);
        }
        check(&grid(|_, _| ()));
        check(&grid(|e, i| u32::from(e) * 4 + u32::from(i)));
        check(&grid(|_, i| OnceLoop::new((), i)));
        check(&grid(|e, i| (u32::from(e), i)));
    }

    /// Updates by epoch: `(row, epoch, weight)`.
    type Changes = Vec<(Edge, u32, i32)>;

    /// Runs `build` over two signed inputs fed epoch by epoch and returns
    /// every output update, consolidated.
    fn signed_updates<B>(left: Changes, right: Changes, build: B) -> Vec<(Edge, u32, i32)>
    where
        B: for<'scope> Fn(
                VecCollection<'scope, Epoch, Edge, diff::Mutable>,
                VecCollection<'scope, Epoch, Edge, diff::Mutable>,
            ) -> VecCollection<'scope, Epoch, Edge, diff::Mutable>
            + Send
            + Sync
            + 'static,
    {
        let mut updates = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut l, mut r) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (l, lc) = scope.new_collection::<Edge, diff::Mutable>();
                let (r, rc) = scope.new_collection::<Edge, diff::Mutable>();
                let seen = Rc::clone(&seen);
                build(lc, rc).inspect(move |update| seen.borrow_mut().push(*update));
                (l, r)
            });
            let last = left
                .iter()
                .chain(&right)
                .map(|(_, e, _)| *e)
                .max()
                .unwrap_or(0);
            for epoch in 0..=last {
                for (row, _, weight) in left.iter().filter(|(_, e, _)| *e == epoch) {
                    l.update(*row, *weight);
                }
                for (row, _, weight) in right.iter().filter(|(_, e, _)| *e == epoch) {
                    r.update(*row, *weight);
                }
                l.advance_to(epoch + 1);
                r.advance_to(epoch + 1);
            }
            l.close();
            r.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut updates);
        updates
    }

    /// Across epochs of inserts and deletes on both sides, a lookup from
    /// each side's changes, one reading the other as of its epoch and one
    /// as of before it, emits exactly the updates a join emits.
    #[test]
    fn two_lookups_emit_the_updates_a_join_emits_across_epochs() {
        let left: Changes = vec![
            ((1, 10), 0, 1),
            ((2, 10), 0, 1),
            ((3, 20), 1, 1),
            ((1, 10), 2, -1),
            ((4, 10), 2, 1),
        ];
        let right: Changes = vec![
            ((10, 100), 0, 1),
            ((20, 200), 0, 1),
            ((10, 101), 1, 1),
            ((10, 100), 2, -1),
            ((20, 201), 2, 1),
        ];
        let joined = signed_updates(left.clone(), right.clone(), |l, r| {
            flowlog_join(
                flowlog_arrange(l.map(|(x, y)| (y, x)), "Left by y"),
                flowlog_arrange(r, "Right"),
                "Join",
                |_, &x, &z| Some((x, z)),
            )
        });
        let looked_up = signed_updates(left, right, |l, r| {
            let from_left = flowlog_lookup(
                l.clone(),
                flowlog_arrange(r.clone(), "Right"),
                "Left looks up right",
                New,
                |&(_, y)| y,
                |&(x, _), &z| Some((x, z)),
            );
            let from_right = flowlog_lookup(
                r,
                flowlog_arrange(l.map(|(x, y)| (y, x)), "Left by y"),
                "Right looks up left",
                Old,
                |&(y, _)| y,
                |&(_, z), &x| Some((x, z)),
            );
            from_left.concat(from_right)
        });
        assert!(
            joined.iter().any(|(_, _, weight)| *weight < 0),
            "deletes reach the output"
        );
        assert_eq!(looked_up, joined);
    }
}
