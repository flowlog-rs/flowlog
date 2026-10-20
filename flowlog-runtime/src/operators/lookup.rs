//! Lookups: a stream of prefixes probing an arrangement, with no
//! arrangement of the stream itself.
//!
//! [`flowlog_join`](crate::operators::flowlog_join) arranges both inputs
//! and answers changes on either. A lookup answers only its prefix stream:
//! each prefix seeks its key in the arrangement and reads the values there,
//! so the prefixes are never arranged and the arrangement is only read.
//! That is what lets a plan extend a row one relation at a time without
//! keeping each partial result, and lets a semijoin reuse the arrangement
//! a later join reads anyway.
//!
//! - [`flowlog_lookup`] emits what each matching value adds to the prefix.
//! - [`flowlog_lookup_exists`] emits the prefix itself when its key has a
//!   value.
//!
//! One lookup is one term of a join. [`Version`] chooses which updates of
//! the arrangement a prefix sees, so that the lookups driven by each side
//! of a join add up to the join once each. Visibility is decided by the
//! clock's `Ord`, which every [`LookupClock`] extends its timely order to,
//! and a match lands at the join of the two times, so one operator serves
//! every clock: a batch loop and a mutable loop of an incremental engine
//! alike. A clock whose timely order is total already pays nothing for the
//! partial case: a visible update's time joins the prefix's to the prefix's
//! own, so it sums each value's weight once and stops an existence check
//! at the first value. The operator follows differential-dogs3 0.25.1's
//! `lookup_map` and `half_join`, with the comparison and the compaction it
//! implies decided here rather than passed in, and without their `Monoid`
//! bound, so presence weights work.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::consolidation::consolidate;
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
use differential_dataflow::trace::implementations::containers::BatchContainer;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::Operator;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use crate::diff::Presence;
use crate::operators::map::flowlog_map;
use crate::time::Epoch;
use crate::time::EpochLoop;
use crate::time::LexLoop;
use crate::time::Once;
use crate::time::OnceLoop;

// =============================================================================
// Version
// =============================================================================

/// Which updates of the arrangement a prefix at time `t` sees, compared by
/// the clock's `Ord`.
///
/// A join of changing collections is the sum of one lookup path per
/// changing side, each reading the other sides as of `t` or as of just
/// before it, so that every pair of updates is matched by exactly one path:
/// the path of the later update, the earlier path among equals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Version {
    /// Updates at times up to and including `t`.
    New,
    /// Updates at times strictly before `t`.
    Old,
}

impl Version {
    /// Returns `true` if a prefix at `prefix` sees an update at `update`.
    fn sees<T: Ord>(self, update: &T, prefix: &T) -> bool {
        match self {
            Version::New => update.cmp(prefix).is_le(),
            Version::Old => update.cmp(prefix).is_lt(),
        }
    }
}

// =============================================================================
// LookupClock
// =============================================================================

/// A clock a lookup can run under. Its `Ord` extends its timely order:
/// a time before another in timely's order is before it in `Ord` too.
pub trait LookupClock: Timestamp + Lattice + Ord + sealed::LookupClock {
    /// Whether timely's order is `Ord` itself. Then a visible update's
    /// time joins a prefix's time to the prefix's own.
    const TOTAL: bool;

    /// Returns a time no later than `self` in timely's order up to which
    /// the arrangement may compact while a prefix at `self` is pending:
    /// compacting to it moves no update across `self` in `Ord`, for either
    /// [`Version`], and keeps every update's join with `self`.
    fn hold(&self) -> Self;
}

/// One time: nothing to hold back.
impl LookupClock for Once {
    const TOTAL: bool = true;

    fn hold(&self) -> Self {}
}

/// Totally ordered: hold at the previous epoch, which every update before
/// `self` reaches and none after it does.
impl LookupClock for Epoch {
    const TOTAL: bool = true;

    fn hold(&self) -> Self {
        self.saturating_sub(1)
    }
}

/// Totally ordered by iteration: hold at the previous iteration.
impl LookupClock for OnceLoop {
    const TOTAL: bool = true;

    fn hold(&self) -> Self {
        OnceLoop::new((), self.inner.saturating_sub(1))
    }
}

/// Totally ordered epoch first: hold at the start of the previous epoch.
/// An update before `self` advances at most to a time still before it,
/// and one at or after it is already past the hold.
impl LookupClock for LexLoop {
    const TOTAL: bool = true;

    fn hold(&self) -> Self {
        let epoch = <LexLoop as Refines<Epoch>>::to_outer(*self);
        <LexLoop as Refines<Epoch>>::to_inner(epoch.saturating_sub(1))
    }
}

/// Ordered only when both coordinates agree, extended epoch first: hold
/// at iteration 0 of the previous epoch. Compacting raises only the epoch
/// of an update from an earlier epoch, which stays earlier in `Ord`, and
/// never past `self`'s epoch, so its join with `self` is unchanged.
impl LookupClock for EpochLoop {
    const TOTAL: bool = false;

    fn hold(&self) -> Self {
        EpochLoop::new(self.outer.saturating_sub(1), 0)
    }
}

mod sealed {
    pub trait LookupClock {}

    impl LookupClock for crate::time::Once {}
    impl LookupClock for crate::time::Epoch {}
    impl LookupClock for crate::time::OnceLoop {}
    impl LookupClock for crate::time::LexLoop {}
    impl LookupClock for crate::time::EpochLoop {}
}

// =============================================================================
// Paths
// =============================================================================

/// The rows of a lookup path: each row with the time it lands at, under
/// the time of the change that drives it.
pub type PathRows<'scope, T, D, R> = VecCollection<'scope, T, (D, T), R>;

/// Starts a lookup path: pairs each row with its own time as the time it
/// lands at, which the lookups along the path move later and
/// [`flowlog_land`] finally applies.
///
/// Along a path every row keeps the time of the change that drives it as
/// its timely time, since that is what decides which updates each lookup
/// sees, and carries in its data the time its matches so far land at. On
/// a [`LookupClock::TOTAL`] clock the two never differ.
pub fn flowlog_path<'scope, T, D, R>(
    rows: VecCollection<'scope, T, D, R>,
    name: &str,
) -> PathRows<'scope, T, D, R>
where
    T: LookupClock,
    D: Data,
    R: Semigroup + 'static,
{
    flowlog_map(rows, name, |row, time: T, weight| {
        std::iter::once(((row, time.clone()), time, weight))
    })
}

/// Ends a lookup path: moves each row to the time it lands at, never
/// earlier than the time it carries.
pub fn flowlog_land<'scope, T, D, R>(
    rows: PathRows<'scope, T, D, R>,
    name: &str,
) -> VecCollection<'scope, T, D, R>
where
    T: LookupClock,
    D: Data,
    R: Semigroup + 'static,
{
    flowlog_map(rows, name, |(row, lands), _, weight| {
        std::iter::once((row, lands, weight))
    })
}

// =============================================================================
// Lookups
// =============================================================================

/// For each path row `d` driven by a change at time `t`, landing at `s`,
/// with weight `r`, seeks `key(d)` in `arranged` and, for each value there
/// with updates `version` lets `t` see, emits every item of `logic(d,
/// value)` with weight `r * w`, landing at each time `s'` where the visible
/// updates at times joining `s` to `s'` weigh `w` in total, when `w` is not
/// zero. On a [`LookupClock::TOTAL`] clock that is one time, `s`.
///
/// Prefixes are exchanged by `key`'s hash, as the arrangement's keys are,
/// and wait until no update the arrangement may still receive is visible
/// to them.
///
/// This is one term of a join, not a join: an update that reaches
/// `arranged` and is not visible to a row is never matched with it. It
/// equals the join of the two only when `arranged` is complete before any
/// row arrives, as a static relation is inside a batch loop. When both
/// sides change, the join is the sum of one path per side, each driven by
/// that side's changes: `Version::New` on one and `Version::Old` on the
/// other.
pub fn flowlog_lookup<'scope, Tr, D, K, R, I, F, L>(
    prefixes: PathRows<'scope, Tr::Time, D, R>,
    arranged: Arranged<'scope, Tr>,
    name: &str,
    version: Version,
    key: F,
    mut logic: L,
) -> PathRows<'scope, Tr::Time, I::Item, <R as Multiply<BatchDiff<Tr>>>::Output>
where
    Tr: TraceReader<Batch: Navigable, Time: LookupClock> + Clone + 'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: Hashable + Ord + 'static,
    D: ExchangeData,
    R: ExchangeData + Semigroup + Multiply<BatchDiff<Tr>, Output: Semigroup + 'static>,
    I: IntoIterator<Item: Data>,
    F: Fn(&D) -> K + Clone + 'static,
    L: FnMut(&D, BatchVal<'_, Tr>) -> I + 'static,
{
    lookup_core(
        prefixes,
        arranged,
        name,
        version,
        key,
        move |prefix, _lands, weight, value, matches, output| {
            match matches {
                [(lands, visible)] => {
                    let weight = weight.clone().multiply(visible);
                    for item in logic(prefix, value) {
                        output.push((item, lands.clone(), weight.clone()));
                    }
                }
                _ => {
                    let items: Vec<_> = logic(prefix, value).into_iter().collect();
                    for (lands, visible) in matches {
                        let weight = weight.clone().multiply(visible);
                        for item in &items {
                            output.push((item.clone(), lands.clone(), weight.clone()));
                        }
                    }
                }
            }
            true
        },
    )
}

/// Emits each path row `d` with its own weight when `key(d)` has in
/// `arranged` a value whose updates `version` lets the row's change see do
/// not sum to zero, landing at the earliest times such a match lands;
/// drops the row otherwise. On a [`LookupClock::TOTAL`] clock that is
/// once, where the row already lands.
///
/// A semijoin that reads the arrangement a later lookup needs anyway, so
/// it costs no arrangement of its own. Presence weights only: a present
/// row stays present, so landing it at a time covers every later one,
/// while a signed count would need the key's membership tracked over time.
/// Rows are exchanged and wait, and see `arranged` only partly, as in
/// [`flowlog_lookup`].
pub fn flowlog_lookup_exists<'scope, Tr, D, K, R, F>(
    prefixes: PathRows<'scope, Tr::Time, D, R>,
    arranged: Arranged<'scope, Tr>,
    name: &str,
    version: Version,
    key: F,
) -> PathRows<'scope, Tr::Time, D, R>
where
    Tr: TraceReader<Batch: Navigable, Time: LookupClock> + Clone + 'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: Hashable + Ord + 'static,
    D: ExchangeData,
    R: Presence,
    F: Fn(&D) -> K + Clone + 'static,
{
    lookup_core(
        prefixes,
        arranged,
        name,
        version,
        key,
        |prefix, lands, weight, _value, matches, output| {
            for (at, _) in matches {
                output.push((prefix.clone(), at.clone(), *weight));
            }
            // The earliest a match can land is where the row lands already,
            // and a landing there covers every later one.
            !matches.iter().any(|(at, _)| at == lands)
        },
    )
}

/// Pending path rows, by the capability that holds their time.
/// A list rather than a map: a capability is not a sound key, and only a
/// few times are ever pending at once.
type Stash<D, T, R> = Vec<(Capability<T>, Vec<((D, T), T, R)>)>;

/// The operator behind both lookups: for each path row whose key holds a
/// value with updates visible to the row's change, calls `emit` with the
/// row, where it lands, its weight, the value, and the value's matches:
/// each time a match
/// lands at with the visible weight landing there, nonzero, sorted by
/// time. `emit` pushes `(output, lands, weight)` and returns whether to go
/// on to the key's next value; each output keeps the row's timely time.
fn lookup_core<'scope, Tr, D, K, R, O, RO, F, E>(
    prefixes: PathRows<'scope, Tr::Time, D, R>,
    arranged: Arranged<'scope, Tr>,
    name: &str,
    version: Version,
    key: F,
    mut emit: E,
) -> PathRows<'scope, Tr::Time, O, RO>
where
    Tr: TraceReader<Batch: Navigable, Time: LookupClock> + Clone + 'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: Hashable + Ord + 'static,
    D: ExchangeData,
    R: ExchangeData + Semigroup,
    O: Data,
    RO: Semigroup + 'static,
    F: Fn(&D) -> K + Clone + 'static,
    E: FnMut(
            &D,
            &Tr::Time,
            &R,
            BatchVal<'_, Tr>,
            &[(Tr::Time, BatchDiff<Tr>)],
            &mut Vec<(O, Tr::Time, RO)>,
        ) -> bool
        + 'static,
{
    let Arranged { stream, mut trace } = arranged;
    // Only logical compaction decides what a lookup can tell apart; merging
    // batches physically never changes an answer.
    trace.set_physical_compaction(Antichain::new().borrow());
    let mut trace = Some(trace);
    let mut stash: Stash<D, Tr::Time, R> = Vec::new();

    let route = key.clone();
    let exchange = Exchange::new(move |((prefix, _), ..): &((D, Tr::Time), Tr::Time, R)| {
        route(prefix).hashed().into()
    });

    prefixes
        .inner
        .binary_frontier(stream, exchange, Pipeline, name, move |_, _| {
            let mut produced = Vec::new();
            let mut matches: Vec<(Tr::Time, BatchDiff<Tr>)> = Vec::new();
            move |(input, input_frontier), (batches, batch_frontier), output| {
                input.for_each(|capability, data| {
                    match stash
                        .iter_mut()
                        .find(|(held, _)| held.time() == capability.time())
                    {
                        Some((_, pending)) => pending.append(data),
                        None => stash.push((capability.retain(0), std::mem::take(data))),
                    }
                });
                // The batches are read through the trace; their stream only
                // tells which updates may still arrive.
                batches.for_each(|_, _| {});

                // A row is ready once no update still to come is visible to
                // its change. Readiness only falls as `Ord` rises, and every
                // time under a capability is at or after it, so a capability
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
                        pending.sort_by_cached_key(|((prefix, _), ..)| key(prefix));
                        let (mut cursor, storage) = trace.cursor();
                        let mut seek = <BatchCursor<Tr> as Cursor>::KeyContainer::with_capacity(1);
                        let mut waiting = Vec::new();
                        let mut session = output.session(capability);
                        for ((prefix, lands), time, weight) in pending.drain(..) {
                            if !ready(&time) {
                                waiting.push(((prefix, lands), time, weight));
                                continue;
                            }
                            seek.clear();
                            seek.push_own(&key(&prefix));
                            cursor.seek_key(&storage, seek.index(0));
                            if cursor.get_key(&storage) != Some(seek.index(0)) {
                                continue;
                            }
                            while let Some(value) = cursor.get_val(&storage) {
                                matches.clear();
                                gather(&mut cursor, &storage, version, &time, &lands, &mut matches);
                                let go_on = matches.is_empty()
                                    || emit(
                                        &prefix,
                                        &lands,
                                        &weight,
                                        value,
                                        &matches,
                                        &mut produced,
                                    );
                                if !go_on {
                                    break;
                                }
                                cursor.step_val(&storage);
                            }
                            cursor.rewind_vals(&storage);
                            session.give_iterator(
                                produced
                                    .drain(..)
                                    .map(|(out, at, w)| ((out, at), time.clone(), w)),
                            );
                        }
                        for row in waiting {
                            match deferred.iter_mut().find(|(held, _)| *held.time() == row.1) {
                                Some((_, rows)) => rows.push(row),
                                None => deferred.push((capability.delayed(&row.1), vec![row])),
                            }
                        }
                    }
                }
                stash.retain(|(_, pending)| !pending.is_empty());
                for (capability, rows) in deferred {
                    match stash
                        .iter_mut()
                        .find(|(held, _)| held.time() == capability.time())
                    {
                        Some((_, pending)) => pending.extend(rows),
                        None => stash.push((capability, rows)),
                    }
                }

                // Hold compaction where every pending or future row's change
                // still tells visible updates from hidden ones.
                let mut bound = Antichain::new();
                for time in input_frontier.frontier().iter() {
                    bound.insert(time.hold());
                }
                for (capability, _) in &stash {
                    bound.insert(capability.time().hold());
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

/// Collects into `matches` the updates of the cursor's current value that
/// `version` lets a change at `time` see, as the times a row landing at
/// `lands` lands at with them, with their summed nonzero weight, sorted by
/// time.
fn gather<C: Cursor<Time: LookupClock>>(
    cursor: &mut C,
    storage: &C::Storage,
    version: Version,
    time: &C::Time,
    lands: &C::Time,
    matches: &mut Vec<(C::Time, C::Diff)>,
) {
    if C::Time::TOTAL {
        // Every visible update is no later than the change, so the row
        // keeps landing where it does: one running sum.
        let mut visible: Option<C::Diff> = None;
        cursor.map_times(storage, |update, diff| {
            if version.sees(&C::owned_time(update), time) {
                let diff = C::owned_diff(diff);
                match visible.as_mut() {
                    Some(sum) => sum.plus_equals(&diff),
                    None => visible = Some(diff),
                }
            }
        });
        if let Some(visible) = visible.filter(|sum| !sum.is_zero()) {
            matches.push((lands.clone(), visible));
        }
    } else {
        cursor.map_times(storage, |update, diff| {
            let update = C::owned_time(update);
            if version.sees(&update, time) {
                matches.push((update.join(lands), C::owned_diff(diff)));
            }
        });
        consolidate(matches);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::rc::Rc;

    use differential_dataflow::input::Input;
    use differential_dataflow::operators::iterate::Variable;
    use rstest::rstest;
    use timely::order::Product;
    use timely::progress::PathSummary;

    use super::*;
    use crate::diff;
    use crate::operators::flowlog_arrange;
    use crate::operators::flowlog_dedup;
    use crate::operators::flowlog_join;
    use crate::operators::flowlog_land;
    use crate::operators::flowlog_path;

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
            flowlog_land(
                flowlog_lookup(
                    flowlog_path(l, "Path"),
                    flowlog_arrange(r, "Right"),
                    "Lookup",
                    Version::New,
                    |&(_, y)| y,
                    |&(x, _), &z| Some((x, z)),
                ),
                "Land",
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
            flowlog_land(
                flowlog_lookup(
                    flowlog_path(l, "Path"),
                    flowlog_arrange(r, "Right"),
                    "Lookup",
                    Version::New,
                    |&(_, y)| y,
                    |&(x, _), &z| (z > x).then_some((x, z)),
                ),
                "Land",
            )
        });
        assert_eq!(rows, BTreeSet::from([(1, 3), (1, 7), (5, 7)]));
    }

    /// A prefix whose key has values passes once however many there are;
    /// one whose key has none is dropped.
    #[test]
    fn an_existence_lookup_passes_each_matching_prefix_once() {
        let passed = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut l, mut r) = worker.dataflow::<Once, _, _>(|scope| {
                let (l, lc) = scope.new_collection::<Edge, diff::Static>();
                let (r, rc) = scope.new_collection::<Edge, diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_land(
                    flowlog_lookup_exists(
                        flowlog_path(lc, "Path"),
                        flowlog_arrange(rc, "Right"),
                        "Exists",
                        Version::New,
                        |&(_, y)| y,
                    ),
                    "Land",
                )
                .inspect(move |(row, ..)| seen.borrow_mut().push(*row));
                (l, r)
            });
            for row in [(1, 10), (2, 10), (3, 30)] {
                l.update(row, diff::Static);
            }
            for row in [(10, 100), (10, 101), (10, 102)] {
                r.update(row, diff::Static);
            }
            l.close();
            r.close();
            while worker.step() {}
            let mut seen = seen.take();
            seen.sort_unstable();
            seen
        });
        assert_eq!(passed, vec![(1, 10), (2, 10)]);
    }

    /// Deterministic pseudo-random edges among `nodes` nodes.
    fn edges(nodes: u64, count: usize, salt: u64) -> Vec<Edge> {
        let mut state = salt;
        let mut next = move || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % nodes
        };
        (0..count).map(|_| (next(), next())).collect()
    }

    /// Computes `T(x, z) :- T(x, y), E(y, w), T(w, z)` from seed `S` in a
    /// loop, by binary joins or by one lookup path per occurrence of `T`.
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
                            // The path driven by the first T reads the second
                            // as it stood before this iteration; the path
                            // driven by the second reads the first as it
                            // stands now, so a derivation from two new rows
                            // is made by the second path alone.
                            let first = flowlog_lookup(
                                flowlog_path(t.clone(), "Path"),
                                e_by_y,
                                "E by y",
                                Version::New,
                                |&(_, y)| y,
                                |&(x, _), &w| Some((w, x)),
                            );
                            let first = flowlog_lookup(
                                first,
                                t_by_w,
                                "old T by w",
                                Version::Old,
                                |&(w, _)| w,
                                |&(_, x), &z| Some((x, z)),
                            );
                            let second = flowlog_lookup(
                                flowlog_path(t.clone(), "Path"),
                                e_by_w,
                                "E by w",
                                Version::New,
                                |&(w, _)| w,
                                |&(_, z), &y| Some((y, z)),
                            );
                            let second = flowlog_lookup(
                                second,
                                t_by_y,
                                "new T by y",
                                Version::New,
                                |&(y, _)| y,
                                |&(_, z), &x| Some((x, z)),
                            );
                            flowlog_land(first, "Land").concat(flowlog_land(second, "Land"))
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

    /// Two lookup paths, one reading the other occurrence of the relation
    /// as it stood before the iteration and one as it stands now, derive
    /// exactly the fixpoint the binary joins derive.
    #[test]
    fn lookup_paths_reach_the_fixpoint_binary_joins_reach() {
        let seed = edges(60, 120, 7);
        let extend = edges(60, 120, 13);
        let joined = fixpoint(seed.clone(), extend.clone(), false);
        let looked_up = fixpoint(seed, extend, true);
        assert!(joined.len() > 120, "the fixpoint grows past the seed");
        assert_eq!(looked_up, joined);
    }

    /// `New` sees up to and including the prefix's time and `Old` strictly
    /// before it; outside a loop there is no earlier time.
    // Cases: version, update iteration, prefix iteration, sees.
    #[rstest]
    #[case(Version::New, 2, 2, true)]
    #[case(Version::New, 1, 2, true)]
    #[case(Version::New, 3, 2, false)]
    #[case(Version::Old, 2, 2, false)]
    #[case(Version::Old, 1, 2, true)]
    #[case(Version::Old, 0, 0, false)]
    fn a_version_sees_updates_up_to_or_before_the_prefix(
        #[case] version: Version,
        #[case] update: u16,
        #[case] prefix: u16,
        #[case] expected: bool,
    ) {
        let at = |iteration| OnceLoop::new((), iteration);
        assert_eq!(version.sees(&at(update), &at(prefix)), expected);
    }

    #[test]
    fn outside_a_loop_old_sees_nothing_and_new_sees_everything() {
        assert!(Version::New.sees(&(), &()));
        assert!(!Version::Old.sees(&(), &()));
    }

    /// Every time of a small grid of each clock, as `(epoch, iteration)`
    /// pairs for the clocks that have both.
    fn grid<T>(make: impl Fn(u16, u16) -> T) -> Vec<T> {
        (0..4)
            .flat_map(|e| (0..4).map(move |i| (e, i)))
            .map(|(e, i)| make(e, i))
            .collect()
    }

    fn lex_loop(epoch: u16, iteration: u16) -> LexLoop {
        let mut time = <LexLoop as Refines<Epoch>>::to_inner(u32::from(epoch));
        for _ in 0..iteration {
            time = PathSummary::results_in(&LexLoop::NEXT_ITERATION, &time).unwrap();
        }
        time
    }

    /// `Ord` extends timely's order on every clock, and it is timely's
    /// order itself exactly on the clocks marked total.
    #[test]
    fn ord_extends_the_timely_order_and_total_clocks_are_marked() {
        fn check<T: LookupClock>(times: &[T]) {
            let mut incomparable = false;
            for a in times {
                for b in times {
                    if a.less_equal(b) {
                        assert!(a.cmp(b).is_le(), "{a:?} <= {b:?} in timely but not in Ord");
                    }
                    incomparable |= !a.less_equal(b) && !b.less_equal(a);
                }
            }
            assert_eq!(T::TOTAL, !incomparable);
        }
        check(&grid(|_, _| ()));
        check(&grid(|e, i| u32::from(e) * 4 + u32::from(i)));
        check(&grid(|_, i| OnceLoop::new((), i)));
        check(&grid(lex_loop));
        check(&grid(|e, i| EpochLoop::new(u32::from(e), i)));
    }

    /// Compacting an update to a pending prefix's hold, as differential
    /// does by joining it with the hold, keeps what either version sees of
    /// it and where its match lands, on every clock.
    #[test]
    fn compaction_to_the_hold_changes_no_answer() {
        fn check<T: LookupClock>(times: &[T]) {
            for prefix in times {
                let hold = prefix.hold();
                assert!(hold.less_equal(prefix), "hold {hold:?} after {prefix:?}");
                for update in times {
                    let compacted = update.join(&hold);
                    for version in [Version::New, Version::Old] {
                        assert_eq!(
                            version.sees(&compacted, prefix),
                            version.sees(update, prefix),
                            "{version:?} of {update:?} from {prefix:?}",
                        );
                    }
                    assert_eq!(compacted.join(prefix), update.join(prefix));
                }
            }
        }
        check(&grid(|_, _| ()));
        check(&grid(|e, i| u32::from(e) * 4 + u32::from(i)));
        check(&grid(|_, i| OnceLoop::new((), i)));
        check(&grid(lex_loop));
        check(&grid(|e, i| EpochLoop::new(u32::from(e), i)));
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
        differential_dataflow::consolidation::consolidate_updates(&mut updates);
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
                flowlog_path(l.clone(), "Path"),
                flowlog_arrange(r.clone(), "Right"),
                "Left looks up right",
                Version::New,
                |&(_, y)| y,
                |&(x, _), &z| Some((x, z)),
            );
            let from_right = flowlog_lookup(
                flowlog_path(r, "Path"),
                flowlog_arrange(l.map(|(x, y)| (y, x)), "Left by y"),
                "Right looks up left",
                Version::Old,
                |&(y, _)| y,
                |&(_, z), &x| Some((x, z)),
            );
            flowlog_land(from_left, "Land").concat(flowlog_land(from_right, "Land"))
        });
        assert!(
            joined.iter().any(|(_, _, weight)| *weight < 0),
            "deletes reach the output"
        );
        assert_eq!(looked_up, joined);
    }

    /// Computes the fixpoint of `T(x, z) :- T(x, y), E(y, w), T(w, z)` from
    /// seed `S` under signed changes by epoch, in a mutable loop, by binary
    /// joins or by one lookup path per occurrence of `T`; returns the
    /// fixpoint's updates by epoch, consolidated.
    fn mutable_fixpoint(seed: Changes, extend: Changes, by_lookup: bool) -> Vec<(Edge, u32, i32)> {
        let mut updates = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut s, mut e) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (s, sc) = scope.new_collection::<Edge, diff::Mutable>();
                let (e, ec) = scope.new_collection::<Edge, diff::Mutable>();
                let e_by_y = flowlog_arrange(ec.clone(), "E by y");
                let ec_outer = ec.clone();
                let e_by_w = flowlog_arrange(ec.map(|(y, w)| (w, y)), "E by w");
                let seen = Rc::clone(&seen);
                scope
                    .iterative::<u16, _, _>(|inner| {
                        let (variable, t) = Variable::new(inner, Product::new(0, 1));
                        // Named so the arrangements below resolve before `set`.
                        let t: VecCollection<'_, EpochLoop, Edge, diff::Mutable> = t;
                        let e_by_y = e_by_y.enter(inner);
                        let e_by_w = e_by_w.enter(inner);
                        let ec_inner = ec_outer.enter(inner);
                        let t_by_w = flowlog_arrange(t.clone(), "T by w");
                        let t_by_y = flowlog_arrange(t.clone().map(|(x, y)| (y, x)), "T by y");
                        let derived = if by_lookup {
                            // E changes across epochs too, so it drives a
                            // path of its own. Each path reads the atoms
                            // written before its own as of now and those
                            // after it as of before.
                            let from_e = flowlog_lookup(
                                flowlog_path(ec_inner.clone(), "Path"),
                                t_by_y.clone(),
                                "new T by y",
                                Version::New,
                                |&(y, _)| y,
                                |&(_, w), &x| Some((w, x)),
                            );
                            let from_e = flowlog_lookup(
                                from_e,
                                t_by_w.clone(),
                                "old T by w",
                                Version::Old,
                                |&(w, _)| w,
                                |&(_, x), &z| Some((x, z)),
                            );
                            let first = flowlog_lookup(
                                flowlog_path(t.clone(), "Path"),
                                e_by_y,
                                "old E by y",
                                Version::Old,
                                |&(_, y)| y,
                                |&(x, _), &w| Some((w, x)),
                            );
                            let first = flowlog_lookup(
                                first,
                                t_by_w,
                                "old T by w",
                                Version::Old,
                                |&(w, _)| w,
                                |&(_, x), &z| Some((x, z)),
                            );
                            let second = flowlog_lookup(
                                flowlog_path(t.clone(), "Path"),
                                e_by_w,
                                "E by w",
                                Version::New,
                                |&(w, _)| w,
                                |&(_, z), &y| Some((y, z)),
                            );
                            let second = flowlog_lookup(
                                second,
                                t_by_y,
                                "new T by y",
                                Version::New,
                                |&(y, _)| y,
                                |&(_, z), &x| Some((x, z)),
                            );
                            flowlog_land(first, "Land")
                                .concat(flowlog_land(second, "Land"))
                                .concat(flowlog_land(from_e, "Land"))
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
                    .inspect(move |update| seen.borrow_mut().push(*update));
                (s, e)
            });
            let last = seed
                .iter()
                .chain(&extend)
                .map(|(_, e, _)| *e)
                .max()
                .unwrap_or(0);
            for epoch in 0..=last {
                for (row, _, weight) in seed.iter().filter(|(_, e, _)| *e == epoch) {
                    s.update(*row, *weight);
                }
                for (row, _, weight) in extend.iter().filter(|(_, e, _)| *e == epoch) {
                    e.update(*row, *weight);
                }
                s.advance_to(epoch + 1);
                e.advance_to(epoch + 1);
            }
            s.close();
            e.close();
            while worker.step() {}
            seen.take()
        });
        differential_dataflow::consolidation::consolidate_updates(&mut updates);
        updates
    }

    /// In a mutable loop, whose times are only partly ordered, one lookup
    /// path per changing atom follows the binary joins epoch by epoch,
    /// deletions included: rows that stop being derivable are retracted.
    /// Unlike a batch loop, `E` needs its own path: a change to it in a
    /// later epoch meets rows `T` derived in earlier ones.
    #[test]
    fn lookup_paths_follow_binary_joins_in_a_mutable_loop() {
        let mut seed: Changes = edges(30, 40, 7)
            .into_iter()
            .map(|row| (row, 0, 1))
            .collect();
        let mut extend: Changes = edges(30, 40, 13)
            .into_iter()
            .map(|row| (row, 0, 1))
            .collect();
        // Epoch 1 deletes some of each input and adds new rows; epoch 2
        // deletes more.
        let deleted: Vec<_> = seed
            .iter()
            .step_by(3)
            .map(|(row, ..)| (*row, 1, -1))
            .collect();
        seed.extend(deleted);
        let removed: Vec<_> = extend
            .iter()
            .step_by(4)
            .map(|(row, ..)| (*row, 1, -1))
            .collect();
        extend.extend(removed);
        extend.extend(edges(30, 10, 21).into_iter().map(|row| (row, 1, 1)));
        let removed: Vec<_> = extend
            .iter()
            .skip(1)
            .step_by(5)
            .filter(|(_, e, w)| *e == 0 && *w > 0)
            .map(|(row, ..)| (*row, 2, -1))
            .collect();
        extend.extend(removed);
        let joined = mutable_fixpoint(seed.clone(), extend.clone(), false);
        let looked_up = mutable_fixpoint(seed, extend, true);
        assert!(
            joined
                .iter()
                .any(|(_, epoch, weight)| *epoch > 0 && *weight < 0),
            "rows are retracted"
        );
        assert_eq!(looked_up, joined);
    }
}
