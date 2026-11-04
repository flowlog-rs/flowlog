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
//!
//! Ready rows are answered in key order, a bounded amount of work per
//! activation, as differential's join paces itself. Rows equal but for
//! weight are merged first, and the rows of one key share one read of its
//! values. `FLOWLOG_LOOKUP_*` environment variables change the pace or
//! turn the rest off, for experiments that measure what each is worth.
//! Rows a lookup holds sit in no arrangement, so differential's logging
//! never counts them; [`lookup_rows_held_peak`] does.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::OnceLock;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::consolidation::consolidate;
use differential_dataflow::consolidation::consolidate_updates;
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
use timely::scheduling::Activator;

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
    logic: L,
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
    lookup_tuned(
        prefixes,
        arranged,
        name,
        version,
        key,
        logic,
        Tuning::from_env(),
    )
}

/// [`flowlog_lookup`] under the given tuning instead of the environment's.
fn lookup_tuned<'scope, Tr, D, K, R, I, F, L>(
    prefixes: PathRows<'scope, Tr::Time, D, R>,
    arranged: Arranged<'scope, Tr>,
    name: &str,
    version: Version,
    key: F,
    mut logic: L,
    tuning: Tuning,
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
        tuning,
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
        Tuning::from_env(),
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

/// A path row as a lookup holds it: the row with the time it lands at,
/// under the time of the change that drives it, and its weight.
type Row<D, T, R> = ((D, T), T, R);

/// A capability and the rows it holds, all at or after its time.
type Held<D, T, R> = (Capability<T>, Vec<Row<D, T, R>>);

/// Rows waiting until no update still to come is visible to them, by the
/// capability that holds their time. A list rather than a map: a
/// capability is not a sound key, and only a few times are ever pending at
/// once. It never holds a capability without rows.
struct Stash<D, T: Timestamp, R> {
    pending: Vec<Held<D, T, R>>,
}

impl<D, T: Timestamp, R> Default for Stash<D, T, R> {
    fn default() -> Self {
        Stash {
            pending: Vec::new(),
        }
    }
}

impl<D, T: Timestamp, R> Stash<D, T, R> {
    /// Holds `rows` under `capability`, or under the capability already
    /// held at its time.
    fn add(&mut self, capability: Capability<T>, mut rows: Vec<Row<D, T, R>>) {
        match self.rows_at(capability.time()) {
            Some(held) => held.append(&mut rows),
            None if rows.is_empty() => {}
            None => self.pending.push((capability, rows)),
        }
    }

    /// Holds `row`, which came under `capability` but is not ready yet,
    /// under a capability at the row's own time, so that `capability` can
    /// be released.
    fn defer(&mut self, capability: &Capability<T>, row: Row<D, T, R>) {
        match self.rows_at(&row.1) {
            Some(held) => held.push(row),
            None => self.pending.push((capability.delayed(&row.1), vec![row])),
        }
    }

    fn rows_at(&mut self, time: &T) -> Option<&mut Vec<Row<D, T, R>>> {
        self.pending
            .iter_mut()
            .find(|(capability, _)| capability.time() == time)
            .map(|(_, rows)| rows)
    }

    /// Removes each capability whose time `is_ready` accepts, and returns
    /// it with its rows.
    fn take_ready(&mut self, is_ready: impl Fn(&T) -> bool) -> impl Iterator<Item = Held<D, T, R>> {
        self.pending
            .extract_if(.., move |(capability, _)| is_ready(capability.time()))
    }

    fn times(&self) -> impl Iterator<Item = &T> {
        self.pending.iter().map(|(capability, _)| capability.time())
    }

    /// How many rows wait, under every capability.
    fn rows(&self) -> usize {
        self.pending.iter().map(|(_, rows)| rows.len()).sum()
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// A capability's rows once its time is ready, sorted by key. They are
/// answered front to back, over as many activations as the fuel needs.
struct ReadyRows<D, T: Timestamp, R> {
    capability: Capability<T>,
    rows: Vec<Row<D, T, R>>,
    /// How many rows, from the front, are answered.
    answered: usize,
}

/// The operator behind both lookups: for each path row whose key holds a
/// value with updates visible to the row's change, calls `emit` with the
/// row, where it lands, its weight, the value, and the value's matches:
/// each time a match lands at with the visible weight landing there,
/// nonzero, sorted by time. `emit` pushes `(output, lands, weight)` and
/// returns whether to go on to the key's next value; each output keeps the
/// row's timely time.
///
/// Rows wait in a [`Stash`] until their time is ready, then queue as
/// [`ReadyRows`]. Each activation answers queued rows until the rows
/// probed plus the records emitted reach `tuning.fuel`, so it overshoots
/// by at most one row's records, and reschedules the operator while rows
/// remain queued.
fn lookup_core<'scope, Tr, D, K, R, O, RO, F, E>(
    prefixes: PathRows<'scope, Tr::Time, D, R>,
    arranged: Arranged<'scope, Tr>,
    name: &str,
    version: Version,
    key: F,
    tuning: Tuning,
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
    let mut stash: Stash<D, Tr::Time, R> = Stash::default();
    let mut queue: VecDeque<ReadyRows<D, Tr::Time, R>> = VecDeque::new();

    let route = key.clone();
    let exchange =
        Exchange::new(move |((prefix, _), ..): &Row<D, Tr::Time, R>| route(prefix).hashed().into());

    let scope = prefixes.inner.scope();
    prefixes
        .inner
        .binary_frontier(stream, exchange, Pipeline, name, move |_, info| {
            // Rows still queued when the fuel runs out reschedule the
            // operator, as differential's join does.
            let activator = Activator::new(info.address, scope.activations());
            let mut updates: Vec<(Tr::Time, BatchDiff<Tr>)> = Vec::new();
            let mut matches: Vec<(Tr::Time, BatchDiff<Tr>)> = Vec::new();
            let mut produced = Vec::new();
            let mut gauge = HeldShare::default();
            move |(input, input_frontier), (batches, batch_frontier), output| {
                input.for_each(|capability, data| {
                    stash.add(capability.retain(0), std::mem::take(data));
                });
                // The batches are read through the trace; their stream only
                // tells which updates may still arrive.
                batches.for_each(|_, _| {});

                // A row is ready once no update still to come is visible to
                // its change. Readiness only falls as `Ord` rises, and every
                // time under a capability is at or after it, so a capability
                // that is not ready holds nothing ready.
                let frontier = batch_frontier.frontier();
                let is_ready = |time: &Tr::Time| frontier.iter().all(|f| !version.sees(f, time));

                if let Some(trace) = trace.as_mut() {
                    for (capability, mut rows) in stash.take_ready(is_ready) {
                        sort_by_key(&mut rows, &key, tuning.consolidate);
                        queue.push_back(ReadyRows {
                            capability,
                            rows,
                            answered: 0,
                        });
                    }

                    let mut fuel = tuning.fuel;
                    while let Some(ReadyRows {
                        capability,
                        rows,
                        answered,
                    }) = queue.front_mut()
                    {
                        // A fresh cursor for each capability's rows on each
                        // activation: the rows are sorted by key, so across
                        // them the cursor only moves forward.
                        let (mut cursor, storage) = trace.cursor();
                        let mut seek = <BatchCursor<Tr> as Cursor>::KeyContainer::with_capacity(1);
                        // The key read last. `values` holds its values, each
                        // with the range of its updates in `updates`.
                        let mut loaded: Option<K> = None;
                        let mut values = Vec::new();
                        // Rows not ready yet. A message may carry rows later
                        // than its capability, and holding that capability
                        // for them would stall everything downstream of it,
                        // the arrangement they wait on included, so they go
                        // back to the stash under their own times. That
                        // happens after the loop: inside it measured 2%
                        // slower.
                        let mut waiting = Vec::new();
                        let mut session = output.session(&*capability);
                        while fuel > 0 && *answered < rows.len() {
                            let row = &rows[*answered];
                            *answered += 1;
                            let ((prefix, lands), time, weight) = row;
                            if !is_ready(time) {
                                waiting.push(row.clone());
                                continue;
                            }
                            // A key's rows are adjacent, so grouping seeks the
                            // key and reads its values once for all of them,
                            // as differential's join loads each key once.
                            // Reading every value up front costs an existence
                            // check, which needs only its first match, more
                            // than it uses when its key has one row.
                            let probe = key(prefix);
                            if !tuning.group_by_key || loaded.as_ref() != Some(&probe) {
                                seek.clear();
                                seek.push_own(&probe);
                                values.clear();
                                updates.clear();
                                cursor.seek_key(&storage, seek.index(0));
                                if cursor.get_key(&storage) == Some(seek.index(0)) {
                                    load(&mut cursor, &storage, &mut values, &mut updates);
                                }
                                loaded = Some(probe);
                            }
                            for (value, range) in &values {
                                matches.clear();
                                sift(version, time, lands, &updates[range.clone()], &mut matches);
                                if !matches.is_empty()
                                    && !emit(prefix, lands, weight, *value, &matches, &mut produced)
                                {
                                    break;
                                }
                            }
                            fuel = fuel.saturating_sub(1 + produced.len());
                            session.give_iterator(
                                produced
                                    .drain(..)
                                    .map(|(out, at, w)| ((out, at), time.clone(), w)),
                            );
                        }
                        drop(session);
                        for row in waiting {
                            stash.defer(capability, row);
                        }
                        if *answered < rows.len() {
                            break;
                        }
                        queue.pop_front();
                    }
                }
                if !queue.is_empty() {
                    activator.activate();
                }

                // Hold compaction where every pending or future row's change
                // still tells visible updates from hidden ones.
                let mut hold = Antichain::new();
                let pending = stash
                    .times()
                    .chain(queue.iter().map(|ready| ready.capability.time()));
                for time in input_frontier.frontier().iter().chain(pending) {
                    hold.insert(time.hold());
                }
                if let Some(trace) = trace.as_mut() {
                    trace.set_logical_compaction(hold.borrow());
                }
                if held_rows_counted() {
                    let queued = queue.iter().map(|ready| ready.rows.len()).sum();
                    gauge.set(stash.rows(), queued);
                }
                if input_frontier.is_empty() && stash.is_empty() && queue.is_empty() {
                    trace = None;
                }
            }
        })
        .as_collection()
}

/// Sorts `rows` by key, so that one cursor serves every row moving forward
/// only, and when `consolidate` is set merges rows equal but for weight,
/// dropping those whose weights cancel.
///
/// Merging lets duplicates probe once, as an arrangement's batcher would
/// consolidate them before a join; it is sound because a match's weight is
/// the row's weight times the arrangement's, which distributes over the
/// merged sum.
///
/// A key that owns heap data, such as a string, allocates each time it is
/// computed, so it is computed once per row: the rows are merged first and
/// then stably sorted by their cached keys. Any other key is a copy of
/// plain data, cheaper to recompute in every comparison of one sort in
/// place than to cache.
fn sort_by_key<D: Ord, T: Ord, R: Semigroup, K: Ord>(
    rows: &mut Vec<Row<D, T, R>>,
    key: &impl Fn(&D) -> K,
    consolidate: bool,
) {
    if std::mem::needs_drop::<K>() {
        if consolidate {
            consolidate_updates(rows);
        }
        rows.sort_by_cached_key(|((prefix, _), ..)| key(prefix));
    } else if consolidate {
        rows.sort_unstable_by(|a, b| (key(&a.0.0), &a.0, &a.1).cmp(&(key(&b.0.0), &b.0, &b.1)));
        rows.dedup_by(|later, kept| {
            let equal = later.0 == kept.0 && later.1 == kept.1;
            if equal {
                kept.2.plus_equals(&later.2);
            }
            equal
        });
        rows.retain(|(.., weight)| !weight.is_zero());
    } else {
        rows.sort_unstable_by_key(|((prefix, _), ..)| key(prefix));
    }
}

/// Reads every value of the cursor's current key into `values`, each with
/// the range of its updates, which it appends to `updates` in the cursor's
/// order.
fn load<'a, C: Cursor>(
    cursor: &mut C,
    storage: &'a C::Storage,
    values: &mut Vec<(C::Val<'a>, Range<usize>)>,
    updates: &mut Vec<(C::Time, C::Diff)>,
) {
    while let Some(value) = cursor.get_val(storage) {
        let start = updates.len();
        cursor.map_times(storage, |time, diff| {
            updates.push((C::owned_time(time), C::owned_diff(diff)));
        });
        values.push((value, start..updates.len()));
        cursor.step_val(storage);
    }
}

/// Collects into `matches` the updates of one value, `updates`, that
/// `version` lets a change at `time` see, as the times a row landing at
/// `lands` lands at with them, with their summed nonzero weight, sorted by
/// time.
fn sift<T: LookupClock, R: Semigroup>(
    version: Version,
    time: &T,
    lands: &T,
    updates: &[(T, R)],
    matches: &mut Vec<(T, R)>,
) {
    let visible = updates
        .iter()
        .filter(|(update, _)| version.sees(update, time));
    if T::TOTAL {
        // Every visible update is no later than the change, so the row
        // keeps landing where it does: one running sum.
        let mut sum: Option<R> = None;
        for (_, diff) in visible {
            match sum.as_mut() {
                Some(sum) => sum.plus_equals(diff),
                None => sum = Some(diff.clone()),
            }
        }
        if let Some(sum) = sum.filter(|sum| !sum.is_zero()) {
            matches.push((lands.clone(), sum));
        }
    } else {
        matches.extend(visible.map(|(update, diff)| (update.join(lands), diff.clone())));
        consolidate(matches);
    }
}

// =============================================================================
// Experiment switches
// =============================================================================

/// How a lookup paces its work and reads the arrangement. These are
/// experiment switches, so that one generated binary can be measured
/// several ways; the defaults are the settings measured best.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tuning {
    /// Rows probed plus records emitted per activation before the operator
    /// yields and reschedules itself.
    fuel: usize,
    /// Whether ready rows equal but for weight are merged before they
    /// probe, rather than only sorted by key.
    consolidate: bool,
    /// Whether the rows of a key share one seek and one read of its values,
    /// rather than each row seeking and reading alone.
    group_by_key: bool,
}

impl Tuning {
    /// Matches differential's join, which emits up to 1_000_000 records per
    /// input before it yields (`join_traces` in differential-dataflow
    /// 0.25.1's `operators/join.rs`). Larger values measured as trading
    /// memory for time, with no value better on both.
    const DEFAULT_FUEL: usize = 1_000_000;

    /// Returns the process's tuning, read from the environment once:
    /// `FLOWLOG_LOOKUP_FUEL` sets the fuel, `0` for no limit;
    /// `FLOWLOG_LOOKUP_CONSOLIDATE=0` and `FLOWLOG_LOOKUP_GROUP=0` turn
    /// those off. A fuel that is not a whole number is reported on stderr
    /// and replaced by the default.
    fn from_env() -> Self {
        static TUNING: OnceLock<Tuning> = OnceLock::new();
        *TUNING.get_or_init(|| {
            let fuel = match std::env::var("FLOWLOG_LOOKUP_FUEL") {
                Err(_) => Self::DEFAULT_FUEL,
                Ok(value) => match value.parse::<usize>() {
                    Ok(0) => usize::MAX,
                    Ok(fuel) => fuel,
                    Err(_) => {
                        eprintln!(
                            "warning: FLOWLOG_LOOKUP_FUEL={value:?} is not a \
                             whole number; using {}",
                            Self::DEFAULT_FUEL
                        );
                        Self::DEFAULT_FUEL
                    }
                },
            };
            let off = |name: &str| std::env::var(name).is_ok_and(|value| value == "0");
            Tuning {
                fuel,
                consolidate: !off("FLOWLOG_LOOKUP_CONSOLIDATE"),
                group_by_key: !off("FLOWLOG_LOOKUP_GROUP"),
            }
        })
    }
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning {
            fuel: Self::DEFAULT_FUEL,
            consolidate: true,
            group_by_key: true,
        }
    }
}

// =============================================================================
// Held-row gauge
// =============================================================================

/// The most path rows lookups have held at once, summed over every lookup
/// and worker in the process. Rows held inside a lookup sit in no
/// arrangement, so differential's logging never counts them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeldRows {
    /// Rows waiting until no update still to come is visible to them.
    pub stashed: u64,
    /// Ready rows queued behind the fuel limit. Answered rows count until
    /// the rest of their capability's rows are answered.
    pub ready: u64,
    /// Stashed and ready rows together. The two peak at different moments,
    /// so this can be less than `stashed + ready`.
    pub total: u64,
}

/// Returns the most path rows held inside lookups at once so far. Rows are
/// counted only when the environment sets `FLOWLOG_PROBE=1`; otherwise
/// every count stays zero.
#[must_use]
pub fn lookup_rows_held_peak() -> HeldRows {
    HeldRows {
        stashed: STASHED.peak(),
        ready: READY.peak(),
        total: HELD.peak(),
    }
}

/// Returns `true` if lookups count the rows they hold, which costs each
/// lookup a pass over its pending capabilities per activation.
fn held_rows_counted() -> bool {
    static COUNTED: OnceLock<bool> = OnceLock::new();
    *COUNTED.get_or_init(|| std::env::var("FLOWLOG_PROBE").is_ok_and(|value| value == "1"))
}

static STASHED: Gauge = Gauge::new();
static READY: Gauge = Gauge::new();
static HELD: Gauge = Gauge::new();

/// A count shared by every worker, and the highest it has been.
struct Gauge {
    now: AtomicI64,
    peak: AtomicI64,
}

impl Gauge {
    const fn new() -> Self {
        Gauge {
            now: AtomicI64::new(0),
            peak: AtomicI64::new(0),
        }
    }

    /// Moves the count by `delta` and raises the peak to the new count.
    fn add(&self, delta: i64) {
        // Relaxed suffices: a peak needs only its own atomic's history,
        // which `fetch_add` orders without help from other memory.
        let now = self.now.fetch_add(delta, Ordering::Relaxed) + delta;
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    fn peak(&self) -> u64 {
        // A peak starts at zero and only rises, so it is never negative.
        u64::try_from(self.peak.load(Ordering::Relaxed)).unwrap_or(0)
    }
}

/// One lookup's part of the gauges, kept so that each activation can
/// replace its counts instead of tracking every row that comes and goes.
#[derive(Debug, Default)]
struct HeldShare {
    stashed: i64,
    ready: i64,
}

impl HeldShare {
    /// Replaces this lookup's counts with `stashed` and `ready` rows.
    fn set(&mut self, stashed: usize, ready: usize) {
        let (stashed, ready) = (stashed as i64, ready as i64);
        STASHED.add(stashed - self.stashed);
        READY.add(ready - self.ready);
        HELD.add(stashed + ready - self.stashed - self.ready);
        *self = HeldShare { stashed, ready };
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
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

    /// Deterministic pseudo-random edges among `nodes` nodes, drawn from a
    /// linear congruential generator that starts at `state`.
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
    /// loop on `workers` workers, by binary joins (`None`) or by one lookup
    /// path per occurrence of `T` under the given tuning.
    fn fixpoint(
        seed: Vec<Edge>,
        extend: Vec<Edge>,
        lookups: Option<Tuning>,
        workers: usize,
    ) -> BTreeSet<Edge> {
        let run = timely::execute(timely::Config::process(workers), move |worker| {
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
                        let derived = if let Some(tuning) = lookups {
                            // The path driven by the first T reads the second
                            // as it stood before this iteration; the path
                            // driven by the second reads the first as it
                            // stands now, so a derivation from two new rows
                            // is made by the second path alone.
                            let first = lookup_tuned(
                                flowlog_path(t.clone(), "Path"),
                                e_by_y,
                                "E by y",
                                Version::New,
                                |&(_, y)| y,
                                |&(x, _), &w| Some((w, x)),
                                tuning,
                            );
                            let first = lookup_tuned(
                                first,
                                t_by_w,
                                "old T by w",
                                Version::Old,
                                |&(w, _)| w,
                                |&(_, x), &z| Some((x, z)),
                                tuning,
                            );
                            let second = lookup_tuned(
                                flowlog_path(t.clone(), "Path"),
                                e_by_w,
                                "E by w",
                                Version::New,
                                |&(w, _)| w,
                                |&(_, z), &y| Some((y, z)),
                                tuning,
                            );
                            let second = lookup_tuned(
                                second,
                                t_by_y,
                                "new T by y",
                                Version::New,
                                |&(y, _)| y,
                                |&(_, z), &x| Some((x, z)),
                                tuning,
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
            // Worker 0 feeds every row, so each must travel to the worker
            // that holds its key's values.
            if worker.index() == 0 {
                for row in &seed {
                    s.update(*row, diff::Static);
                }
                for row in &extend {
                    e.update(*row, diff::Static);
                }
            }
            s.close();
            e.close();
            while worker.step() {}
            seen.take()
        });
        run.expect("the workers start")
            .join()
            .into_iter()
            .flat_map(|seen| seen.expect("a worker finishes"))
            .collect()
    }

    /// Every row costs fuel, so a fuel of one yields and reschedules the
    /// operator after each row.
    const FUEL_OF_ONE: Tuning = Tuning {
        fuel: 1,
        consolidate: true,
        group_by_key: true,
    };

    /// Each row probes alone, duplicates included, and yields after it.
    const PER_ROW_FUEL_OF_ONE: Tuning = Tuning {
        fuel: 1,
        consolidate: false,
        group_by_key: false,
    };

    /// Two lookup paths, one reading the other occurrence of the relation
    /// as it stood before the iteration and one as it stands now, derive
    /// exactly the fixpoint the binary joins derive, however they are tuned
    /// and however many workers share the keys.
    // Cases: tuning, workers.
    #[rstest]
    #[case::default(Tuning::default(), 1)]
    #[case::fuel_of_one(FUEL_OF_ONE, 1)]
    #[case::per_row_fuel_of_one(PER_ROW_FUEL_OF_ONE, 1)]
    #[case::four_workers(Tuning::default(), 4)]
    fn lookup_paths_reach_the_fixpoint_binary_joins_reach(
        #[case] tuning: Tuning,
        #[case] workers: usize,
    ) {
        let seed = edges(60, 120, 7);
        let extend = edges(60, 120, 13);
        let joined = fixpoint(seed.clone(), extend.clone(), None, 1);
        let looked_up = fixpoint(seed, extend, Some(tuning), workers);
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

    /// Differential compacts an update by joining it with the meet of the
    /// holds a lookup sets, a time at or before the hold of every prefix
    /// pending or still to come, as holds never fall while times rise.
    /// Compacting to any time at or before a prefix's hold keeps what
    /// either version sees of an update and where its match lands, on
    /// every clock.
    #[test]
    fn compaction_to_the_hold_changes_no_answer() {
        fn check<T: LookupClock>(times: &[T]) {
            for prefix in times {
                let hold = prefix.hold();
                assert!(hold.less_equal(prefix), "hold {hold:?} after {prefix:?}");
                for later in times.iter().filter(|later| prefix.less_equal(later)) {
                    let later_hold = later.hold();
                    assert!(hold.less_equal(&later_hold), "hold falls to {later:?}");
                }
                for bound in times.iter().filter(|bound| bound.less_equal(&hold)) {
                    for update in times {
                        let compacted = update.join(bound);
                        for version in [Version::New, Version::Old] {
                            assert_eq!(
                                version.sees(&compacted, prefix),
                                version.sees(update, prefix),
                                "{version:?} of {update:?} from {prefix:?} to {bound:?}",
                            );
                        }
                        assert_eq!(compacted.join(prefix), update.join(prefix));
                    }
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
    /// as of before it, emits exactly the updates a join emits, however the
    /// lookups are tuned.
    // Cases: tuning.
    #[rstest]
    #[case::default(Tuning::default())]
    #[case::fuel_of_one(FUEL_OF_ONE)]
    #[case::per_row_fuel_of_one(PER_ROW_FUEL_OF_ONE)]
    fn two_lookups_emit_the_updates_a_join_emits_across_epochs(#[case] tuning: Tuning) {
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
        let looked_up = signed_updates(left, right, move |l, r| {
            let from_left = lookup_tuned(
                flowlog_path(l.clone(), "Path"),
                flowlog_arrange(r.clone(), "Right"),
                "Left looks up right",
                Version::New,
                |&(_, y)| y,
                |&(x, _), &z| Some((x, z)),
                tuning,
            );
            let from_right = lookup_tuned(
                flowlog_path(r, "Path"),
                flowlog_arrange(l.map(|(x, y)| (y, x)), "Left by y"),
                "Right looks up left",
                Version::Old,
                |&(y, _)| y,
                |&(_, z), &x| Some((x, z)),
                tuning,
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
    /// joins (`None`) or by one lookup path per changing atom under the
    /// given tuning; returns the fixpoint's updates by epoch, consolidated.
    fn mutable_fixpoint(
        seed: Changes,
        extend: Changes,
        lookups: Option<Tuning>,
    ) -> Vec<(Edge, u32, i32)> {
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
                        let derived = if let Some(tuning) = lookups {
                            // E changes across epochs too, so it drives a
                            // path of its own. Each path reads the atoms
                            // written before its own as of now and those
                            // after it as of before.
                            let from_e = lookup_tuned(
                                flowlog_path(ec_inner.clone(), "Path"),
                                t_by_y.clone(),
                                "new T by y",
                                Version::New,
                                |&(y, _)| y,
                                |&(_, w), &x| Some((w, x)),
                                tuning,
                            );
                            let from_e = lookup_tuned(
                                from_e,
                                t_by_w.clone(),
                                "old T by w",
                                Version::Old,
                                |&(w, _)| w,
                                |&(_, x), &z| Some((x, z)),
                                tuning,
                            );
                            let first = lookup_tuned(
                                flowlog_path(t.clone(), "Path"),
                                e_by_y,
                                "old E by y",
                                Version::Old,
                                |&(_, y)| y,
                                |&(x, _), &w| Some((w, x)),
                                tuning,
                            );
                            let first = lookup_tuned(
                                first,
                                t_by_w,
                                "old T by w",
                                Version::Old,
                                |&(w, _)| w,
                                |&(_, x), &z| Some((x, z)),
                                tuning,
                            );
                            let second = lookup_tuned(
                                flowlog_path(t.clone(), "Path"),
                                e_by_w,
                                "E by w",
                                Version::New,
                                |&(w, _)| w,
                                |&(_, z), &y| Some((y, z)),
                                tuning,
                            );
                            let second = lookup_tuned(
                                second,
                                t_by_y,
                                "new T by y",
                                Version::New,
                                |&(y, _)| y,
                                |&(_, z), &x| Some((x, z)),
                                tuning,
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
    /// later epoch meets rows `T` derived in earlier ones. Tuning changes
    /// none of it.
    // Cases: tuning.
    #[rstest]
    #[case::default(Tuning::default())]
    #[case::fuel_of_one(FUEL_OF_ONE)]
    #[case::per_row_fuel_of_one(PER_ROW_FUEL_OF_ONE)]
    fn lookup_paths_follow_binary_joins_in_a_mutable_loop(#[case] tuning: Tuning) {
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
        let joined = mutable_fixpoint(seed.clone(), extend.clone(), None);
        let looked_up = mutable_fixpoint(seed, extend, Some(tuning));
        assert!(
            joined
                .iter()
                .any(|(_, epoch, weight)| *epoch > 0 && *weight < 0),
            "rows are retracted"
        );
        assert_eq!(looked_up, joined);
    }

    /// Ready rows equal but for weight are merged before they probe:
    /// duplicates emit one update with their summed weight, and rows that
    /// cancel emit nothing.
    #[test]
    fn rows_equal_but_for_weight_probe_once() {
        let emitted = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut l, mut r) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (l, lc) = scope.new_collection::<Edge, diff::Mutable>();
                let (r, rc) = scope.new_collection::<Edge, diff::Mutable>();
                let seen = Rc::clone(&seen);
                let looked_up = lookup_tuned(
                    flowlog_path(lc, "Path"),
                    flowlog_arrange(rc, "Right"),
                    "Lookup",
                    Version::New,
                    |&(_, y)| y,
                    |&(x, _), &z| Some((x, z)),
                    Tuning::default(),
                );
                flowlog_land(looked_up, "Land")
                    .inspect(move |update| seen.borrow_mut().push(*update));
                (l, r)
            });
            // Input sessions send updates as given, without consolidating.
            for (row, weight) in [((1, 10), 1), ((1, 10), 1), ((2, 10), 1), ((2, 10), -1)] {
                l.update(row, weight);
            }
            r.update((10, 100), 1);
            l.close();
            r.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(emitted, vec![((1, 100), 0, 2)]);
    }

    /// Ready rows sort by key, and consolidating merges rows equal but for
    /// weight and drops those that cancel, alike whether the key is plain
    /// data, recomputed in each comparison, or a string, cached per row:
    /// consolidated rows come out by key, row, landing and time.
    // Cases: consolidate.
    #[rstest]
    #[case::consolidated(true)]
    #[case::as_they_came(false)]
    fn rows_sort_alike_by_plain_and_string_keys(#[case] consolidate: bool) {
        type Row = ((Edge, u32), u32, i32);
        let once: Vec<Row> = edges(6, 30, 5)
            .into_iter()
            .zip(0u32..)
            .map(|(edge, n)| ((edge, n % 2), n % 3, 1))
            .collect();
        // Every row twice, so each kept row merges two, and every third
        // retracted twice, so some cancel.
        let retracted = once
            .iter()
            .step_by(3)
            .map(|&(row, time, _)| (row, time, -2));
        let rows: Vec<Row> = once.iter().chain(&once).copied().chain(retracted).collect();
        let by_y = |&(_, y): &Edge| y;
        let mut plain = rows.clone();
        sort_by_key(&mut plain, &by_y, consolidate);
        let mut string = rows.clone();
        sort_by_key(
            &mut string,
            &|edge: &Edge| format!("{:02}", by_y(edge)),
            consolidate,
        );

        if consolidate {
            let mut summed = BTreeMap::new();
            for &((edge, lands), time, weight) in &rows {
                *summed.entry((by_y(&edge), edge, lands, time)).or_insert(0) += weight;
            }
            let distinct = summed.len();
            let expected: Vec<Row> = summed
                .into_iter()
                .filter(|&(_, weight)| weight != 0)
                .map(|((_, edge, lands, time), weight)| ((edge, lands), time, weight))
                .collect();
            assert!(expected.len() < distinct, "some rows cancel");
            assert_eq!(plain, expected);
            assert_eq!(string, expected);
        } else {
            for sorted in [&plain, &string] {
                let keys: Vec<u64> = sorted.iter().map(|((edge, _), ..)| by_y(edge)).collect();
                assert!(keys.is_sorted(), "{keys:?}");
                let mut sorted = sorted.clone();
                let mut rows = rows.clone();
                sorted.sort_unstable();
                rows.sort_unstable();
                assert_eq!(sorted, rows);
            }
        }
    }

    /// A gauge's peak is the highest count reached, kept after the count
    /// falls. Pinned on a gauge of its own, since the process-wide gauges
    /// would see every other test's lookups.
    #[test]
    fn a_gauge_keeps_the_highest_count_reached() {
        let gauge = Gauge::new();
        for delta in [5, -3, 4, -6] {
            gauge.add(delta);
        }
        assert_eq!(gauge.peak(), 6);
    }
}
