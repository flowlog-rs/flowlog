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
//! - [`flowlog_lookup_split`] does the same for rows already split into
//!   their key and the rest.
//! - [`flowlog_lookup_exists`] emits the prefix itself when its key has a
//!   value.
//!
//! One lookup is one term of a join. [`Version`] chooses which updates of
//! the arrangement a prefix sees, so that the lookups driven by each side
//! of a join add up to the join once each. Visibility is decided by the
//! clock's `Ord`, which every [`LookupClock`] extends its timely order to,
//! and a match lands at the join of the two times, so one operator serves
//! every clock: a batch loop and a mutable loop of an incremental engine
//! alike.
//!
//! The operator is differential-dogs3's cursor `half_join`, `half_join2`
//! before dogs3 0.26. This module only shapes each row as the half join
//! takes it, `(key, row, lands)`, turns a [`Version`] into its strictness
//! and a clock's [`LookupClock::hold`] into its compaction frontier, and
//! applies `logic` to its matches.
//!
//! The half join answers ready rows in key order and yields once it has
//! shipped a million output rows, or as many as `FLOWLOG_LOOKUP_FUEL` sets.
//! It does not expose the rows it holds, so [`lookup_rows_held_peak`]
//! reports zeros.

use std::sync::OnceLock;

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
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
use differential_dogs3::operators::half_join::cursors::half_join_internal_unsafe;
use timely::container::CapacityContainerBuilder;
use timely::container::PushInto;
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
    /// Returns `true` if a prefix sees only updates strictly before its
    /// time: the half join's `strict` argument.
    fn strict(self) -> bool {
        match self {
            Version::New => false,
            Version::Old => true,
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
    K: Hashable + ExchangeData,
    D: ExchangeData,
    R: ExchangeData + Semigroup + Multiply<BatchDiff<Tr>, Output: Semigroup + 'static>,
    I: IntoIterator<Item: Data>,
    F: Fn(&D) -> K + 'static,
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
    K: Hashable + ExchangeData,
    D: ExchangeData,
    R: ExchangeData + Semigroup + Multiply<BatchDiff<Tr>, Output: Semigroup + 'static>,
    I: IntoIterator<Item: Data>,
    F: Fn(&D) -> K + 'static,
    L: FnMut(&D, BatchVal<'_, Tr>) -> I + 'static,
{
    lookup_rows(
        keyed(prefixes, name, key),
        arranged,
        version,
        tuning,
        move |_, row, value| logic(row, value),
    )
}

/// [`flowlog_lookup`] for rows already split into their key and the rest,
/// `(key, rest)`, as a half join takes them: the key is neither built
/// again nor carried twice, on its own and in the row, and `logic` gets it
/// back beside the rest.
pub fn flowlog_lookup_split<'scope, Tr, K, V, R, I, L>(
    prefixes: PathRows<'scope, Tr::Time, (K, V), R>,
    arranged: Arranged<'scope, Tr>,
    name: &str,
    version: Version,
    logic: L,
) -> PathRows<'scope, Tr::Time, I::Item, <R as Multiply<BatchDiff<Tr>>>::Output>
where
    Tr: TraceReader<Batch: Navigable, Time: LookupClock> + Clone + 'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Semigroup + Multiply<BatchDiff<Tr>, Output: Semigroup + 'static>,
    I: IntoIterator<Item: Data>,
    L: FnMut(&K, &V, BatchVal<'_, Tr>) -> I + 'static,
{
    let rows = flowlog_map(prefixes, name, |((key, rest), lands), time, weight| {
        std::iter::once(((key, rest, lands), time, weight))
    });
    lookup_rows(rows, arranged, version, Tuning::from_env(), logic)
}

/// Emits each path row `d` with its own weight when `key(d)` has in
/// `arranged` a value whose updates `version` lets the row's change see do
/// not sum to zero, landing at each time such a match lands, once per
/// time; drops the row otherwise. On a [`LookupClock::TOTAL`] clock that is
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
    K: Hashable + ExchangeData,
    D: ExchangeData,
    R: Presence,
    F: Fn(&D) -> K + 'static,
{
    // The half join hands over a row's values one after another and
    // cannot stop at the first that matches, so a row passes at each time
    // it lands at once: when the first value lands it there.
    let mut current: Option<(D, Tr::Time)> = None;
    let mut landed: Vec<Tr::Time> = Vec::new();
    half_join(
        keyed(prefixes, name, key),
        arranged,
        version,
        Tuning::from_env(),
        move |output, _, row, _, time, weight, matches| {
            if matches.is_empty() {
                return;
            }
            if current
                .as_ref()
                .is_none_or(|(seen, at)| seen != row || at != time)
            {
                current = Some((row.clone(), time.clone()));
                landed.clear();
            }
            for (lands, _) in matches {
                if !landed.contains(lands) {
                    output.push_into(((row.clone(), lands.clone()), time.clone(), *weight));
                    landed.push(lands.clone());
                }
            }
        },
    )
}

/// Pairs each path row with its key, as a half join takes its rows:
/// `(key, row, lands)`.
fn keyed<'scope, T, D, K, R, F>(
    prefixes: PathRows<'scope, T, D, R>,
    name: &str,
    key: F,
) -> VecCollection<'scope, T, (K, D, T), R>
where
    T: LookupClock,
    D: Data,
    K: Clone + 'static,
    R: Clone + 'static,
    F: Fn(&D) -> K + 'static,
{
    flowlog_map(prefixes, name, move |(row, lands), time, weight| {
        std::iter::once(((key(&row), row, lands), time, weight))
    })
}

/// Half-joins `(key, row, lands)` rows with `arranged` and emits every
/// item of `logic(key, row, value)` for each value of the key, once for
/// each time the row lands at with it, weighted by the row's weight times
/// the visible weight landing there.
fn lookup_rows<'scope, Tr, K, V, R, I, L>(
    rows: VecCollection<'scope, Tr::Time, (K, V, Tr::Time), R>,
    arranged: Arranged<'scope, Tr>,
    version: Version,
    tuning: Tuning,
    mut logic: L,
) -> PathRows<'scope, Tr::Time, I::Item, <R as Multiply<BatchDiff<Tr>>>::Output>
where
    Tr: TraceReader<Batch: Navigable, Time: LookupClock> + Clone + 'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Semigroup + Multiply<BatchDiff<Tr>, Output: Semigroup + 'static>,
    I: IntoIterator<Item: Data>,
    L: FnMut(&K, &V, BatchVal<'_, Tr>) -> I + 'static,
{
    half_join(
        rows,
        arranged,
        version,
        tuning,
        move |output, key, row, value, time, weight, matches| match matches {
            [] => {}
            // One landing time, as on every totally ordered clock: the
            // items go out as `logic` yields them, with no copy kept.
            [(lands, visible)] => {
                let weight = weight.clone().multiply(visible);
                for item in logic(key, row, value) {
                    output.push_into(((item, lands.clone()), time.clone(), weight.clone()));
                }
            }
            _ => {
                let items: Vec<_> = logic(key, row, value).into_iter().collect();
                for (lands, visible) in matches {
                    let weight = weight.clone().multiply(visible);
                    for item in &items {
                        output.push_into((
                            (item.clone(), lands.clone()),
                            time.clone(),
                            weight.clone(),
                        ));
                    }
                }
            }
        },
    )
}

/// The path rows a half join emits, gathered into vectors.
type Output<O, T, R> = CapacityContainerBuilder<Vec<((O, T), T, R)>>;

/// Runs differential-dogs3's cursor half join over `(key, row, lands)`
/// rows: `version` sets its strictness, the clock's [`LookupClock::hold`]
/// its compaction frontier, and the tuning's fuel the output rows after
/// which it yields. For each row and each value of its key, `emit` gets
/// the key, the row, the value, the row's time and weight, and the value's
/// matches: each time the row lands at with the visible updates, with
/// their summed weight, nonzero.
fn half_join<'scope, Tr, K, V, R, O, RO, E>(
    rows: VecCollection<'scope, Tr::Time, (K, V, Tr::Time), R>,
    arranged: Arranged<'scope, Tr>,
    version: Version,
    tuning: Tuning,
    mut emit: E,
) -> PathRows<'scope, Tr::Time, O, RO>
where
    Tr: TraceReader<Batch: Navigable, Time: LookupClock> + Clone + 'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Semigroup,
    O: Data,
    RO: Semigroup + 'static,
    E: FnMut(
            &mut Output<O, Tr::Time, RO>,
            &K,
            &V,
            BatchVal<'_, Tr>,
            &Tr::Time,
            &R,
            &[(Tr::Time, BatchDiff<Tr>)],
        ) + 'static,
{
    let fuel = tuning.fuel;
    half_join_internal_unsafe(
        rows,
        arranged,
        |time, frontier| {
            frontier.insert(time.hold());
        },
        version.strict(),
        move |_, shipped| shipped >= fuel,
        // `emit` takes the matches as a slice: it only reads them.
        move |output: &mut Output<O, Tr::Time, RO>, key, row, value, time, weight, matches| {
            emit(output, key, row, value, time, weight, matches);
        },
    )
    .as_collection()
}

// =============================================================================
// Experiment switches
// =============================================================================

/// How a lookup paces its work. An experiment switch, so that one
/// generated binary can be measured several ways.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tuning {
    /// Output rows per activation before the half join yields and
    /// reschedules itself. It counts whole containers, so it may ship up
    /// to a container more.
    fuel: usize,
}

impl Tuning {
    /// The same as differential's join, which ships up to 1_000_000
    /// output records per input before it yields (`join_with_tactic` in
    /// differential-dataflow 0.26's `operators/join.rs`).
    const DEFAULT_FUEL: usize = 1_000_000;

    /// Returns the process's tuning, read from the environment once:
    /// `FLOWLOG_LOOKUP_FUEL` sets the fuel, `0` for no limit. A fuel that
    /// is not a whole number is reported on stderr and replaced by the
    /// default.
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
            Tuning { fuel }
        })
    }
}

impl Default for Tuning {
    fn default() -> Self {
        Tuning {
            fuel: Self::DEFAULT_FUEL,
        }
    }
}

// =============================================================================
// Held rows
// =============================================================================

/// The most path rows lookups have held at once, summed over every lookup
/// and worker in the process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeldRows {
    /// Rows waiting until no update still to come is visible to them.
    pub stashed: u64,
    /// Ready rows queued behind the fuel limit.
    pub ready: u64,
    /// Stashed and ready rows together.
    pub total: u64,
}

/// Returns zeros: differential-dogs3's half join does not expose the rows
/// it holds. Kept so that programs that print the gauge, as the study's
/// probe does, still build.
#[must_use]
pub fn lookup_rows_held_peak() -> HeldRows {
    HeldRows::default()
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::rc::Rc;

    use differential_dataflow::input::Input;
    use differential_dataflow::operators::iterate::Variable;
    use rstest::rstest;
    use timely::dataflow::operators::CapabilitySet;
    use timely::dataflow::operators::generic::operator::source;
    use timely::order::Product;

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

    /// Rows split into their key and the rest find what whole rows find.
    #[test]
    fn a_split_lookup_yields_what_a_lookup_yields() {
        let left = vec![(1, 10), (2, 10), (3, 20), (4, 30)];
        let right = vec![(10, 100), (10, 101), (20, 200)];
        let split = rows_of(left.clone(), right.clone(), |l, r| {
            flowlog_land(
                flowlog_lookup_split(
                    flowlog_path(l.map(|(x, y)| (y, x)), "Path"),
                    flowlog_arrange(r, "Right"),
                    "Lookup",
                    Version::New,
                    |_, &x, &z| Some((x, z)),
                ),
                "Land",
            )
        });
        let whole = rows_of(left, right, |l, r| {
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
        assert_eq!(split, whole);
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

    /// The half join yields and reschedules itself after each container it
    /// ships.
    const FUEL_OF_ONE: Tuning = Tuning { fuel: 1 };

    /// Two lookup paths, one reading the other occurrence of the relation
    /// as it stood before the iteration and one as it stands now, derive
    /// exactly the fixpoint the binary joins derive, however they are tuned
    /// and however many workers share the keys.
    // Cases: tuning, workers.
    #[rstest]
    #[case::default(Tuning::default(), 1)]
    #[case::fuel_of_one(FUEL_OF_ONE, 1)]
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

    /// Every time of a small grid of each clock, as `(epoch, iteration)`
    /// pairs for the clocks that have both.
    fn grid<T>(make: impl Fn(u16, u16) -> T) -> Vec<T> {
        (0..4)
            .flat_map(|e| (0..4).map(move |i| (e, i)))
            .map(|(e, i)| make(e, i))
            .collect()
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
        check(&grid(|e, i| (u32::from(e), i)));
        check(&grid(|e, i| EpochLoop::new(u32::from(e), i)));
    }

    /// Differential compacts an update by joining it with the meet of the
    /// holds a lookup sets, a time at or before the hold of every prefix
    /// pending or still to come, as holds never fall while times rise.
    /// Compacting to any time at or before a prefix's hold keeps where an
    /// update falls against the prefix in `Ord`, all that either version
    /// reads of it, and where its match lands, on every clock.
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
                        assert_eq!(
                            compacted.cmp(prefix),
                            update.cmp(prefix),
                            "{update:?} against {prefix:?} compacted to {bound:?}",
                        );
                        assert_eq!(compacted.join(prefix), update.join(prefix));
                    }
                }
            }
        }
        check(&grid(|_, _| ()));
        check(&grid(|e, i| u32::from(e) * 4 + u32::from(i)));
        check(&grid(|_, i| OnceLoop::new((), i)));
        check(&grid(|e, i| (u32::from(e), i)));
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

    /// A prefix at epoch 1 under `New` sees the values added up to and at
    /// epoch 1; under `Old`, only the one added before it. Neither sees the
    /// value added at epoch 2.
    // Cases: version, emitted.
    #[rstest]
    #[case::new(Version::New, vec![((1, 99), 1, 1), ((1, 100), 1, 1)])]
    #[case::old(Version::Old, vec![((1, 99), 1, 1)])]
    fn new_sees_the_prefix_epoch_and_old_only_epochs_before_it(
        #[case] version: Version,
        #[case] emitted: Vec<(Edge, u32, i32)>,
    ) {
        let left: Changes = vec![((1, 10), 1, 1)];
        let right: Changes = vec![((10, 99), 0, 1), ((10, 100), 1, 1), ((10, 101), 2, 1)];
        let looked_up = signed_updates(left, right, move |l, r| {
            flowlog_land(
                flowlog_lookup(
                    flowlog_path(l, "Path"),
                    flowlog_arrange(r, "Right"),
                    "Lookup",
                    version,
                    |&(_, y)| y,
                    |&(x, _), &z| Some((x, z)),
                ),
                "Land",
            )
        });
        assert_eq!(looked_up, emitted);
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

    /// A message may be stamped with several times, as a batch is when its
    /// arrangement retires several at once. Its rows are still answered
    /// each at its own time: the rows at `(0, 1)` and `(0, 2)` while the
    /// value added at `(1, 0)` is still to come, as they never see it, and
    /// the rows at `(1, 0)` and `(1, 1)` once it has arrived.
    #[test]
    fn rows_stamped_with_several_times_are_answered_at_their_own() {
        let at = EpochLoop::new;
        let (early, all) = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut right, probe) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (right, rc) = scope.new_collection::<Edge, diff::Mutable>();
                let seen = Rc::clone(&seen);
                let probe = scope.iterative::<u16, _, _>(|inner| {
                    // One message stamped with `(0, 1)` and `(1, 0)`, of
                    // which neither is before the other.
                    let rows = source(inner, "Stamped", move |capability, _| {
                        let mut capability = Some(capability);
                        move |output| {
                            if let Some(capability) = capability.take() {
                                let stamp: CapabilitySet<EpochLoop> = [at(0, 1), at(1, 0)]
                                    .iter()
                                    .map(|time| capability.delayed(time))
                                    .collect();
                                let rows: [(Edge, EpochLoop, diff::Mutable); 4] = [
                                    ((1, 10), at(0, 1), 1),
                                    ((2, 10), at(0, 2), 1),
                                    ((3, 10), at(1, 0), 1),
                                    ((4, 10), at(1, 1), 1),
                                ];
                                output.session(&stamp).give_iterator(rows.into_iter());
                            }
                        }
                    });
                    let looked_up = flowlog_lookup(
                        flowlog_path(rows.as_collection(), "Path"),
                        flowlog_arrange(rc.enter(inner), "Right"),
                        "Lookup",
                        Version::New,
                        |&(_, y)| y,
                        |&(x, _), &z| Some((x, z)),
                    );
                    let (probe, _) = flowlog_land(looked_up, "Land")
                        .inspect(move |update| seen.borrow_mut().push(*update))
                        .probe();
                    probe
                });
                (right, probe)
            });
            right.update((10, 100), 1);
            right.advance_to(1);
            right.flush();
            worker.step_while(|| probe.less_equal(&at(0, 2)));
            let early = seen.borrow().clone();
            right.update((10, 101), 1);
            right.close();
            while worker.step() {}
            (early, seen.take())
        });
        let sorted = |mut updates: Vec<_>| {
            updates.sort_unstable();
            updates
        };
        let early = sorted(early);
        assert_eq!(
            early,
            vec![((1, 100), at(0, 1), 1), ((2, 100), at(0, 2), 1)]
        );
        let late = vec![
            ((3, 100), at(1, 0), 1),
            ((3, 101), at(1, 0), 1),
            ((4, 100), at(1, 1), 1),
            ((4, 101), at(1, 1), 1),
        ];
        assert_eq!(sorted(all), [early, late].concat());
    }
}
