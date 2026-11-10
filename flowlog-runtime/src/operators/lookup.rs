//! [`flowlog_lookup`] probes an arrangement with unarranged rows.
//! [`Version`] defines which updates each row sees; [`Pending`] groups
//! rows by time until they can be answered, and [`Reader`] holds the
//! trace history needed by pending and future rows.

use std::collections::VecDeque;

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::consolidation::consolidate;
use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::BatchDiff;
use differential_dataflow::trace::BatchVal;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::CursorList;
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::trace::implementations::containers::BatchContainer;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::InputCapability;
use timely::dataflow::operators::generic::Operator;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use crate::diff;
use crate::diff::Max;
use crate::diff::Weight;
use crate::time::TotalClock;

// =============================================================================
// Version
// =============================================================================

/// Which updates of the arrangement a row sees: [`New`] or [`Old`], a
/// token [`flowlog_lookup`] takes so that the choice is made at compile
/// time.
///
/// A join of two changing collections is the sum of exactly one lookup
/// per side, one reading the other side as of the row's time and the
/// other as of just before it, so that every pair of updates is matched
/// by exactly one of them: the lookup driven by the later update, the
/// first among equals. The pairing needs every two times ordered, so
/// [`Old`] runs only on a clock that orders its times totally; [`New`]
/// runs on any clock.
pub trait Version<T: Timestamp>: Copy + sealed::Version + 'static {
    /// Returns `true` if a row at `row` sees an update at `update`. A row
    /// that sees an update sees every update at or before that one too.
    #[must_use]
    fn sees(self, update: &T, row: &T) -> bool;

    /// Returns a time at or before `row` to which the arrangement may
    /// compact while a row at `row` is pending: compacting there moves no
    /// update across what the row sees. The time never falls as `row`
    /// rises.
    #[must_use]
    fn compaction(self, row: &T) -> T;

    /// Returns `true` if a row at `row` sees no update still to come, the
    /// arrangement's frontier being `frontier`.
    #[must_use]
    fn ready(self, frontier: AntichainRef<'_, T>, row: &T) -> bool {
        // An update still to come is at or after a time of the frontier,
        // so a row that sees the update sees that time too.
        !frontier.iter().any(|time| self.sees(time, row))
    }
}

/// Updates at times up to and including the row's.
#[derive(Debug, Clone, Copy)]
pub struct New;

impl<T: Timestamp> Version<T> for New {
    fn sees(self, update: &T, row: &T) -> bool {
        update.less_equal(row)
    }

    fn compaction(self, row: &T) -> T {
        // Compaction keeps how an update compares with every time at or
        // after the bound, the row's included.
        row.clone()
    }
}

/// Updates at times strictly before the row's.
///
/// A mutable loop's clock orders its times only partly, so `Old` does not
/// run there:
///
/// ```compile_fail,E0277
/// use differential_dataflow::VecCollection;
/// use flowlog_runtime::diff;
/// use flowlog_runtime::operators::Old;
/// use flowlog_runtime::operators::flowlog_arrange;
/// use flowlog_runtime::operators::flowlog_lookup;
/// use flowlog_runtime::time::EpochLoop;
///
/// fn lookup<'scope>(
///     rows: VecCollection<'scope, EpochLoop, (u64, u64), diff::Mutable>,
///     values: VecCollection<'scope, EpochLoop, (u64, u64), diff::Mutable>,
/// ) {
///     let values = flowlog_arrange(values, "Values");
///     flowlog_lookup(rows, values, "Lookup", Old, |_, _, _| None::<u64>);
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Old;

impl<T: TotalClock> Version<T> for Old {
    fn sees(self, update: &T, row: &T) -> bool {
        update.less_than(row)
    }

    fn compaction(self, row: &T) -> T {
        // On a total order an update is before the row exactly when it is
        // at or before the time just before it, which compaction there
        // keeps. A row at the minimum sees nothing.
        row.previous().unwrap_or_else(T::minimum)
    }
}

mod sealed {
    pub trait Version {}

    impl Version for super::New {}
    impl Version for super::Old {}
}

// =============================================================================
// Lookup
// =============================================================================

/// Soft activation budget, charged per row and per matching value.
// A worker runs one operator at a time, so answering a large group at
// once stalls the rest of the dataflow and queues all of its output
// before anything downstream runs. The million-unit scale follows
// Differential's join budget per input, but counts rows and matches here,
// not output records. A row finishes before yielding; reading its values
// and running its output iterator can exceed the budget. Tests use eight
// units to exercise resumption with small, explicit expectations.
const FUEL: usize = if cfg!(test) { 8 } else { 1_000_000 };

// Follows differential-dogs3 0.26's `half_join`:
// https://github.com/TimelyDataflow/differential-dataflow/tree/e7c2470
// See `dogsdogsdogs/src/operators/half_join.rs` at that revision.
// It diverges as follows:
//
// - `Version` tokens replace its `strict` flag and its `frontier_func`
//   compaction closure; the compaction is derived here.
// - A row sees updates by the clock's own order and is ready once no
//   frontier time is visible to it, where `half_join` compares under
//   `Ord` against the frontier's least time; so `New` runs on clocks
//   that order their times only partly.
// - A visible update is at or before the row, so a match lands at the
//   row's time with one summed weight per value, where `half_join` puts
//   the join of the two times in the data, to be delayed later.
// - Weights multiply by `diff::product` under `Max` rather than `Mul`,
//   so presence weights work.
// - Rows wait per time, each time under its own capability, where
//   `half_join` holds one capability set for everything staged and
//   stamps each unit of work's output with all of it.
// - Each key's values are read once per run of rows with that key, where
//   `half_join` reads them for each row.
// - Yielding uses `FUEL` at row boundaries. The upstream driver accepts
//   a time/output-count yield function at container boundaries, but its
//   default wrapper disables yielding.
// - An activation takes one cursor over the trace's batches, where
//   `half_join` holds batches for each unit of work; the compaction bound
//   keeps answers fixed in between.

/// For each row `(k, d)` at time `t` with weight `r`, seeks `k` in
/// `arranged` and, for each value `v` there whose updates `version` lets
/// `t` see sum to a nonzero `w`, emits every item of `logic(k, d, v)` at
/// `t` with weight `diff::product(r, w)`.
///
/// Rows are exchanged by the hash of their key, as the arrangement's keys
/// are, and wait until no update still to come is visible to them. Rows
/// waiting together at one time are consolidated before being answered,
/// with equal rows' weights summed. Later arrivals at that same time form
/// another group and may produce more updates for an already answered row.
///
/// The rows' weight must be at least the arrangement's under [`Max`], and
/// the output keeps it. Presence rows against a signed arrangement would
/// be unsound: a presence row may arrive twice and still count once, so
/// each copy would match while the mirrored [`Old`] lookup takes back
/// only one. A row at epochs 0 and 1 against a value added at 0 and
/// removed at 2 would leave a count of one for a pair that is gone.
/// Static rows against an appending arrangement are rejected too; lift
/// such rows first. In practice the changing side drives the rows, and a
/// static arrangement is a whole join under one [`New`].
///
/// ```compile_fail,E0271
/// use differential_dataflow::VecCollection;
/// use flowlog_runtime::diff;
/// use flowlog_runtime::operators::New;
/// use flowlog_runtime::operators::flowlog_arrange;
/// use flowlog_runtime::operators::flowlog_lookup;
///
/// fn lookup<'scope>(
///     rows: VecCollection<'scope, u32, (u64, u64), diff::Static>,
///     values: VecCollection<'scope, u32, (u64, u64), diff::Mutable>,
/// ) {
///     let values = flowlog_arrange(values, "Values");
///     flowlog_lookup(rows, values, "Lookup", New, |_, _, _| None::<u64>);
/// }
/// ```
///
/// One term of a join, not a join: an update that reaches `arranged` and
/// is not visible to a row is never matched with it, so the lookup equals
/// the join of the two only when every update of `arranged` is visible to
/// every row, as a static relation's are to `New`: all at the scope's
/// minimum time, whenever they arrive. [`Version`] says how one lookup
/// per changing side adds up to the join.
///
/// # Panics
///
/// Panics if a row's time is not at or after some time of the stamp of
/// the message that carries it, as every update of a collection is.
#[must_use]
pub fn flowlog_lookup<'scope, Tr, K, D, R, V, I, L>(
    rows: VecCollection<'scope, Tr::Time, (K, D), R>,
    arranged: Arranged<'scope, Tr>,
    name: &str,
    version: V,
    mut logic: L,
) -> VecCollection<'scope, Tr::Time, I::Item, R>
where
    Tr: TraceReader<Batch: Navigable> + 'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time, KeyContainer: BatchContainer<Owned = K>>,
    K: ExchangeData + Hashable,
    D: ExchangeData,
    R: Max<BatchDiff<Tr>, Output = R>,
    BatchDiff<Tr>: Weight,
    V: Version<Tr::Time>,
    I: IntoIterator<Item: Data>,
    L: FnMut(&K, &D, BatchVal<'_, Tr>) -> I + 'static,
{
    let Arranged { stream, trace } = arranged;
    let mut reader = Reader::new(trace);
    let exchange = Exchange::new(|((key, _), ..): &((K, D), Tr::Time, R)| key.hashed().into());
    let scope = rows.scope();
    rows.inner
        .binary_frontier(stream, exchange, Pipeline, name, move |_, info| {
            let activator = scope.activator_for(info.address);
            let mut pending = Pending {
                waiting: Vec::new(),
                ready: VecDeque::new(),
            };
            move |(input, input_frontier), (batches, batch_frontier), output| {
                input.for_each(|capability, data| pending.insert(&capability, data));
                // The batches are read through the trace; their stream only
                // tells which updates may still arrive.
                batches.for_each(|_, _| {});
                let frontier = batch_frontier.frontier();
                pending.release(|time| version.ready(frontier, time));

                if !pending.ready.is_empty() {
                    let (mut cursor, storage) = reader.cursor();
                    // The cursor seeks a key as the batches lend it, which
                    // only a container of their keys can build.
                    let mut seek = <BatchCursor<Tr> as Cursor>::KeyContainer::with_capacity(1);
                    let mut matches = Vec::new();
                    let mut fuel = FUEL;
                    'answer: while let Some(group) = pending.ready.front_mut() {
                        let time = group.capability.time();
                        let mut session = output.session(&group.capability);
                        cursor.rewind_keys(&storage);
                        let unanswered = &group.rows[group.answered..];
                        let runs =
                            unanswered.chunk_by(|((left, _), _), ((right, _), _)| left == right);
                        for run in runs {
                            if fuel == 0 {
                                break 'answer;
                            }
                            let ((key, _), _) = &run[0];
                            seek.clear();
                            seek.push_own(key);
                            read_values(
                                &mut cursor,
                                &storage,
                                seek.index(0),
                                version,
                                time,
                                &mut matches,
                            );
                            for ((_, data), weight) in run {
                                if fuel == 0 {
                                    break 'answer;
                                }
                                for &(value, sum) in &matches {
                                    let weight = diff::product(*weight, sum);
                                    let items = logic(key, data, value).into_iter();
                                    session.give_iterator(
                                        items.map(|item| (item, time.clone(), weight)),
                                    );
                                }
                                group.answered += 1;
                                fuel = fuel.saturating_sub(1 + matches.len());
                            }
                        }
                        drop(session);
                        pending.ready.pop_front();
                    }
                    if !pending.ready.is_empty() {
                        activator.activate();
                    }
                }

                // Rows still to come arrive at or after the input frontier.
                let to_come = input_frontier.frontier();
                reader.advance(version, to_come.iter().chain(pending.times()));
            }
        })
        .as_collection()
}

/// Collects into `matches` each value of `key` whose updates a row at
/// `time` sees sum to nonzero, with that sum. Between two rewinds of the
/// cursor's keys, the keys sought must not decrease.
fn read_values<'storage, C: Cursor, V: Version<C::Time>>(
    cursor: &mut C,
    storage: &'storage C::Storage,
    key: C::Key<'_>,
    version: V,
    time: &C::Time,
    matches: &mut Vec<(C::Val<'storage>, C::Diff)>,
) {
    matches.clear();
    cursor.seek_key(storage, key);
    // Keys are invariant in their lifetime, so they compare only once
    // narrowed to a common one.
    if cursor.get_key(storage) != Some(<C::KeyContainer as BatchContainer>::reborrow(key)) {
        return;
    }
    while let Some(value) = cursor.get_val(storage) {
        if let Some(sum) = visible(cursor, storage, version, time) {
            matches.push((value, sum));
        }
        cursor.step_val(storage);
    }
}

/// Sums the updates of the cursor's current value that a row at `time`
/// sees; `None` when they sum to zero.
#[must_use]
fn visible<C: Cursor, V: Version<C::Time>>(
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

// =============================================================================
// Pending rows
// =============================================================================

/// Rows at one time, held under a capability at that time.
#[derive(Debug)]
struct Group<T: Timestamp, K, D, R> {
    capability: Capability<T>,
    /// Consolidated, and so sorted by key, once ready.
    rows: Vec<((K, D), R)>,
    /// How many rows, from the front, are answered.
    answered: usize,
}

/// The rows not yet answered: groups waiting for the arrangement, one per
/// time, and groups ready to answer, in the order they became ready.
#[derive(Debug)]
struct Pending<T: Timestamp, K, D, R> {
    waiting: Vec<Group<T, K, D, R>>,
    ready: VecDeque<Group<T, K, D, R>>,
}

impl<T: Timestamp, K: Ord, D: Ord, R: Semigroup> Pending<T, K, D, R> {
    /// Moves `rows` into the waiting group at each row's time, making one
    /// under `capability` where there is none.
    ///
    /// # Panics
    ///
    /// Panics if a row's time is not at or after some time of
    /// `capability`'s stamp.
    fn insert(&mut self, capability: &InputCapability<T>, rows: &mut Vec<((K, D), T, R)>) {
        let updates = rows.drain(..);
        for (row, time, weight) in updates {
            // Rows of a message mostly share a time, so the latest group is
            // the likeliest.
            let held = self
                .waiting
                .iter()
                .rposition(|group| *group.capability.time() == time);
            let index = match held {
                Some(index) => index,
                None => {
                    let capability = capability.delayed(&time, 0);
                    self.waiting.push(Group {
                        capability,
                        rows: Vec::new(),
                        answered: 0,
                    });
                    self.waiting.len() - 1
                }
            };
            self.waiting[index].rows.push((row, weight));
        }
    }

    /// Moves each waiting group whose time is `ready` to the back of the
    /// ready queue, consolidated, dropping groups that consolidate away.
    fn release(&mut self, ready: impl Fn(&T) -> bool) {
        let released = self
            .waiting
            .extract_if(.., |group| ready(group.capability.time()));
        for mut group in released {
            consolidate(&mut group.rows);
            if !group.rows.is_empty() {
                self.ready.push_back(group);
            }
        }
    }

    /// The time of every group, waiting or ready.
    #[must_use = "pending times must be included in the compaction bound"]
    fn times(&self) -> impl Iterator<Item = &T> {
        let groups = self.waiting.iter().chain(&self.ready);
        groups.map(|group| group.capability.time())
    }
}

// =============================================================================
// Reader
// =============================================================================

/// The arrangement's trace, read whole and compacted only as far as the
/// lookup allows.
#[derive(Debug)]
struct Reader<Tr> {
    /// `None` once released, when no row is pending or still to come.
    trace: Option<Tr>,
}

impl<Tr: TraceReader<Batch: Navigable>> Reader<Tr> {
    #[must_use]
    fn new(mut trace: Tr) -> Self {
        // Only logical compaction decides answers; merging batches
        // physically never changes one.
        trace.set_physical_compaction(Antichain::new().borrow());
        Self { trace: Some(trace) }
    }

    /// Returns a cursor over every batch of the trace, with the batches it
    /// reads.
    ///
    /// # Panics
    ///
    /// Panics if the trace is released or cannot supply its complete history.
    #[must_use]
    fn cursor(&mut self) -> (CursorList<BatchCursor<Tr>>, Vec<Tr::Batch>) {
        let trace = self
            .trace
            .as_mut()
            .expect("the trace is held while rows are pending");
        let batches = trace
            .batches_through(Antichain::new().borrow())
            .expect("a trace yields every batch through the empty frontier");
        cursor_list(batches)
    }

    /// Lets the trace compact as far as `version` allows rows at `times`,
    /// the times of every row pending or still to come, and releases it
    /// once there are none.
    ///
    /// Each time must be at or after one passed before, so that the bound,
    /// which rises with the row, never falls.
    fn advance<'time, V: Version<Tr::Time>>(
        &mut self,
        version: V,
        times: impl Iterator<Item = &'time Tr::Time>,
    ) where
        Tr::Time: 'time,
    {
        let bound: Antichain<_> = times.map(|time| version.compaction(time)).collect();
        if bound.is_empty() {
            self.trace = None;
        } else if let Some(trace) = self.trace.as_mut() {
            trace.set_logical_compaction(bound.borrow());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::fmt::Debug;
    // Inspect callbacks are 'static; shared buffers let the test retain
    // access while the dataflow owns the callbacks.
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::operators::iterate::Variable;
    use rstest::rstest;
    use timely::dataflow::operators::probe::Handle;
    use timely::order::Product;

    use super::*;
    use crate::diff;
    use crate::operators::flowlog_arrange;
    use crate::operators::flowlog_dedup;
    use crate::operators::flowlog_join;
    use crate::time::Epoch;
    use crate::time::EpochLoop;
    use crate::time::Iteration;
    use crate::time::Once;
    use crate::time::OnceLoop;

    #[derive(Debug, Clone, Copy)]
    enum Evaluation {
        Join,
        Lookup,
    }

    /// Runs `build` over two static inputs, `left` and `right`, outside any
    /// loop, and returns the rows it produces as a set.
    fn rows_of<B>(left: Vec<(u64, u64)>, right: Vec<(u64, u64)>, build: B) -> BTreeSet<(u64, u64)>
    where
        B: for<'scope> Fn(
                VecCollection<'scope, Once, (u64, u64), diff::Static>,
                VecCollection<'scope, Once, (u64, u64), diff::Static>,
            ) -> VecCollection<'scope, Once, (u64, u64), diff::Static>
            + Send
            + Sync
            + 'static,
    {
        timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(BTreeSet::new()));
            let (mut l, mut r) = worker.dataflow::<Once, _, _>(|scope| {
                let (l, lc) = scope.new_collection::<(u64, u64), diff::Static>();
                let (r, rc) = scope.new_collection::<(u64, u64), diff::Static>();
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

    #[test]
    fn a_lookup_yields_what_a_join_yields() {
        let left = vec![(1, 10), (2, 10), (3, 20), (4, 30)];
        let right = vec![(10, 100), (10, 101), (20, 200)];
        let looked_up = rows_of(left.clone(), right.clone(), |l, r| {
            flowlog_lookup(
                l.map(|(x, y)| (y, x)),
                flowlog_arrange(r, "Right"),
                "Lookup",
                New,
                |_, &x, &z| Some((x, z)),
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
                l.map(|(x, y)| (y, x)),
                flowlog_arrange(r, "Right"),
                "Lookup",
                New,
                |_, &x, &z| (z > x).then_some((x, z)),
            )
        });
        assert_eq!(rows, BTreeSet::from([(1, 3), (1, 7), (5, 7)]));
    }

    #[test]
    fn logic_can_emit_multiple_rows_per_match() {
        let rows = rows_of(vec![(10, 1)], vec![(10, 3)], |rows, values| {
            flowlog_lookup(
                rows,
                flowlog_arrange(values, "Values"),
                "Lookup",
                New,
                |_, &data, &value| [(data, value), (data, value + 1)],
            )
        });
        assert_eq!(rows, BTreeSet::from([(1, 3), (1, 4)]));
    }

    /// Rows are routed by their key's hash, as the arrangement is, so on
    /// several workers each row probes the worker that holds its key.
    #[test]
    fn rows_reach_the_worker_that_holds_their_key() {
        const KEYS: u64 = 8;
        let mut actual: Vec<(u64, u64)> = timely::execute(timely::Config::process(4), |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut l, mut r) = worker.dataflow::<Once, _, _>(|scope| {
                let (l, lc) = scope.new_collection::<(u64, u64), diff::Static>();
                let (r, rc) = scope.new_collection::<(u64, u64), diff::Static>();
                let seen = Rc::clone(&seen);
                flowlog_lookup(
                    lc.map(|(x, y)| (y, x)),
                    flowlog_arrange(rc, "Right"),
                    "Lookup",
                    New,
                    |_, &x, &z| Some((x, z)),
                )
                .inspect(move |(row, ..)| seen.borrow_mut().push(*row));
                (l, r)
            });
            if worker.index() == 0 {
                for key in 0..KEYS {
                    l.update((key, key), diff::Static);
                    r.update((key, key + 100), diff::Static);
                }
            }
            l.close();
            r.close();
            while worker.step() {}
            seen.take()
        })
        .unwrap()
        .join()
        .into_iter()
        .flat_map(Result::unwrap)
        .collect();
        actual.sort_unstable();
        assert_eq!(
            actual,
            vec![
                (0, 100),
                (1, 101),
                (2, 102),
                (3, 103),
                (4, 104),
                (5, 105),
                (6, 106),
                (7, 107)
            ]
        );
    }

    /// A row is answered once the arrangement's frontier passes its time,
    /// with both inputs still open; a row later than the capability of the
    /// message that carried it waits under its own time, and that earlier
    /// capability is released meanwhile.
    #[test]
    fn rows_are_answered_at_their_own_time_while_the_inputs_stay_open() {
        let (early, mut all) = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut l, mut r) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (l, lc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let (r, rc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_lookup(
                    lc.map(|(x, y)| (y, x)),
                    flowlog_arrange(rc, "Right"),
                    "Lookup",
                    New,
                    |_, &x, &z| Some((x, z)),
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (l, r)
            });
            // Bounded, so a capability held too long fails instead of
            // hanging the test.
            let mut passed = |epoch: Epoch| {
                (0..1_000).any(|_| {
                    worker.step();
                    !probe.less_equal(&epoch)
                })
            };
            r.update((10, 100), 1);
            r.advance_to(1);
            r.flush();
            // One message under epoch 0's capability carries a row at each
            // of epochs 0 and 1.
            l.update_at((1, 10), 0, 1);
            l.update_at((2, 10), 1, 1);
            l.advance_to(1);
            l.flush();
            assert!(
                passed(0),
                "epoch 0 is released while the row at epoch 1 waits"
            );
            let early = seen.borrow().clone();
            r.update((10, 101), 1);
            r.advance_to(2);
            r.flush();
            l.advance_to(2);
            l.flush();
            assert!(
                passed(1),
                "the row at epoch 1 is answered once the arrangement passes it"
            );
            l.close();
            r.close();
            while worker.step() {}
            (early, seen.take())
        });
        assert_eq!(early, vec![((1, 100), 0, 1)]);
        all.sort_unstable();
        assert_eq!(
            all,
            vec![((1, 100), 0, 1), ((2, 100), 1, 1), ((2, 101), 1, 1)]
        );
    }

    /// Equal waiting rows share one answer with their weights summed;
    /// rows that cancel out are not answered at all.
    #[test]
    fn equal_waiting_rows_are_answered_once() {
        let output = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut rows, mut values) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (rows, rc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let (values, vc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_lookup(
                    rc,
                    flowlog_arrange(vc, "Values"),
                    "Lookup",
                    New,
                    |_, &data, &value| Some((data, value)),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                (rows, values)
            });
            values.update((10, 100), 1);
            for (row, weight) in [((10, 1), 1), ((10, 1), 1), ((10, 2), 1), ((10, 2), -1)] {
                rows.update(row, weight);
            }
            rows.close();
            values.close();
            while worker.step() {}
            seen.take()
        });
        // As the operator emits it, before anything downstream consolidates.
        assert_eq!(output, vec![((1, 100), 0, 2)]);
    }

    /// A value whose updates up to a row's time cancel out is not matched,
    /// though the arrangement still holds them.
    #[test]
    fn a_value_gone_by_the_row_time_is_not_matched() {
        let output = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut rows, mut values) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (rows, rc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let (values, vc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_lookup(
                    rc,
                    flowlog_arrange(vc, "Values"),
                    "Lookup",
                    New,
                    |_, &data, &value| Some((data, value)),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                (rows, values)
            });
            values.update_at((10, 100), 0, 1);
            values.update_at((10, 100), 1, -1);
            // The row at epoch 0 holds compaction back, so the row at epoch
            // 1 reads both updates rather than none.
            rows.update_at((10, 1), 0, 1);
            rows.update_at((10, 2), 1, 1);
            rows.close();
            values.close();
            while worker.step() {}
            seen.take()
        });
        // As the operator emits it, before anything downstream consolidates.
        assert_eq!(output, vec![((1, 100), 0, 1)]);
    }

    #[test]
    fn a_waiting_row_is_answered_as_of_its_time_while_later_rows_run_ahead() {
        let mut output = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut rows, mut values) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (rows, rc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let (values, vc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_lookup(
                    rc,
                    flowlog_arrange(vc, "Values"),
                    "Lookup",
                    New,
                    |_, &data, &value| Some((data, value)),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                (rows, values)
            });
            rows.update_at((10, 1), 5, 1);
            rows.advance_to(10);
            rows.flush();
            // One batch an epoch, each merged into the trace under whatever
            // compaction the lookup allows by then.
            for epoch in 0..5 {
                values.update((10, 100 + u64::from(epoch)), 1);
                values.advance_to(epoch + 1);
                values.flush();
                for _ in 0..10 {
                    worker.step();
                }
            }
            rows.close();
            values.close();
            while worker.step() {}
            seen.take()
        });
        output.sort_unstable();
        assert_eq!(
            output,
            vec![
                ((1, 100), 5, 1),
                ((1, 101), 5, 1),
                ((1, 102), 5, 1),
                ((1, 103), 5, 1),
                ((1, 104), 5, 1),
            ]
        );
    }

    /// The group must yield before it finishes, then resume within a key's
    /// rows without losing or repeating an answer.
    #[test]
    fn a_group_beyond_the_fuel_yields_and_resumes_in_full() {
        let (done, yielded, mut output) = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let calls = Rc::new(Cell::new(0));
            let probe = Handle::new();
            let (mut rows, mut values) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (rows, rows_stream) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let (values, values_stream) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let seen = Rc::clone(&seen);
                let calls = Rc::clone(&calls);
                flowlog_lookup(
                    rows_stream,
                    flowlog_arrange(values_stream, "Values"),
                    "Lookup",
                    New,
                    move |&key, &data, &value| {
                        calls.set(calls.get() + 1);
                        Some((key, data, value))
                    },
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (rows, values)
            });
            // Four rows per key and time cost twelve units, exceeding the
            // test budget of eight before the next key is reached.
            for key in 0..2 {
                for value in 0..2 {
                    values.update((key, value), 1);
                }
                for data in 0..8 {
                    rows.update_at((key, data), Epoch::from(data % 2 == 1), 1);
                }
            }
            rows.close();
            values.close();
            let mut yielded = false;
            let done = (0..1_000).any(|_| {
                worker.step();
                yielded |= (1..32).contains(&calls.get());
                probe.done()
            });
            (done, yielded, seen.take())
        });
        assert!(done, "lookup did not finish within 1,000 scheduler steps");
        assert!(
            yielded,
            "all matches ran without returning control to the worker"
        );
        output.sort_unstable();
        assert_eq!(
            output,
            vec![
                ((0, 0, 0), 0, 1),
                ((0, 0, 1), 0, 1),
                ((0, 1, 0), 1, 1),
                ((0, 1, 1), 1, 1),
                ((0, 2, 0), 0, 1),
                ((0, 2, 1), 0, 1),
                ((0, 3, 0), 1, 1),
                ((0, 3, 1), 1, 1),
                ((0, 4, 0), 0, 1),
                ((0, 4, 1), 0, 1),
                ((0, 5, 0), 1, 1),
                ((0, 5, 1), 1, 1),
                ((0, 6, 0), 0, 1),
                ((0, 6, 1), 0, 1),
                ((0, 7, 0), 1, 1),
                ((0, 7, 1), 1, 1),
                ((1, 0, 0), 0, 1),
                ((1, 0, 1), 0, 1),
                ((1, 1, 0), 1, 1),
                ((1, 1, 1), 1, 1),
                ((1, 2, 0), 0, 1),
                ((1, 2, 1), 0, 1),
                ((1, 3, 0), 1, 1),
                ((1, 3, 1), 1, 1),
                ((1, 4, 0), 0, 1),
                ((1, 4, 1), 0, 1),
                ((1, 5, 0), 1, 1),
                ((1, 5, 1), 1, 1),
                ((1, 6, 0), 0, 1),
                ((1, 6, 1), 0, 1),
                ((1, 7, 0), 1, 1),
                ((1, 7, 1), 1, 1),
            ]
        );
    }

    /// Deterministic pseudo-random edges among `nodes` nodes.
    #[must_use]
    fn edges(nodes: u64, count: usize, mut state: u64) -> Vec<(u64, u64)> {
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
    fn fixpoint(
        seed: Vec<(u64, u64)>,
        extend: Vec<(u64, u64)>,
        evaluation: Evaluation,
    ) -> BTreeSet<(u64, u64)> {
        timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(BTreeSet::new()));
            let (mut s, mut e) = worker.dataflow::<Once, _, _>(|scope| {
                let (s, sc) = scope.new_collection::<(u64, u64), diff::Static>();
                let (e, ec) = scope.new_collection::<(u64, u64), diff::Static>();
                let e_by_y = flowlog_arrange(ec.clone(), "E by y");
                let e_by_w = flowlog_arrange(ec.map(|(y, w)| (w, y)), "E by w");
                let seen = Rc::clone(&seen);
                scope
                    .iterative::<u16, _, _>(|inner| {
                        let (variable, t) = Variable::new(inner, Product::new((), 1));
                        // Named so the arrangements below resolve before `set`.
                        let t: VecCollection<'_, OnceLoop, (u64, u64), diff::Static> = t;
                        let e_by_y = e_by_y.enter(inner);
                        let e_by_w = e_by_w.enter(inner);
                        let t_by_w = flowlog_arrange(t.clone(), "T by w");
                        let t_by_y = flowlog_arrange(t.clone().map(|(x, y)| (y, x)), "T by y");
                        let derived = match evaluation {
                            Evaluation::Lookup => {
                                // The chain driven by the first T reads the
                                // second as it stood before this iteration; the
                                // chain driven by the second reads the first as
                                // it stands now, so a derivation from two new
                                // rows is made by the second chain alone.
                                let first = flowlog_lookup(
                                    t.clone().map(|(x, y)| (y, x)),
                                    e_by_y,
                                    "E by y",
                                    New,
                                    |_, &x, &w| Some((w, x)),
                                );
                                let first = flowlog_lookup(
                                    first,
                                    t_by_w,
                                    "old T by w",
                                    Old,
                                    |_, &x, &z| Some((x, z)),
                                );
                                let second = flowlog_lookup(
                                    t.clone(),
                                    e_by_w,
                                    "E by w",
                                    New,
                                    |_, &z, &y| Some((y, z)),
                                );
                                let second = flowlog_lookup(
                                    second,
                                    t_by_y,
                                    "new T by y",
                                    New,
                                    |_, &z, &x| Some((x, z)),
                                );
                                first.concat(second)
                            }
                            Evaluation::Join => {
                                let te = flowlog_join(t_by_y, e_by_y, "T join E", |_, &x, &w| {
                                    Some((w, x))
                                });
                                flowlog_join(
                                    flowlog_arrange(te, "TE by w"),
                                    t_by_w,
                                    "TE join T",
                                    |_, &x, &z| Some((x, z)),
                                )
                            }
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
        let joined = fixpoint(seed.clone(), extend.clone(), Evaluation::Join);
        let looked_up = fixpoint(seed, extend, Evaluation::Lookup);
        assert!(joined.len() > 120, "the fixpoint grows past the seed");
        assert_eq!(looked_up, joined);
    }

    // Cases: evaluation.
    #[rstest]
    #[case(Evaluation::Join)]
    #[case(Evaluation::Lookup)]
    fn nonlinear_recursion_reaches_the_transitive_closure(#[case] evaluation: Evaluation) {
        let seed = vec![(1, 2), (2, 3), (3, 4)];
        let extend = vec![(2, 2), (3, 3), (4, 4)];
        assert_eq!(
            fixpoint(seed, extend, evaluation),
            BTreeSet::from([(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)]),
        );
    }

    // Cases: version, update, row, visible.
    #[rstest]
    #[case(New, OnceLoop::new((), 2), OnceLoop::new((), 2), true)]
    #[case(New, OnceLoop::new((), 1), OnceLoop::new((), 2), true)]
    #[case(New, OnceLoop::new((), 3), OnceLoop::new((), 2), false)]
    #[case(Old, OnceLoop::new((), 2), OnceLoop::new((), 2), false)]
    #[case(Old, OnceLoop::new((), 1), OnceLoop::new((), 2), true)]
    #[case(Old, OnceLoop::new((), 3), OnceLoop::new((), 2), false)]
    #[case(Old, OnceLoop::new((), 0), OnceLoop::new((), 0), false)]
    #[case(New, (), (), true)]
    #[case(New, EpochLoop::new(0, 1), EpochLoop::new(1, 0), false)]
    #[case(New, EpochLoop::new(1, 0), EpochLoop::new(0, 1), false)]
    fn a_version_sees_only_visible_updates<T: Timestamp, V: Version<T>>(
        #[case] version: V,
        #[case] update: T,
        #[case] row: T,
        #[case] expected: bool,
    ) {
        assert_eq!(version.sees(&update, &row), expected);
    }

    // Cases: version, frontier, row, ready.
    #[rstest]
    #[case(New, vec![1_u32], 2, false)]
    #[case(Old, vec![1_u32], 2, false)]
    #[case(New, vec![2_u32], 2, false)]
    #[case(Old, vec![2_u32], 2, true)]
    #[case(New, vec![3_u32], 2, true)]
    #[case(Old, vec![3_u32], 2, true)]
    #[case(New, vec![], 2_u32, true)]
    #[case(Old, vec![], 2_u32, true)]
    #[case(New, vec![EpochLoop::new(1, 0)], EpochLoop::new(0, 5), true)]
    #[case(New, vec![EpochLoop::new(0, 3), EpochLoop::new(1, 0)], EpochLoop::new(0, 5), false)]
    fn a_row_is_ready_once_no_future_update_is_visible<T: Timestamp, V: Version<T>>(
        #[case] version: V,
        #[case] frontier: Vec<T>,
        #[case] row: T,
        #[case] expected: bool,
    ) {
        assert_eq!(version.ready(AntichainRef::new(&frontier), &row), expected);
    }

    /// Every time of a small grid of each clock, as `(epoch, iteration)`
    /// pairs for the clocks that have both.
    #[must_use]
    fn grid<T>(make: impl Fn(u16, u16) -> T) -> Vec<T> {
        (0..4)
            .flat_map(|e| (0..4).map(move |i| (e, i)))
            .map(|(e, i)| make(e, i))
            .collect()
    }

    /// Compacting an update to a pending row's bound, as differential does
    /// by joining it with the bound, keeps what the row sees of it, on
    /// every clock; and a later row's bound is no earlier.
    // Cases: version, clock samples.
    #[rstest]
    #[case(New, grid(|_, _| ()))]
    #[case(New, grid(|epoch, iteration| u32::from(epoch) * 4 + u32::from(iteration)))]
    #[case(Old, grid(|epoch, iteration| u32::from(epoch) * 4 + u32::from(iteration)))]
    #[case(New, grid(|_, iteration| OnceLoop::new((), iteration)))]
    #[case(Old, grid(|_, iteration| OnceLoop::new((), iteration)))]
    #[case(New, grid(|epoch, iteration| (u32::from(epoch), iteration)))]
    #[case(Old, grid(|epoch, iteration| (u32::from(epoch), iteration)))]
    #[case(New, grid(|epoch, iteration| EpochLoop::new(u32::from(epoch), iteration)))]
    fn the_compaction_bound_keeps_answers_and_rises_with_the_row<
        T: Timestamp + Lattice,
        V: Version<T> + Debug,
    >(
        #[case] version: V,
        #[case] times: Vec<T>,
    ) {
        for row in &times {
            let bound = version.compaction(row);
            assert!(
                bound.less_equal(row),
                "{version:?} bound {bound:?} after {row:?}"
            );
            for update in &times {
                assert_eq!(
                    version.sees(&update.join(&bound), row),
                    version.sees(update, row),
                    "{version:?} of {update:?} from {row:?}",
                );
            }
            let later_times = times.iter().filter(|later| row.less_equal(later));
            for later in later_times {
                assert!(
                    bound.less_equal(&version.compaction(later)),
                    "{version:?} bound falls from {row:?} to {later:?}",
                );
            }
        }
    }

    /// Updates by epoch: `(row, epoch, weight)`.
    type Changes = Vec<((u64, u64), u32, i32)>;

    /// Runs `build` over two signed inputs fed epoch by epoch and returns
    /// every output update, consolidated.
    fn signed_updates<B>(left: Changes, right: Changes, build: B) -> Vec<((u64, u64), u32, i32)>
    where
        B: for<'scope> Fn(
                VecCollection<'scope, Epoch, (u64, u64), diff::Mutable>,
                VecCollection<'scope, Epoch, (u64, u64), diff::Mutable>,
            ) -> VecCollection<'scope, Epoch, (u64, u64), diff::Mutable>
            + Send
            + Sync
            + 'static,
    {
        let mut updates = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut l, mut r) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (l, lc) = scope.new_collection::<(u64, u64), diff::Mutable>();
                let (r, rc) = scope.new_collection::<(u64, u64), diff::Mutable>();
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
                let left_updates = left.iter().filter(|(_, time, _)| *time == epoch);
                for (row, _, weight) in left_updates {
                    l.update(*row, *weight);
                }
                let right_updates = right.iter().filter(|(_, time, _)| *time == epoch);
                for (row, _, weight) in right_updates {
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
                l.clone().map(|(x, y)| (y, x)),
                flowlog_arrange(r.clone(), "Right"),
                "Left looks up right",
                New,
                |_, &x, &z| Some((x, z)),
            );
            let from_right = flowlog_lookup(
                r,
                flowlog_arrange(l.map(|(x, y)| (y, x)), "Left by y"),
                "Right looks up left",
                Old,
                |_, &z, &x| Some((x, z)),
            );
            from_left.concat(from_right)
        });
        assert_eq!(looked_up, joined);
        assert_eq!(
            looked_up,
            vec![
                ((1, 100), 0, 1),
                ((1, 100), 2, -1),
                ((1, 101), 1, 1),
                ((1, 101), 2, -1),
                ((2, 100), 0, 1),
                ((2, 100), 2, -1),
                ((2, 101), 1, 1),
                ((3, 200), 1, 1),
                ((3, 201), 2, 1),
                ((4, 101), 2, 1),
            ]
        );
    }

    /// Inside a mutable loop, whose clock orders its times only partly, a
    /// lookup against a relation fixed from the first epoch yields what a
    /// join yields, as the rows change from epoch to epoch.
    #[test]
    fn a_lookup_in_a_mutable_loop_yields_what_a_join_yields() {
        /// Every node each start reaches by hops, one lookup or join a step.
        fn reachable(starts: Changes, hops: Changes, evaluation: Evaluation) -> Changes {
            signed_updates(starts, hops, move |starts, hops| {
                let scope = starts.scope();
                let hops = flowlog_arrange(hops, "Hops");
                scope.iterative::<Iteration, _, _>(|inner| {
                    let (variable, reach) = Variable::new(inner, Product::new(0, 1));
                    // Named so the arrangements below resolve before `set`.
                    let reach: VecCollection<'_, EpochLoop, (u64, u64), diff::Mutable> = reach;
                    let hops = hops.enter(inner);
                    let by_node = reach.map(|(start, node)| (node, start));
                    let step = match evaluation {
                        Evaluation::Lookup => {
                            flowlog_lookup(by_node, hops, "Step", New, |_, &start, &next| {
                                Some((start, next))
                            })
                        }
                        Evaluation::Join => {
                            let by_node = flowlog_arrange(by_node, "Reach by node");
                            flowlog_join(by_node, hops, "Step", |_, &start, &next| {
                                Some((start, next))
                            })
                        }
                    };
                    let next = flowlog_dedup(step.concat(starts.enter(inner)));
                    variable.set(next.clone());
                    next.leave(scope)
                })
            })
        }
        let starts: Changes = vec![
            ((1, 10), 0, 1),
            ((2, 20), 0, 1),
            ((3, 30), 1, 1),
            ((1, 10), 2, -1),
        ];
        let hops: Changes = vec![((10, 20), 0, 1), ((20, 30), 0, 1), ((30, 10), 0, 1)];
        let joined = reachable(starts.clone(), hops.clone(), Evaluation::Join);
        let looked_up = reachable(starts, hops, Evaluation::Lookup);
        assert_eq!(looked_up, joined);
        assert_eq!(
            looked_up,
            vec![
                ((1, 10), 0, 1),
                ((1, 10), 2, -1),
                ((1, 20), 0, 1),
                ((1, 20), 2, -1),
                ((1, 30), 0, 1),
                ((1, 30), 2, -1),
                ((2, 10), 0, 1),
                ((2, 20), 0, 1),
                ((2, 30), 0, 1),
                ((3, 10), 1, 1),
                ((3, 20), 1, 1),
                ((3, 30), 1, 1),
            ]
        );
    }
}
