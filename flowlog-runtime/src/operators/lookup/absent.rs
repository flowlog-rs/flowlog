//! [`lookup_absent`] keeps an arrangement's rows whose key a static
//! arrangement lacks, each decided once by the filter's frontier;
//! [`Undecided`] holds a message's batches until their rows are.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::VecCollection;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::BatchDiff;
use differential_dataflow::trace::BatchVal;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::SpanOf;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::containers::BatchContainer;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::InputCapability;
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::generic::OutputBuilderSession;
use timely::order::PartialOrder;
use timely::progress::Antichain;

use super::New;
use super::Reader;
use super::Version;
use crate::diff;

// Differential's `antijoin` concatenates the rows with their `semijoin`,
// negated, so a blocked row is emitted and then cancelled, which takes
// signed weights and a consolidation downstream. A static filter's keys
// never leave, so here each row is decided once, by the filter's
// frontier, and keeps the source's weight, presence included. The cost
// is holding each source batch until its last row is decided, and
// reading it whole again each time the frontier passes some of its rows.

/// Keeps the rows of `source` whose key `filter` lacks, each mapped
/// through `logic` and emitted at its own time and weight.
///
/// A row at time `t` is kept iff `filter` holds its key at no time at or
/// before `t`, the updates [`New`] lets the row see. Keys never leave a
/// static filter, so a row whose key is held is dropped at once, and any
/// other row waits until no update still to come is visible to it.
/// `logic` never sees a dropped row, and each read of a batch maps a key
/// and value through it at most once.
///
/// `source` must be partitioned by key as `filter` is, so that a worker
/// holds every key its rows seek; arranging both by the key's hash, as
/// [`flowlog_arrange`](crate::operators::flowlog_arrange) does, is
/// enough. Every row of a batch must be at or after some time of the
/// stamp of the message that carries it, as an arrangement's are; a row
/// at or after none of them is taken for one decided already and never
/// emitted.
#[must_use]
pub(crate) fn lookup_absent<'scope, Tr1, Tr2, KC, D, L>(
    filter: Arranged<'scope, Tr1>,
    source: Arranged<'scope, Tr2>,
    name: &str,
    mut logic: L,
) -> VecCollection<'scope, Tr1::Time, D, BatchDiff<Tr2>>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time> + 'static,
    BatchCursor<Tr1>: Cursor<Diff = diff::Static, Time = Tr1::Time, KeyContainer = KC>,
    BatchCursor<Tr2>: Cursor<Time = Tr1::Time>,
    KC: BatchContainer,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    D: Data,
    L: FnMut(KC::ReadItem<'_>, BatchVal<'_, Tr2>) -> D + 'static,
{
    let mut reader = Reader::new(filter.trace);
    // Rows come from the source's batch stream, as `as_collection` reads
    // them, so its trace handle goes now rather than hold back the
    // source's compaction.
    drop(source.trace);
    source
        .stream
        .binary_frontier(filter.stream, Pipeline, Pipeline, name, move |_, _| {
            let mut pending: Vec<Undecided<Tr2>> = Vec::new();
            move |(source_input, source_frontier), (filter_input, filter_frontier), output| {
                source_input.for_each(|capability, spans| {
                    pending.push(Undecided::new(&capability, std::mem::take(spans)));
                });
                // The filter is read through its trace; its stream only
                // tells which keys may still arrive.
                filter_input.for_each(|_, _| {});
                let frontier = filter_frontier.frontier();
                let ready = |time: &Tr1::Time| New.ready(frontier, time);
                // No fuel, unlike `flowlog_lookup`: a batch emits no more
                // rows than it holds, so it is answered whole, as
                // `as_collection` reads one.
                let mut cursor = None;
                pending.retain_mut(|undecided| {
                    if !undecided.may_be_ready(ready) {
                        return true;
                    }
                    let (filter, storage) = cursor.get_or_insert_with(|| reader.cursor());
                    undecided.decide(filter, storage, ready, output, &mut logic)
                });
                // Rows still to come arrive at or after the source's
                // frontier, and undecided rows are at or after their
                // batch's capabilities.
                let to_come = source_frontier.frontier();
                let undecided = pending.iter().flat_map(Undecided::times);
                reader.advance(New, to_come.iter().chain(undecided));
            }
        })
        .as_collection()
}

/// The output an absent lookup over `Tr`'s batches emits its rows `D` to.
type AbsentOutput<'a, Tr, D> = OutputBuilderSession<
    'a,
    <Tr as TraceReader>::Time,
    CapacityContainerBuilder<Vec<(D, <Tr as TraceReader>::Time, BatchDiff<Tr>)>>,
>;

/// The source batches of one message, held under capabilities at the
/// times of their undecided rows: every undecided row is at or after one
/// of the capabilities, and a row at or after none was decided already.
// No `Debug`: a trace's batch need not have one.
struct Undecided<Tr: TraceReader> {
    capabilities: CapabilitySet<Tr::Time>,
    spans: Vec<SpanOf<Tr>>,
}

impl<Tr: TraceReader<Batch: Navigable>> Undecided<Tr> {
    /// Holds `spans`, every row of which is undecided, under the stamp of
    /// the message that carried them.
    #[must_use]
    fn new(capability: &InputCapability<Tr::Time>, spans: Vec<SpanOf<Tr>>) -> Self {
        Self {
            capabilities: capability.retain_stamp(0),
            spans,
        }
    }

    /// Returns `true` if some undecided row may be `ready`.
    #[must_use]
    fn may_be_ready(&self, ready: impl Fn(&Tr::Time) -> bool) -> bool {
        // A row is ready only if every capability at or before it is, and
        // every undecided row is at or after one.
        self.capabilities.iter().any(|c| ready(c.time()))
    }

    /// The times from which rows are undecided.
    #[must_use = "undecided times must be included in the compaction bound"]
    fn times(&self) -> impl Iterator<Item = &Tr::Time> {
        self.capabilities.iter().map(Capability::time)
    }

    /// Decides the rows that are `ready`: drops those whose key `filter`
    /// holds at or before them, emits the others through `logic` to
    /// `output` at their own time and weight, and moves the capabilities
    /// on to the times of the rows still waiting. Returns `true` while
    /// some row waits. A batch that [`Self::may_be_ready`] denies has no
    /// row to decide, and the read is wasted on it.
    fn decide<C, KC, D, L>(
        &mut self,
        filter: &mut C,
        storage: &C::Storage,
        ready: impl Fn(&Tr::Time) -> bool,
        output: &mut AbsentOutput<'_, Tr, D>,
        logic: &mut L,
    ) -> bool
    where
        C: Cursor<Time = Tr::Time, Diff = diff::Static, KeyContainer = KC>,
        BatchCursor<Tr>: Cursor<Time = Tr::Time>,
        KC: BatchContainer,
        for<'a> C: Cursor<Key<'a> = KC::ReadItem<'a>>,
        for<'a> BatchCursor<Tr>: Cursor<Key<'a> = KC::ReadItem<'a>>,
        D: Data,
        L: FnMut(KC::ReadItem<'_>, BatchVal<'_, Tr>) -> D,
    {
        let capabilities = &self.capabilities;
        let mut held = Antichain::new();
        let mut waiting = Antichain::new();
        let mut session = output.session(capabilities);
        let batches = self.spans.iter().filter_map(|span| span.inner.as_ref());
        for batch in batches {
            filter.rewind_keys(storage);
            let mut rows = batch.cursor();
            while let Some(key) = rows.get_key(batch) {
                times_held(filter, storage, key, &mut held);
                // Every undecided row is at or after some capability, so a
                // key held at or before each drops them all.
                let blocked = capabilities.iter().all(|c| held.less_equal(c.time()));
                while !blocked && let Some(val) = rows.get_val(batch) {
                    let mut kept = None;
                    rows.map_times(batch, |time, diff| {
                        let time = <BatchCursor<Tr> as Cursor>::owned_time(time);
                        // A row at or after no capability was decided in
                        // an earlier read.
                        let decided = !capabilities.iter().any(|c| c.time().less_equal(&time));
                        if decided || held.less_equal(&time) {
                            return;
                        }
                        if ready(&time) {
                            let kept = kept.get_or_insert_with(|| logic(key, val));
                            let diff = <BatchCursor<Tr> as Cursor>::owned_diff(diff);
                            session.give((kept.clone(), time, diff));
                        } else {
                            waiting.insert(time);
                        }
                    });
                    rows.step_val(batch);
                }
                rows.step_key(batch);
            }
        }
        drop(session);
        self.capabilities.downgrade(waiting.elements());
        !waiting.is_empty()
    }
}

/// Collects into `held` the earliest times of `filter`'s updates at
/// `key`, each a time from which a static filter holds it. Between two
/// rewinds of the cursor's keys, the keys sought must not decrease.
fn times_held<C>(
    filter: &mut C,
    storage: &C::Storage,
    key: C::Key<'_>,
    held: &mut Antichain<C::Time>,
) where
    C: Cursor<Diff = diff::Static>,
{
    held.clear();
    filter.seek_key(storage, key);
    if filter.get_key(storage) == Some(<C::KeyContainer as BatchContainer>::reborrow(key)) {
        while filter.val_valid(storage) {
            filter.map_times(storage, |time, _| {
                held.insert(C::owned_time(time));
            });
            filter.step_val(storage);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    // Inspect callbacks are 'static; shared buffers let the test retain
    // access while the dataflow owns the callbacks.
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::iterate::Variable;
    use timely::dataflow::operators::probe::Handle;
    use timely::dataflow::operators::probe::Probe;
    use timely::order::Product;

    use super::*;
    use crate::operators::flowlog_arrange;
    use crate::operators::flowlog_arrange_self;
    use crate::operators::flowlog_dedup;
    use crate::time::Epoch;
    use crate::time::EpochLoop;
    use crate::time::LexLoop;

    /// Rows of two epochs, arranged while the filter is open, wait for it,
    /// so keys that arrive after them, at the minimum time, still drop
    /// them.
    #[test]
    fn rows_wait_until_the_filter_is_complete() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let arranged = Handle::new();
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<(u64, char), diff::Mutable>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let rows = rows.arrange_by_key();
                rows.stream.clone().probe_with(&arranged);
                let seen = Rc::clone(&seen);
                lookup_absent(keys.arrange_by_self(), rows, "Lookup", |key, val| {
                    (*key, *val)
                })
                .inspect(move |update| seen.borrow_mut().push(*update));
                (source, blocked)
            });
            source.update((3, 'c'), 1);
            source.advance_to(1);
            source.flush();
            worker.step_while(|| arranged.less_than(&1));
            source.update((1, 'a'), 1);
            source.update((2, 'b'), 1);
            source.close();
            worker.step_while(|| !arranged.done());
            worker.step();
            assert!(seen.borrow().is_empty(), "rows wait for the filter");
            blocked.update(2, diff::Static);
            blocked.update(3, diff::Static);
            blocked.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, vec![((1, 'a'), 1, 1)]);
    }

    /// Rows are decided as the filter's frontier passes them. Of one batch
    /// with rows at epochs 0, 1 and 2, the row at 0 is out, once, while the
    /// filter is open, and keys 0 and 2 at epoch 1 drop the others before
    /// `logic`, which divides by the key, sees them.
    #[test]
    fn rows_are_decided_as_the_filter_frontier_passes_them() {
        let actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<(u64, char), diff::Static>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let seen = Rc::clone(&seen);
                lookup_absent(
                    keys.arrange_by_self(),
                    rows.arrange_by_key(),
                    "Lookup",
                    |key, val| (100 / key, *val),
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (source, blocked)
            });
            source.update((2, 'a'), diff::Static);
            source.advance_to(1);
            source.update((2, 'b'), diff::Static);
            source.advance_to(2);
            source.update((0, 'c'), diff::Static);
            source.close();
            blocked.advance_to(1);
            blocked.update(0, diff::Static);
            blocked.update(2, diff::Static);
            blocked.flush();
            worker.step_while(|| probe.less_than(&1));
            let decided = [((50, 'a'), 0, diff::Static)];
            assert_eq!(*seen.borrow(), decided, "epoch 0 is decided");
            blocked.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, [((50, 'a'), 0, diff::Static)]);
    }

    /// A key is held from the earliest time of any of its values: key 1,
    /// held through `b` at epoch 0 and `a` at epoch 2, drops its row at
    /// epoch 1, while key 2, held only from epoch 2, keeps its row at 1 and
    /// drops the one at 3.
    #[test]
    fn a_key_is_held_through_any_of_its_values() {
        let actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<(u64, char), diff::Static>();
                let (blocked, pairs) = scope.new_collection::<(u64, char), diff::Static>();
                let seen = Rc::clone(&seen);
                lookup_absent(
                    flowlog_arrange(pairs, "Filter"),
                    flowlog_arrange(rows, "Source"),
                    "Lookup",
                    |key, val| (*key, *val),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                (source, blocked)
            });
            blocked.update_at((1, 'a'), 2, diff::Static);
            blocked.update_at((1, 'b'), 0, diff::Static);
            blocked.update_at((2, 'c'), 2, diff::Static);
            source.update_at((1, 'x'), 1, diff::Static);
            source.update_at((2, 'y'), 1, diff::Static);
            source.update_at((2, 'z'), 3, diff::Static);
            source.close();
            blocked.close();
            while worker.step() {}
            seen.take()
        });
        assert_eq!(actual, [((2, 'y'), 1, diff::Static)]);
    }

    /// A batch stamped at the incomparable times (0, 1) and (1, 0) decides
    /// its rows at each apart: key 1, held from (0, 1), drops its row there
    /// but not at (1, 0), and while the filter stands at (1, 0) the rows at
    /// (0, 1) are decided and the one at (1, 0) waits. It is the only row
    /// that waits, so nothing else keeps the batch to read it again.
    #[test]
    fn rows_at_incomparable_times_are_decided_apart() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut left, mut right, mut blocked) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (left, early) = scope.new_collection::<(u64, char), diff::Static>();
                let (right, late) = scope.new_collection::<(u64, char), diff::Static>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let seen = Rc::clone(&seen);
                scope.scoped::<EpochLoop, _, _>("Iterative", |inner| {
                    // Moves epoch 0, stamp included, on to (0, 1).
                    let next = |time: &EpochLoop| Product::new(time.outer, 1);
                    let rows = early.enter(inner).delay(next).concat(late.enter(inner));
                    lookup_absent(
                        keys.enter(inner).delay(next).arrange_by_self(),
                        rows.arrange_by_key(),
                        "Lookup",
                        |key, val| (*key, *val),
                    )
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                });
                (left, right, blocked)
            });
            left.update((1, 'a'), diff::Static);
            left.update((2, 'b'), diff::Static);
            left.flush();
            // Advanced first, so that the rows are sent stamped at epoch 1.
            right.advance_to(1);
            right.flush();
            right.update((1, 'c'), diff::Static);
            right.close();
            blocked.update(1, diff::Static);
            blocked.advance_to(1);
            blocked.flush();
            // The rows reach the source's arrangement while `left` holds it
            // at epoch 0, so closing `left` retires both times in one batch.
            for _ in 0..10 {
                worker.step();
            }
            left.close();
            worker.step_while(|| probe.less_equal(&Product::new(0, 1)));
            let decided = [((2, 'b'), Product::new(0, 1), diff::Static)];
            assert_eq!(*seen.borrow(), decided, "(0, 1) is decided");
            blocked.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                ((1, 'c'), Product::new(1, 0), diff::Static),
                ((2, 'b'), Product::new(0, 1), diff::Static),
            ]
        );
    }

    /// Rows still to come hold back the filter's compaction: while the
    /// source stands at epoch 5, keys arrive at epochs 0 to 9 and merge
    /// into the filter as it compacts, and rows at 5 that come afterwards
    /// see exactly the keys of epochs up to 5.
    #[test]
    fn rows_still_to_come_hold_back_the_filter_compaction() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<(u64, char), diff::Static>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let seen = Rc::clone(&seen);
                lookup_absent(
                    flowlog_arrange_self(keys, "Filter"),
                    flowlog_arrange(rows, "Source"),
                    "Lookup",
                    |key, val| (*key, *val),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                (source, blocked)
            });
            source.advance_to(5);
            source.flush();
            // One batch an epoch, each merged into the filter under whatever
            // compaction the lookup allows by then.
            for epoch in 0..10 {
                blocked.update(u64::from(epoch), diff::Static);
                blocked.advance_to(epoch + 1);
                blocked.flush();
                for _ in 0..10 {
                    worker.step();
                }
            }
            for key in 0..10 {
                source.update((key, 'x'), diff::Static);
            }
            source.close();
            blocked.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(
            actual,
            vec![
                ((6, 'x'), 5, diff::Static),
                ((7, 'x'), 5, diff::Static),
                ((8, 'x'), 5, diff::Static),
                ((9, 'x'), 5, diff::Static),
            ]
        );
    }

    /// Each worker looks its rows up in its own part of the filter: on
    /// four workers, every even key is dropped and every odd key passes
    /// with both its values, once.
    #[test]
    fn every_worker_looks_up_its_own_keys() {
        const KEYS: u64 = 8;
        let mut actual: Vec<_> = timely::execute(timely::Config::process(4), |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<(u64, char), diff::Append>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let seen = Rc::clone(&seen);
                lookup_absent(
                    flowlog_arrange_self(keys, "Filter"),
                    flowlog_arrange(rows, "Source"),
                    "Lookup",
                    |key, val| (*key, *val),
                )
                .inspect(move |update| seen.borrow_mut().push(*update));
                (source, blocked)
            });
            if worker.index() == 0 {
                for key in 0..KEYS {
                    source.update((key, 'x'), diff::Append);
                    source.update((key, 'y'), diff::Append);
                    if key % 2 == 0 {
                        blocked.update(key, diff::Static);
                    }
                }
            }
            source.close();
            blocked.close();
            while worker.step() {}
            seen.take()
        })
        .unwrap()
        .join()
        .into_iter()
        .flat_map(Result::unwrap)
        .collect();
        actual.sort();
        assert_eq!(
            actual,
            vec![
                ((1, 'x'), 0, diff::Append),
                ((1, 'y'), 0, diff::Append),
                ((3, 'x'), 0, diff::Append),
                ((3, 'y'), 0, diff::Append),
                ((5, 'x'), 0, diff::Append),
                ((5, 'y'), 0, diff::Append),
                ((7, 'x'), 0, diff::Append),
                ((7, 'y'), 0, diff::Append),
            ]
        );
    }

    /// A filter entered into a loop drops rows of every iteration and
    /// epoch: walks `0 -> 1 -> ...` from epoch 0 and `4 -> 5 -> 6` from
    /// epoch 1 never pass the blocked node 3.
    #[test]
    fn entered_filter_drops_rows_of_every_iteration() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut seeds, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (seeds, starts) = scope.new_collection::<u64, diff::Static>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let keys = flowlog_arrange_self(keys, "Blocked");
                let walk = scope.scoped::<LexLoop, _, _>("Iterative", |inner| {
                    let (variable, feedback) = Variable::new(inner, (0, 1));
                    let steps = feedback.flat_map(|node| (node < 6).then_some((node + 1, ())));
                    let open = lookup_absent(
                        keys.enter(inner),
                        steps.arrange_by_key(),
                        "Lookup",
                        |node, _| *node,
                    );
                    let next = flowlog_dedup(starts.enter(inner).concat(open));
                    variable.set(next.clone());
                    next.leave(scope)
                });
                let seen = Rc::clone(&seen);
                walk.inspect(move |update| seen.borrow_mut().push(*update));
                (seeds, blocked)
            });
            blocked.update(3, diff::Static);
            blocked.close();
            seeds.update(0, diff::Static);
            seeds.advance_to(1);
            seeds.update(4, diff::Static);
            seeds.close();
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
                (4, 1, diff::Static),
                (5, 1, diff::Static),
                (6, 1, diff::Static),
            ]
        );
    }

    /// In a loop over a changing input, rows sit at epoch and iteration
    /// pairs, which are only partly ordered. Walks from seed 0 at epoch 0
    /// and seed 4 at epoch 1 stop before the blocked node 3, and removing
    /// seed 0 at epoch 2 retracts its walk.
    #[test]
    fn entered_filter_decides_rows_of_partly_ordered_times() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut seeds, mut blocked) = worker.dataflow::<Epoch, _, _>(|scope| {
                let (seeds, starts) = scope.new_collection::<u64, diff::Mutable>();
                let (blocked, keys) = scope.new_collection::<u64, diff::Static>();
                let keys = flowlog_arrange_self(keys, "Blocked");
                let walk = scope.scoped::<EpochLoop, _, _>("Iterative", |inner| {
                    let (variable, feedback) = Variable::new(inner, Product::new(0, 1));
                    let steps = feedback.flat_map(|node| (node < 6).then_some((node + 1, ())));
                    let open = lookup_absent(
                        keys.enter(inner),
                        steps.arrange_by_key(),
                        "Lookup",
                        |node, _| *node,
                    );
                    let next = flowlog_dedup(starts.enter(inner).concat(open));
                    variable.set(next.clone());
                    next.leave(scope)
                });
                let seen = Rc::clone(&seen);
                walk.inspect(move |update| seen.borrow_mut().push(*update));
                (seeds, blocked)
            });
            blocked.update(3, diff::Static);
            blocked.close();
            seeds.update(0, 1);
            seeds.advance_to(1);
            seeds.update(4, 1);
            seeds.advance_to(2);
            seeds.update(0, -1);
            seeds.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(
            actual,
            vec![
                (0, 0, 1),
                (0, 2, -1),
                (1, 0, 1),
                (1, 2, -1),
                (2, 0, 1),
                (2, 2, -1),
                (4, 1, 1),
                (5, 1, 1),
                (6, 1, 1),
            ]
        );
    }

    /// A filter fed by its own lookup, as negation through recursion makes
    /// it, completes only after the lookup's rows, so they are decided
    /// iteration by iteration rather than wait for it. With
    /// `Open(n) :- Node(n), !Marked(n).` and `Marked(n) :- Seed(n) | Open(n).`
    /// the loop ends with seed 1 dropped and node 2 open.
    #[test]
    fn filter_fed_by_its_own_lookup_lets_the_loop_finish() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut nodes, mut seeds) = worker.dataflow::<u32, _, _>(|scope| {
                let (nodes, base) = scope.new_collection::<(u64, ()), diff::Static>();
                let (seeds, marks) = scope.new_collection::<u64, diff::Static>();
                let base = base.arrange_by_key();
                let open = scope.scoped::<LexLoop, _, _>("Iterative", |inner| {
                    let (variable, reached) = Variable::new(inner, (0, 1));
                    let marked = flowlog_arrange_self(marks.enter(inner).concat(reached), "Marked");
                    let next = flowlog_dedup(lookup_absent(
                        marked,
                        base.enter(inner),
                        "Lookup",
                        |node, _| *node,
                    ));
                    variable.set(next.clone());
                    next.leave(scope)
                });
                let seen = Rc::clone(&seen);
                open.inspect(move |update| seen.borrow_mut().push(*update));
                (nodes, seeds)
            });
            nodes.update((1, ()), diff::Static);
            nodes.update((2, ()), diff::Static);
            nodes.close();
            seeds.update(1, diff::Static);
            seeds.close();
            while worker.step() {}
            seen.take()
        });
        actual.sort();
        assert_eq!(actual, vec![(2, 0, diff::Static)]);
    }
}
