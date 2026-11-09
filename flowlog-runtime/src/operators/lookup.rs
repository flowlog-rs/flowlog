//! Lookups: rows that read an arrangement at their key, without being
//! arranged themselves.
//!
//! A join arranges both inputs and answers a change on either side. A
//! lookup answers only its rows, and only reads the arrangement. It
//! follows three rules:
//!
//! - A row at time `t` sees the updates at times at or before `t`, and is
//!   answered at `t`.
//! - A row waits until the arrangement's frontier passes `t`, so that no
//!   update it sees is still to come.
//! - Each activation opens one cursor on the arrangement, and a sorted run
//!   of rows seeks it in key order, so the run reads each key once.
//!
//! [`lookup_absent`] reads its rows from another arrangement's batches,
//! which are already sorted by key and partitioned as the looked-up
//! arrangement is, and keeps the rows whose key is absent. It answers the
//! antijoin of a static filter.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::VecCollection;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::BatchDiff;
use differential_dataflow::trace::BatchVal;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::trace::implementations::containers::BatchContainer;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::order::PartialOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;

use crate::diff;

/// Keeps the rows of `source` whose key `filter` lacks, each mapped
/// through `logic` and emitted at its own time and weight.
///
/// A row at time `t` is kept iff `filter` holds its key at no time at or
/// before `t`. Keys never leave a static filter, so a row whose key is
/// held is dropped at once, and any other row waits for `filter`'s
/// frontier to pass `t`. `logic` never sees a dropped row, and maps a key
/// and value once for all the times kept with it. A lookup emits no more
/// rows than it reads, so it answers a ready batch whole, as
/// `as_collection` reads one.
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
    let minimum = Tr1::Time::minimum();
    let mut filter_trace = filter.trace;
    assert!(
        filter_trace.get_logical_compaction().less_equal(&minimum),
        "rows compare with the exact times of their key, so the filter's \
         logical compaction must stay at the minimum"
    );
    // Merging batches keeps the filter's times, so its physical compaction
    // is released.
    filter_trace.set_physical_compaction(Antichain::new().borrow());
    let mut filter_trace = Some(filter_trace);
    // Rows come from the source's batch stream, as `as_collection` reads
    // them, so its trace handle goes now rather than hold back the
    // source's compaction.
    drop(source.trace);

    source
        .stream
        .binary_frontier(filter.stream, Pipeline, Pipeline, name, move |_, _| {
            // Source batches, each with capabilities at the times of its
            // undecided rows.
            let mut pending = Vec::new();
            let mut held = Antichain::new();
            let mut waiting = Antichain::new();
            move |(source_input, source_frontier), (filter_input, filter_frontier), output| {
                // The filter is read through its trace, but its stream must
                // still be drained for its frontier to advance.
                filter_input.for_each(|_, _| {});
                source_input.for_each(|capability, spans| {
                    pending.push((capability.retain_stamp(0), std::mem::take(spans)));
                });
                let mut reader = None;
                pending.retain_mut(|(capabilities, spans)| {
                    // Every row of the batch waits, so it is not read.
                    if capabilities
                        .iter()
                        .all(|c| filter_frontier.less_equal(c.time()))
                    {
                        return true;
                    }
                    let (filter, storage) = reader.get_or_insert_with(|| {
                        let batches = filter_trace
                            .as_mut()
                            .expect("the filter is held while rows wait")
                            .batches_through(Antichain::new().borrow())
                            .expect("the filter reads through the empty frontier");
                        cursor_list(batches)
                    });
                    waiting.clear();
                    let mut session = output.session(&*capabilities);
                    for batch in spans.iter().filter_map(|span| span.inner.as_ref()) {
                        filter.rewind_keys(storage);
                        let mut cursor = batch.cursor();
                        while let Some(key) = cursor.get_key(batch) {
                            times_held(filter, storage, key, &mut held);
                            // A key held from the minimum drops every row.
                            while !held.less_equal(&minimum)
                                && let Some(val) = cursor.get_val(batch)
                            {
                                let mut kept = None;
                                cursor.map_times(batch, |time, diff| {
                                    let time = <BatchCursor<Tr2> as Cursor>::owned_time(time);
                                    // A row before every capability was
                                    // decided in an earlier activation.
                                    let decided =
                                        !capabilities.iter().any(|c| c.time().less_equal(&time));
                                    if decided || held.less_equal(&time) {
                                        return;
                                    }
                                    if filter_frontier.less_equal(&time) {
                                        waiting.insert(time);
                                    } else {
                                        let kept = kept.get_or_insert_with(|| logic(key, val));
                                        let diff = <BatchCursor<Tr2> as Cursor>::owned_diff(diff);
                                        session.give((kept.clone(), time, diff));
                                    }
                                });
                                cursor.step_val(batch);
                            }
                            cursor.step_key(batch);
                        }
                    }
                    drop(session);
                    capabilities.downgrade(waiting.elements());
                    !waiting.is_empty()
                });
                // No row waits and none can come, so the filter is done.
                if pending.is_empty() && source_frontier.is_empty() {
                    filter_trace = None;
                }
            }
        })
        .as_collection()
}

/// Collects into `held` the times from which `filter` holds `key`: those
/// of its updates, as a static filter's updates are presence. The cursor
/// stops at `key` or past it, so between rewinds the keys sought must
/// increase.
fn times_held<'a, C>(
    filter: &mut C,
    storage: &'a C::Storage,
    key: C::Key<'a>,
    held: &mut Antichain<C::Time>,
) where
    C: Cursor<Diff = diff::Static>,
{
    held.clear();
    filter.seek_key(storage, key);
    if filter.get_key(storage) == Some(key) {
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
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::iterate::Variable;
    use timely::dataflow::operators::probe::Handle;
    use timely::dataflow::operators::probe::Probe;
    use timely::order::Product;

    use super::*;
    use crate::operators::dedup::flowlog_dedup;
    use crate::operators::flowlog_arrange;
    use crate::operators::flowlog_arrange_self;
    use crate::time::Epoch;
    use crate::time::EpochLoop;
    use crate::time::LexLoop;

    type Row = (u64, char);

    /// Rows of two epochs, arranged while the filter is open, wait for it,
    /// so keys that arrive after them, at the minimum time, still drop
    /// them.
    #[test]
    fn rows_wait_until_the_filter_is_complete() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let arranged = Handle::new();
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<Row, diff::Mutable>();
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
                let (source, rows) = scope.new_collection::<Row, diff::Static>();
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

    /// Each worker looks its rows up in its own part of the filter: on
    /// four workers, every even key is dropped and every odd key passes
    /// with both its values, once.
    #[test]
    fn every_worker_looks_up_its_own_keys() {
        const KEYS: u64 = 64;
        let mut actual: Vec<_> = timely::execute(timely::Config::process(4), |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut source, mut blocked) = worker.dataflow::<u32, _, _>(|scope| {
                let (source, rows) = scope.new_collection::<Row, diff::Append>();
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
        let odd = (1..KEYS)
            .step_by(2)
            .flat_map(|key| [(key, 'x'), (key, 'y')])
            .map(|row| (row, 0, diff::Append));
        assert_eq!(actual, odd.collect::<Vec<_>>());
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
