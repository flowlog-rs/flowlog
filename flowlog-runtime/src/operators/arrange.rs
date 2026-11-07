//! Arrangements for generated FlowLog rules, by key or by self, kept as
//! sets where the weight asks for it.
//!
//! [`flowlog_arrange`] and [`flowlog_arrange_self`] select by weight.
//! `diff::Static` and `diff::Mutable` arrange as differential does.
//! `diff::Append` arranges as a set: a pair the trace already holds is
//! dropped before it becomes a batch, so the trace holds each pair once,
//! at its first time.
//!
//! [`flowlog_arrange_set`] is the operator behind that and behind the
//! signed input dedup: differential's `arrange_core` driving a
//! [`SetBatcher`], which passes each extracted chain of updates through
//! the weight's [`SetRewrite`] before building its batch. The rewrite
//! reads the trace as it stands below the chain and changes or drops the
//! chain's updates so that the trace stays a set. The trace then holds
//! what the rewrite decided, and so does the batch stream, which makes the
//! trace the operator's only state.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::batcher::Batcher;
use differential_dataflow::batcher::Extract;
use differential_dataflow::batcher::merge::MergeBatcher;
use differential_dataflow::batcher::merge::Sealer;
use differential_dataflow::batcher::merge::vec::VecMerger;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::Trace;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::trace::implementations::ContainerChunker;
use differential_dataflow::trace::implementations::KeyBuilder;
use differential_dataflow::trace::implementations::KeySpine;
use differential_dataflow::trace::implementations::ValBuilder;
use differential_dataflow::trace::implementations::ValSpine;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::Exchange;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use crate::diff;

/// Arranges a `(key, value)` collection by key, under the name FlowLog
/// gives the step, as a set when its weight is `diff::Append`.
pub fn flowlog_arrange<C: FlowlogArrange>(collection: C, name: &str) -> C::Arranged {
    collection.arrange(name)
}

/// Arranges a key-only collection by itself, under the name FlowLog gives
/// the step, as a set when its weight is `diff::Append`.
pub fn flowlog_arrange_self<C: FlowlogArrangeSelf>(collection: C, name: &str) -> C::Arranged {
    collection.arrange_self(name)
}

// =============================================================================
// FlowlogArrange
// =============================================================================

/// Compile-time dispatch behind [`flowlog_arrange`].
pub trait FlowlogArrange: Sized {
    /// The arrangement, the same type whichever weight selects it.
    type Arranged;

    /// See [`flowlog_arrange`].
    fn arrange(self, name: &str) -> Self::Arranged;
}

/// Compile-time dispatch behind [`flowlog_arrange_self`].
pub trait FlowlogArrangeSelf: Sized {
    /// The arrangement, the same type whichever weight selects it.
    type Arranged;

    /// See [`flowlog_arrange_self`].
    fn arrange_self(self, name: &str) -> Self::Arranged;
}

/// Arranges a weight as differential does, with no rewrite.
macro_rules! flowlog_arrange {
    ($weight:ty) => {
        impl<'scope, T, K, V> FlowlogArrange for VecCollection<'scope, T, (K, V), $weight>
        where
            T: Timestamp + Lattice + Ord,
            K: ExchangeData + Hashable,
            V: ExchangeData,
        {
            type Arranged = Arranged<'scope, TraceAgent<ValSpine<K, V, T, $weight>>>;

            fn arrange(self, name: &str) -> Self::Arranged {
                self.arrange_by_key_named(name)
            }
        }

        impl<'scope, T, K> FlowlogArrangeSelf for VecCollection<'scope, T, K, $weight>
        where
            T: Timestamp + Lattice + Ord,
            K: ExchangeData + Hashable,
        {
            type Arranged = Arranged<'scope, TraceAgent<KeySpine<K, T, $weight>>>;

            fn arrange_self(self, name: &str) -> Self::Arranged {
                self.arrange_by_self_named(name)
            }
        }
    };
}

// A static collection is a set already: every update is at its scope's
// minimum time, where the batcher consolidates repeats. A signed one
// carries counts by design.
flowlog_arrange!(diff::Static);
flowlog_arrange!(diff::Mutable);

impl<'scope, T, K, V> FlowlogArrange for VecCollection<'scope, T, (K, V), diff::Append>
where
    T: Timestamp + Lattice + TotalOrder,
    K: ExchangeData + Hashable,
    V: ExchangeData,
{
    type Arranged = Arranged<'scope, TraceAgent<ValSpine<K, V, T, diff::Append>>>;

    fn arrange(self, name: &str) -> Self::Arranged {
        flowlog_arrange_set::<
            K,
            V,
            T,
            diff::Append,
            ValBuilder<K, V, T, diff::Append>,
            ValSpine<K, V, T, diff::Append>,
        >(self.inner, name)
    }
}

impl<'scope, T, K> FlowlogArrangeSelf for VecCollection<'scope, T, K, diff::Append>
where
    T: Timestamp + Lattice + TotalOrder,
    K: ExchangeData + Hashable,
{
    type Arranged = Arranged<'scope, TraceAgent<KeySpine<K, T, diff::Append>>>;

    fn arrange_self(self, name: &str) -> Self::Arranged {
        flowlog_arrange_set::<
            K,
            (),
            T,
            diff::Append,
            KeyBuilder<K, T, diff::Append>,
            KeySpine<K, T, diff::Append>,
        >(self.map(|key| (key, ())).inner, name)
    }
}

// =============================================================================
// Set arrangement
// =============================================================================

/// One update of a `(key, value)` collection.
type Update<K, V, T, R> = ((K, V), T, R);

/// One chunk of an extracted chain: updates sorted by pair then time.
type Chunk<K, V, T, R> = Vec<Update<K, V, T, R>>;

/// Differential's merge batcher for vector updates, with each extracted
/// chain handed back unbuilt.
type ChainBatcher<K, V, T, R> =
    MergeBatcher<ContainerChunker<Chunk<K, V, T, R>>, VecMerger<(K, V), T, R>, Unsealed>;

/// Arranges `stream` by key, exchanging updates by the key's hash, as a
/// set: each extracted chain passes through `R`'s [`SetRewrite`] before
/// `Bu` builds it into a batch.
pub(super) fn flowlog_arrange_set<'scope, K, V, T, R, Bu, Tr>(
    stream: Stream<'scope, T, Vec<Update<K, V, T, R>>>,
    name: &str,
) -> Arranged<'scope, TraceAgent<Tr>>
where
    K: ExchangeData + Hashable,
    V: ExchangeData,
    T: Timestamp + TotalOrder,
    R: ExchangeData + SetRewrite<K, V, Tr>,
    Bu: Sealer<Chunk<K, V, T, R>, Output: Into<Tr::Batch>> + 'static,
    Tr: Trace<Time = T> + 'static,
{
    let exchange = Exchange::new(move |update: &Update<K, V, T, R>| (update.0).0.hashed().into());
    // `arrange_core` builds the batcher before the trace it feeds, so the
    // batcher reaches that trace through a cell filled here, once both
    // exist and before the dataflow runs.
    let lookup = Rc::new(RefCell::new(None));
    let batcher_lookup = Rc::clone(&lookup);
    let arranged = arrange_core(stream, exchange, name, move |logger, operator_id| {
        SetBatcher::<K, V, T, R, Bu, Tr> {
            batcher: MergeBatcher::new(logger, operator_id),
            lookup: batcher_lookup,
            builder: PhantomData,
        }
    });
    let mut handle = arranged.trace.clone();
    // The rewrite reads whole spans, never a cut through the trace, so this
    // handle leaves merging to the trace's other readers.
    handle.set_physical_compaction(Antichain::new().borrow());
    *lookup.borrow_mut() = Some(handle);
    arranged
}

/// A [`ChainBatcher`] that runs `R`'s [`SetRewrite`] on each extracted
/// chain before `Bu` builds it into a batch.
struct SetBatcher<K, V, T, R, Bu, Tr>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Timestamp,
    R: ExchangeData + Semigroup,
    Tr: TraceReader,
{
    batcher: ChainBatcher<K, V, T, R>,
    /// The rewrite's own handle on the trace this batcher feeds.
    lookup: Rc<RefCell<Option<TraceAgent<Tr>>>>,
    builder: PhantomData<Bu>,
}

impl<K, V, T, R, Bu, Tr> Batcher<Vec<Update<K, V, T, R>>> for SetBatcher<K, V, T, R, Bu, Tr>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Timestamp + TotalOrder,
    R: ExchangeData + SetRewrite<K, V, Tr>,
    Bu: Sealer<Chunk<K, V, T, R>>,
    Tr: TraceReader<Time = T>,
{
    fn insert(&mut self, container: &mut Vec<Update<K, V, T, R>>) {
        self.batcher.insert(container);
    }
}

impl<K, V, T, R, Bu, Tr> Extract for SetBatcher<K, V, T, R, Bu, Tr>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Timestamp + TotalOrder,
    R: ExchangeData + SetRewrite<K, V, Tr>,
    Bu: Sealer<Chunk<K, V, T, R>>,
    Tr: TraceReader<Time = T>,
{
    type Time = T;
    type Output = Bu::Output;

    fn extract<'a>(
        &'a mut self,
        upper: AntichainRef<'_, T>,
    ) -> (Option<Bu::Output>, AntichainRef<'a, T>) {
        let (chain, retained) = self.batcher.extract(upper);
        let mut lookup = self.lookup.borrow_mut();
        let lookup = lookup
            .as_mut()
            .expect("flowlog_arrange_set fills the lookup before the dataflow runs");
        let batch = chain.and_then(|mut chain| {
            // `arrange_core` inserts this batch only after extraction
            // returns, so the trace contains only previously emitted updates.
            let mut below = Vec::new();
            lookup.map_spans(|span| below.extend(span.inner.clone()));
            R::rewrite(below, &mut chain);
            chain.retain(|chunk| !chunk.is_empty());
            Bu::seal(&mut chain)
        });
        // The rewrite reads counts and presence, never times, so this
        // handle lets the trace advance its times to `upper`. A frontier
        // advance with no update never reaches the batcher and leaves the
        // bound behind until one does. That costs nothing: every time in
        // the trace lies at or below the bound, and on a total clock advancing
        // them to it merges them as far as advancing to the frontier would.
        lookup.set_logical_compaction(upper);
        (batch, retained)
    }
}

/// A [`Sealer`] whose output is the extracted chain itself, so the
/// rewrite can run before the batch is built.
struct Unsealed;

impl<C> Sealer<C> for Unsealed {
    type Output = Vec<C>;

    fn seal(chain: &mut Vec<C>) -> Option<Vec<C>> {
        (!chain.is_empty()).then(|| std::mem::take(chain))
    }
}

// =============================================================================
// SetRewrite
// =============================================================================

/// How a weight keeps an arrangement a set: a rewrite of each extracted
/// chain against the trace below it, before the chain becomes a batch.
pub(super) trait SetRewrite<K, V, Tr: TraceReader>: Semigroup + Sized {
    /// Rewrites `chain`, sorted by pair then time and consolidated, against
    /// `below`: all previously emitted trace batches. Logical compaction
    /// may have advanced their update times to the chain's first time;
    /// rewrites must use counts and presence, not timestamp comparisons.
    /// The chunks must stay sorted; empty chunks are removed afterwards.
    fn rewrite(below: Vec<Tr::Batch>, chain: &mut [Chunk<K, V, Tr::Time, Self>]);
}

/// A signed arrangement is a set when it holds each key's membership: a
/// positive net on an absent key becomes `+1` and makes it present, a
/// negative net on a present key becomes `-1` and makes it absent, and
/// anything else is dropped. A key's updates in one chain apply in time
/// order. Membership is read through one merged cursor over the batches
/// below the chain, as `threshold_total` reads its counts; the clock is
/// total so that those batches contain the preceding updates and their
/// weights sum to the key's count.
impl<K, T> SetRewrite<K, (), KeySpine<K, T, diff::Mutable>> for diff::Mutable
where
    K: ExchangeData,
    T: Timestamp + Lattice + TotalOrder,
{
    fn rewrite(
        below: Vec<<KeySpine<K, T, diff::Mutable> as TraceReader>::Batch>,
        chain: &mut [Chunk<K, (), T, diff::Mutable>],
    ) {
        let (mut cursor, storage) = cursor_list(below);
        let mut last: Option<(K, bool)> = None;
        for chunk in chain.iter_mut() {
            for ((key, ()), _, diff) in chunk.iter_mut() {
                if !matches!(&last, Some((seen, _)) if seen == key) {
                    let mut count: i64 = 0;
                    cursor.seek_key(&storage, key);
                    if cursor.get_key(&storage) == Some(key) {
                        cursor.map_times(&storage, |_, d| count += i64::from(*d));
                    }
                    last = Some((key.clone(), count > 0));
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
}

/// An append arrangement is a set when it holds each pair once, at its
/// first time: an update whose pair the trace holds below the chain, or
/// that an earlier update of the chain announces, is dropped. The clock is
/// total so that "below the chain" is every earlier time.
///
/// Each batch below the chain is probed through its own cursor rather
/// than one merged cursor over all of them: `CursorList::seek_val`
/// forwards to every batch, including one whose keys are exhausted, which
/// indexes past that batch's offsets.
impl<K, V, T, Tr> SetRewrite<K, V, Tr> for diff::Append
where
    K: ExchangeData,
    V: ExchangeData,
    T: Timestamp + Lattice + TotalOrder,
    Tr: Trace<Time = T, Batch: Navigable + Clone> + 'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T>,
{
    fn rewrite(below: Vec<Tr::Batch>, chain: &mut [Chunk<K, V, T, diff::Append>]) {
        let mut cursors: Vec<_> = below.iter().map(|batch| (batch.cursor(), batch)).collect();
        let mut last: Option<(K, V)> = None;
        for chunk in chain.iter_mut() {
            chunk.retain(|((key, val), ..)| {
                if last.as_ref().is_some_and(|(k, v)| k == key && v == val) {
                    return false;
                }
                last = Some((key.clone(), val.clone()));
                !cursors.iter_mut().any(|(cursor, batch)| {
                    cursor.seek_key(batch, key);
                    cursor.get_key(batch) == Some(key) && {
                        cursor.seek_val(batch, val);
                        cursor.get_val(batch) == Some(val)
                    }
                })
            });
        }
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
    use crate::operators::flowlog_join;
    use crate::time::LexLoop;

    type Row = (u64, char);

    /// Runs `updates`, one inner vector per epoch, through
    /// `flowlog_arrange`, and returns the arranged collection's updates
    /// consolidated.
    fn arranged_updates<R>(updates: Vec<Vec<(Row, R)>>) -> Vec<(Row, u32, R)>
    where
        R: ExchangeData + Semigroup + Sync,
        for<'scope> VecCollection<'scope, u32, Row, R>:
            FlowlogArrange<Arranged = Arranged<'scope, TraceAgent<ValSpine<u64, char, u32, R>>>>,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, R>();
                let seen = Rc::clone(&seen);
                flowlog_arrange(rows, "Arrange")
                    .as_collection(|key, val| (*key, *val))
                    .inspect(move |update| seen.borrow_mut().push(update.clone()))
                    .probe_with(&probe);
                input
            });
            for (epoch, updates) in updates.into_iter().enumerate() {
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

    /// An append pair is arranged once, at its first epoch, however often
    /// later epochs or the same epoch announce it again; other values under
    /// the same key are kept.
    #[test]
    fn append_arranges_each_pair_once_at_its_first_time() {
        let actual = arranged_updates(vec![
            vec![
                ((1, 'a'), diff::Append),
                ((1, 'a'), diff::Append),
                ((2, 'b'), diff::Append),
            ],
            vec![((1, 'a'), diff::Append), ((1, 'c'), diff::Append)],
            vec![((2, 'b'), diff::Append), ((3, 'd'), diff::Append)],
        ]);
        assert_eq!(
            actual,
            vec![
                ((1, 'a'), 0, diff::Append),
                ((1, 'c'), 1, diff::Append),
                ((2, 'b'), 0, diff::Append),
                ((3, 'd'), 2, diff::Append),
            ]
        );
    }

    #[test]
    fn append_remembers_pairs_across_empty_epochs() {
        let actual = arranged_updates(vec![
            vec![((1, 'a'), diff::Append)],
            vec![],
            vec![],
            vec![((1, 'a'), diff::Append), ((1, 'b'), diff::Append)],
            vec![],
            vec![((1, 'b'), diff::Append)],
        ]);
        assert_eq!(
            actual,
            vec![((1, 'a'), 0, diff::Append), ((1, 'b'), 3, diff::Append)]
        );
    }

    /// A static collection arranges as differential does, with no rewrite:
    /// a pair announced at two epochs is held at both.
    #[test]
    fn static_arranges_every_announcement() {
        let actual = arranged_updates(vec![
            vec![((1, 'a'), diff::Static), ((1, 'a'), diff::Static)],
            vec![((1, 'a'), diff::Static)],
        ]);
        assert_eq!(
            actual,
            vec![((1, 'a'), 0, diff::Static), ((1, 'a'), 1, diff::Static)]
        );
    }

    /// A signed collection keeps its counts: arranging is differential's.
    #[test]
    fn signed_arranges_with_its_counts() {
        let actual = arranged_updates(vec![
            vec![((1, 'a'), 2), ((2, 'b'), 1)],
            vec![((1, 'a'), 1), ((2, 'b'), -1)],
        ]);
        assert_eq!(
            actual,
            vec![
                ((1, 'a'), 0, 2),
                ((1, 'a'), 1, 1),
                ((2, 'b'), 0, 1),
                ((2, 'b'), 1, -1)
            ]
        );
    }

    /// A pair announced twice inside one batch, at two times the frontier
    /// skipped past together, is still arranged once.
    #[test]
    fn append_keeps_the_first_of_two_times_in_one_batch() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<Row, diff::Append>();
                let seen = Rc::clone(&seen);
                flowlog_arrange(rows, "Arrange")
                    .as_collection(|key, val| (*key, *val))
                    .inspect(move |update| seen.borrow_mut().push(*update));
                input
            });
            input.update((1, 'a'), diff::Append);
            input.advance_to(1);
            input.update((1, 'a'), diff::Append);
            input.update((2, 'b'), diff::Append);
            input.advance_to(2);
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(
            actual,
            vec![((1, 'a'), 0, diff::Append), ((2, 'b'), 1, diff::Append)]
        );
    }

    /// A key-only append collection arranges by itself as a set too.
    #[test]
    fn append_arranges_keys_by_self_once() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, keys) = scope.new_collection::<u64, diff::Append>();
                let seen = Rc::clone(&seen);
                flowlog_arrange_self(keys, "Arrange")
                    .as_collection(|key, ()| *key)
                    .inspect(move |update| seen.borrow_mut().push(*update))
                    .probe_with(&probe);
                input
            });
            for (epoch, keys) in [vec![1, 2, 2], vec![1, 3], vec![3]].into_iter().enumerate() {
                for key in keys {
                    input.update(key, diff::Append);
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
        assert_eq!(
            actual,
            vec![
                (1, 0, diff::Append),
                (2, 0, diff::Append),
                (3, 1, diff::Append)
            ]
        );
    }

    /// The case the set arrangement exists for: a pair announced at two
    /// epochs, read as `+1` by a signed join, is retracted in full when the
    /// signed side deletes, because the join met it once.
    #[test]
    fn a_signed_join_against_a_set_arrangement_cancels_exactly() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let (mut rows, mut marks) = worker.dataflow::<u32, _, _>(|scope| {
                let (rows, pairs) = scope.new_collection::<(u64, ()), diff::Append>();
                let (marks, keys) = scope.new_collection::<(u64, ()), diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_join(
                    flowlog_arrange(keys, "Marks"),
                    flowlog_arrange(pairs, "Pairs"),
                    "Join",
                    |key, _, _| Some(*key),
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                (rows, marks)
            });
            rows.update((5, ()), diff::Append);
            marks.update((5, ()), 1);
            rows.advance_to(1);
            marks.advance_to(1);
            rows.update((5, ()), diff::Append);
            rows.advance_to(3);
            marks.advance_to(3);
            marks.update((5, ()), -1);
            rows.close();
            marks.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, vec![(5, 0, 1), (5, 3, -1)]);
    }

    /// The set arrangement compiles at the total clocks an append
    /// collection lives at, and the pass-through weights at every clock.
    #[test]
    fn every_supported_weight_and_clock_pairing_arranges() {
        fn admits<T: Timestamp + Lattice + Ord>()
        where
            VecCollection<'static, T, Row, diff::Static>: FlowlogArrange,
            VecCollection<'static, T, u64, diff::Static>: FlowlogArrangeSelf,
            VecCollection<'static, T, Row, diff::Mutable>: FlowlogArrange,
            VecCollection<'static, T, u64, diff::Mutable>: FlowlogArrangeSelf,
        {
        }
        fn admits_append<T: Timestamp + Lattice + TotalOrder>()
        where
            VecCollection<'static, T, Row, diff::Append>: FlowlogArrange,
            VecCollection<'static, T, u64, diff::Append>: FlowlogArrangeSelf,
        {
        }
        admits::<()>();
        admits::<u32>();
        admits::<Product<(), u16>>();
        admits::<Product<u32, u16>>();
        admits::<LexLoop>();
        admits_append::<u32>();
        admits_append::<LexLoop>();
    }
}
