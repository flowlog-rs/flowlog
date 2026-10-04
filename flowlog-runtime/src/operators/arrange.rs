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
//! signed input dedup: differential's `arrange_core`
//! (differential-dataflow 0.25, `operators/arrange/arrangement.rs`) with
//! one step added between sealing a chain of updates and building its
//! batch: the weight's [`SetRewrite`], which reads the trace as it stands
//! below the chain and changes or drops the chain's updates so that the
//! trace stays a set. The trace then holds what the rewrite decided, and
//! so does the batch stream, which makes the trace the operator's only
//! state. The rest follows upstream line for line, so the operator keeps
//! the capability and batch discipline a trace relies on; a bump of that
//! crate is the time to re-read both side by side.

use std::rc::Rc;

use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEventBuilder;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::Batcher;
use differential_dataflow::trace::Builder;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::ExertionLogic;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::Trace;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::KeyBatcher;
use differential_dataflow::trace::implementations::KeyBuilder;
use differential_dataflow::trace::implementations::KeySpine;
use differential_dataflow::trace::implementations::ValBatcher;
use differential_dataflow::trace::implementations::ValBuilder;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use timely::container::ContainerBuilder;
use timely::container::PushInto;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::Operator;
use timely::order::PartialOrder;
use timely::order::TotalOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp;

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
            ValBatcher<K, V, T, diff::Append>,
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
            KeyBatcher<K, T, diff::Append>,
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

/// One chunk of a sealed chain: updates sorted by pair then time.
type Chunk<K, V, T, R> = Vec<Update<K, V, T, R>>;

/// Arranges `stream` by key, exchanging updates by the key's hash, as a
/// set: each sealed chain passes through `R`'s [`SetRewrite`] before it
/// becomes a batch.
pub(super) fn flowlog_arrange_set<'scope, K, V, T, R, B, Bu, Tr>(
    stream: Stream<'scope, T, Vec<Update<K, V, T, R>>>,
    name: &str,
) -> Arranged<'scope, TraceAgent<Tr>>
where
    K: ExchangeData + Hashable,
    V: ExchangeData,
    T: Timestamp,
    R: ExchangeData + SetRewrite<K, V, Tr>,
    B: Batcher<Output = Vec<Update<K, V, T, R>>, Time = T> + 'static,
    Bu: Builder<Time = T, Input = Vec<Update<K, V, T, R>>, Output = Tr::Batch>,
    Tr: Trace<Time = T> + 'static,
{
    let exchange = Exchange::new(move |update: &Update<K, V, T, R>| (update.0).0.hashed().into());
    let mut reader: Option<TraceAgent<Tr>> = None;
    let reader_ref = &mut reader;
    let scope = stream.scope();

    let stream = stream.unary_frontier(exchange, name, move |_capability, info| {
        let logger = scope
            .worker()
            .logger_for::<DifferentialEventBuilder>("differential/arrange")
            .map(Into::into);
        let mut batcher = B::new(logger.clone(), info.global_id);
        let mut capabilities = Antichain::<Capability<T>>::new();
        let activator = Some(scope.activator_for(Rc::clone(&info.address)));
        let mut empty_trace = Tr::new(info.clone(), logger.clone(), activator);
        if let Some(exert_logic) = scope
            .worker()
            .config()
            .get::<ExertionLogic>("differential/default_exert_logic")
            .cloned()
        {
            empty_trace.set_exert_logic(exert_logic);
        }
        let (reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);
        // The rewrite's own view of the trace. Its compaction follows the
        // sealed batches so the trace can still compact.
        let mut lookup = reader_local.clone();
        *reader_ref = Some(reader_local);

        let mut prev_frontier = Antichain::from_elem(T::minimum());
        let mut chunker = ContainerChunker::<Vec<Update<K, V, T, R>>>::default();

        move |(input, frontier), output| {
            input.for_each(|cap, data| {
                capabilities.insert(cap.retain(0));
                chunker.push_into(data);
                while let Some(chunk) = chunker.extract() {
                    batcher.push_into(std::mem::take(chunk));
                }
            });

            assert!(PartialOrder::less_equal(
                &prev_frontier.borrow(),
                &frontier.frontier()
            ));

            if prev_frontier.borrow() != frontier.frontier() {
                while let Some(chunk) = chunker.finish() {
                    batcher.push_into(std::mem::take(chunk));
                }

                if capabilities
                    .elements()
                    .iter()
                    .any(|c| !frontier.less_equal(c.time()))
                {
                    let mut upper = Antichain::new();
                    for (index, capability) in capabilities.elements().iter().enumerate() {
                        if !frontier.less_equal(capability.time()) {
                            upper.clear();
                            for time in frontier.frontier().iter() {
                                upper.insert(time.clone());
                            }
                            for other_capability in &capabilities.elements()[(index + 1)..] {
                                upper.insert(other_capability.time().clone());
                            }

                            let (mut chain, description) = batcher.seal(upper.clone());
                            R::rewrite(&mut lookup, &mut chain, &description);
                            chain.retain(|chunk| !chunk.is_empty());
                            let batch = Bu::seal(&mut chain, description);

                            writer.insert(batch.clone(), Some(capability.time().clone()));
                            output.session(&capabilities.elements()[index]).give(batch);
                        }
                    }

                    let mut new_capabilities = Antichain::new();
                    for time in batcher.frontier().iter() {
                        if let Some(capability) = capabilities
                            .elements()
                            .iter()
                            .find(|c| c.time().less_equal(time))
                        {
                            new_capabilities.insert(capability.delayed(time));
                        } else {
                            panic!("failed to find capability");
                        }
                    }
                    capabilities = new_capabilities;
                } else {
                    let _ = batcher.seal(frontier.frontier().to_owned());
                    writer.seal(frontier.frontier().to_owned());
                }

                prev_frontier.clear();
                prev_frontier.extend(frontier.frontier().iter().cloned());
                lookup.set_logical_compaction(prev_frontier.borrow());
                lookup.set_physical_compaction(prev_frontier.borrow());
            }

            writer.exert();
        }
    });

    Arranged {
        stream,
        trace: reader.unwrap(),
    }
}

// =============================================================================
// SetRewrite
// =============================================================================

/// How a weight keeps an arrangement a set: a rewrite of each sealed chain
/// against the trace below it, before the chain becomes a batch.
pub(super) trait SetRewrite<K, V, Tr: TraceReader>: Semigroup + Sized {
    /// Rewrites `chain`, sorted by pair then time and consolidated, against
    /// `lookup`. The trace is complete below `description`'s lower bound
    /// and the handle's compaction never passes it, so a cursor through
    /// that bound is always available. The chunks must stay sorted; empty
    /// chunks are removed afterwards.
    fn rewrite(
        lookup: &mut TraceAgent<Tr>,
        chain: &mut [Chunk<K, V, Tr::Time, Self>],
        description: &Description<Tr::Time>,
    );
}

/// A signed arrangement is a set when it holds each key's membership: a
/// positive net on an absent key becomes `+1` and makes it present, a
/// negative net on a present key becomes `-1` and makes it absent, and
/// anything else is dropped. A key's updates in one chain apply in time
/// order. Membership is read through one merged cursor over the batches
/// below the chain, as `threshold_total` reads its counts; the clock is
/// total so that every one of those batches lies before the chain and
/// their weights sum to the key's count.
impl<K, T> SetRewrite<K, (), KeySpine<K, T, diff::Mutable>> for diff::Mutable
where
    K: ExchangeData,
    T: Timestamp + Lattice + TotalOrder,
{
    fn rewrite(
        lookup: &mut TraceAgent<KeySpine<K, T, diff::Mutable>>,
        chain: &mut [Chunk<K, (), T, diff::Mutable>],
        description: &Description<T>,
    ) {
        let (mut cursor, storage) = lookup
            .cursor_through(description.lower().borrow())
            .expect("the lookup handle's compaction never passes a batch's lower bound");
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
    fn rewrite(
        lookup: &mut TraceAgent<Tr>,
        chain: &mut [Chunk<K, V, T, diff::Append>],
        description: &Description<T>,
    ) {
        let mut batches = Vec::new();
        lookup.map_batches(|batch| {
            if PartialOrder::less_equal(batch.upper(), description.lower()) {
                batches.push(batch.clone());
            }
        });
        let mut cursors: Vec<_> = batches
            .iter()
            .map(|batch| (batch.cursor(), batch))
            .collect();
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
