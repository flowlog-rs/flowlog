//! An arrangement kept as a set, by the rule its weight gives.
//!
//! [`flowlog_arrange_set`] is differential's `arrange_core`
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
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEventBuilder;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::Batcher;
use differential_dataflow::trace::Builder;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Description;
use differential_dataflow::trace::ExertionLogic;
use differential_dataflow::trace::Trace;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::KeySpine;
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
