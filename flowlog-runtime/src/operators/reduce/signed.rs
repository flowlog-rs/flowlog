//! Signed building blocks: Differential Dataflow's `reduce_abelian`,
//! keeping each key's current answer and replacing it through insertions
//! and retractions, so the answers are `diff::Mutable` whatever the
//! input: an append group only grows, but its answer still changes.
//!
//! - [`reduce`] re-reads all of a key's arranged entries whenever one
//!   changes. It works for any input and any reduction, retractions
//!   included.
//! - [`replace`] reads a key's accumulated weight instead, one entry once
//!   compacted. It needs weights that only add up, or whose reduction is
//!   invertible.
//! - [`defaults`] answers one default per group.

use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::ValBuilder;
use differential_dataflow::trace::implementations::ValSpine;
use timely::progress::Timestamp;

use super::semiring::Semiring;
use crate::diff;

/// The accumulation that the sums of some keys start from, their
/// reduction's empty group: the keys of groups whose default answers it.
pub(in crate::operators) struct Seed<S, K> {
    /// The sum a seeded key starts from; finished, it is the key's default.
    pub(in crate::operators) empty: S,
    /// The keys it seeds.
    pub(in crate::operators) keys: SeededKeys<K>,
}

/// The keys a [`Seed`] seeds.
pub(in crate::operators) enum SeededKeys<K> {
    /// Every key, as every key is a group's.
    Every,
    /// Only the one given.
    One(K),
}

impl<S, K: PartialEq> Seed<S, K> {
    /// Returns the accumulation `key`'s sum starts from, if `key` is
    /// seeded.
    fn empty_for(&self, key: &K) -> Option<&S> {
        let seeded = match &self.keys {
            SeededKeys::Every => true,
            SeededKeys::One(one) => one == key,
        };
        seeded.then_some(&self.empty)
    }
}

/// Reduces each key's arranged entries to its answer, read back as a row
/// through `merge`: the sum of their contributions, each entry counting
/// once whatever its count. A key that `seed` seeds is a group whose
/// default answers the seed's empty accumulation: its sum starts from it,
/// and its answer retracts the default. Any other key's sum starts from its first
/// entry. So each sum is the one a presence aggregate's weights accumulate
/// to.
///
/// # Panics
///
/// During execution, panics if `reduce_abelian` reduces a key without
/// entries.
#[must_use]
pub(in crate::operators) fn reduce<'scope, Tr, T, K, V, S, O>(
    arranged: Arranged<'scope, Tr>,
    name: &str,
    contribute: impl Fn(&V) -> S + 'static,
    seed: Option<Seed<S, K>>,
    mut merge: impl FnMut(K, S::Value) -> O + 'static,
) -> VecCollection<'scope, T, O, diff::Mutable>
where
    Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = T>,
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    S: Semiring + ExchangeData,
    O: Data,
{
    arranged
        .reduce_abelian::<_, ValBuilder<K, S::Value, T, diff::Mutable>, ValSpine<K, S::Value, T, diff::Mutable>, _, _>(
            name,
            move |key, input, output| {
                // This callback describes the desired current output;
                // reduce_abelian emits its difference from the old output.
                let empty = seed.as_ref().and_then(|seed| seed.empty_for(key));
                let mut contributions = input.iter().map(|(value, _)| contribute(value));
                // reduce_abelian reduces only a key with entries, so a key
                // without a seed starts from its first entry.
                let mut accumulated = match empty {
                    Some(empty) => empty.clone(),
                    None => contributions
                        .next()
                        .expect("reduce_abelian reduces only a key with entries"),
                };
                for contribution in contributions {
                    accumulated.plus_equals(&contribution);
                }
                output.push((accumulated.finish(), 1));
                // For default z, the group's default contributes (z, +1). The
                // desired reduced state and their combined result are:
                //
                //   Input group   Reduced state          Combined result
                //   empty         {}                     {(z, +1)}
                //   answer v      {(v, +1), (z, -1)}      {(v, +1)}
                //   answer z      {(z, +1), (z, -1)}      {(z, +1)}
                //
                // For 5 -> 6, the unchanged (z, -1) cancels inside reduce;
                // only (5, -1) and (6, +1) are emitted. When input empties,
                // reduce retracts its entire old state without calling this
                // closure, restoring z. A real result equal to z also leaves
                // exactly one copy of z in the combined set.
                if let Some(empty) = empty {
                    output.push((empty.clone().finish(), -1));
                }
            },
            push,
        )
        .as_collection(move |key, value| merge(key.clone(), value.clone()))
}

/// Emits one answer per key from its accumulated weight through `merge`,
/// retracting the previous answer when it changes. A key whose weight
/// `is_zero` has no answer.
#[must_use]
pub(in crate::operators) fn replace<'scope, T, K, S, O>(
    weights: VecCollection<'scope, T, K, S>,
    name: &str,
    mut merge: impl FnMut(K, S::Value) -> O + 'static,
) -> VecCollection<'scope, T, O, diff::Mutable>
where
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    S: Semiring + ExchangeData,
    O: Data,
{
    // Arrange contributions as weights so compaction can merge history.
    // Each update reads the remaining weights instead of all prior rows,
    // at the cost of a separate arrangement for the accumulated state.
    weights
        .arrange_by_self_named(name)
        .reduce_abelian::<_, ValBuilder<K, S::Value, T, diff::Mutable>, ValSpine<K, S::Value, T, diff::Mutable>, _, _>(
            name,
            // A key's one value, `()`, carries its accumulated weight;
            // a key without one is not reduced.
            |_key, input, output| output.push((input[0].1.clone().finish(), 1)),
            push,
        )
        .as_collection(move |key, value| merge(key.clone(), value.clone()))
}

/// Answers `default` once for each key of `groups`, however many times
/// the arrangement holds it, read back as a row through `merge`.
#[must_use]
pub(in crate::operators) fn defaults<'scope, Tr, T, K, C, O>(
    groups: Arranged<'scope, Tr>,
    name: &str,
    default: C,
    mut merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'scope, T, O, diff::Mutable>
where
    Tr: TraceReader<Time = T, Batch: Navigable> + 'static,
    for<'a> BatchCursor<Tr>: Cursor<Key<'a> = &'a K, Time = T>,
    T: Timestamp + Lattice,
    K: ExchangeData + Hashable,
    C: ExchangeData,
    O: Data,
{
    groups
        .reduce_abelian::<_, ValBuilder<K, C, T, diff::Mutable>, ValSpine<K, C, T, diff::Mutable>, _, _>(
            name,
            move |_key, _input, output| output.push((default.clone(), 1)),
            push,
        )
        .as_collection(move |key, value| merge(key.clone(), value.clone()))
}

/// Moves a key's reduced updates into the batch builder's input. Every
/// `reduce_abelian` takes this closure, and Differential Dataflow's own
/// `reduce`, `count` and `threshold` pass this same one.
fn push<K, C, T>(
    buffer: &mut Vec<((K, C), T, diff::Mutable)>,
    key: &K,
    updates: &mut Vec<(C, T, diff::Mutable)>,
) where
    K: Clone,
{
    buffer.clear();
    buffer.extend(
        updates
            .drain(..)
            .map(|(value, time, diff)| ((key.clone(), value), time, diff)),
    );
}
