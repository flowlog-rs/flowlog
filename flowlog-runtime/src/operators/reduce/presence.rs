//! Presence building blocks, for `diff::Static` and `diff::Append`:
//! contributions carried as [`Accumulator`] weights, which only add up.
//!
//! - [`lift`] maps a lattice's or a choice's rows to weighted keys;
//!   `fold::fold` does the same for an aggregate's arranged matches.
//! - [`settle`] turns the weights into `diff::Static` answers as they
//!   first appear or supersede the one before; `diff::Append` replaces
//!   its answers instead, through `signed::replace`.
//! - [`lower`] maps the settled weights back to rows.

use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use super::accumulator::Accumulator;
use crate::diff;
use crate::diff::Presence;
use crate::operators::map::flowlog_map;

/// Converts each row to a group key weighted by its contribution. A row
/// contributes each time it arrives, which an extreme merges as one.
#[must_use]
pub(in crate::operators) fn lift<'scope, T, D, K, V, S, R>(
    rows: VecCollection<'scope, T, D, R>,
    name: &str,
    contribute: impl Fn(&V) -> S + 'static,
    mut split: impl FnMut(D) -> (K, V) + 'static,
) -> VecCollection<'scope, T, K, S>
where
    T: Timestamp,
    D: Data,
    K: Data,
    S: Accumulator,
    R: Presence,
{
    flowlog_map(rows, name, move |row, time, _| {
        let (key, value) = split(row);
        std::iter::once((key, time, contribute(&value)))
    })
}

/// Settles weighted keys into answers: each group's accumulated weight,
/// emitted when it first appears and whenever it supersedes the answer
/// before.
#[must_use]
pub(in crate::operators) fn settle<'scope, T, K, S>(
    weights: VecCollection<'scope, T, K, S>,
) -> VecCollection<'scope, T, K, S>
where
    T: Timestamp + TotalOrder + Lattice,
    K: ExchangeData + Hashable,
    S: Accumulator + ExchangeData,
{
    // Unchanged weights carry no new answer. Suppressing them avoids
    // restating a contribution downstream at later timestamps.
    weights.threshold_semigroup(|_key, new, current| {
        current
            .is_none_or(|current| new.supersedes(current))
            .then(|| new.clone())
    })
}

/// Converts settled weights back into output rows with `diff::Static`.
pub(in crate::operators) fn lower<'scope, T, K, S, O>(
    collection: VecCollection<'scope, T, K, S>,
    name: &str,
    mut merge: impl FnMut(K, S::Value) -> O + 'static,
) -> VecCollection<'scope, T, O, diff::Static>
where
    T: Timestamp,
    K: Data,
    S: Accumulator,
    O: Data,
{
    flowlog_map(collection, name, move |key, time, accumulated| {
        std::iter::once((merge(key, accumulated.finish()), time, diff::Static))
    })
}
