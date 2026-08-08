//! Group-by aggregation for generated FlowLog rules.
//!
//! A call names an aggregation and how to cut a row apart and put one
//! back together; which semiring accumulates the column, and whether the
//! aggregate rides in the difference position or through an arrangement,
//! are internal.
//!
//! [`flowlog_reduce`] serves a rule under either weight. A recursive rule
//! under `Present` needs [`flowlog_reduce_leave`] as well, because
//! `Present` cannot retract a superseded aggregate: the group can only be
//! folded once the fixpoint is final, at the loop boundary. An `i32` reduce
//! withdraws its own previous answer and so needs no boundary.

mod semiring;

use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Present;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use differential_dataflow::trace::implementations::ValBuilder;
use differential_dataflow::trace::implementations::ValSpine;
use semiring::Largest;
use semiring::Mean;
use semiring::Scalar;
use semiring::Semiring;
use semiring::Smallest;
use semiring::Total;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

use crate::operators::map::flowlog_map;

// =========================================================================
// Operators
// =========================================================================

/// Groups rows by key and reduces each group under `aggregation`.
///
/// `split` cuts each row into its group-by key and the aggregated column;
/// `merge` rebuilds an output row from a key and the group's result.
/// `aggregation` is read for its type alone, which is what picks the
/// semiring. Output rows carry unit weights, so the result is a set again
/// and downstream operators see no trace of the accumulation.
pub fn flowlog_reduce<'scope, A, T, D, K, V, C, O, R>(
    collection: VecCollection<'scope, T, D, R>,
    name: &str,
    _aggregation: A,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'scope, T, O, R>
where
    A: Aggregation<V, C>,
    A::Semiring: ExchangeData,
    C: Scalar,
    T: Timestamp + Lattice,
    R: ReduceStrategy<T>,
    D: Data,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    O: Data,
{
    R::reduce(collection, name, A::contribute, split, merge)
}

/// Completes a `Present` aggregate across a recursive scope: the single
/// fold over every iteration's contributions, deferred to `leave`, where
/// the fixpoint is final.
///
/// Inside the scope each row's contribution is lifted into the semiring
/// weight; `leave` lands every iteration on one outer timestamp, and one
/// consolidation there is the whole fold. The in-scope [`flowlog_reduce`]
/// output this consumes exists for feedback; the answer is produced here.
pub fn flowlog_reduce_leave<'inner, 'outer, A, TInner, TOuter, D, K, V, C, O>(
    collection: VecCollection<'inner, TInner, D, Present>,
    outer: Scope<'outer, TOuter>,
    name: &str,
    _aggregation: A,
    split: impl FnMut(D) -> (K, V) + 'static,
    merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'outer, TOuter, O, Present>
where
    A: Aggregation<V, C>,
    C: Scalar,
    TInner: Timestamp + Refines<TOuter>,
    TOuter: Timestamp + Lattice,
    D: Data,
    K: ExchangeData + Hashable,
    V: Data,
    A::Semiring: ExchangeData,
    O: Data,
{
    lower(
        lift(collection, name, A::contribute, split)
            .leave(outer)
            .consolidate(),
        name,
        merge,
    )
}

/// Shared last half of the `Present` pipeline: the settled aggregate comes
/// back out of the difference position and into a row, which is a set member
/// again.
fn lower<'scope, T, K, S, O>(
    collection: VecCollection<'scope, T, K, S>,
    name: &str,
    mut merge: impl FnMut(K, S::Value) -> O + 'static,
) -> VecCollection<'scope, T, O, Present>
where
    T: Timestamp,
    K: Data,
    S: Semiring,
    O: Data,
{
    flowlog_map(collection, name, move |key, time, aggregate| {
        std::iter::once((merge(key, aggregate.finish()), time, Present))
    })
}

/// Shared first half of the `Present` pipeline: one contribution per row,
/// keyed by group, carried in the difference position.
fn lift<'scope, T, D, K, V, S>(
    collection: VecCollection<'scope, T, D, Present>,
    name: &str,
    contribute: impl Fn(&V) -> S + 'static,
    mut split: impl FnMut(D) -> (K, V) + 'static,
) -> VecCollection<'scope, T, K, S>
where
    T: Timestamp,
    D: Data,
    K: Data,
    S: Semiring,
{
    flowlog_map(collection, name, move |row, time, _| {
        let (key, value) = split(row);
        std::iter::once((key, time, contribute(&value)))
    })
}

// =========================================================================
// Strategies
// =========================================================================

/// How a weight family computes a group-by aggregate.
///
/// Not a performance choice: `Present` has no inverse, so it cannot retract
/// a superseded aggregate, and the only way to accumulate under it is where
/// consolidation will find it, in the weight position. `i32` can negate, so
/// it takes Differential Dataflow's own reduce and lets it withdraw the
/// previous answer.
///
/// Parameterized by the timestamp so each impl asks of the clock only what
/// it needs: the `Present` half thresholds and wants a total order, the
/// `i32` half arranges and does not. That is what lets an incremental rule
/// aggregate inside a loop, where the clock is partially ordered.
pub trait ReduceStrategy<T: Timestamp + Lattice>: Semigroup + Sized {
    /// See [`flowlog_reduce`].
    fn reduce<'scope, D, K, V, S, O>(
        collection: VecCollection<'scope, T, D, Self>,
        name: &str,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, Self>
    where
        D: Data,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data;
}

impl<T: Timestamp + TotalOrder + Lattice> ReduceStrategy<T> for Present {
    fn reduce<'scope, D, K, V, S, O>(
        collection: VecCollection<'scope, T, D, Self>,
        name: &str,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, Self>
    where
        D: Data,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
    {
        let thresholded =
            lift(collection, name, contribute, split).threshold_semigroup(|_key, new, current| {
                match current {
                    // An aggregate that does not supersede the one already
                    // reported is not news: emitting it would restate a row
                    // downstream already has.
                    Some(current) => new.supersedes(current).then_some(*new),
                    None => (!new.is_zero()).then_some(*new),
                }
            });
        lower(thresholded, name, merge)
    }
}

impl<T: Timestamp + Lattice> ReduceStrategy<T> for i32 {
    fn reduce<'scope, D, K, V, S, O>(
        collection: VecCollection<'scope, T, D, Self>,
        name: &str,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        mut merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, Self>
    where
        D: Data,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
    {
        collection
            .map(split)
            .arrange_by_key()
            .reduce_abelian::<_, ValBuilder<K, S::Value, T, i32>, ValSpine<K, S::Value, T, i32>, _, _>(
                name,
                move |_key, input, updates| {
                    // `reduce_abelian` withdraws whatever it reported last
                    // time, so this only ever states the current answer.
                    let mut accumulated = S::zero();
                    for (value, _) in input {
                        accumulated.plus_equals(&contribute(value));
                    }
                    updates.push((accumulated.finish(), 1));
                },
                |vec, key, upds| {
                    vec.clear();
                    vec.extend(upds.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r)));
                },
            )
            .as_collection(move |key, value| merge(key.clone(), *value))
    }
}

// =========================================================================
// Aggregations
// =========================================================================

/// What one row contributes to its group, for a given aggregated column.
///
/// The aggregation decides this, not the caller's closures, because it is the one
/// place the two strategies must agree: `Present` contributes per row while
/// `i32` contributes per arranged entry, and an aggregation that meant
/// different things in each would silently disagree between modes.
pub trait Aggregation<V, C>: 'static {
    /// The accumulator this aggregation runs in.
    type Semiring: Semiring<Value = C>;

    /// Turns one row's column into that row's contribution.
    fn contribute(value: &V) -> Self::Semiring;
}

/// Declares one aggregation over the column's own type.
macro_rules! aggregation {
    ($name:ident, $semiring:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub struct $name;

        impl<V: Scalar> Aggregation<V, V> for $name {
            type Semiring = $semiring<V>;

            #[inline]
            fn contribute(value: &V) -> Self::Semiring {
                $semiring::lift(*value)
            }
        }
    };
}

aggregation!(Min, Smallest, "Smallest value in the column.");
aggregation!(Max, Largest, "Largest value in the column.");
aggregation!(Sum, Total, "Total of the column.");
aggregation!(Avg, Mean, "Mean of the column.");

/// Number of rows in the group.
///
/// Unlike the others this one reports a type unrelated to the column
/// it reads: `count` accepts a column of anything and answers with a number.
/// The column is still read, because both strategies rely on it to tell
/// otherwise-identical rows apart, and dropping it earlier would let two
/// rows that differ only there collapse into one.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Count;

impl<V, C: Scalar> Aggregation<V, C> for Count {
    type Semiring = Total<C>;

    #[inline]
    fn contribute(_value: &V) -> Self::Semiring {
        Total::lift(C::ONE)
    }
}
