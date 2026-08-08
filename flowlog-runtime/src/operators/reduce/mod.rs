//! Group-by aggregation for generated FlowLog rules.
//!
//! [`flowlog_reduce`] is the whole surface for a non-recursive rule. Callers
//! pass an aggregation token and two closures describing the row shape --
//! `split` cuts a row into its group-by key and the aggregated column,
//! `merge` puts an output row back together -- and name nothing else. Which
//! semiring accumulates the column, and whether the aggregate rides in the
//! difference position or through an arrangement, are both internal.
//!
//! Recursive rules under `Present` defer completion to the loop boundary,
//! because a group is only complete once every iteration has contributed:
//! [`flowlog_reduce_leave`] lifts contributions inside the scope, leaves,
//! and folds them once at the outer timestamp, where the fixpoint is
//! final.

mod semiring;

use differential_dataflow::AsCollection;
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
use timely::dataflow::operators::vec::Map;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

// =========================================================================
// Operators
// =========================================================================

/// Groups rows by key and reduces each group under `aggregation`.
///
/// `split` cuts each row into its group-by key and the aggregated column;
/// `merge` rebuilds an output row from a key and the group's result. Output
/// rows carry unit differences, so the result is a set again and downstream
/// operators see no trace of the accumulation.
///
/// # Panics
///
/// Panics if the surrounding dataflow violates Differential Dataflow's trace
/// progress invariants, which the backing arrangement asserts.
pub fn flowlog_reduce<'scope, A, T, D, K, V, C, O, R>(
    collection: VecCollection<'scope, T, D, R>,
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
    R::reduce(collection, A::contribute, split, merge)
}

/// Completes an aggregate across a recursive scope: the single fold over
/// every iteration's contributions, deferred to `leave`, where the
/// fixpoint is final.
///
/// Inside the scope, each row's contribution is lifted into the semiring
/// difference; `leave` lands every iteration on one outer timestamp, and
/// one consolidation there is the whole fold -- the same
/// defer-the-arithmetic pattern as `flowlog_antijoin`. The in-scope
/// [`flowlog_reduce`] output this consumes exists for feedback; the
/// answer is produced here.
///
/// # Panics
///
/// Panics if the surrounding dataflow violates Differential Dataflow's
/// trace progress invariants, which the backing arrangement asserts.
pub fn flowlog_reduce_leave<'inner, 'outer, A, TInner, TOuter, D, K, V, C, O>(
    collection: VecCollection<'inner, TInner, D, Present>,
    outer: Scope<'outer, TOuter>,
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
        lift(collection, A::contribute, split)
            .leave(outer)
            .consolidate(),
        merge,
    )
}

/// Shared last half of the `Present` pipeline: the settled aggregate comes
/// back out of the difference position and into a row, which is a set member
/// again.
fn lower<'scope, T, K, S, O>(
    collection: VecCollection<'scope, T, K, S>,
    mut merge: impl FnMut(K, S::Value) -> O + 'static,
) -> VecCollection<'scope, T, O, Present>
where
    T: Timestamp,
    K: Data,
    S: Semiring,
    O: Data,
{
    collection
        .inner
        .map(move |(key, time, aggregate)| (merge(key, aggregate.finish()), time, Present))
        .as_collection()
}

/// Shared first half of the `Present` pipeline: one contribution per row,
/// keyed by group, carried in the difference position.
fn lift<'scope, T, D, K, V, S>(
    collection: VecCollection<'scope, T, D, Present>,
    contribute: impl Fn(&V) -> S + 'static,
    mut split: impl FnMut(D) -> (K, V) + 'static,
) -> VecCollection<'scope, T, K, S>
where
    T: Timestamp,
    D: Data,
    K: Data,
    S: Semiring,
{
    collection
        .inner
        .map(move |(row, time, _)| {
            let (key, value) = split(row);
            let contribution = contribute(&value);
            (key, time, contribution)
        })
        .as_collection()
}

// =========================================================================
// Strategies
// =========================================================================

/// How a difference type computes a group-by aggregate.
///
/// The two are not a performance choice. `Present` has no inverse, so it
/// cannot retract a superseded aggregate, and the only way to aggregate
/// under it is to put the accumulation where consolidation will find it:
/// the difference position. `i32` can negate, so it takes Differential
/// Dataflow's own reduce and lets it withdraw the previous answer.
///
/// The trait is parameterized by the timestamp so each half can ask of the
/// clock only what it needs: the `Present` half thresholds, which requires a
/// total order, while the `i32` half arranges, which does not. That is what
/// lets an incremental rule aggregate inside a loop, where the clock is only
/// partially ordered.
pub trait ReduceStrategy<T: Timestamp + Lattice>: Semigroup + Sized {
    /// See [`flowlog_reduce`].
    fn reduce<'scope, D, K, V, S, O>(
        collection: VecCollection<'scope, T, D, Self>,
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
        lift(collection, contribute, split)
            .threshold_semigroup(|_key, new, current| match current {
                // An aggregate that does not supersede the one already
                // reported is not news: emitting it would restate a row
                // downstream already has.
                Some(current) => new.supersedes(current).then_some(*new),
                None => (!new.is_zero()).then_some(*new),
            })
            .inner
            .map(move |(key, time, aggregate)| (merge(key, aggregate.finish()), time, Present))
            .as_collection()
    }
}

impl<T: Timestamp + Lattice> ReduceStrategy<T> for i32 {
    fn reduce<'scope, D, K, V, S, O>(
        collection: VecCollection<'scope, T, D, Self>,
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
                "aggregation",
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
// Tokens
// =========================================================================

/// What one row contributes to its group, for a given aggregated column.
///
/// The token decides this, not the caller's closures, because it is the one
/// place the two strategies must agree: `Present` contributes per row while
/// `i32` contributes per arranged entry, and an aggregation that meant
/// different things in each would silently disagree between modes.
pub trait Aggregation<V, C>: 'static {
    /// The accumulator this aggregation runs in.
    type Semiring: Semiring<Value = C>;

    /// Turns one row's column into that row's contribution.
    fn contribute(value: &V) -> Self::Semiring;
}

/// Declares one aggregation token over the column's own type.
macro_rules! token {
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

token!(Min, Smallest, "Smallest value in the column.");
token!(Max, Largest, "Largest value in the column.");
token!(Sum, Total, "Total of the column.");
token!(Avg, Mean, "Mean of the column.");

/// Number of rows in the group.
///
/// Unlike the other tokens this one reports a type unrelated to the column
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
