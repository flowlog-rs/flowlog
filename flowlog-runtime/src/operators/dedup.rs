//! Set-semantics dedup for generated FlowLog rules.
//!
//! Two entry points split the one choice types cannot make:
//! [`flowlog_dedup`] normalizes within each timestamp, while
//! [`flowlog_dedup_retained`] also suppresses tuples re-derived at later
//! timestamps (loop iterations). Every other choice is compile-time
//! dispatch on the collection's type parameters: the diff family
//! (`Present` or `i32`) picks the operator, and for `i32` clamps the
//! clock picks the threshold flavor via [`DedupTime`].

use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Present;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use timely::order::Product;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

/// Normalizes a collection to set semantics: every tuple accumulates to
/// at most one unit weight at each timestamp.
///
/// Under `Present` this compacts duplicates batch by batch and forgets,
/// so a tuple re-derived at a later timestamp is emitted again; where
/// that re-emission must stay suppressed (feedback inside `iterate`),
/// use [`flowlog_dedup_retained`]. The `i32` clamp accumulates through a
/// trace and needs no such distinction.
///
/// Generated code calls this path-qualified, with no `use` line, which is
/// why the dispatch lives behind a free function rather than a trait
/// method.
///
/// # Panics
///
/// Panics if the surrounding dataflow violates Differential Dataflow's
/// trace progress invariants, which the backing arrangement asserts.
pub fn flowlog_dedup<C: FlowlogDedup>(collection: C) -> C {
    collection.dedup()
}

/// Normalizes a collection to set semantics across timestamps, rather than
/// within each one: a tuple already carried by an earlier timestamp is not
/// emitted again. Targeting `Present` makes that absolute, since nothing
/// can retract it; targeting `i32` keeps the clamp's retractions, so a
/// tuple whose count returns to zero is withdrawn and may reappear.
///
/// `R` is the target diff, independent of the input's, so a collection
/// carrying `i32` weight arithmetic can clamp back to any ambient diff.
/// That independence is also why `R` cannot be inferred -- an `i32` input
/// converts to `Present` or stays `i32` -- so it is named explicitly, as
/// in `flowlog_dedup_retained::<_, Present>(rows)`. There is deliberately
/// no `Present` to `i32` impl, so that direction fails to compile.
///
/// # Panics
///
/// Panics if the surrounding dataflow violates Differential Dataflow's
/// trace progress invariants, which the backing arrangement asserts.
pub fn flowlog_dedup_retained<C, R>(collection: C) -> C::Output
where
    C: FlowlogDedupRetained<R>,
{
    collection.dedup_retained()
}

// =========================================================================
// FlowlogDedup
// =========================================================================

/// Diff-family dispatch behind [`flowlog_dedup`].
pub trait FlowlogDedup: Sized {
    /// See [`flowlog_dedup`].
    fn dedup(self) -> Self;
}

impl<'scope, T, D> FlowlogDedup for VecCollection<'scope, T, D, Present>
where
    T: Timestamp + Lattice,
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        self.consolidate()
    }
}

impl<'scope, T, D> FlowlogDedup for VecCollection<'scope, T, D, i32>
where
    T: DedupTime,
    D: ExchangeData + Hashable,
{
    fn dedup(self) -> Self {
        T::clamp_i32(self)
    }
}

// =========================================================================
// FlowlogDedupRetained
// =========================================================================

/// Target-diff dispatch behind [`flowlog_dedup_retained`]; each impl
/// owns the unit weight of its target.
pub trait FlowlogDedupRetained<R>: Sized {
    /// The input collection with its diff type replaced by `R`.
    type Output;

    /// See [`flowlog_dedup_retained`].
    fn dedup_retained(self) -> Self::Output;
}

/// Any diff to `Present`: an arrangement-backed presence check, which
/// reads only whether the tuple was seen before and so ignores the input
/// diff entirely. Admits every totally ordered clock, the ordering the
/// underlying streaming operator requires.
impl<'scope, T, D, R> FlowlogDedupRetained<Present> for VecCollection<'scope, T, D, R>
where
    T: Timestamp + TotalOrder + Lattice,
    D: ExchangeData + Hashable,
    R: ExchangeData + Semigroup,
{
    type Output = VecCollection<'scope, T, D, Present>;

    fn dedup_retained(self) -> Self::Output {
        self.threshold_semigroup(|_, _, prior| prior.is_none().then_some(Present))
    }
}

/// `i32` to `i32`: the clamp accumulates through a trace, so retention
/// adds nothing.
impl<'scope, T, D> FlowlogDedupRetained<i32> for VecCollection<'scope, T, D, i32>
where
    T: DedupTime,
    D: ExchangeData + Hashable,
{
    type Output = Self;

    fn dedup_retained(self) -> Self::Output {
        self.dedup()
    }
}

// =========================================================================
// DedupTime
// =========================================================================

/// Clocks FlowLog dataflows run at, each carrying the `i32` multiplicity
/// clamp its ordering supports: totally ordered clocks take the streaming
/// `threshold_total`, while `Product<u32, u16>` is only partially ordered
/// and falls back to the general `threshold`. Sealed, so widening the set
/// is a deliberate edit here rather than an accident of inference.
pub trait DedupTime: Timestamp + Lattice + sealed::Sealed {
    /// Clamps accumulated `i32` multiplicities to at most `1` per tuple.
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable;
}

mod sealed {
    use timely::order::Product;

    pub trait Sealed {}

    impl Sealed for () {}
    impl Sealed for u32 {}
    impl Sealed for Product<(), u16> {}
    impl Sealed for Product<u32, u16> {}
}

/// Streaming clamp every totally ordered clock delegates to; the
/// `TotalOrder` bound is what keeps the partially ordered incremental
/// loop clock out.
fn clamp_total<'scope, T, D>(
    collection: VecCollection<'scope, T, D, i32>,
) -> VecCollection<'scope, T, D, i32>
where
    T: Timestamp + TotalOrder + Lattice,
    D: ExchangeData + Hashable,
{
    collection.threshold_total(|_, &count| if count > 0 { 1 } else { 0 })
}

impl DedupTime for () {
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable,
    {
        clamp_total(collection)
    }
}

impl DedupTime for u32 {
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable,
    {
        clamp_total(collection)
    }
}

impl DedupTime for Product<(), u16> {
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable,
    {
        clamp_total(collection)
    }
}

impl DedupTime for Product<u32, u16> {
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable,
    {
        // A u16 iteration counter under a u32 epoch is only partially
        // ordered, which rules out the streaming total-order clamp.
        collection.threshold(|_, &count| if count > 0 { 1 } else { 0 })
    }
}
