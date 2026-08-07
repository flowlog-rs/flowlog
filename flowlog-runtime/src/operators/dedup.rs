//! Set-semantics dedup for generated FlowLog rules.
//!
//! The diff picks the mechanism and [`DedupTime`] picks the `i32` flavor
//! by clock, so no call site chooses either. Every diff-and-clock pairing
//! works but one: retained into `Present` at a partially ordered clock
//! needs a threshold `Present` cannot drive, having no inverse.

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

/// Holds each tuple to at most unit weight at each timestamp.
///
/// `Present` compacts a timestamp and forgets it, so a later re-derivation
/// is emitted again; loop feedback wants [`flowlog_dedup_retained`]. The
/// `i32` clamp accumulates through a trace, and suppresses it anyway.
pub fn flowlog_dedup<C: FlowlogDedup>(collection: C) -> C {
    collection.dedup()
}

/// Holds each tuple to unit weight, and suppresses one already held from
/// an earlier timestamp.
///
/// Into `Present` that is absolute, since nothing retracts; into `i32` a
/// tuple whose count returns to zero is withdrawn and may reappear.
///
/// `R` is the target diff, independent of the input's, which is how the
/// antijoin's `i32` arithmetic settles back to the ambient one. It cannot
/// be inferred, so name it: `flowlog_dedup_retained::<_, Present>(rows)`.
/// `Present` to `i32` deliberately has no impl.
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

/// The clocks FlowLog dataflows run at, each knowing how to clamp `i32`
/// multiplicities back to set semantics.
///
/// A clock is a root (`()` for batch, an [`Epoch`] for incremental) or
/// `Product<root, epoch>` inside recursion. Where at most one coordinate
/// advances the clock is a line and the streaming `threshold_total`
/// applies; in `Product<epoch, epoch>` both advance, times like (1, 2)
/// and (2, 1) are incomparable, and only the general `threshold` is
/// sound.
///
/// Sealed, so a new width, root or counter, is one marker impl on
/// `sealed::Epoch` and never a new clamp body. The `()` clocks stay
/// although batch never clamps through them today, since diff and clock
/// are independent axes.
pub trait DedupTime: Timestamp + Lattice + sealed::Sealed {
    /// Clamps accumulated `i32` multiplicities to at most `1` per tuple.
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable;
}

/// A time that advances: `u16` and `u32` today. Serves at either clock
/// position, root or counter; `()` is root-only, since a counter that
/// cannot advance is a loop that cannot iterate. Declare new widths on
/// `sealed::Epoch`.
pub trait Epoch: Timestamp + TotalOrder + Lattice + sealed::Epoch {}

impl<E> Epoch for E where E: sealed::Epoch + Timestamp + TotalOrder + Lattice {}

mod sealed {
    use timely::order::Product;

    /// The one list of advancing widths. Everything else derives from
    /// membership here, so adding a width is one marker impl
    /// (`impl Epoch for u64 {}`).
    pub trait Epoch {}

    impl Epoch for u16 {}
    impl Epoch for u32 {}

    /// The four clock shapes, one line per `DedupTime` impl: each root
    /// bare, and each root under an iteration counter.
    pub trait Sealed {}

    impl Sealed for () {}
    impl<E: Epoch> Sealed for E {}
    impl<I: Epoch> Sealed for Product<(), I> {}
    impl<E: Epoch, I: Epoch> Sealed for Product<E, I> {}
}

/// Streaming clamp every totally ordered clock delegates to; the
/// `TotalOrder` bound is what keeps epoch-rooted products out.
fn clamp_total<'scope, T, D>(
    collection: VecCollection<'scope, T, D, i32>,
) -> VecCollection<'scope, T, D, i32>
where
    T: Timestamp + TotalOrder + Lattice,
    D: ExchangeData + Hashable,
{
    collection.threshold_total(|_, &count| if count > 0 { 1 } else { 0 })
}

/// Batch: time never advances, trivially totally ordered.
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

/// A bare epoch advances along a line: one body for every width.
impl<E: Epoch> DedupTime for E {
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable,
    {
        clamp_total(collection)
    }
}

/// Iterations under the batch root stay on a line, whatever the counter
/// width (timely: `Product<T1, T2>: TotalOrder where T1: Empty`).
impl<I: Epoch> DedupTime for Product<(), I> {
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable,
    {
        clamp_total(collection)
    }
}

/// The one partially ordered clock: epoch and iteration both advance, so
/// the streaming clamp is unsound and only the general `threshold` works.
impl<E: Epoch, I: Epoch> DedupTime for Product<E, I> {
    fn clamp_i32<'scope, D>(
        collection: VecCollection<'scope, Self, D, i32>,
    ) -> VecCollection<'scope, Self, D, i32>
    where
        D: ExchangeData + Hashable,
    {
        collection.threshold(|_, &count| if count > 0 { 1 } else { 0 })
    }
}

#[cfg(test)]
mod tests {
    use timely::order::Product;

    use super::*;

    type Row = u64;
    type Batch = ();
    type Inc = u32;
    type BatchLoop = Product<(), u16>;
    type IncLoop = Product<u32, u16>;

    /// Every supported diff-and-clock pairing type-checks through the two
    /// entry points. The one exclusion holds by construction and cannot be
    /// asserted here: retained into `Present` at the partially ordered
    /// `IncLoop` clock does not compile.
    #[test]
    fn every_supported_diff_and_clock_pairing_compiles() {
        fn dedups<C: FlowlogDedup>() {
            let _ = core::marker::PhantomData::<C>;
        }
        fn retains<C: FlowlogDedupRetained<R>, R>() {
            let _ = core::marker::PhantomData::<C>;
        }

        // `flowlog_dedup`: either diff, every clock.
        dedups::<VecCollection<'static, Batch, Row, Present>>();
        dedups::<VecCollection<'static, Inc, Row, Present>>();
        dedups::<VecCollection<'static, BatchLoop, Row, Present>>();
        dedups::<VecCollection<'static, IncLoop, Row, Present>>();
        dedups::<VecCollection<'static, Batch, Row, i32>>();
        dedups::<VecCollection<'static, Inc, Row, i32>>();
        dedups::<VecCollection<'static, BatchLoop, Row, i32>>();
        dedups::<VecCollection<'static, IncLoop, Row, i32>>();

        // Retained into `i32`: every clock.
        retains::<VecCollection<'static, Batch, Row, i32>, i32>();
        retains::<VecCollection<'static, Inc, Row, i32>, i32>();
        retains::<VecCollection<'static, BatchLoop, Row, i32>, i32>();
        retains::<VecCollection<'static, IncLoop, Row, i32>, i32>();

        // Retained into `Present`, from either diff: every totally
        // ordered clock.
        retains::<VecCollection<'static, Batch, Row, i32>, Present>();
        retains::<VecCollection<'static, Batch, Row, Present>, Present>();
        retains::<VecCollection<'static, Inc, Row, i32>, Present>();
        retains::<VecCollection<'static, Inc, Row, Present>, Present>();
        retains::<VecCollection<'static, BatchLoop, Row, i32>, Present>();
        retains::<VecCollection<'static, BatchLoop, Row, Present>, Present>();
    }

    /// A new width, root or counter, is one marker impl away -- never a
    /// new clamp body. (`Product<root, ()>`, a loop that cannot iterate,
    /// names no clock at all.)
    #[test]
    fn any_declared_width_is_a_dedup_clock() {
        fn admits<T: DedupTime>(_clock: T) {}
        admits(());
        admits(0u16);
        admits(0u32);
        admits(Product::new((), 0u16));
        admits(Product::new((), 0u32));
        admits(Product::new(0u32, 0u16));
        admits(Product::new(0u32, 0u32));
    }
}
