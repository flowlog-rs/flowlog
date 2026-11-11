//! Update weights, one per relation mutability.
//!
//! Operators dispatch on the weight, so each mutability selects its own
//! implementation: [`Static`] and [`Append`] carry presence, [`Mutable`]
//! a signed count. Code names them with the module prefix
//! (`diff::Static`), since the bare words are too common to read alone.
//! [`Weight`] reads each as a count, presence as one, and [`Presence`]
//! marks the two that carry presence, so an operator that treats them
//! alike dispatches on it rather than on each. [`Max`] orders the weights
//! by mutability: a join weighs its output at the larger of its sides'
//! weights, with the [`product`] of their counts.

mod append;
mod mutable;
mod r#static;

pub use append::Append;
use differential_dataflow::ExchangeData;
use differential_dataflow::difference::Semigroup;
pub use mutable::Mutable;
pub use r#static::Static;

/// The weight of an update, read as a count of the datum's copies: a
/// [`Mutable`] weight is its own count, and a presence weight counts one.
///
/// Counts multiply in a join, but only [`Mutable`] adds them: two
/// presences add to one presence, which still counts one. A presence
/// count is thus exact only where its collection holds each datum once,
/// as each presence weight's impl states.
pub trait Weight: Semigroup + ExchangeData + Copy + sealed::Weight {
    /// The weight of one insertion.
    #[must_use]
    #[inline]
    fn one() -> Self {
        Self::from_count(1)
    }

    /// The weight of `count` copies of a datum. A presence weight takes a
    /// positive count, which it holds as present.
    ///
    /// # Panics
    ///
    /// A presence weight panics on a count below one, which it cannot
    /// hold.
    #[must_use]
    fn from_count(count: Mutable) -> Self;

    /// The number of copies the weight stands for.
    #[must_use]
    fn count(self) -> Mutable;
}

/// A presence weight: a datum is present or absent, addition is
/// idempotent, and there is no negation. Sealed to [`Static`] and
/// [`Append`], which differ only in whether rows may still arrive.
pub trait Presence: Weight + sealed::Presence {}

impl<R> Presence for R where R: Weight + sealed::Presence {}

/// Orders the weights as the planner orders mutabilities, [`Static`] below
/// [`Append`] below [`Mutable`]: `Output` is the larger of `Self` and `W`.
///
/// Each weight allows the changes of those below it: none at [`Static`],
/// insertions at [`Append`], insertions and deletions at [`Mutable`]. A
/// collection's history thus stays valid at any larger weight. A weight
/// is at most `W` exactly when it implements `Max<W, Output = W>`.
pub trait Max<W: Weight>: Weight {
    /// The larger of `Self` and `W`.
    type Output: Weight;
}

/// The weight of a join's output: the product of its sides' counts, at the
/// larger of their weights.
#[must_use]
#[inline]
pub fn product<R1: Max<R2>, R2: Weight>(left: R1, right: R2) -> R1::Output {
    // A presence output has two presence sides, whose counts multiply to
    // one.
    R1::Output::from_count(left.count() * right.count())
}

mod sealed {
    pub trait Weight {}
    pub trait Presence {}

    impl Weight for super::Static {}
    impl Weight for super::Append {}
    impl Weight for super::Mutable {}

    impl Presence for super::Static {}
    impl Presence for super::Append {}
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// One insertion is the presence of a presence weight and a count of one
    /// of a signed weight.
    #[test]
    fn one_insertion_is_presence_or_a_count_of_one() {
        assert_eq!(Static::one(), Static);
        assert_eq!(Append::one(), Append);
        assert_eq!(Mutable::one(), 1);
    }

    /// A presence side keeps the signed side's count, whichever side it is
    /// on, retractions included.
    // Cases: signed count.
    #[rstest]
    #[case::insertion(3)]
    #[case::retraction(-2)]
    fn a_presence_side_keeps_the_signed_count(#[case] count: Mutable) {
        assert_eq!(product(Static, count), count);
        assert_eq!(product(count, Static), count);
        assert_eq!(product(Append, count), count);
        assert_eq!(product(count, Append), count);
    }

    /// Two presence sides give presence at the larger of their weights,
    /// whichever side each is on.
    #[test]
    fn two_presence_sides_give_presence_at_the_larger_weight() {
        assert_eq!(product(Static, Static), Static);
        assert_eq!(product(Static, Append), Append);
        assert_eq!(product(Append, Static), Append);
        assert_eq!(product(Append, Append), Append);
    }

    /// Two signed sides multiply their counts, signs included.
    #[test]
    fn two_signed_sides_multiply_their_counts() {
        assert_eq!(product(3, -2), -6);
    }

    /// A presence weight holds neither absence nor a retraction, so it
    /// refuses a count below one.
    // Cases: presence weight, count.
    #[rstest]
    #[case::static_zero(Static::from_count, 0)]
    #[case::static_retraction(Static::from_count, -1)]
    #[case::append_zero(Append::from_count, 0)]
    #[case::append_retraction(Append::from_count, -1)]
    #[should_panic(expected = "presence holds only a positive count, found")]
    fn a_presence_weight_refuses_a_count_below_one<P: Presence>(
        #[case] from_count: fn(Mutable) -> P,
        #[case] count: Mutable,
    ) {
        from_count(count);
    }
}
