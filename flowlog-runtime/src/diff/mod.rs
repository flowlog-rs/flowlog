//! Update weights, one per relation mutability.
//!
//! Operators dispatch on the weight, so each mutability selects its own
//! implementation: [`Static`] and [`Append`] carry presence, [`Mutable`]
//! a signed count. Code names them with the module prefix
//! (`diff::Static`), since the bare words are too common to read alone.
//! [`Weight`] reads each as a count, presence as one, and [`Presence`]
//! marks the two that carry presence, so an operator that treats them
//! alike dispatches on it rather than on each.
//!
//! A join weighs each match with Differential's
//! [`Multiply`](differential_dataflow::difference::Multiply). Presence
//! multiplies as one, and the product takes the larger of the two
//! weights:
//!
//! | left \ right | `Static` | `Append` | `Mutable` |
//! |---|---|---|---|
//! | `Static` | `Static` | `Append` | `Mutable` |
//! | `Append` | `Append` | `Append` | `Mutable` |
//! | `Mutable` | `Mutable` | `Mutable` | `Mutable` |
//!
//! The order is the planner's order of mutabilities. Each weight allows
//! the changes of those below it: none at `Static`, insertions at
//! `Append`, insertions and deletions at `Mutable`, so a collection's
//! history stays valid at any larger weight. A weight `R1` is at most
//! `R2` exactly when `R1: Multiply<R2, Output = R2>`.

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
    /// The weight of one insertion, which multiplies as one.
    #[must_use]
    fn one() -> Self;

    /// The number of copies the weight stands for.
    #[must_use]
    fn count(self) -> Mutable;
}

/// A presence weight: a datum is present or absent, addition is
/// idempotent, and there is no negation. Sealed to [`Static`] and
/// [`Append`], which differ only in whether rows may still arrive.
pub trait Presence: Weight + sealed::Presence {}

impl<R> Presence for R where R: Weight + sealed::Presence {}

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
    use differential_dataflow::difference::Multiply;
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
        assert_eq!(Static.multiply(&count), count);
        assert_eq!(count.multiply(&Static), count);
        assert_eq!(Append.multiply(&count), count);
        assert_eq!(count.multiply(&Append), count);
    }

    /// Two presence sides give presence at the larger of their weights,
    /// whichever side each is on.
    #[test]
    fn two_presence_sides_give_presence_at_the_larger_weight() {
        assert_eq!(Static.multiply(&Static), Static);
        assert_eq!(Static.multiply(&Append), Append);
        assert_eq!(Append.multiply(&Static), Append);
        assert_eq!(Append.multiply(&Append), Append);
    }

    /// Two signed sides multiply their counts, signs included.
    #[test]
    fn two_signed_sides_multiply_their_counts() {
        assert_eq!(Mutable::multiply(3, &-2), -6);
    }
}
