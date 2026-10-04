//! Update weights, one per relation mutability.
//!
//! Operators dispatch on the weight, so each mutability selects its own
//! implementation: [`Static`] and [`Append`] carry presence, [`Mutable`]
//! a signed count. Code names them with the module prefix
//! (`diff::Static`), since the bare words are too common to read alone.
//! [`Unit`] names the weight one inserted row carries in each, and
//! [`Presence`] marks the two that carry presence, so an operator that
//! treats them alike dispatches on it rather than on each. The `Multiply`
//! impls between the types say what weight a join of two of them produces.

mod append;
mod mutable;
mod r#static;

pub use append::Append;
use differential_dataflow::ExchangeData;
use differential_dataflow::difference::Semigroup;
pub use mutable::Mutable;
pub use r#static::Static;

/// A weight with a value for one inserted row.
pub trait Unit {
    /// The weight of one insertion.
    fn one() -> Self;
}

/// A presence weight: a datum is present or absent, addition is
/// idempotent, and there is no negation. Sealed to [`Static`] and
/// [`Append`], which differ only in whether rows may still arrive.
pub trait Presence: Unit + Semigroup + ExchangeData + Copy + sealed::Presence {}

impl<R> Presence for R where R: Unit + Semigroup + ExchangeData + Copy + sealed::Presence {}

mod sealed {
    pub trait Presence {}

    impl Presence for super::Static {}
    impl Presence for super::Append {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One insertion is the presence of a presence weight and a count of one
    /// of a signed weight.
    #[test]
    fn one_insertion_is_presence_or_a_count_of_one() {
        assert_eq!(Static::one(), Static);
        assert_eq!(Append::one(), Append);
        assert_eq!(Mutable::one(), 1);
    }
}
