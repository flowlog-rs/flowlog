//! Update weights, one per relation mutability.
//!
//! Operators dispatch on the weight, so each mutability selects its own
//! implementation: [`Static`] and [`Append`] carry presence, [`Mutable`]
//! a signed count. Code names them with the module prefix
//! (`diff::Static`), since the bare words are too common to read alone.
//! [`Unit`] names the weight one inserted row carries in each.

mod append;
mod mutable;
mod r#static;

pub use append::Append;
pub use mutable::Mutable;
pub use r#static::Static;

/// A weight with a value for one inserted row.
pub trait Unit {
    /// The weight of one insertion.
    fn one() -> Self;
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
