//! [`Append`]: presence for relations that only grow.

use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Semigroup;
use serde::Deserialize;
use serde::Serialize;

use super::Mutable;
use super::Static;
use super::Unit;

/// Presence for a relation that only grows: a datum is present from its
/// first announcement at every later time.
///
/// The arithmetic matches [`Static`]; the type differs so operators can
/// tell a relation that may still receive rows from one that cannot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Append;

impl IsZero for Append {
    #[inline]
    fn is_zero(&self) -> bool {
        false
    }
}

impl Semigroup for Append {
    #[inline]
    fn plus_equals(&mut self, _rhs: &Self) {}
}

impl Multiply for Append {
    type Output = Self;

    #[inline]
    fn multiply(self, _rhs: &Self) -> Self {
        self
    }
}

/// A static side joined into an append collection leaves it append: a set
/// complete at the first epoch is a valid history of a set that only
/// grows.
impl Multiply<Static> for Append {
    type Output = Self;

    #[inline]
    fn multiply(self, _rhs: &Static) -> Self {
        self
    }
}

/// The mirror of `Multiply<Static> for Append`, for a join whose static
/// side is on the left.
impl Multiply<Append> for Static {
    type Output = Append;

    #[inline]
    fn multiply(self, _rhs: &Append) -> Append {
        Append
    }
}

/// An append row reads as a count of one, so a join against a signed count
/// keeps that count. Exact when the arranged append collection holds each
/// datum once, as [`flowlog_arrange`](crate::operators::flowlog_arrange)
/// arranges it: a datum held at two times would read as two once the join
/// accumulates its history.
impl Multiply<Mutable> for Append {
    type Output = Mutable;

    #[inline]
    fn multiply(self, rhs: &Mutable) -> Mutable {
        *rhs
    }
}

/// The mirror of `Multiply<Mutable> for Append`, for a join whose signed
/// side is on the left.
impl Multiply<Append> for Mutable {
    type Output = Mutable;

    #[inline]
    fn multiply(self, _rhs: &Append) -> Mutable {
        self
    }
}

impl Unit for Append {
    #[inline]
    fn one() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A static side joined with an append side, from either side, gives
    /// append presence, as two append sides do.
    #[test]
    fn a_static_side_joined_with_append_is_append() {
        assert_eq!(Append.multiply(&Static), Append);
        assert_eq!(Static.multiply(&Append), Append);
        assert_eq!(Append.multiply(&Append), Append);
    }

    /// An append side multiplies a signed count to itself, from either
    /// side, retractions included.
    #[test]
    fn an_append_side_keeps_the_signed_count() {
        for count in [3, -2] {
            assert_eq!(Append.multiply(&count), count);
            assert_eq!(count.multiply(&Append), count);
        }
    }
}
