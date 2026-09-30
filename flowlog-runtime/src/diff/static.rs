//! [`Static`]: presence for relations fixed after their first epoch.

use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Semigroup;
use serde::Deserialize;
use serde::Serialize;

use super::Mutable;
use super::Unit;

/// Presence for a relation whose contents are fixed in outer time.
///
/// Addition is idempotent, and multiplication preserves presence. This
/// zero-sized weight has no zero or negation. The marker does not restrict
/// iteration timestamps or close input handles.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Static;

impl IsZero for Static {
    #[inline]
    fn is_zero(&self) -> bool {
        false
    }
}

impl Semigroup for Static {
    #[inline]
    fn plus_equals(&mut self, _rhs: &Self) {}
}

impl Multiply for Static {
    type Output = Self;

    #[inline]
    fn multiply(self, _rhs: &Self) -> Self {
        self
    }
}

/// A static row reads as a count of one, so a join against a signed count
/// keeps that count. Exact because an arranged static collection holds each
/// datum once: it reads only static inputs, so every update is at its
/// scope's minimum time, where presence consolidates.
impl Multiply<Mutable> for Static {
    type Output = Mutable;

    #[inline]
    fn multiply(self, rhs: &Mutable) -> Mutable {
        *rhs
    }
}

/// The mirror of `Multiply<Mutable> for Static`, for a join whose signed
/// side is on the left.
impl Multiply<Static> for Mutable {
    type Output = Mutable;

    #[inline]
    fn multiply(self, _rhs: &Static) -> Mutable {
        self
    }
}

impl Unit for Static {
    #[inline]
    fn one() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A static side multiplies a signed count to itself, from either side,
    /// retractions included.
    #[test]
    fn a_static_side_keeps_the_signed_count() {
        for count in [3, -2] {
            assert_eq!(Static.multiply(&count), count);
            assert_eq!(count.multiply(&Static), count);
        }
    }
}
