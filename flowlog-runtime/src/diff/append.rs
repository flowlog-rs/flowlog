//! [`Append`]: presence for relations that only grow.

use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Semigroup;
use serde::Deserialize;
use serde::Serialize;

/// Presence for a relation that only grows: a datum is present from its
/// first announcement at every later time.
///
/// The arithmetic matches [`Static`](super::Static); the type differs so operators can
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
