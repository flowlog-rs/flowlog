use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Semigroup;
use serde::Deserialize;
use serde::Serialize;

/// Presence for a relation whose contents are fixed in outer time.
///
/// Addition is idempotent, and multiplication preserves presence. This
/// zero-sized weight has no zero or negation. The marker does not restrict
/// iteration timestamps or close input handles.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StaticPresent;

impl IsZero for StaticPresent {
    #[inline]
    fn is_zero(&self) -> bool {
        false
    }
}

impl Semigroup for StaticPresent {
    #[inline]
    fn plus_equals(&mut self, _rhs: &Self) {}
}

impl Multiply for StaticPresent {
    type Output = Self;

    #[inline]
    fn multiply(self, _rhs: &Self) -> Self {
        self
    }
}
