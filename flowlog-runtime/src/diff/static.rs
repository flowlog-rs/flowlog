//! [`Static`]: presence for relations fixed after their first epoch.

use differential_dataflow::difference::IsZero;
use differential_dataflow::difference::Multiply;
use differential_dataflow::difference::Semigroup;
use serde::Deserialize;
use serde::Serialize;

use super::Mutable;
use super::Weight;

/// Presence for a relation whose contents are fixed in outer time.
///
/// Addition is idempotent. This zero-sized weight has no zero or negation.
/// The marker does not restrict iteration timestamps or close input
/// handles.
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

/// A static row counts one. Exact because an arranged static collection
/// holds each datum once: it reads only static inputs, so every update is
/// at its scope's minimum time, where presence consolidates.
impl Weight for Static {
    #[inline]
    fn one() -> Self {
        Self
    }

    #[inline]
    fn count(self) -> Mutable {
        1
    }
}

/// The least weight: a history fixed at the first epoch is valid at every
/// weight, so a product keeps the other side.
impl<W: Weight> Multiply<W> for Static {
    type Output = W;

    #[inline]
    fn multiply(self, rhs: &W) -> W {
        *rhs
    }
}
