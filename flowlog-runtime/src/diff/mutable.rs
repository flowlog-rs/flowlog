//! [`Mutable`]: signed counts for relations with insertions and deletions.

use super::Max;
use super::Weight;

/// Signed multiplicity for a relation that accepts insertions and deletions.
///
/// Any signed integer ring would do: after a dedup, operators test only
/// whether a count is positive. `i32` is the width every mutable collection
/// uses today.
pub type Mutable = i32;

impl Weight for Mutable {
    #[inline]
    fn from_count(count: Mutable) -> Self {
        count
    }

    #[inline]
    fn count(self) -> Mutable {
        self
    }
}

/// The greatest weight: a signed count holds every change.
impl<W: Weight> Max<W> for Mutable {
    type Output = Self;
}
