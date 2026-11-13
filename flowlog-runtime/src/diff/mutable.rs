//! [`Mutable`]: signed counts for relations with insertions and deletions.

use differential_dataflow::difference::Multiply;

use super::Append;
use super::Static;
use super::Weight;

/// Signed multiplicity for a relation that accepts insertions and deletions.
///
/// Any signed integer ring would do: after a dedup, operators test only
/// whether a count is positive. `i32` is the width every mutable collection
/// uses today.
pub type Mutable = i32;

impl Weight for Mutable {
    #[inline]
    fn one() -> Self {
        1
    }

    #[inline]
    fn count(self) -> Mutable {
        self
    }
}

// One impl over every presence weight would break the orphan rule, as
// `Mutable` is the foreign `i32`, so each has its own. Differential
// multiplies two counts.
impl Multiply<Static> for Mutable {
    type Output = Self;

    #[inline]
    fn multiply(self, _: &Static) -> Self {
        self
    }
}

impl Multiply<Append> for Mutable {
    type Output = Self;

    #[inline]
    fn multiply(self, _: &Append) -> Self {
        self
    }
}
