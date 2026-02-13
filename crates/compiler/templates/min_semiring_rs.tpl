/// Min semiring for aggregating minimum values via differential dataflow's
/// built-in consolidation and `threshold_semigroup` operator.
///
/// Instead of using `reduce_core` (which maintains full value traces per key),
/// this encodes the minimum into the *diff* position of the DD triple
/// `(data, time, diff)`.  Consolidation then computes `min` for free via
/// `plus_equals`, and `threshold_semigroup` emits updates only when the
/// running minimum decreases.

use differential_dataflow::difference::{IsZero, Monoid, Semigroup};
use differential_dataflow::difference::Multiply;

use serde::{Deserialize, Serialize};

/// A semiring whose additive operation is `min`.
#[derive(Copy, Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct Min {
    pub value: u32,
}

impl Min {
    /// Wrap a concrete value.
    #[inline]
    pub fn new(value: u32) -> Self {
        Min { value }
    }

    /// Additive identity: any real value is less than infinity.
    #[inline]
    pub fn infinity() -> Self {
        Min { value: u32::MAX }
    }
}

impl IsZero for Min {
    #[inline]
    fn is_zero(&self) -> bool {
        false
    }
}

impl Semigroup for Min {
    #[inline]
    fn plus_equals(&mut self, rhs: &Self) {
        self.value = std::cmp::min(self.value, rhs.value);
    }
}

impl Monoid for Min {
    #[inline]
    fn zero() -> Self {
        Min::infinity()
    }
}

impl Multiply<i64> for Min {
    type Output = Min;
    #[inline]
    fn multiply(self, _rhs: &i64) -> Self::Output {
        self
    }
}
