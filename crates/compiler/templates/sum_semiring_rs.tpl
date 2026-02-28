/// Sum-semiring for aggregating sum values via differential dataflow's
/// built-in consolidation and `threshold_semigroup` operator.
///
/// Instead of using `reduce_core` (which maintains full value traces per key),
/// this encodes the aggregated value into the *diff* position of the DD triple
/// `(data, time, diff)`.  Consolidation then computes sum for free via
/// `plus_equals`, and `threshold_semigroup` emits updates only when the
/// running sum changes.

use differential_dataflow::difference::{IsZero, Monoid, Semigroup};
use differential_dataflow::difference::Multiply;

use serde::{Deserialize, Serialize};

macro_rules! define_sum {
    ($name:ident, $ty:ty) => {
        #[derive(Copy, Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name {
            pub value: $ty,
        }

        impl $name {
            #[inline]
            pub fn new(value: $ty) -> Self {
                $name { value }
            }
        }

        impl IsZero for $name {
            #[inline]
            fn is_zero(&self) -> bool {
                false
            }
        }

        impl Semigroup for $name {
            #[inline]
            fn plus_equals(&mut self, rhs: &Self) {
                self.value += rhs.value;
            }
        }

        impl Monoid for $name {
            #[inline]
            fn zero() -> Self {
                $name { value: 0 }
            }
        }

        impl Multiply<i64> for $name {
            type Output = $name;
            #[inline]
            fn multiply(self, rhs: &i64) -> Self::Output {
                $name { value: self.value * (*rhs as $ty) }
            }
        }
    };
}
