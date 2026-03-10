/// Integer min-semiring for aggregating minimum values.

use differential_dataflow::difference::{IsZero, Monoid, Semigroup};
use differential_dataflow::difference::Multiply;

use serde::{Deserialize, Serialize};

macro_rules! define_min {
    ($name:ident, $ty:ty, $max:expr) => {
        #[derive(Copy, Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name {
            pub value: $ty,
        }

        impl $name {
            #[inline]
            pub fn new(value: $ty) -> Self {
                $name { value }
            }

            #[inline]
            pub fn infinity() -> Self {
                $name { value: $max }
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
                self.value = std::cmp::min(self.value, rhs.value);
            }
        }

        impl Monoid for $name {
            #[inline]
            fn zero() -> Self {
                $name::infinity()
            }
        }

        impl Multiply<i64> for $name {
            type Output = $name;
            #[inline]
            fn multiply(self, _rhs: &i64) -> Self::Output {
                self
            }
        }
    };
}
