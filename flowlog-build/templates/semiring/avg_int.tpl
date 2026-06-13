/// Integer avg-semiring for aggregating average values.
/// Average is decomposed into (sum, count).

use differential_dataflow::difference::{IsZero, Monoid, Semigroup};
use differential_dataflow::difference::Multiply;

use serde::{Deserialize, Serialize};

macro_rules! define_avg {
    ($name:ident, $ty:ty) => {
        #[derive(Copy, Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name {
            pub sum: $ty,
            pub count: $ty,
        }

        impl $name {
            #[inline]
            pub fn new(value: $ty) -> Self {
                $name { sum: value, count: 1 }
            }

            #[inline]
            pub fn avg(&self) -> $ty {
                if self.count == 0 {
                    debug_assert!(
                        false,
                        "Attempted to compute avg() for {} with zero count",
                        stringify!($name)
                    );
                    0
                } else {
                    self.sum / self.count
                }
            }
        }

        impl IsZero for $name {
            #[inline]
            fn is_zero(&self) -> bool {
                self.count == 0
            }
        }

        impl Semigroup for $name {
            #[inline]
            fn plus_equals(&mut self, rhs: &Self) {
                self.sum += rhs.sum;
                self.count += rhs.count;
            }
        }

        impl Monoid for $name {
            #[inline]
            fn zero() -> Self {
                $name { sum: 0, count: 0 }
            }
        }

        impl Multiply<i64> for $name {
            type Output = $name;
            #[inline]
            fn multiply(self, rhs: &i64) -> Self::Output {
                $name {
                    sum: self.sum * (*rhs as $ty),
                    count: self.count * (*rhs as $ty),
                }
            }
        }
    };
}
