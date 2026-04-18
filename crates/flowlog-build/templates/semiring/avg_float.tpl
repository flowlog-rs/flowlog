/// Float avg-semiring for aggregating average values.
/// Average is decomposed into (sum, count).

use differential_dataflow::difference::{IsZero, Monoid, Semigroup};
use differential_dataflow::difference::Multiply;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

macro_rules! define_avg {
    ($name:ident, $inner:ty) => {
        #[derive(Copy, Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name {
            pub sum: OrderedFloat<$inner>,
            pub count: OrderedFloat<$inner>,
        }

        impl $name {
            #[inline]
            pub fn new(value: OrderedFloat<$inner>) -> Self {
                $name { sum: value, count: OrderedFloat(1.0) }
            }

            #[inline]
            pub fn avg(&self) -> OrderedFloat<$inner> {
                if self.count.into_inner() == 0.0 {
                    debug_assert!(
                        false,
                        "Attempted to compute avg() for {} with zero count",
                        stringify!($name)
                    );
                    OrderedFloat(0.0)
                } else {
                    OrderedFloat(self.sum.into_inner() / self.count.into_inner())
                }
            }
        }

        impl IsZero for $name {
            #[inline]
            fn is_zero(&self) -> bool {
                self.count.into_inner() == 0.0
            }
        }

        impl Semigroup for $name {
            #[inline]
            fn plus_equals(&mut self, rhs: &Self) {
                self.sum = OrderedFloat(self.sum.into_inner() + rhs.sum.into_inner());
                self.count = OrderedFloat(self.count.into_inner() + rhs.count.into_inner());
            }
        }

        impl Monoid for $name {
            #[inline]
            fn zero() -> Self {
                $name { sum: OrderedFloat(0.0), count: OrderedFloat(0.0) }
            }
        }

        impl Multiply<i64> for $name {
            type Output = $name;
            #[inline]
            fn multiply(self, rhs: &i64) -> Self::Output {
                $name {
                    sum: OrderedFloat(self.sum.into_inner() * (*rhs as $inner)),
                    count: OrderedFloat(self.count.into_inner() * (*rhs as $inner)),
                }
            }
        }
    };
}
