/// Float sum-semiring for aggregating sum values.

use differential_dataflow::difference::{IsZero, Monoid, Semigroup};
use differential_dataflow::difference::Multiply;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

macro_rules! define_sum {
    ($name:ident, $inner:ty) => {
        #[derive(Copy, Debug, Clone, Hash, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
        pub struct $name {
            pub value: OrderedFloat<$inner>,
        }

        impl $name {
            #[inline]
            pub fn new(value: OrderedFloat<$inner>) -> Self {
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
                self.value = OrderedFloat(self.value.into_inner() + rhs.value.into_inner());
            }
        }

        impl Monoid for $name {
            #[inline]
            fn zero() -> Self {
                $name { value: OrderedFloat(0.0) }
            }
        }

        impl Multiply<i64> for $name {
            type Output = $name;
            #[inline]
            fn multiply(self, rhs: &i64) -> Self::Output {
                $name { value: OrderedFloat(self.value.into_inner() * (*rhs as $inner)) }
            }
        }
    };
}
